import functions_framework
import requests
import json
import pandas as pd
from google.cloud import storage, bigquery
from datetime import datetime
import os

BUCKET_NAME = "dev-deinsoluciones-ingestas"

def parse_vars(vars_str):
    serie_fields = []
    flat_fields = []
    for item in vars_str.split(";"):
        if item.startswith("serie."):
            serie_fields.append(item.replace("serie.", ""))
        else:
            flat_fields.append(item)
    return flat_fields, serie_fields

@functions_framework.http
def fetch_and_store_mindicador(request):
    try:
        request_json = request.get_json(silent=True)
        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name", "fn-request-to-api")
        arquetype_name = request_json.get("arquetype_name", "workflow-arquetipo-request-to-api")

        if not process_name:
            return {"error": "Falta 'process_name' en el request"}, 400

        bq_client = bigquery.Client()

        query = """
            SELECT params
            FROM `deinsoluciones-serverless.dev_config_zone.process_params`
            WHERE process_name = @process_name
              AND process_fn_name = @process_fn_name
              AND arquetype_name = @arquetype_name
              AND active = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name),
                bigquery.ScalarQueryParameter("arquetype_name", "STRING", arquetype_name)
            ]
        )

        result = bq_client.query(query, job_config=job_config).result()
        row = next(iter(result), None)

        if not row:
            return {"error": "No se encontraron parámetros activos para ese proceso"}, 404

        param_list = row["params"]
        if not isinstance(param_list, list):
            return {"error": "El campo 'params' debe ser una lista"}, 400

        storage_client = storage.Client()
        resultados = []

        for param in param_list:
            url = param.get("url")
            filename = param.get("filename")
            vars_str = param.get("vars")

            if not url or not filename or not vars_str:
                resultados.append({
                    "filename": filename or "desconocido",
                    "status": "SKIPPED",
                    "error": "Faltan uno o más campos requeridos: url, filename o vars"
                })
                continue

            flat_fields, serie_fields = parse_vars(vars_str)
            response = requests.get(url)
            if response.status_code != 200:
                resultados.append({
                    "filename": filename,
                    "status": "ERROR",
                    "error": f"Error en la API: {response.status_code}"
                })
                continue

            data = response.json()

            flat_data = {}
            for field in flat_fields:
                if field not in data:
                    resultados.append({"filename": filename, "status": "ERROR", "error": f"Campo plano '{field}' no encontrado"})
                    break
                flat_data[field] = data[field]
            else:
                if serie_fields:
                    if "serie" not in data or not data["serie"]:
                        resultados.append({"filename": filename, "status": "ERROR", "error": "La sección 'serie' no está presente o está vacía"})
                        continue
                    df = pd.DataFrame(data["serie"])
                    if not all(f in df.columns for f in serie_fields):
                        resultados.append({"filename": filename, "status": "ERROR", "error": "Algunos campos de 'serie' no existen"})
                        continue
                    df = df[serie_fields]
                    if "fecha" in df.columns:
                        df["fecha"] = pd.to_datetime(df["fecha"]).dt.date
                    for k, v in flat_data.items():
                        df[k] = v
                else:
                    df = pd.DataFrame([flat_data])

                filename_ext = filename if filename.endswith(".parquet") else f"{filename}.parquet"
                local_path = f"/tmp/{filename_ext}"
                destination_path = f"origin-files/{filename_ext}"

                df.to_parquet(local_path, engine="pyarrow")
                bucket = storage_client.bucket(BUCKET_NAME)
                blob = bucket.blob(destination_path)
                blob.upload_from_filename(local_path, content_type="application/octet-stream")
                os.remove(local_path)

                resultados.append({
                    "filename": filename,
                    "status": "OK",
                    "path": f"gs://{BUCKET_NAME}/{destination_path}"
                })

        return {"resultados": resultados}, 200

    except Exception as e:
        return {"error": f"Error inesperado: {str(e)}"}, 500
