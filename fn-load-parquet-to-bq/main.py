import functions_framework
from google.cloud import bigquery, storage
import json
from datetime import datetime
import os

@functions_framework.http
def cargar_parquet_a_bigquery(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Debe enviar un JSON con process_name y process_fn_name"}, 400

        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")
        arquetype_name = request_json.get("arquetype_name")

        if not process_name or not process_fn_name:
            return {"error": "Faltan los campos 'process_name' , 'process_fn_name' o 'arquetype_name' "}, 400

        bq_client = bigquery.Client()

        # 1. Obtener parámetros desde la tabla
        param_query = f"""
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
        result = bq_client.query(param_query, job_config=job_config).result()
        row = next(iter(result), None)

        if not row:
            return {"error": "No se encontraron parámetros activos"}, 404

        params_list = row["params"]
        if not params_list or not isinstance(params_list, list):
            return {"error": "Formato de parámetros inválido"}, 400

        params = params_list[0]
        input_uri = params.get("input")
        output_table = params.get("output")
        periodicidad = params.get("periodicidad").lower()

        if not input_uri or not output_table:
            return {"error": "input y output son obligatorios en los parámetros"}, 400

        if not input_uri.startswith("gs://"):
            return {"error": "input_uri no tiene el formato gs://"}, 400

        # 2. Separar ruta de GCS
        ruta_gcs = input_uri[5:]
        partes = ruta_gcs.split("/", 1)
        if len(partes) != 2:
            return {"error": "Formato inválido de input_uri"}, 400

        bucket_name, path_completo = partes
        dir_path, file_name = os.path.split(path_completo)
        nombre_base, extension = os.path.splitext(file_name)

        storage_client = storage.Client()

        # 3. Ajustar input_uri según periodicidad
        if periodicidad == "mensual":
            blobs = list(storage_client.list_blobs(bucket_name, prefix=dir_path + "/"))
            candidatos = [
                blob for blob in blobs
                if nombre_base in os.path.basename(blob.name) and blob.name.endswith(".parquet")
            ]
            if not candidatos:
                return {"error": f"No se encontró ningún archivo que contenga '{nombre_base}'"}, 404

            archivo_mas_reciente = max(candidatos, key=lambda b: b.updated)
            input_uri = f"gs://{bucket_name}/{archivo_mas_reciente.name}"

        else:
            fecha_str = datetime.now().strftime("%Y-%m-%d")
            nuevo_nombre = f"{nombre_base}_{fecha_str}{extension}"
            input_uri = f"gs://{bucket_name}/{dir_path}/{nuevo_nombre}"

        # 4. Cargar archivo en BigQuery
        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND
        )

        load_job = bq_client.load_table_from_uri(
            input_uri,
            output_table,
            job_config=job_config,
            location="us-east4"
        )
        load_job.result()

        return {
            "message": f"Archivo {input_uri} cargado correctamente en {output_table}",
            "status": "OK"
        }, 200

    except Exception as e:
        return {"error": str(e)}, 500
