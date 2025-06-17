import os
import json
from datetime import datetime
import functions_framework
from google.cloud import storage, bigquery
import pandas as pd
from io import BytesIO

# Valores fijos (puedes mover a variables de entorno si quieres)
BUCKET_ORIGEN = "dev-deinsoluciones-ingestas"
BUCKET_DESTINO = "dev-deinsoluciones-ingestas"

@functions_framework.http
def mover_archivo_gcs(request):
    try:
        request_json = request.get_json(silent=True)
        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")

        if not process_name or not process_fn_name:
            return {"error": "Faltan 'process_name' o 'process_fn_name'"}, 400

        bq = bigquery.Client()
        query = f"""
            SELECT params, is_multi_param
            FROM `deinsoluciones-serverless.dev_config_zone.process_params`
            WHERE process_name = @process_name
              AND process_fn_name = @process_fn_name
              AND estatus = TRUE
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name),
            ]
        )

        results = list(bq.query(query, job_config=job_config).result())
        if not results:
            return {"error": "No se encontraron parámetros activos para el proceso"}, 404

        row = results[0]
        params = json.loads(row["params"])
        is_multi = row["is_multi_param"]

        if not isinstance(params, list):
            params = [params]

        client = storage.Client()
        bucket_src = client.bucket(BUCKET_ORIGEN)
        bucket_dst = client.bucket(BUCKET_DESTINO)

        resultados = []
        for p in params:
            path_origen = p["path_origen"].rstrip("/")
            path_destino = p["path_destino"].rstrip("/")
            nombre_archivo = p["nombre_archivo"]
            periodicidad = p["periodicidad"].lower()

            full_path_origen = f"{path_origen}/{nombre_archivo}"
            blob_src = bucket_src.blob(full_path_origen)

            nombre_base, extension = os.path.splitext(nombre_archivo)

            # Determinar la fecha según periodicidad
            if periodicidad == "espóradica":
                fecha_str = datetime.now().strftime("%Y-%m-%d")
            else:
                parquet_data = blob_src.download_as_bytes()
                df = pd.read_parquet(BytesIO(parquet_data))

                if periodicidad == "diaria" and "periodo_dia" in df.columns:
                    fecha_valor = df["periodo_dia"].unique()[0]
                elif periodicidad == "mensual" and "periodo_mes" in df.columns:
                    fecha_valor = df["periodo_mes"].unique()[0]
                else:
                    return {"error": f"Campo de fecha no encontrado para periodicidad '{periodicidad}'"}, 400

                fecha_str = (
                    fecha_valor if isinstance(fecha_valor, str)
                    else fecha_valor.strftime("%Y-%m-%d")
                )

            nuevo_nombre = f"{nombre_base}_{fecha_str}{extension}"
            full_path_destino = f"{path_destino}/{nuevo_nombre}"

            bucket_src.copy_blob(blob_src, bucket_dst, full_path_destino)
            blob_src.delete()

            resultados.append({
                "archivo": nombre_archivo,
                "nuevo_path": f"gs://{BUCKET_DESTINO}/{full_path_destino}",
                "status": "OK"
            })

        return {
            "message": "Archivos procesados correctamente",
            "resultados": resultados
        }, 200

    except Exception as e:
        return {"error": f"Error inesperado: {str(e)}"}, 500
