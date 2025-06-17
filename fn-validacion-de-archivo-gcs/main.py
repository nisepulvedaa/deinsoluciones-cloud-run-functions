import functions_framework
import json
import pyarrow.parquet as pq
from google.cloud import storage, bigquery
from io import BytesIO
from datetime import datetime, timezone, timedelta
import os

DEFAULT_BUCKET = "dev-deinsoluciones-ingestas"

def check_parquet_records(bucket_name, file_name):
    """Verifica si un archivo Parquet en Cloud Storage tiene registros."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)

    if not blob.exists():
        return {"exists": False, "has_records": False}

    file_stream = BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)

    try:
        table = pq.read_table(file_stream)
        has_records = len(table) > 0
    except Exception as e:
        return {"exists": True, "has_records": False, "error": str(e)}

    return {"exists": True, "has_records": has_records}

@functions_framework.http
def validate_parquet(request):
    """Valida que los archivos Parquet de una ejecución reciente existan y tengan registros."""

    request_json = request.get_json(silent=True)
    process_name = request_json.get("process_name")
    process_fn_name = request_json.get("process_fn_name")

    if not process_name or not process_fn_name:
        return json.dumps({"error": "Faltan 'process_name' o 'process_fn_name'"}), 400

    # Consultar parámetros desde BigQuery
    bq = bigquery.Client()
    query = """
        SELECT params
        FROM `deinsoluciones-serverless.dev_config_zone.process_params`
        WHERE process_name = @process_name
          AND process_fn_name = @process_fn_name
          AND active = TRUE
        LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
            bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name)
        ]
    )
    results = list(bq.query(query, job_config=job_config).result())
    if not results:
        return json.dumps({"error": "Parámetros no encontrados para el proceso"}), 404

    params = json.loads(results[0]["params"])
    bucket_name = DEFAULT_BUCKET

    # Revisar últimos archivos actualizados por cada path
    storage_client = storage.Client()
    now = datetime.now(timezone.utc)
    delta = timedelta(minutes=10)  # Considera archivos modificados en los últimos 10 minutos
    archivos_recientes = []

    for p in params:
        path_name = p["path_name"].rstrip("/")
        periodicidad = p.get("periodicidad", "esporadica").lower()

        blobs = list(storage_client.list_blobs(bucket_name, prefix=path_name + "/"))
        candidatos = [
            blob for blob in blobs
            if blob.name.endswith(".parquet") and (now - blob.updated) <= delta
        ]

        if not candidatos:
            return json.dumps({
                "path": path_name,
                "exists": False,
                "has_records": False,
                "error": "No se encontraron archivos recientes .parquet"
            }), 404

        archivos_recientes.extend([(blob.name, periodicidad) for blob in candidatos])

    # Validar archivos
    resultados = []
    for nombre_archivo, periodicidad in archivos_recientes:
        result = check_parquet_records(bucket_name, nombre_archivo)
        result["archivo"] = f"gs://{bucket_name}/{nombre_archivo}"
        result["periodicidad"] = periodicidad
        resultados.append(result)

    return json.dumps({
        "bucket": bucket_name,
        "procesados": len(resultados),
        "archivos": resultados
    }), 200
