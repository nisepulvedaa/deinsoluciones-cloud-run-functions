import functions_framework
from google.cloud import bigquery
import json

@functions_framework.http
def cargar_parquet_a_bigquery(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Debe enviar un JSON con 'input' y 'output'"}, 400

        input_uri = request_json.get("input")   # ruta GCS
        output_table = request_json.get("output")  # tabla BQ: project.dataset.table

        if not input_uri or not output_table:
            return {"error": "Faltan los parámetros 'input' o 'output'"}, 400

        client = bigquery.Client()

        job_config = bigquery.LoadJobConfig(
            source_format=bigquery.SourceFormat.PARQUET,
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  
        )

        load_job = client.load_table_from_uri(
            input_uri,
            output_table,
            job_config=job_config,
            location="us-east4"  # asegúrate que coincida con tu dataset
        )

        load_job.result()  # Espera a que termine
        return {"message": f"Archivo {input_uri} cargado correctamente en {output_table}"}, 200

    except Exception as e:
        return {"error": str(e)}, 500
