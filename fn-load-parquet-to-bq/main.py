import functions_framework
from google.cloud import bigquery
import json
from datetime import datetime
import os

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
        
        # === Modificar input_uri para incluir fecha ===
        # Ejemplo: gs://bucket/path/file.parquet → gs://bucket/path/file_YYYY-MM-DD.parquet
        if input_uri.startswith("gs://"):
            ruta_gcs = input_uri[5:]  # remove gs://
            partes = ruta_gcs.split("/", 1)
            if len(partes) != 2:
                return {"error": "El formato de la ruta GCS no es válido"}, 400

            bucket_name, path_completo = partes
            dir_path, file_name = os.path.split(path_completo)
            nombre_base, extension = os.path.splitext(file_name)
            fecha_str = datetime.now().strftime("%Y-%m-%d")
            nuevo_nombre = f"{nombre_base}_{fecha_str}{extension}"
            input_uri = f"gs://{bucket_name}/{dir_path}/{nuevo_nombre}"

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
