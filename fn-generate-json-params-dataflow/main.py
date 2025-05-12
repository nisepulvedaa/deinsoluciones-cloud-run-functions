import functions_framework
from google.cloud import bigquery, storage
import json

@functions_framework.http
def generate_schema_file(request):
    try:
        request_json = request.get_json(silent=True)

        process_name = request_json.get("process_name")
        output_filename = request_json.get("output_filename")

        if not process_name or not output_filename:
            return {"error": "Faltan parámetros requeridos: process_name o output_filename"}, 400

        # Inicializar cliente de BigQuery
        client = bigquery.Client()

        # Consulta al catálogo de parámetros
        query = """
            SELECT params
            FROM `dev_config_zone.process_param`
            WHERE process_name = @process_name
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name)
            ]
        )

        query_job = client.query(query, job_config=job_config)
        results = query_job.result()

        rows = list(results)
        if not rows:
            return {"error": f"No se encontró configuración para '{process_name}'"}, 404

        # Cargar contenido del campo 'params' como JSON
        schema_data = json.loads(rows[0]["params"])

        # Validación de estructura mínima esperada
        if not isinstance(schema_data, list):
            return {"error": "El esquema no es una lista válida de campos"}, 400

        required_keys = {"name", "type", "mode"}
        for field in schema_data:
            if not isinstance(field, dict):
                return {"error": "Uno de los elementos del esquema no es un objeto JSON"}, 400
            if not required_keys.issubset(field.keys()):
                return {
                    "error": f"Campo inválido: faltan claves en {field}. Se requieren 'name', 'type', y 'mode'"
                }, 400

        # Subir JSON a Cloud Storage
        storage_client = storage.Client()
        bucket_name = "dev-deinsoluciones-ingestas"
        destination_blob_path = f"schemas/{output_filename}"

        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_path)
        blob.upload_from_string(
            data=json.dumps(schema_data, indent=2),
            content_type="application/json"
        )

        return {"message": f"Archivo '{output_filename}' subido exitosamente a 'schemas/'"}, 200

    except Exception as e:
        return {"error": str(e)}, 500
