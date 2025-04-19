import json
import functions_framework
from google.cloud import storage

# Nombre del bucket donde se guardarán los archivos JSON
BUCKET_NAME = "dev-deinsoluciones-ingestas"

@functions_framework.http
def generate_schema_file(request):
    """Genera un archivo JSON con el esquema de los datos y lo sube a Cloud Storage."""

    # Asegurar que la solicitud sea JSON
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Solicitud inválida, se esperaba un JSON"}, 400

    # Extraer campos y nombre del archivo
    fields = request_json.get("fields")
    file_name = request_json.get("file_name")

    if not fields or not file_name:
        return {"error": "Faltan parámetros obligatorios: 'fields' y 'file_name'"}, 400

    # Construcción del esquema en el formato requerido
    schema = []
    for field_name, field_type in fields.items():
        schema.append({
            "name": field_name,
            "mode": "NULLABLE",
            "type": field_type
        })

    # Convertir a JSON
    schema_json = json.dumps(schema, indent=4)

    # Subir el archivo a Cloud Storage en la carpeta "schemas/"
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    blob = bucket.blob(f"schemas/{file_name}.json")  # Se coloca la carpeta aquí

    blob.upload_from_string(schema_json, content_type="application/json")

    return {
        "message": f"Archivo {file_name}.json generado y subido a gs://{BUCKET_NAME}/schemas/{file_name}.json",
        "schema": schema
    }, 200
