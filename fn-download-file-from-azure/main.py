import functions_framework
from azure.storage.blob import ContainerClient
from google.cloud import storage, secretmanager
import os
import tempfile
import json

def get_azure_sas_url():
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/182035274443/secrets/azure-secret-key/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    payload = json.loads(response.payload.data.decode("UTF-8"))
    return payload["azure_sas_key"]  # Ya contiene la URL completa

def find_most_recent_blob(container_client, path, partial_file_name):
    blobs = container_client.list_blobs(name_starts_with=path)
    candidates = []

    for blob in blobs:
        if partial_file_name in blob.name:
            candidates.append((blob.name, blob.last_modified))

    if not candidates:
        raise FileNotFoundError(f"No se encontró ningún archivo con '{partial_file_name}' en '{path}'")

    return max(candidates, key=lambda x: x[1])[0]

def upload_to_gcs(local_path, gcs_path):
    bucket_name, *blob_parts = gcs_path.replace("gs://", "").split("/", 1)
    blob_name = blob_parts[0] if blob_parts else os.path.basename(local_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

@functions_framework.http
def download_from_azure(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Falta body JSON con parámetros"}, 400

    path = request_json.get("path")
    partial_file_name = request_json.get("partial_file_name")
    gcs_target_path = request_json.get("gcs_target_path")

    if not all([path, partial_file_name, gcs_target_path]):
        return {"error": "Parámetros incompletos"}, 400

    try:
        container_url = get_azure_sas_url()  # Ya incluye el SAS y el nombre del contenedor
        container_client = ContainerClient.from_container_url(container_url)

        blob_name = find_most_recent_blob(container_client, path, partial_file_name)
        print(f"📦 Archivo encontrado: {blob_name}")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            download_stream = container_client.download_blob(blob_name)
            tmp_file.write(download_stream.readall())
            tmp_file_path = tmp_file.name

        filename = os.path.basename(blob_name)
        upload_to_gcs(tmp_file_path, f"{gcs_target_path}{filename}")
        os.remove(tmp_file_path)

        return {"status": "OK", "message": f"Archivo {filename} transferido correctamente a GCS."}, 200

    except Exception as e:
        return {"error": str(e)}, 500
