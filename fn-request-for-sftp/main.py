import functions_framework
import paramiko
from google.cloud import storage, secretmanager
import tempfile
import os

@functions_framework.http
def sftp_to_gcs(request):
    request_json = request.get_json(silent=True)

    hostname = request_json.get("hostname")
    port = int(request_json.get("port", 22))
    username = request_json.get("username")
    private_key_secret = request_json.get("private_key_secret")
    remote_path = request_json.get("remote_path")
    bucket_name = request_json.get("bucket_name")
    destination_blob_name = request_json.get("destination_blob_name")

    if not all([hostname, port, username, private_key_secret, remote_path, bucket_name, destination_blob_name]):
        return {"error": "Faltan parámetros requeridos"}, 400

    try:
        # Obtener la clave privada desde Secret Manager
        sm_client = secretmanager.SecretManagerServiceClient()
        secret_name = f"projects/deinsoluciones-devops-ci-core/secrets/{private_key_secret}/versions/latest"
        response = sm_client.access_secret_version(request={"name": secret_name})
        private_key_data = response.payload.data.decode("UTF-8")

        # Guardar la clave privada en un archivo temporal
        with tempfile.NamedTemporaryFile(delete=False, mode="w") as key_file:
            key_file.write(private_key_data)
            key_file_path = key_file.name

        key = paramiko.RSAKey.from_private_key_file(key_file_path)
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh.connect(hostname, port=port, username=username, pkey=key)

        # Descargar el archivo remoto a archivo local temporal
        sftp = ssh.open_sftp()
        temp_local_path = tempfile.NamedTemporaryFile(delete=False).name
        sftp.get(remote_path, temp_local_path)
        sftp.close()
        ssh.close()

        # Subir a GCS
        storage_client = storage.Client()
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(temp_local_path)

        # Limpieza
        os.remove(key_file_path)
        os.remove(temp_local_path)

        return {"message": f"Archivo '{remote_path}' subido a 'gs://{bucket_name}/{destination_blob_name}'"}, 200

    except Exception as e:
        return {"error": str(e)}, 500
