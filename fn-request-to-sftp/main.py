import functions_framework
import paramiko
from google.cloud import storage, secretmanager, bigquery
import tempfile
import os
import json

@functions_framework.http
def multi_sftp_to_gcs(request):
    request_json = request.get_json(silent=True)

    process_name = request_json.get("process_name")
    process_fn_name = request_json.get("process_fn_name")
    arquetype_name = request_json.get("arquetype_name")

    if not all([process_name, process_fn_name, arquetype_name]):
        return {"error": "Faltan parámetros de consulta"}, 400

    try:
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
                bigquery.ScalarQueryParameter("arquetype_name", "STRING", arquetype_name),
            ]
        )

        results = bq_client.query(query, job_config=job_config).result()
        count = 0

        for row in results:
            config = row["params"]
            hostname = config.get("hostname")
            port = int(config.get("port", 22))
            username = config.get("username")
            private_key_secret = config.get("private_key_secret")
            bucket_name = config.get("bucket_name")
            destination_blob_prefix = config.get("destination_blob_name")  # puede ser 'origin-files' o ''

            if not all([hostname, port, username, private_key_secret, bucket_name, destination_blob_prefix]):
                continue

            # Obtener clave privada
            sm_client = secretmanager.SecretManagerServiceClient()
            secret_name = f"projects/deinsoluciones-devops-ci-core/secrets/{private_key_secret}/versions/latest"
            response = sm_client.access_secret_version(request={"name": secret_name})
            private_key_data = response.payload.data.decode("UTF-8")

            with tempfile.NamedTemporaryFile(delete=False, mode="w") as key_file:
                key_file.write(private_key_data)
                key_file_path = key_file.name

            # Conexión SFTP
            key = paramiko.RSAKey.from_private_key_file(key_file_path)
            ssh = paramiko.SSHClient()
            ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh.connect(hostname, port=port, username=username, pkey=key)

            sftp = ssh.open_sftp()

            # Buscar el archivo más reciente
            files = sftp.listdir_attr()
            if not files:
                continue
            latest_file = max(files, key=lambda f: f.st_mtime)
            remote_path = latest_file.filename

            # Descargar archivo
            temp_local_path = tempfile.NamedTemporaryFile(delete=False).name
            sftp.get(remote_path, temp_local_path)
            sftp.close()
            ssh.close()

            # Subir a GCS con el mismo nombre
            storage_client = storage.Client()
            bucket = storage_client.bucket(bucket_name)
            full_blob_path = os.path.join(destination_blob_prefix, remote_path)
            blob = bucket.blob(full_blob_path)
            blob.upload_from_filename(temp_local_path)

            # Limpieza
            os.remove(key_file_path)
            os.remove(temp_local_path)
            count += 1

        return {"message": f"Se procesaron {count} archivo(s) correctamente."}, 200

    except Exception as e:
        return {"error": str(e)}, 500
