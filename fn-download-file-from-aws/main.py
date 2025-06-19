import functions_framework
import boto3
import os
import json
import tempfile
from google.cloud import storage
from datetime import datetime

from google.cloud import secretmanager

def get_aws_credentials():
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/182035274443/secrets/aws-secret-key/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    payload = json.loads(response.payload.data.decode("UTF-8"))
    return payload["aws_access_key_id"], payload["aws_secret_access_key"]

def find_most_recent_file(s3_client, bucket, prefix, partial_file_name):
    paginator = s3_client.get_paginator("list_objects_v2")
    page_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    
    candidates = []
    for page in page_iterator:
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if partial_file_name in key:
                candidates.append((key, obj["LastModified"]))

    if not candidates:
        raise FileNotFoundError(f"No se encontró ningún archivo con '{partial_file_name}' en '{prefix}'")

    return max(candidates, key=lambda x: x[1])[0]

def upload_to_gcs(local_path, gcs_path):
    bucket_name, *blob_parts = gcs_path.replace("gs://", "").split("/", 1)
    blob_name = blob_parts[0] if blob_parts else os.path.basename(local_path)

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.upload_from_filename(local_path)

@functions_framework.http
def download_from_aws(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Falta body JSON con parámetros"}, 400

    bucket_name = request_json.get("bucket_name")
    prefix = request_json.get("prefix")
    partial_file_name = request_json.get("partial_file_name")
    gcs_target_path = request_json.get("gcs_target_path")

    if not all([bucket_name, prefix, partial_file_name, gcs_target_path]):
        return {"error": "Parámetros incompletos"}, 400

    try:
        aws_key, aws_secret = get_aws_credentials()
        s3 = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

        key = find_most_recent_file(s3, bucket_name, prefix, partial_file_name)
        print(f"📁 Archivo encontrado: {key}")

        with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
            s3.download_fileobj(bucket_name, key, tmp_file)
            tmp_file_path = tmp_file.name

        filename = os.path.basename(key)
        gcs_blob_path = os.path.join(gcs_target_path.replace("gs://", "").split("/", 1)[-1], filename)

        upload_to_gcs(tmp_file_path, f"{gcs_target_path}{filename}")
        os.remove(tmp_file_path)

        return {"status": "OK", "message": f"Archivo {filename} transferido correctamente a GCS."}, 200

    except Exception as e:
        return {"error": str(e)}, 500
