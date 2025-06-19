import functions_framework
import boto3
import os
import json
import tempfile
from google.cloud import storage, secretmanager, bigquery
from datetime import datetime

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

def get_params_from_bigquery(process_name, process_fn_name, arquetype_name):
    client = bigquery.Client()
    query = f"""
        SELECT params
        FROM `dev_config_zone.process_params`
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

    results = client.query(query, job_config=job_config).result()
    param_list = []

    for row in results:
        param_value = row["params"]
        print(f"[DEBUG] Valor de params desde BQ: {param_value} — Tipo: {type(param_value)}")

        try:
            if isinstance(param_value, dict):
                param_list.append(param_value)
            elif isinstance(param_value, list):
                param_list.extend(param_value)
            elif isinstance(param_value, str):
                loaded = json.loads(param_value)
                if isinstance(loaded, list):
                    param_list.extend(loaded)
                elif isinstance(loaded, dict):
                    param_list.append(loaded)
                else:
                    print(f"[WARNING] Estructura inesperada tras json.loads: {type(loaded)}")
            else:
                print(f"[WARNING] Tipo no manejado: {type(param_value)}")

        except Exception as e:
            print(f"[ERROR] Falló parseo de param_value: {e}")

    return param_list

@functions_framework.http
def download_from_aws(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Debe enviar un JSON con process_name, process_fn_name y arquetype_name"}, 400

    process_name = request_json.get("process_name")
    process_fn_name = request_json.get("process_fn_name")
    arquetype_name = request_json.get("arquetype_name")

    if not all([process_name, process_fn_name, arquetype_name]):
        return {"error": "Faltan uno o más parámetros obligatorios"}, 400

    try:
        aws_key, aws_secret = get_aws_credentials()
        s3 = boto3.client("s3", aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

        params_list = get_params_from_bigquery(process_name, process_fn_name, arquetype_name)
        if not params_list:
            return {"error": "No se encontraron parámetros activos en process_params"}, 404

        for params in params_list:
            bucket_name = params.get("bucket_name")
            prefix = params.get("prefix")
            partial_file_name = params.get("partial_file_name")
            gcs_target_path = params.get("gcs_target_path")

            if not all([bucket_name, prefix, partial_file_name, gcs_target_path]):
                print("[WARNING] Saltando parámetro incompleto:", params)
                continue

            key = find_most_recent_file(s3, bucket_name, prefix, partial_file_name)
            print(f"Archivo encontrado en AWS S3: {key}")

            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                s3.download_fileobj(bucket_name, key, tmp_file)
                tmp_file_path = tmp_file.name

            filename = os.path.basename(key)
            upload_to_gcs(tmp_file_path, f"{gcs_target_path}{filename}")
            os.remove(tmp_file_path)

        return {"status": "OK", "message": f"{len(params_list)} archivo(s) procesado(s) correctamente."}, 200

    except Exception as e:
        return {"error": str(e)}, 500
