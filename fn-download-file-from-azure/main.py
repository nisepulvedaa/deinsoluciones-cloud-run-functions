import functions_framework
from azure.storage.blob import ContainerClient
from google.cloud import storage, secretmanager, bigquery
import os
import tempfile
import json

def get_azure_sas_url():
    client = secretmanager.SecretManagerServiceClient()
    secret_name = "projects/182035274443/secrets/azure-secret-key/versions/latest"
    response = client.access_secret_version(request={"name": secret_name})
    payload = json.loads(response.payload.data.decode("UTF-8"))
    return payload["azure_sas_key"]  # URL completa del container con SAS

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
def download_from_azure(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Debe enviar un JSON con process_name, process_fn_name y arquetype_name"}, 400

    process_name = request_json.get("process_name")
    process_fn_name = request_json.get("process_fn_name")
    arquetype_name = request_json.get("arquetype_name")

    if not all([process_name, process_fn_name, arquetype_name]):
        return {"error": "Faltan uno o más parámetros obligatorios"}, 400

    try:
        container_url = get_azure_sas_url()
        container_client = ContainerClient.from_container_url(container_url)

        params_list = get_params_from_bigquery(process_name, process_fn_name, arquetype_name)
        if not params_list:
            return {"error": "No se encontraron parámetros activos en process_params"}, 404

        for params in params_list:
            path = params.get("path")
            partial_file_name = params.get("partial_file_name")
            gcs_target_path = params.get("gcs_target_path")

            if not all([path, partial_file_name, gcs_target_path]):
                continue  # skip si vienen incompletos

            blob_name = find_most_recent_blob(container_client, path, partial_file_name)
            print(f"Archivo encontrado: {blob_name}")

            with tempfile.NamedTemporaryFile(delete=False) as tmp_file:
                download_stream = container_client.download_blob(blob_name)
                tmp_file.write(download_stream.readall())
                tmp_file_path = tmp_file.name

            filename = os.path.basename(blob_name)
            upload_to_gcs(tmp_file_path, f"{gcs_target_path}{filename}")
            os.remove(tmp_file_path)

        return {"status": "OK", "message": f"{len(params_list)} archivo(s) procesado(s) correctamente."}, 200

    except Exception as e:
        return {"error": str(e)}, 500
