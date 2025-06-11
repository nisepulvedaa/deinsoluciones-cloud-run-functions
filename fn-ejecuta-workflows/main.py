import json
import requests
from datetime import datetime
import functions_framework
from google.cloud import bigquery

# Configuración
PROJECT_ID = "deinsoluciones-serverless"
REGION = "us-east4"
BQ_TABLE = "dev_config_zone.process_schedule"
FN_DIAS_HABILES_URL = "https://us-east4-deinsoluciones-serverless.cloudfunctions.net/fn-dias-habiles"

def get_identity_token():
    """Obtiene un ID Token para la Cloud Function de días hábiles."""
    metadata_url = f"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience={FN_DIAS_HABILES_URL}"
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(metadata_url, headers=headers)
    if response.status_code == 200:
        return response.text
    else:
        raise Exception(f"Error al obtener ID Token: {response.text}")

def get_access_token():
    """Obtiene un Access Token para llamar a Workflows."""
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(metadata_url, headers=headers)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Error al obtener Access Token: {response.text}")

@functions_framework.http
def verificar_y_ejecutar(request):
    request_json = request.get_json(silent=True)
    if not request_json:
        return {"error": "Debe enviar un JSON válido"}, 400

    process_name = request_json.get("process_name")
    workflow_name = request_json.get("workflow_name")
    json_name = request_json.get("json_name")  # raw o cleansed

    if not (process_name and workflow_name and json_name):
        return {"error": "Faltan parámetros: process_name, workflow_name o json_name"}, 400

    # Fecha actual
    hoy = datetime.today()
    fecha_ejecucion = hoy.strftime("%d-%m-%Y")
    fecha_ejecucion_ddmmYYYY = hoy.strftime("%d-%m-%Y")
    fecha_ejecucion_YYYYmmdd = hoy.strftime("%Y-%m-%d")
    dia_semana = hoy.isoweekday()

    # Verificar día hábil
    habil = verificar_dia_habil(fecha_ejecucion_ddmmYYYY)
    if not habil:
        return {"message": f"{fecha_ejecucion_ddmmYYYY} no es día hábil. No se ejecuta el workflow."}, 200

    # Obtener configuración desde BigQuery
    bq = bigquery.Client()
    query = f"""
    SELECT  process_name, 
            periodicidad, 
            dias_semana, 
            dias_mes, 
            solo_dia_habil,
            json_config_raw AS raw, 
            json_config_cleansed AS cleansed
    FROM `{PROJECT_ID}.{BQ_TABLE}`
    WHERE process_name = @process_name AND active = TRUE
    LIMIT 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_name", "STRING", process_name)
        ]
    )
    rows = list(bq.query(query, job_config=job_config).result())

    if not rows:
        return {"error": f"No se encontró configuración activa para '{process_name}'"}, 404

    row = rows[0]

    # Validar día de la semana
    if dia_semana not in row["dias_semana"]:
        return {"message": f"Hoy es día {dia_semana} y no está entre los configurados para ejecutar el proceso."}, 200

    # Cargar el JSON del tipo solicitado
    config_json_str = row[json_name]
    config_json = config_json_str[0]

    # Reemplazar `${fecha_param}` por la fecha real
    for k, v in config_json.items():
        if isinstance(v, str) and "${fecha_param}" in v:
            config_json[k] = v.replace("${fecha_param}", fecha_ejecucion_YYYYmmdd)

    # Ejecutar el Workflow
    return ejecutar_workflow(workflow_name, config_json)

def verificar_dia_habil(fecha):
    headers = {
        "Authorization": f"Bearer {get_identity_token()}",
        "Content-Type": "application/json"
    }
    payload = {"fecha": fecha}
    response = requests.post(FN_DIAS_HABILES_URL, headers=headers, json=payload)
    return response.status_code == 200 and response.json().get("habil") == "S"

def ejecutar_workflow(workflow_name, args):
    workflow_url = f"https://workflowexecutions.googleapis.com/v1/projects/{PROJECT_ID}/locations/{REGION}/workflows/{workflow_name}/executions"
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }
    payload = {
        "argument": json.dumps(args)
    }

    response = requests.post(workflow_url, headers=headers, json=payload)
    if response.status_code == 200:
        print(f"Workflow '{workflow_name}' ejecutado con éxito.")
        return {"message": f"Workflow '{workflow_name}' ejecutado correctamente"}, 200
    else:
        print(f"Error al ejecutar el workflow: {response.text}")
        return {"error": f"No se pudo ejecutar el Workflow '{workflow_name}'"}, 500
