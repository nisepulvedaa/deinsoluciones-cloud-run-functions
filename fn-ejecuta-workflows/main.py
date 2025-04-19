import json
import requests
import functions_framework
from datetime import datetime

# Configuración
FIRST_CLOUD_FUNCTION_URL = "https://us-east4-deinsoluciones-serverless.cloudfunctions.net/fn-dias-habiles"
PROJECT_ID = "deinsoluciones-serverless"
REGION = "us-east4"

def get_identity_token():
    """Obtiene un ID Token desde el servidor de metadatos de Compute Engine."""
    metadata_url = f"http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/identity?audience={FIRST_CLOUD_FUNCTION_URL}"
    headers = {"Metadata-Flavor": "Google"}
    
    response = requests.get(metadata_url, headers=headers)

    if response.status_code == 200:
        return response.text  # Devuelve el token de identidad
    else:
        raise Exception(f" Error obteniendo el ID Token: {response.text}")

def get_access_token():
    """Obtiene un Access Token para autenticar la ejecución del Workflow."""
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}

    response = requests.get(metadata_url, headers=headers)

    if response.status_code == 200:
        return response.json()["access_token"]  #  Devuelve el token de acceso
    else:
        raise Exception(f" Error obteniendo el Access Token: {response.text}")

def obtener_fecha_desde_frecuencia(request_json):
    """Genera la fecha según la frecuencia recibida en el JSON de entrada."""
    if "fecha" in request_json:  
        return request_json["fecha"]  # Usa la fecha proporcionada sin modificarla
    
    hoy = datetime.today()
    
    if request_json.get("frecuencia") == "diaria":
        return hoy.strftime("%d-%m-%Y")  # Formato: DD-MM-YYYY
    elif request_json.get("frecuencia") == "mensual":
        return hoy.strftime("01-%m-%Y")  # Formato: 01-MM-YYYY
    else:
        raise ValueError("Frecuencia no válida. Debe ser 'diaria' o 'mensual'.")

def verificar_dia_habil(fecha):
    """Consulta `fn-dias-habiles` y devuelve la respuesta."""
    headers = {
        "Authorization": f"Bearer {get_identity_token()}",
        "Content-Type": "application/json"
    }
    data = json.dumps({"fecha": fecha})

    response = requests.post(FIRST_CLOUD_FUNCTION_URL, headers=headers, data=data)

    if response.status_code == 200:
        return response.json()  #  Retorna la respuesta de la función
    else:
        print(f" Error al consultar: {response.status_code} - {response.text}")
        return None

def ejecutar_workflow(workflow_name, args):
    """Ejecuta el Workflow en GCP si el día es hábil."""
    url = f"https://workflowexecutions.googleapis.com/v1/projects/{PROJECT_ID}/locations/{REGION}/workflows/{workflow_name}/executions"
    
    headers = {
        "Authorization": f"Bearer {get_access_token()}",
        "Content-Type": "application/json"
    }

    payload = {
        "argument": json.dumps(args)
    }

    response = requests.post(url, headers=headers, json=payload)

    if response.status_code == 200:
        print(f" Workflow '{workflow_name}' ejecutado correctamente con args: {args}")
        return {"message": f"Workflow '{workflow_name}' ejecutado exitosamente"}, 200
    else:
        print(f" Error al ejecutar Workflow: {response.text}")
        return {"error": f"No se pudo ejecutar el Workflow '{workflow_name}'"}, 500

@functions_framework.http
def verificar_y_ejecutar(request):
    """Función HTTP que consulta si es día hábil y ejecuta el Workflow si aplica."""
    request_json = request.get_json(silent=True)
    print("Request: " , request_json)

    # Validar si se recibió un JSON
    if not request_json:
        return {"error": "Debe enviar un JSON con los parámetros requeridos."}, 400

    # Obtener la fecha según la lógica definida
    try:
        fecha = obtener_fecha_desde_frecuencia(request_json)
        print("Fecha Obtenida: " , fecha)
    except ValueError as e:
        return {"error": str(e)}, 400

    # Validar si el JSON tiene workflow_name y args
    workflow_name = request_json.get("workflow_name")
    args = request_json.get("args", {})

    if not workflow_name:
        return {"error": "El parámetro 'workflow_name' es obligatorio."}, 400

    resultado = verificar_dia_habil(fecha)

    if resultado and resultado.get("habil") == "S":
        return ejecutar_workflow(workflow_name, args)
    else:
        return {"message": f"El día {fecha} no es hábil. No se ejecuta el Workflow '{workflow_name}'."}, 200
