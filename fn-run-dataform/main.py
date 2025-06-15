import functions_framework
import requests
import json

# Configuración base (puedes mover a variables de entorno si quieres)
DEFAULT_PROJECT_ID = "deinsoluciones-serverless"
DEFAULT_REGION = "us-east4"

def get_access_token():
    """Obtiene un Access Token para autenticación en GCP desde metadata server."""
    metadata_url = "http://metadata.google.internal/computeMetadata/v1/instance/service-accounts/default/token"
    headers = {"Metadata-Flavor": "Google"}
    response = requests.get(metadata_url, headers=headers)
    if response.status_code == 200:
        return response.json()["access_token"]
    else:
        raise Exception(f"Error al obtener Access Token: {response.text}")

@functions_framework.http
def run_dataform(request):
    request_json = request.get_json(silent=True)

    if not request_json:
        return {"error": "Debe enviar un JSON con project_id, region, repository, workspace, vars"}, 400

    # Extraer parámetros del JSON
    project_id = request_json.get("project_id", DEFAULT_PROJECT_ID)
    region = request_json.get("region", DEFAULT_REGION)
    repository = request_json.get("repository")
    workspace = request_json.get("workspace")
    vars_ = request_json.get("vars", {})  # variables a pasar al workflow
    included_tags = request_json.get("includedTags", [])
    included_paths = request_json.get("includedPaths", [])

    # Validaciones básicas
    if not repository or not workspace:
        return {"error": "Faltan campos requeridos: 'repository' o 'workspace'"}, 400

    # Preparar endpoint y headers
    token = get_access_token()
    endpoint = f"https://dataform.googleapis.com/v1beta1/projects/{project_id}/locations/{region}/repositories/{repository}/workspaces/{workspace}:execute"

    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }

    # Cuerpo de la ejecución
    body = {
        "fullCompilation": True,
        "transitiveDependenciesIncluded": True,
        "includedTags": included_tags,
        "includedPaths": included_paths,
        "executionConfig": {
            "vars": vars_
        }
    }

    # Ejecutar llamada a Dataform
    response = requests.post(endpoint, headers=headers, json=body)

    if response.status_code == 200:
        return {
            "message": "Ejecución iniciada correctamente",
            "workspace": workspace,
            "vars": vars_
        }, 200
    else:
        return {
            "error": "Fallo al ejecutar el proyecto de Dataform",
            "status_code": response.status_code,
            "details": response.text
        }, 500
