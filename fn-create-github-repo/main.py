import base64
import json
import os
import requests
from google.cloud import secretmanager
import functions_framework

# ==== Config por entorno ====
GITHUB_OWNER   = os.getenv("GITHUB_OWNER", "nisepulvedaa")
GITHUB_TOKEN_SECRET = os.getenv("GITHUB_TOKEN_SECRET", "github-token")

BASE_OWNER = os.getenv("BASE_OWNER", "nisepulvedaa")
BASE_REPO  = os.getenv("BASE_REPO", "deinsoluciones-base-config")
BASE_REF   = os.getenv("BASE_REF", "main")

TARGET_BRANCH = os.getenv("TARGET_BRANCH", "main")

API_USER_REPOS = "https://api.github.com/user/repos"
API_REPO       = "https://api.github.com/repos/{owner}/{repo}"
API_CONTENTS   = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"

def _headers(token: str):
    return {"Authorization": f"token {token}", "Accept": "application/vnd.github+json"}

def _get_github_token() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/182035274443/secrets/{GITHUB_TOKEN_SECRET}/versions/latest"
    return client.access_secret_version(request={"name": name}).payload.data.decode("utf-8")

def _ensure_repo(token: str, repo_name: str) -> str:
    """Crea repo público con auto_init. Idempotente."""
    r = requests.post(
        API_USER_REPOS,
        headers=_headers(token),
        json={"name": repo_name, "private": False, "auto_init": True}
    )
    if r.status_code in (201, 202):
        return "created"
    if r.status_code == 422:  # ya existe
        return "exists"
    raise RuntimeError(f"Error creando repo: {r.status_code} {r.text}")

def _get_base_config_content(token: str, config_name: str) -> bytes:
    """Lee el archivo desde el repo base (vía API contents)."""
    url = API_CONTENTS.format(owner=BASE_OWNER, repo=BASE_REPO, path=config_name)
    r = requests.get(url, headers=_headers(token), params={"ref": BASE_REF})
    if r.status_code == 200:
        js = r.json()
        enc = js.get("encoding")
        content = js.get("content", "")
        if enc == "base64" and content:
            return base64.b64decode(content)
        # fallback por si no viene codificado
        return js.get("content", "").encode("utf-8")
    if r.status_code == 404:
        raise FileNotFoundError(f"No se encontró {config_name} en {BASE_OWNER}/{BASE_REPO}@{BASE_REF}")
    raise RuntimeError(f"Error leyendo config base: {r.status_code} {r.text}")

def _create_or_update_file(token: str, owner: str, repo: str, path: str, content_bytes: bytes):
    """Crea o actualiza archivo en el repo destino (PUT /contents)."""
    url = API_CONTENTS.format(owner=owner, repo=repo, path=path)

    # ¿Existe ya?
    sha = None
    r_get = requests.get(url, headers=_headers(token), params={"ref": TARGET_BRANCH})
    if r_get.status_code == 200:
        sha = r_get.json().get("sha")

    payload = {
        "message": f"chore: add {path} from {BASE_REPO}",
        "content": base64.b64encode(content_bytes).decode("utf-8"),
        "branch": TARGET_BRANCH
    }
    if sha:
        payload["sha"] = sha

    r_put = requests.put(url, headers=_headers(token), json=payload)
    if r_put.status_code not in (200, 201):
        raise RuntimeError(f"Error subiendo {path}: {r_put.status_code} {r_put.text}")
    return r_put.json()

@functions_framework.http
def create_repo_with_config(request):
    data = request.get_json(silent=True) or {}
    repo_name   = data.get("repo_name")
    config_name = data.get("config_name")  # ej: "config_arq_002_slim.py"
    target_name = data.get("target_name", "config.py")

    if not repo_name or not config_name:
        return (json.dumps({"error": "repo_name y config_name son obligatorios"}), 400)

    token = _get_github_token()

    status = _ensure_repo(token, repo_name)
    content = _get_base_config_content(token, config_name)
    commit = _create_or_update_file(token, GITHUB_OWNER, repo_name, target_name, content)

    return (
        json.dumps({
            "repo_status": status,
            "repo_url": f"https://github.com/{GITHUB_OWNER}/{repo_name}",
            "added_file": target_name,
            "from_config": config_name,
            "commit_url": commit.get("commit", {}).get("html_url")
        }),
        200
    )
