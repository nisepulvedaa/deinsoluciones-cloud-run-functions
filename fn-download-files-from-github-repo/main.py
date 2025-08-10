import base64
import io
import json
import os
import re
import ast
from typing import Dict, List, Tuple

import requests
from google.cloud import secretmanager, storage
import functions_framework

# ===== Config =====
GITHUB_TOKEN_SECRET = os.getenv("GITHUB_TOKEN_SECRET", "github-auth")
ARQ_BRANCH = os.getenv("ARQ_BRANCH", "main")

# Diccionario de arquetipos -> repos base
REPOS_ARQ = {
    "ARQ001": "https://github.com/nisepulvedaa/deinsoluciones-base-arq001.git",
    "ARQ002-SLIM": "https://github.com/nisepulvedaa/deinsoluciones-base-arq002-slim.git",
    "ARQ002-FULL": "https://github.com/nisepulvedaa/deinsoluciones-base-arq002-full.git",
    "ARQ002-CLEANSED": "https://github.com/nisepulvedaa/deinsoluciones-base-arq002-cleansed.git",
    "ARQ003": "https://github.com/nisepulvedaa/deinsoluciones-base-arq003.git",
    "ARQ004": "https://github.com/nisepulvedaa/deinsoluciones-base-arq004.git",
    "ARQ005": "https://github.com/nisepulvedaa/deinsoluciones-base-arq005.git",
    "ARQ006": "https://github.com/nisepulvedaa/deinsoluciones-base-arq006.git",
}

API_CONTENTS = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"
API_TREES    = "https://api.github.com/repos/{owner}/{repo}/git/trees/{branch}?recursive=1"


# ===== Helpers =====
def _project() -> str:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or "deinsoluciones-serverless"

def _get_pat() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/182035274443/secrets/{GITHUB_TOKEN_SECRET}/versions/latest"
    token = client.access_secret_version(request={"name": name}).payload.data.decode("utf-8").strip()
    if not token or "BEGIN " in token:
        raise ValueError("El secreto no es un PAT de GitHub (usa ghp_/github_pat_).")
    return token

def _gh_headers(t: str) -> Dict[str, str]:
    return {"Authorization": f"Bearer {t}", "Accept": "application/vnd.github+json"}

def _parse_owner_repo_from_url(url: str) -> Tuple[str, str]:
    m = re.match(r"https?://github\.com/([^/]+)/([^/.]+)(?:\.git)?/?$", url.strip())
    if not m:
        raise ValueError(f"URL inválida: {url}")
    return m.group(1), m.group(2)

def _list_sql_files(token: str, owner: str, repo: str, branch: str) -> List[str]:
    r = requests.get(API_TREES.format(owner=owner, repo=repo, branch=branch), headers=_gh_headers(token))
    if r.status_code != 200:
        raise RuntimeError(f"No se pudo listar {owner}/{repo}@{branch}: {r.status_code} {r.text}")
    tree = r.json().get("tree", [])
    return [n["path"] for n in tree if n.get("type") == "blob" and n.get("path", "").lower().endswith(".sql")]

def _get_file_content(token: str, owner: str, repo: str, path: str, ref: str) -> bytes:
    r = requests.get(API_CONTENTS.format(owner=owner, repo=repo, path=path),
                     headers=_gh_headers(token), params={"ref": ref})
    if r.status_code != 200:
        raise RuntimeError(f"Error obteniendo {owner}/{repo}/{path}: {r.status_code} {r.text}")
    js = r.json()
    if js.get("encoding") == "base64":
        return base64.b64decode(js["content"])
    return js.get("content", "").encode("utf-8")

def _read_config_from_process_repo(token: str, process_repo: str, path_config: str = "config.py") -> Dict[str, str]:
    """process_repo: 'owner/repo' """
    owner, repo = process_repo.split("/", 1)
    raw = _get_file_content(token, owner, repo, path_config, "main").decode("utf-8")
    cfg: Dict[str, str] = {}
    tree = ast.parse(raw)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            k = node.targets[0].id
            if isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                cfg[k] = node.value.value
    return cfg

def _gcs_split(uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/?(.*)$", uri.strip())
    if not m:
        raise ValueError(f"Ruta GCS inválida: {uri}")
    bucket, prefix = m.group(1), m.group(2)
    prefix = re.sub(r"/+$", "", prefix)  # sin slash final
    return bucket, prefix

def _gcs_upload_bytes(bucket: storage.Bucket, blob_name: str, data: bytes, content_type="application/sql"):
    blob = bucket.blob(blob_name)
    blob.upload_from_file(io.BytesIO(data), rewind=True, content_type=content_type)


# ===== HTTP entry =====
@functions_framework.http
def stage_sql(request):
    data = request.get_json(silent=True) or {}
    arquetipo    = data.get("arquetipo")
    process_repo = data.get("process_repo")
    sql_dir_gcs  = (data.get("sql_subdir") or "").strip()

    # Validaciones básicas
    if not arquetipo or not process_repo or not sql_dir_gcs:
        return (json.dumps({"error": "arquetipo, process_repo y sql_subdir son obligatorios"}), 400)
    if arquetipo not in REPOS_ARQ:
        return (json.dumps({"error": f"arquetipo '{arquetipo}' no soportado"}), 400)
    if not sql_dir_gcs.startswith("gs://"):
        return (json.dumps({"error": "sql_subdir debe ser gs://bucket/carpeta"}), 400)

    # PAT y config del proceso
    token = _get_pat()
    cfg = _read_config_from_process_repo(token, process_repo)
    process_name = cfg.get("nombre_proceso") or process_repo.split("/")[-1]

    # Listar y leer .sql del repo base del arquetipo
    base_owner, base_repo = _parse_owner_repo_from_url(REPOS_ARQ[arquetipo])
    sql_paths = _list_sql_files(token, base_owner, base_repo, ARQ_BRANCH)
    files = []
    for path in sql_paths:
        content = _get_file_content(token, base_owner, base_repo, path, ARQ_BRANCH)
        files.append((os.path.basename(path), content))

    # Subir a GCS directamente en la carpeta del proceso (sin subcarpetas)
    bucket_name, base_prefix0 = _gcs_split(sql_dir_gcs)  # ej: dev-deinsoluciones-ingestas, sql-files/raw-slim
    storage_client = storage.Client(project=_project())
    bucket = storage_client.bucket(bucket_name)

    target_prefix = f"{base_prefix0}/{process_name}"
    for fname, content in files:
        _gcs_upload_bytes(bucket, f"{target_prefix}/{fname}", content)

    return (json.dumps({
        "status": "ok",
        "arquetipo": arquetipo,
        "process_repo": process_repo,
        "process_name": process_name,
        "gcs_prefix": f"gs://{bucket_name}/{target_prefix}",
        "archivos_sql": [f for f, _ in files]
    }), 200)
