import ast
import io
import json
import os
import re
from typing import Dict, Tuple, List

import requests
from google.cloud import storage, secretmanager
import functions_framework

# ========= Config =========
# Si se usa GitHub para leer config.py:
GITHUB_TOKEN_SECRET = os.getenv("GITHUB_TOKEN_SECRET", "github-token")
GITHUB_BRANCH = os.getenv("GITHUB_BRANCH", "main")
GITHUB_ACCEPT = "application/vnd.github+json"

API_CONTENTS = "https://api.github.com/repos/{owner}/{repo}/contents/{path}"

# Variables permitidas (coinciden con tu script)
VARIABLES_PERMITIDAS = {
    "nombre_proceso",
    "version_arquetipo_raw",
    "periodicidad",
    "buckets_detino",
    "path_destino",
    "proyecto",
    "nombre_archivo",
    "dataset_raw_zone",
    "nombre_tabla_raw",
    "campo_fecha_tabla_raw",
    "fecha_ejecucion",
    "ddl_raw",
    "correos_destinatarios",
}

# ========= Helpers =========
def _project_id() -> str:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or os.getenv("GCP_PROJECT")

def _gcs_split(gs_uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/?(.*)$", (gs_uri or "").strip())
    if not m:
        raise ValueError("sql_dir_gcs debe ser una URI GCS (gs://bucket/path)")
    bucket, prefix = m.group(1), re.sub(r"/+$", "", m.group(2))
    return bucket, prefix

def _get_pat() -> str:
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{_project_id()}/secrets/{GITHUB_TOKEN_SECRET}/versions/latest"
    tok = client.access_secret_version(request={"name": name}).payload.data.decode("utf-8").strip()
    if not tok or "BEGIN " in tok:
        raise ValueError("El secreto no es un PAT de GitHub (usa ghp_/github_pat_).")
    return tok

def _read_config_from_github(owner_repo: str, path: str = "config.py") -> Dict[str, str]:
    """owner_repo: 'owner/repo'"""
    owner, repo = owner_repo.split("/", 1)
    headers = {"Authorization": f"Bearer {_get_pat()}", "Accept": GITHUB_ACCEPT}
    r = requests.get(API_CONTENTS.format(owner=owner, repo=repo, path=path),
                     headers=headers, params={"ref": GITHUB_BRANCH})
    if r.status_code != 200:
        raise RuntimeError(f"No pude leer {owner_repo}/{path}@{GITHUB_BRANCH}: {r.status_code} {r.text}")
    js = r.json()
    if js.get("encoding") == "base64":
        import base64
        src = base64.b64decode(js["content"]).decode("utf-8")
    else:
        src = js.get("content", "")

    # Parse seguro de asignaciones simples
    cfg: Dict[str, str] = {}
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            k = node.targets[0].id
            if k in VARIABLES_PERMITIDAS and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                cfg[k] = node.value.value
    if not cfg:
        raise ValueError("No se encontraron variables válidas en config.py")
    return cfg

def _read_config_from_gcs(bucket: storage.Bucket, prefix: str, filename: str = "config.py") -> Dict[str, str]:
    blob = bucket.blob(f"{prefix}/{filename}")
    if not blob.exists():
        raise FileNotFoundError(f"No existe {filename} en gs://{bucket.name}/{prefix}/")
    src = blob.download_as_text(encoding="utf-8")

    cfg: Dict[str, str] = {}
    tree = ast.parse(src)
    for node in tree.body:
        if isinstance(node, ast.Assign) and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name):
            k = node.targets[0].id
            if k in VARIABLES_PERMITIDAS and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
                cfg[k] = node.value.value
    if not cfg:
        raise ValueError("No se encontraron variables válidas en config.py (GCS)")
    return cfg

def _apply_config_text(text: str, cfg: Dict[str, str]) -> str:
    # Reemplaza {{ var }} o {{var}}
    out = text
    for k, v in cfg.items():
        pattern = re.compile(r"\{\{\s*" + re.escape(k) + r"\s*\}\}")
        out = pattern.sub(v, out)
    return out

def _list_objects(bucket: storage.Bucket, prefix: str, exts=(".sql", ".json")) -> List[str]:
    return [b.name for b in bucket.list_blobs(prefix=prefix)
            if os.path.splitext(b.name)[1].lower() in exts]

# ========= HTTP entry =========
@functions_framework.http
def apply_config(request):
    data = request.get_json(silent=True) or {}
    sql_dir_gcs = (data.get("sql_dir_gcs") or data.get("sql_subdir") or "").strip()
    process_name = (data.get("process_name") or "").strip()
    process_repo = (data.get("process_repo") or "").strip()
    config_from_gcs = bool(data.get("config_from_gcs", False))

    if not sql_dir_gcs or not process_name:
        return (json.dumps({"error": "sql_dir_gcs y process_name son obligatorios"}), 400)

    # Normaliza destino GCS
    bucket_name, base_prefix = _gcs_split(sql_dir_gcs)
    target_prefix = f"{base_prefix}/{process_name}"

    storage_client = storage.Client(project=_project_id())
    bucket = storage_client.bucket(bucket_name)

    # Obtiene variables de config.py
    if config_from_gcs:
        cfg = _read_config_from_gcs(bucket, target_prefix)
    else:
        if not process_repo:
            return (json.dumps({"error": "Falta process_repo (o envía config_from_gcs=true)"}), 400)
        cfg = _read_config_from_github(process_repo)

    # Aplica sobre cada .sql/.json
    updated = []
    errors = []
    for obj in _list_objects(bucket, target_prefix):
        try:
            blob = bucket.blob(obj)
            src = blob.download_as_text(encoding="utf-8")
            dst = _apply_config_text(src, cfg)
            if dst != src:
                # sobrescribe
                bio = io.BytesIO(dst.encode("utf-8"))
                blob.upload_from_file(bio, rewind=True,
                                      content_type="application/sql" if obj.endswith(".sql") else "application/json")
            updated.append(os.path.basename(obj))
        except Exception as e:
            errors.append({"file": os.path.basename(obj), "error": str(e)})

    return (json.dumps({
        "status": "ok" if not errors else "partial",
        "gcs_prefix": f"gs://{bucket_name}/{target_prefix}",
        "updated_files": updated,
        "errors": errors
    }), 200 if not errors else 207)
