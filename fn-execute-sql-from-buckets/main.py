import io
import json
import os
import re
import time
from datetime import datetime
from typing import List, Tuple

from google.cloud import storage, bigquery
import functions_framework

# --- Helpers ---
def _project_id() -> str:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or "deinsoluciones-serverless"

def _gcs_split(gs_uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/?(.*)$", (gs_uri or "").strip())
    if not m:
        raise ValueError("sql_dir_gcs debe ser una URI GCS (gs://bucket/path)")
    bucket, prefix = m.group(1), re.sub(r"/+$", "", m.group(2))
    return bucket, prefix

def _list_sql_objects(bucket: storage.Bucket, prefix: str) -> List[str]:
    return [b.name for b in bucket.list_blobs(prefix=prefix)
            if b.name.lower().endswith(".sql")]

def _natural_key(s: str):
    # ordena 001_x.sql < 02_y.sql < 10_z.sql
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]

def _write_log(bucket: storage.Bucket, path: str, content: str, content_type="application/json"):
    blob = bucket.blob(path)
    bio = io.BytesIO(content.encode("utf-8"))
    blob.upload_from_file(bio, rewind=True, content_type=content_type)

# --- HTTP entry ---
@functions_framework.http
def exec_sql(request):
    data = request.get_json(silent=True) or {}
    sql_dir_gcs = (data.get("sql_dir_gcs") or data.get("sql_subdir") or "").strip()
    process_name = (data.get("process_name") or "").strip()
    bq_project = os.getenv("BQ_PROJECT", "deinsoluciones-serverless")
    bq_location = os.getenv("BQ_LOCATION", "us-east4")
    dry_run = bool(data.get("dry_run", False))

    if not sql_dir_gcs or not process_name:
        return (json.dumps({"error": "sql_dir_gcs y process_name son obligatorios"}), 400)

    bucket_name, base_prefix = _gcs_split(sql_dir_gcs)
    target_prefix = f"{base_prefix}/{process_name}"

    storage_client = storage.Client(project=_project_id())
    bucket = storage_client.bucket(bucket_name)

    # listar archivos .sql y ordenar de forma natural
    objs = _list_sql_objects(bucket, target_prefix)
    if not objs:
        return (json.dumps({"error": f"No se encontraron .sql en gs://{bucket_name}/{target_prefix}"}), 404)
    objs = sorted(objs, key=_natural_key)

    bq_client = bigquery.Client(project=bq_project)

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_dir = f"{target_prefix}/_logs"
    log_json_path = f"{log_dir}/exec_{ts}.json"
    log_txt_path  = f"{log_dir}/exec_{ts}.log"

    results = []
    lines = []
    ok = True

    for obj_name in objs:
        fname = obj_name.split("/")[-1]
        sql = bucket.blob(obj_name).download_as_text(encoding="utf-8").strip()
        if not sql:
            results.append({"file": fname, "status": "skipped", "reason": "empty file"})
            lines.append(f"[SKIP] {fname} (vacío)")
            continue

        try:
            job_config = bigquery.QueryJobConfig(dry_run=dry_run)
            job = bq_client.query(sql, job_config=job_config)
            if not dry_run:
                job.result()  # espera a que termine
            state = "done" if not dry_run else "dry-run"
            results.append({
                "file": fname,
                "status": state,
                "job_id": job.job_id,
                "location": job.location,
            })
            lines.append(f"[OK] {fname} -> job_id={job.job_id}")
        except Exception as e:
            ok = False
            results.append({"file": fname, "status": "error", "error": str(e)})
            lines.append(f"[ERR] {fname} -> {e}")

    # subir logs a GCS
    osumm = {
        "sql_dir_gcs": f"gs://{bucket_name}/{target_prefix}",
        "bq_project": bq_project,
        "dry_run": dry_run,
        "results": results,
        "finished_utc": datetime.utcnow().isoformat() + "Z",
    }
    _write_log(bucket, log_json_path, json.dumps(osumm, ensure_ascii=False, indent=2))
    _write_log(bucket, log_txt_path, "\n".join(lines), content_type="text/plain")

    return (json.dumps({
        "status": "ok" if ok else "partial",
        "processed": len(results),
        "gcs_log_json": f"gs://{bucket_name}/{log_json_path}",
        "gcs_log_txt": f"gs://{bucket_name}/{log_txt_path}",
        "results": results
    }), 200 if ok else 207)
