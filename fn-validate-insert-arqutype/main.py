import io
import json
import os
import re
from datetime import datetime
from typing import Tuple, List

from google.cloud import storage, bigquery
import functions_framework

# -------- Defaults / helpers --------
def _project_id() -> str:
    return os.getenv("GOOGLE_CLOUD_PROJECT") or "deinsoluciones-serverless"

def _bq_project() -> str:
    return os.getenv("BQ_PROJECT", "deinsoluciones-serverless")

def _bq_location() -> str:
    return os.getenv("BQ_LOCATION", "us-east4")

def _gcs_split(gs_uri: str) -> Tuple[str, str]:
    m = re.match(r"^gs://([^/]+)/?(.*)$", (gs_uri or "").strip())
    if not m:
        raise ValueError("sql_dir_gcs debe ser gs://bucket/path")
    bucket, prefix = m.group(1), re.sub(r"/+$", "", m.group(2))
    return bucket, prefix

def _write_text(bucket: storage.Bucket, path: str, content: str, content_type="text/plain"):
    blob = bucket.blob(path)
    bio = io.BytesIO(content.encode("utf-8"))
    blob.upload_from_file(bio, rewind=True, content_type=content_type)

def _count_query(bq: bigquery.Client, sql: str, params: List[bigquery.ScalarQueryParameter]):
    job = bq.query(sql, location=_bq_location(),
                   job_config=bigquery.QueryJobConfig(query_parameters=params))
    rows = list(job.result())
    return int(rows[0][0]) if rows else 0

# -------- HTTP entry --------
@functions_framework.http
def validate(request):
    data = request.get_json(silent=True) or {}
    sql_dir_gcs  = (data.get("sql_dir_gcs") or data.get("sql_subdir") or "").strip()
    process_name = (data.get("process_name") or "").strip()
    arq_type     = (data.get("arquetipo_type") or "").strip().lower()

    arquety_name = (data.get("arquety_name") or "").strip()
    zone_email   = (data.get("zone_email") or "").strip()
    zone_ddl     = (data.get("zone_ddl") or "").strip()
    table_types  = data.get("table_types") or ["satelite", "hub", "link"]

    if not sql_dir_gcs or not process_name or arq_type not in ("raw", "cleansed"):
        return (json.dumps({"error": "sql_dir_gcs, process_name y arquetipo_type ('raw'|'cleansed') son obligatorios"}), 400)

    # GCS target (para dejar logs junto a los SQL)
    bucket_name, base_prefix = _gcs_split(sql_dir_gcs)
    target_prefix = f"{base_prefix}/{process_name}"
    log_dir = f"{target_prefix}/_logs"
    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    log_json = f"{log_dir}/validation_{ts}.json"
    log_txt  = f"{log_dir}/validation_{ts}.log"

    storage_client = storage.Client(project=_project_id())
    bucket = storage_client.bucket(bucket_name)

    # BQ client
    bq = bigquery.Client(project=_bq_project())

    # --- Queries (todas con COUNT(*)) ---
    q_params = """
      SELECT COUNT(*) 
      FROM `deinsoluciones-serverless.dev_config_zone.process_params`
      WHERE process_name = @proc AND arquety_name = @arq
    """

    q_email = """
      SELECT COUNT(*)
      FROM `deinsoluciones-serverless.dev_config_zone.process_email`
      WHERE process_name = @proc AND zone_name = @zone_email
    """

    q_ddl = """
      SELECT COUNT(*)
      FROM `deinsoluciones-serverless.dev_config_zone.process_ddl`
      WHERE process_name = @proc AND zone_name = @zone_ddl
    """

    q_insert = """
      SELECT COUNT(*)
      FROM `deinsoluciones-serverless.dev_config_zone.process_insert`
      WHERE process_name = @proc AND table_type IN UNNEST(@ttypes)
    """

    # --- Ejecutar según arquetipo_type ---
    counts = {}

    counts["process_params"] = _count_query(
        bq, q_params,
        [
            bigquery.ScalarQueryParameter("proc", "STRING", process_name),
            bigquery.ScalarQueryParameter("arq", "STRING", arquety_name),
        ],
    )

    counts["process_email"] = _count_query(
        bq, q_email,
        [
            bigquery.ScalarQueryParameter("proc", "STRING", process_name),
            bigquery.ScalarQueryParameter("zone_email", "STRING", zone_email),
        ],
    )

    counts["process_ddl"] = _count_query(
        bq, q_ddl,
        [
            bigquery.ScalarQueryParameter("proc", "STRING", process_name),
            bigquery.ScalarQueryParameter("zone_ddl", "STRING", zone_ddl),
        ],
    )

    if arq_type == "cleansed":
        # array parameter
        job = bq.query(
            q_insert,
            location=_bq_location(),
            job_config=bigquery.QueryJobConfig(
                query_parameters=[
                    bigquery.ScalarQueryParameter("proc", "STRING", process_name),
                    bigquery.ArrayQueryParameter("ttypes", "STRING", table_types),
                ]
            ),
        )
        rows = list(job.result())
        counts["process_insert"] = int(rows[0][0]) if rows else 0

    # --- Evaluación de estado ---
    required_keys = ["process_params", "process_email", "process_ddl"] + (["process_insert"] if arq_type == "cleansed" else [])
    ok = all(counts.get(k, 0) > 0 for k in required_keys)
    status = "ok" if ok else "fail"

    summary = {
        "project": _bq_project(),
        "location": _bq_location(),
        "process_name": process_name,
        "arquetipo_type": arq_type,
        "arquety_name": arquety_name,
        "zone_email": zone_email,
        "zone_ddl": zone_ddl,
        "table_types": table_types if arq_type == "cleansed" else None,
        "counts": counts,
        "required": required_keys,
        "gcs_prefix": f"gs://{bucket_name}/{target_prefix}",
        "finished_utc": datetime.utcnow().isoformat() + "Z",
        "status": status
    }

    # Logs en GCS
    _write_text(bucket, log_json, json.dumps(summary, ensure_ascii=False, indent=2), content_type="application/json")
    _write_text(
        bucket, log_txt,
        "\n".join([
            f"[VALIDATION] proc={process_name} type={arq_type}",
            f"params(arquety_name='{arquety_name}'): {counts.get('process_params',0)}",
            f"email(zone='{zone_email}'): {counts.get('process_email',0)}",
            f"ddl(zone='{zone_ddl}'): {counts.get('process_ddl',0)}",
            *( [f"insert(types={table_types}): {counts.get('process_insert',0)}"] if arq_type == "cleansed" else [] ),
            f"status: {status}"
        ]),
        content_type="text/plain"
    )

    return (json.dumps({
        "status": status,
        "gcs_log_json": f"gs://{bucket_name}/{log_json}",
        "gcs_log_txt":  f"gs://{bucket_name}/{log_txt}",
        "counts": counts,
        "required": required_keys
    }), 200 if status == "ok" else 200)
