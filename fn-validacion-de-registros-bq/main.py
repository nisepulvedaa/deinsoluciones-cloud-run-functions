import functions_framework
import json
from google.cloud import bigquery

@functions_framework.http
def validate_bigquery_table(request):
    """ Cloud Function HTTP que valida si una tabla de BigQuery tiene registros. """
    request_json = request.get_json(silent=True)
    project_id = request_json.get("project_id")
    dataset_id = request_json.get("dataset_id")
    table_id = request_json.get("table_id")
    
    if not project_id or not dataset_id or not table_id:
        return json.dumps({"error": "Se requieren 'project_id', 'dataset_id' y 'table_id'."}), 400
    
    client = bigquery.Client()
    query = f"""
        SELECT COUNT(*) as row_count
        FROM `{project_id}.{dataset_id}.{table_id}`
    """
    
    try:
        query_job = client.query(query)
        results = query_job.result()
        row_count = next(results).row_count
        has_records = row_count > 0
    except Exception as e:
        return json.dumps({"exists": False, "has_records": False, "error": str(e)}), 500
    
    return json.dumps({"exists": True, "has_records": has_records}), 200