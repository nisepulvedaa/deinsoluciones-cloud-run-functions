import functions_framework
import json
from google.cloud import bigquery

@functions_framework.http
def validate_bigquery_table(request):
    """Cloud Function HTTP que valida si una tabla de BigQuery tiene registros
    y actualiza la tabla process_detail con el resultado del proceso.
    """
    request_json = request.get_json(silent=True)
    project_id = request_json.get("project_id")
    dataset_id = request_json.get("dataset_id")
    table_id = request_json.get("table_id")
    process_name = request_json.get("process_name")   # nuevo parámetro
    zone_name = request_json.get("zone_name")         # nuevo parámetro

    if not all([project_id, dataset_id, table_id, process_name, zone_name]):
        return json.dumps({
            "error": "Se requieren 'project_id', 'dataset_id', 'table_id', 'process_name' y 'zone_name'."
        }), 400

    client = bigquery.Client()
    
    try:
        # 1. Consulta para contar los registros de la tabla
        query = f"""
            SELECT COUNT(*) as row_count
            FROM `{project_id}.{dataset_id}.{table_id}`
        """
        query_job = client.query(query)
        results = query_job.result()
        row_count = next(results).row_count
        has_records = row_count > 0

        # 2. Actualización de process_detail con el total de registros
        update_query = f"""
            UPDATE `deinsoluciones-serverless.dev_config_zone.process_detail`
            SET end_process = CAST(CURRENT_DATETIME("America/Santiago") AS TIMESTAMP),
                qantity_of_records = {row_count}
            WHERE process_name = '{process_name}'
              AND zone_name = '{zone_name}'
              AND start_process = (
                  SELECT MAX(start_process)
                  FROM `deinsoluciones-serverless.dev_config_zone.process_detail`
                  WHERE process_name = '{process_name}'
                    AND zone_name = '{zone_name}'
                    AND end_process IS NULL
              )
        """
        update_job = client.query(update_query)
        update_job.result()  # Esperar ejecución del UPDATE

    except Exception as e:
        return json.dumps({
            "exists": False,
            "has_records": False,
            "error": str(e)
        }), 500

    return json.dumps({
        "exists": True,
        "has_records": has_records,
        "row_count": row_count
    }), 200
