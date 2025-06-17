import functions_framework
import json
from google.cloud import bigquery

@functions_framework.http
def validate_bigquery_table(request):
    """Valida si una tabla de BigQuery tiene registros y actualiza process_detail, usando config."""
    request_json = request.get_json(silent=True)

    process_name = request_json.get("process_name")
    zone_name = request_json.get("zone_name")

    if not process_name or not zone_name:
        return json.dumps({
            "error": "Se requieren 'process_name' y 'zone_name'."
        }), 400

    project_id = "deinsoluciones-serverless"
    bq_client = bigquery.Client()

    try:
        # Obtener parámetros desde la tabla de configuración
        config_query = f"""
            SELECT params
            FROM `{project_id}.dev_config_zone.process_params`
            WHERE process_name = '{process_name}'
              AND process_fn_name = 'fn-validate-bq-table'
              AND active = TRUE
            LIMIT 1
        """
        config_result = bq_client.query(config_query).result()
        row = next(iter(config_result), None)

        if not row:
            return json.dumps({
                "error": f"No se encontró configuración activa para {process_name}"
            }), 404

        params_list = json.loads(row["params"])
        if not params_list or not isinstance(params_list, list):
            return json.dumps({"error": "El campo 'params' no tiene un formato válido"}), 400

        config = params_list[0]
        dataset_id = config.get("dataset_id")
        table_id = config.get("table_id")
        fecha_columna = config.get("fecha_columna")
        fecha_param = config.get("fecha_param")
        periodicidad = config.get("periodicidad", "esporadica").lower()

        if not dataset_id or not table_id:
            return json.dumps({
                "error": "Faltan 'dataset_id' o 'table_id' en la configuración"
            }), 400

        # Construir query de conteo condicional
        if periodicidad == "esporadica" or not fecha_columna or not fecha_param:
            count_query = f"""
                SELECT COUNT(*) as row_count
                FROM `{project_id}.{dataset_id}.{table_id}`
            """
        else:
            count_query = f"""
                SELECT COUNT(*) as row_count
                FROM `{project_id}.{dataset_id}.{table_id}`
                WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
            """

        count_result = bq_client.query(count_query).result()
        row_count = next(count_result).row_count
        has_records = row_count > 0

        # Actualizar process_detail
        update_query = f"""
            UPDATE `{project_id}.dev_config_zone.process_detail`
            SET end_process = CURRENT_DATETIME("America/Santiago"),
                qantity_of_records = {row_count}
            WHERE process_name = '{process_name}'
              AND zone_name = '{zone_name}'
              AND start_process = (
                  SELECT MAX(start_process)
                  FROM `{project_id}.dev_config_zone.process_detail`
                  WHERE process_name = '{process_name}'
                    AND zone_name = '{zone_name}'
                    AND end_process IS NULL
              )
        """
        bq_client.query(update_query).result()

        return json.dumps({
            "exists": True,
            "has_records": has_records,
            "row_count": row_count
        }), 200

    except Exception as e:
        return json.dumps({
            "exists": False,
            "has_records": False,
            "error": str(e)
        }), 500
