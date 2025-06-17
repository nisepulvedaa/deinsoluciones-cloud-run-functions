import functions_framework
import json
from google.cloud import bigquery

@functions_framework.http
def validate_bigquery_table(request):
    """Valida si una tabla de BigQuery tiene registros y actualiza process_detail."""
    request_json = request.get_json(silent=True)

    process_name = request_json.get("process_name")
    process_fn_name = request_json.get("process_fn_name")
    zone_name = request_json.get("zone_name")
    arquetype_name = request_json.get("arquetype_name")

    if not all([process_name, process_fn_name, zone_name]):
        return json.dumps({
            "error": "Se requieren 'process_name', 'process_fn_name' , 'zone_name' y 'arquetype_name'."
        }), 400

    project_id = "deinsoluciones-serverless"
    bq_client = bigquery.Client()

    try:
        # Obtener configuración desde process_params
        config_query = f"""
            SELECT params
            FROM `{project_id}.dev_config_zone.process_params`
            WHERE process_name = '{process_name}'
            AND process_fn_name = '{process_fn_name}'
            AND arquetype_name = '{arquetype_name}'
            AND active = TRUE

        """
        config_result = bq_client.query(config_query).result()
        row = next(iter(config_result), None)

        if not row:
            return json.dumps({
                "error": f"No se encontró configuración activa para {process_name}"
            }), 404

        params_list = row["params"]
        if not params_list or not isinstance(params_list, list):
            return json.dumps({"error": "El campo 'params' no tiene un formato válido"}), 400

        resultados = []
        for config in params_list:
            dataset_id = config.get("dataset_name")
            table_id = config.get("table_name")
            fecha_columna = config.get("fecha_columna")
            fecha_param = config.get("fecha_param")
            periodicidad = config.get("periodicidad").lower()

            if not dataset_id or not table_id:
                resultados.append({
                    "dataset_id": dataset_id,
                    "table_id": table_id,
                    "error": "Faltan 'dataset_id' o 'table_id' en la configuración"
                })
                continue

            # Construcción dinámica del query de conteo
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

            # Actualizar tabla process_detail
            update_query = f"""
                UPDATE `{project_id}.dev_config_zone.process_detail`
                SET end_process = CAST(CURRENT_DATETIME("America/Santiago") AS TIMESTAMP),
                    qantity_of_records = {row_count}
                WHERE process_name = '{process_name}'
                  AND zone_name = '{zone_name}'
                  AND dataset_name = '{dataset_id}'
                  AND table_name = '{table_id}'
                  AND start_process = (
                      SELECT MAX(start_process)
                      FROM `{project_id}.dev_config_zone.process_detail`
                      WHERE process_name = '{process_name}'
                        AND zone_name = '{zone_name}'
                        AND dataset_name = '{dataset_id}'
                        AND table_name = '{table_id}'
                        AND end_process IS NULL
                  )
            """
            bq_client.query(update_query).result()

            resultados.append({
                "dataset_id": dataset_id,
                "table_id": table_id,
                "row_count": row_count,
                "has_records": has_records
            })

        return json.dumps(resultados), 200

    except Exception as e:
        return json.dumps({
            "error": str(e)
        }), 500
