import functions_framework
from google.cloud import bigquery
import json

@functions_framework.http
def insert_data_process(request):
    """Inserta en process_detail los inicios de ejecución para un proceso, obteniendo datos desde process_params."""
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Se requiere JSON con 'process_name' , 'process_fn_name' y 'arquetype_name'"}, 400

        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")
        arquetype_name = request_json.get("arquetype_name")

        if not process_name or not process_fn_name:
            return {"error": "Se requieren 'process_name' y 'process_fn_name'"}, 400

        project_id = "deinsoluciones-serverless"
        client = bigquery.Client()

        # Consulta para obtener configuración de parámetros
        query = f"""
        SELECT params
        FROM `{project_id}.dev_config_zone.process_params`
        WHERE process_name = '{process_name}'
        AND process_fn_name = '{process_fn_name}'
        AND arquetype_name = '{arquetype_name}'
        AND active = TRUE
        """
        results = client.query(query).result()

        registros_insertados = 0

        for row in results:
            params_list = row["params"]

            for param in params_list:
                dataset_name = param.get("dataset_name")
                table_name = param.get("table_name")
                zone_name = param.get("zone_name")

                if not dataset_name or not table_name:
                    continue  # Saltar si faltan campos obligatorios

                insert_query = f"""
                INSERT INTO `{project_id}.dev_config_zone.process_detail`
                (process_name, dataset_name, table_name, start_process, end_process, qantity_of_records, zone_name)
                VALUES (
                    '{process_name}',
                    '{dataset_name}',
                    '{table_name}',
                    CAST(CURRENT_DATETIME("America/Santiago") AS TIMESTAMP),
                    NULL,
                    NULL,
                    '{zone_name}'
                )
                """
                client.query(insert_query).result()
                registros_insertados += 1

        if registros_insertados == 0:
            return {
                "status": "WARNING",
                "message": "No se encontraron parámetros válidos para insertar"
            }, 200

        return {
            "status": "OK",
            "message": f"{registros_insertados} registros insertados en process_detail"
        }, 200

    except Exception as e:
        return {"error": str(e)}, 500
