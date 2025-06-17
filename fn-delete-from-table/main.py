import functions_framework
import json
from google.cloud import bigquery

bq_client = bigquery.Client()
PROJECT_ID = "deinsoluciones-serverless"

@functions_framework.http
def eliminar_por_fecha(request):
    try:
        request_json = request.get_json(silent=True)
        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")
        arquetype_name = request_json.get("arquetype_name")

        if not process_name or not process_fn_name:
            return json.dumps({"error": "Faltan 'process_name' , 'process_fn_name' o 'arquetype_name' ."}), 400

        param_query = f"""
            SELECT params
            FROM `{PROJECT_ID}.dev_config_zone.process_params`
            WHERE process_name = @process_name
            AND process_fn_name = @process_fn_name
            AND arquetype_name = @arquetype_name
            AND active = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name),
                bigquery.ScalarQueryParameter("arquetype_name", "STRING", arquetype_name)
            ]
        )
        result_iterator = bq_client.query(param_query, job_config=job_config).result()

        param_list = []
        for row in result_iterator:
            row_params = row.get("params")
            if isinstance(row_params, list):
                param_list.extend(row_params)
            elif row_params:
                param_list.append(row_params)

        if not param_list:
            return json.dumps({"error": "No se encontraron parámetros válidos."}), 404

        respuestas = []

        for param in param_list:
            table_name = param.get("table_name")
            fecha_param = param.get("fecha_param", "").strip()
            fecha_columna = param.get("fecha_columna", "").strip()
            periodicidad = param.get("periodicidad", "").lower()
            dataset_name = param.get("dataset_name")

            if dataset_name and table_name:
                dataset, table = dataset_name, table_name
            elif table_name and "." in table_name:
                parts = table_name.split(".")
                if len(parts) == 2:
                    dataset, table = parts
                else:
                    respuestas.append({
                        "table_name": table_name,
                        "status": "ERROR",
                        "error": "Formato de table_name inválido."
                    })
                    continue
            else:
                respuestas.append({
                    "table_name": table_name,
                    "status": "ERROR",
                    "error": "Faltan 'table_name' o 'dataset_name'."
                })
                continue

            full_table_path = f"{PROJECT_ID}.{dataset}.{table}"

            if periodicidad == "esporadica" or not fecha_param or not fecha_columna:
                ddl_query = f"""
                    SELECT ddl
                    FROM `{PROJECT_ID}.dev_config_zone.process_ddl`
                    WHERE process_name = @process_name
                    AND table_type = 'raw'
                    LIMIT 1
                """
                ddl_config = bigquery.QueryJobConfig(
                    query_parameters=[
                        bigquery.ScalarQueryParameter("process_name", "STRING", process_name)
                    ]
                )
                ddl_result = bq_client.query(ddl_query, job_config=ddl_config).result()
                ddl_row = next(iter(ddl_result), None)

                if not ddl_row or not ddl_row.get("ddl"):
                    respuestas.append({
                        "table_name": full_table_path,
                        "status": "ERROR",
                        "message": "DDL no encontrado o vacío."
                    })
                    continue

                ddl = ddl_row["ddl"].replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE").replace("CREATE TABLE", "CREATE OR REPLACE TABLE")
                bq_client.query(ddl).result()

                respuestas.append({
                    "table_name": full_table_path,
                    "status": "OK",
                    "message": "Tabla recreada desde DDL."
                })

            else:
                delete_sql = f"""
                    DELETE FROM `{full_table_path}`
                    WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
                """
                bq_client.query(delete_sql).result()

                respuestas.append({
                    "table_name": full_table_path,
                    "fecha_columna": fecha_columna,
                    "fecha_param": fecha_param,
                    "status": "OK",
                    "message": "Registros eliminados por fecha."
                })

        return json.dumps(respuestas), 200

    except Exception as e:
        return json.dumps({
            "status": "ERROR",
            "error": str(e),
            "message": "Ocurrió un error inesperado."
        }), 500
