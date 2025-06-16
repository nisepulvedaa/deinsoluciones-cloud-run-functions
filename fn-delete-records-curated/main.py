import functions_framework
import json
from google.cloud import bigquery

bq_client = bigquery.Client()
project_id = "deinsoluciones-serverless"  # Proyecto fijo

@functions_framework.http
def delete_records_by_date(request):
    try:
        request_json = request.get_json(silent=True)

        process_name = request_json.get("process_name")
        fecha_param = request_json.get("fecha_param")
        fecha_columna = request_json.get("fecha_columna")

        if not process_name:
            return {"error": "Falta el parámetro 'process_name'"}, 400
        if not fecha_param or not fecha_columna:
            return {"error": "Faltan los parámetros 'fecha_param' o 'fecha_columna'"}, 400

        # Consulta la tabla de configuración
        metadata_query = """
            SELECT dataset_name, table_name
            FROM `deinsoluciones-serverless.dev_config_zone.process_table_list`
            WHERE process_name = @process_name
            AND is_active = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name)
            ]
        )

        query_job = bq_client.query(metadata_query, job_config=job_config)
        rows = list(query_job.result())

        if not rows:
            return {"error": "No se encontraron tablas activas para el process_name entregado"}, 404

        resultados = []
        for row in rows:
            dataset_name = row["dataset_name"]
            table_name_only = row["table_name"]
            full_table_name = f"{project_id}.{dataset_name}.{table_name_only}"

            try:
                delete_sql = f"""
                    DELETE FROM `{full_table_name}`
                    WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
                """
                delete_job = bq_client.query(delete_sql)
                delete_job.result()

                resultados.append({
                    "table_name": full_table_name,
                    "status": "OK",
                    "message": "Registros eliminados correctamente"
                })
            except Exception as e:
                resultados.append({
                    "table_name": full_table_name,
                    "status": "ERROR",
                    "error": str(e)
                })

        return {"resultados": resultados}, 200

    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e)
        }, 500
