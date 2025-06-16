import functions_framework
import json
from google.cloud import bigquery

bq_client = bigquery.Client()

@functions_framework.http
def delete_records_by_date(request):
    try:
        request_json = request.get_json(silent=True)

        process_name = request_json.get("process_name")
        table_type = request_json.get("table_type")
        fecha_param = request_json.get("fecha_param")
        fecha_columna = request_json.get("fecha_columna")

        if not process_name or not table_type:
            return {"error": "Faltan los parámetros 'process_name' o 'table_type'"}, 400
        if not fecha_param or not fecha_columna:
            return {"error": "Faltan los parámetros 'fecha_param' o 'fecha_columna'"}, 400

        # Obtener todas las tablas asociadas
        metadata_query = """
            SELECT table_name
            FROM `deinsoluciones-serverless.dev_config_zone.process_schemas`
            WHERE process_name = @process_name
              AND table_type = @table_type
              AND is_active = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("table_type", "STRING", table_type),
            ]
        )

        query_job = bq_client.query(metadata_query, job_config=job_config)
        tables = list(query_job.result())

        if not tables:
            return {"error": "No se encontraron tablas activas para los parámetros entregados"}, 404

        resultados = []
        for row in tables:
            table_name = row["table_name"]
            try:
                delete_sql = f"""
                    DELETE FROM `{table_name}`
                    WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
                """
                delete_job = bq_client.query(delete_sql)
                delete_job.result()
                resultados.append({
                    "table_name": table_name,
                    "status": "OK",
                    "message": "Registros eliminados correctamente"
                })
            except Exception as e:
                resultados.append({
                    "table_name": table_name,
                    "status": "ERROR",
                    "error": str(e)
                })

        return {"resultados": resultados}, 200

    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e)
        }, 500
