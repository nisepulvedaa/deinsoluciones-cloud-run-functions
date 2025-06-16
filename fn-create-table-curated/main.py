import functions_framework
from google.cloud import bigquery
import json

@functions_framework.http
def create_table_curated(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Debe enviar un JSON con 'process_name' y 'table_type'"}, 400

        process_name = request_json.get("process_name")
        table_type = request_json.get("table_type")

        if not process_name or not table_type:
            return {"error": "Faltan los parámetros 'process_name' o 'table_type'"}, 400

        client = bigquery.Client()

        # Consulta parametrizada para obtener el DDL
        query = """
            SELECT ddl_statement
            FROM `deinsoluciones-serverless.dev_config_zone.process_ddl`
            WHERE process_name = @process_name
              AND table_type = @table_type
              AND is_active = TRUE
            LIMIT 1
        """

        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("table_type", "STRING", table_type),
            ]
        )

        query_job = client.query(query, job_config=job_config)
        results = list(query_job.result())

        if not results:
            return {"error": "No se encontró una instrucción DDL activa para los parámetros entregados"}, 404

        ddl_statement = results[0]["ddl_statement"]

        # Ejecutar el DDL
        ddl_job = client.query(ddl_statement)
        ddl_job.result()  # Espera a que se complete

        return {"message": "DDL ejecutado exitosamente"}, 200

    except Exception as e:
        return {"error": str(e)}, 500
