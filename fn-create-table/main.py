import functions_framework
from google.cloud import bigquery
import json

@functions_framework.http
def create_table(request):
    try:
        request_json = request.get_json(silent=True)
        if not request_json:
            return {"error": "Debe enviar un JSON con 'process_name' y 'table_type'"}, 400

        process_name = request_json.get("process_name")
        table_type = request_json.get("table_type")

        if not process_name or not table_type:
            return {"error": "Faltan los parámetros 'process_name' o 'table_type'"}, 400

        client = bigquery.Client()

        # Consulta para obtener múltiples DDLs
        query = """
            SELECT ddl_statement
            FROM `deinsoluciones-serverless.dev_config_zone.process_ddl`
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

        query_job = client.query(query, job_config=job_config)
        ddl_rows = list(query_job.result())

        if not ddl_rows:
            return {"error": "No se encontraron DDLs activas para los parámetros entregados"}, 404

        resultados = []
        for row in ddl_rows:
            ddl = row["ddl_statement"]
            try:
                ddl_job = client.query(ddl)
                ddl_job.result()
                resultados.append({"ddl": ddl, "status": "ejecutado correctamente"})
            except Exception as ddl_error:
                resultados.append({"ddl": ddl, "status": f"error: {str(ddl_error)}"})

        return {"message": "Proceso finalizado", "resultados": resultados}, 200

    except Exception as e:
        return {"error": str(e)}, 500
