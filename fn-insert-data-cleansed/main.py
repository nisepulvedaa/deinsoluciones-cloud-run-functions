import functions_framework
from google.cloud import bigquery

bq_client = bigquery.Client()

@functions_framework.http
def ejecutar_insert(request):
    try:
        request_json = request.get_json(silent=True)
        process_name = request_json.get("process_name")
        table_type = request_json.get("table_type")
        fecha_param = request_json.get("fecha_param")

        if not process_name or not table_type:
            return {
                "error": "Faltan parámetros: process_name y/o table_type"
            }, 400
        
        if not fecha_param:
            return {
                "error": "Falta parámetro: fecha_param (formato esperado: YYYY-MM-DD)"
            }, 400

        # Consulta para obtener el INSERT activo
        query = """
        SELECT insert_statement
        FROM `deinsoluciones-serverless.dev_config_zone.process_insert`
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

        query_job = bq_client.query(query, job_config=job_config)
        rows = list(query_job.result())

        if not rows:
            return {
                "process_name": process_name,
                "table_type": table_type,
                "status": "ERROR",
                "error": "No se encontró una instrucción INSERT activa"
            }, 404

        insert_sql = rows[0]["insert_statement"]

        insert_sql = insert_sql.replace("${fecha_param}", fecha_param)


        # Ejecutar el INSERT
        insert_job = bq_client.query(insert_sql)
        insert_job.result()

        return {
            "process_name": process_name,
            "table_type": table_type,
            "fecha_param": fecha_param,
            "status": "OK",
            "message": "INSERT ejecutado correctamente"
        }, 200

    except Exception as e:
        return {
            "process_name": process_name if 'process_name' in locals() else None,
            "table_type": table_type if 'table_type' in locals() else None,
            "fecha_param": fecha_param if 'fecha_param' in locals() else None,
            "status": "ERROR",
            "error": str(e)
        }, 500