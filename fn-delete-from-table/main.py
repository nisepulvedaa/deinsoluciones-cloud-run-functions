import functions_framework
import json
from google.cloud import bigquery

bq_client = bigquery.Client()

@functions_framework.http
def eliminar_por_fecha(request):
    try:
        request_json = request.get_json(silent=True)
        process_name = request_json.get("process_name")
        process_fn_name = request_json.get("process_fn_name")

        if not process_name or not process_fn_name:
            return {"error": "Faltan process_name o process_fn_name"}, 400

        # Buscar parámetros en process_params
        param_query = f"""
            SELECT params
            FROM `deinsoluciones-serverless.dev_config_zone.process_params`
            WHERE process_name = @process_name
            AND process_fn_name = @process_fn_name
            AND active = TRUE
            LIMIT 1
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("process_fn_name", "STRING", process_fn_name)
            ]
        )
        result = bq_client.query(param_query, job_config=job_config).result()
        row = next(iter(result), None)
        if not row:
            return {"error": "Parámetros no encontrados"}, 404

        param_list = json.loads(row["params"])
        if not param_list or not isinstance(param_list, list):
            return {"error": "Formato de parámetros inválido"}, 400

        param = param_list[0]  # solo uno por ahora
        table_name = param.get("table_name")
        fecha_param = param.get("fecha_param", "").strip()
        fecha_columna = param.get("fecha_columna", "").strip()
        periodicidad = param.get("periodicidad", "esporadica").lower()

        if not table_name:
            return {"error": "table_name es requerido en params"}, 400

        if periodicidad == "esporadica" or not fecha_param or not fecha_columna:
            # Obtener DDL desde process_ddl
            ddl_query = f"""
                SELECT ddl
                FROM `deinsoluciones-serverless.dev_config_zone.process_ddl`
                WHERE process_name = @process_name
                AND table_type = 'raw'
                LIMIT 1
            """
            ddl_config = bigquery.QueryJobConfig(
                query_parameters=[bigquery.ScalarQueryParameter("process_name", "STRING", process_name)]
            )
            ddl_result = bq_client.query(ddl_query, job_config=ddl_config).result()
            ddl_row = next(iter(ddl_result), None)

            if not ddl_row:
                return {"error": f"No se encontró DDL para process_name='{process_name}'"}, 404

            ddl_original = ddl_row["ddl"]
            ddl_reemplazado = ddl_original.replace("CREATE TABLE IF NOT EXISTS", "CREATE OR REPLACE TABLE")
            ddl_reemplazado = ddl_reemplazado.replace("CREATE TABLE", "CREATE OR REPLACE TABLE")

            bq_client.query(ddl_reemplazado).result()
            return {
                "table_name": table_name,
                "process_name": process_name,
                "status": "OK",
                "message": "Tabla creada/reemplazada correctamente desde DDL"
            }, 200

        else:
            # Ejecutar DELETE
            delete_sql = f"""
                DELETE FROM `{table_name}`
                WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
            """
            bq_client.query(delete_sql).result()
            return {
                "table_name": table_name,
                "fecha_columna": fecha_columna,
                "fecha_param": fecha_param,
                "status": "OK",
                "message": "Registros eliminados correctamente"
            }, 200

    except Exception as e:
        return {
            "status": "ERROR",
            "error": str(e)
        }, 500
