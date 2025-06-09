from google.cloud import bigquery
import functions_framework
import json
import re

@functions_framework.http
def ejecutar_ddl_por_tipo(request):
    request_json = request.get_json(silent=True)
    if not request_json or 'cfg_proc' not in request_json:
        return ('Missing required parameter: cfg_proc', 400)

    process_name = request_json['cfg_proc']
    ejecutar_sat = request_json.get('ejecutar_sat', False)
    ejecutar_hub = request_json.get('ejecutar_hub', False)
    ejecutar_link = request_json.get('ejecutar_link', False)

    tipos_a_ejecutar = []
    if ejecutar_sat:
        tipos_a_ejecutar.append("satelite")
    if ejecutar_hub:
        tipos_a_ejecutar.append("hub")
    if ejecutar_link:
        tipos_a_ejecutar.append("link")

    if not tipos_a_ejecutar:
        return ('No hay tipos de tabla activados para procesar', 400)

    client = bigquery.Client()
    resultados = []

    for tipo in tipos_a_ejecutar:
        sql = """
            SELECT ddl_statement
            FROM `deinsoluciones-serverless.dev_config_zone.process_ddl`
            WHERE process_name = @process_name
              AND table_type = @table_type
              AND is_active = TRUE
        """
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("process_name", "STRING", process_name),
                bigquery.ScalarQueryParameter("table_type", "STRING", tipo)
            ]
        )

        try:
            rows = client.query(sql, job_config=job_config).result()
            for row in rows:
                ddl = row.ddl_statement

                # Extraer tabla del DDL
                match = re.search(r"CREATE TABLE\s+`?([\w\-]+)\.([\w]+)\.([\w]+)`?", ddl, re.IGNORECASE)
                if not match:
                    resultados.append({"table_type": tipo, "status": "ERROR", "error": "No se pudo extraer el nombre de la tabla del DDL"})
                    continue

                project_id, dataset_id, table_id = match.groups()
                table_ref = f"{project_id}.{dataset_id}.{table_id}"

                # Verificar si la tabla ya existe
                try:
                    client.get_table(table_ref)
                    resultados.append({"table_type": tipo, "status": "SKIPPED", "reason": "Tabla ya existe"})
                except Exception:
                    client.query(ddl).result()
                    resultados.append({"table_type": tipo, "status": "CREATED"})

        except Exception as e:
            resultados.append({"table_type": tipo, "status": "ERROR", "error": str(e)})

    return json.dumps({
        "process_name": process_name,
        "resultados": resultados
    }), 200
