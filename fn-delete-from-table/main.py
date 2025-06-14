import functions_framework
import json
from google.cloud import bigquery

bq_client = bigquery.Client()

@functions_framework.http
def eliminar_por_fecha(request):
    try:
        request_json = request.get_json(silent=True)

        table_name = request_json.get("table_name")
        fecha_param = request_json.get("fecha_param")
        fecha_columna = request_json.get("fecha_columna")
        process_name = request_json.get("process_name","") #opcional

        if not table_name:
            return {"error": "Falta parámetro: table_name"}, 400
        
        if fecha_param and fecha_columna:
            # Ejecutar DELETE si hay fecha
            delete_sql = f"""
            DELETE FROM `{table_name}`
            WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
            """
            delete_job = bq_client.query(delete_sql)
            delete_job.result()

            return {
                "table_name": table_name,
                "fecha_columna": fecha_columna,
                "fecha_param": fecha_param,
                "status": "OK",
                "message": "Registros eliminados correctamente"
            }, 200

        # Si no hay fecha_param ni fecha_columna, hacer CREATE OR REPLACE
        if not process_name:
            return {"error": "process_name es requerido para crear la tabla"}, 400
        
        # Buscar schema desde la tabla de configuración
        schema_query = f"""
        SELECT params
        FROM `dev_config_zone.process_schemas`
        WHERE process_name = '{process_name}'
        LIMIT 1
        """
        schema_result = bq_client.query(schema_query).result()
        row = next(iter(schema_result), None)

        if not row:
            return {"error": f"No se encontró schema para process_name='{process_name}'"}, 404
        
        schema_json = row["params"]

        schema_fields = json.loads(schema_json)
        bq_schema = [
            bigquery.SchemaField(f["name"], f["type"], mode=f["mode"])
            for f in schema_fields
        ]

        # Crear tabla vacía con ese schema
        table = bigquery.Table(table_name, schema=bq_schema)
        bq_client.create_table(table, exists_ok=True)

        return {
            "table_name": table_name,
            "process_name": process_name,
            "status": "OK",
            "message": "Tabla <reemplazada correctamente usando el schema configurado"
        }, 200

    except Exception as e:
        return {
            "table_name": table_name if 'table_name' in locals() else None,
            "fecha_columna": fecha_columna if 'fecha_columna' in locals() else None,
            "fecha_param": fecha_param if 'fecha_param' in locals() else None,
            "status": "ERROR",
            "error": str(e)
        }, 500