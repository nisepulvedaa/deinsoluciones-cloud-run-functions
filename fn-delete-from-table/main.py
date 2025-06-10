import functions_framework
from google.cloud import bigquery

bq_client = bigquery.Client()

@functions_framework.http
def eliminar_por_fecha(request):
    try:
        request_json = request.get_json(silent=True)

        table_name = request_json.get("table_name")
        fecha_param = request_json.get("fecha_param")
        fecha_columna = request_json.get("fecha_columna")

        if not table_name or not fecha_param or not fecha_columna:
            return {
                "error": "Faltan parámetros: table_name, fecha_param y/o fecha_columna"
            }, 400

        # Construir la query DELETE dinámicamente
        delete_sql = f"""
        DELETE FROM `{table_name}`
        WHERE DATE({fecha_columna}) = DATE('{fecha_param}')
        """

        # Ejecutar el DELETE
        delete_job = bq_client.query(delete_sql)
        delete_job.result()

        return {
            "table_name": table_name,
            "fecha_columna": fecha_columna,
            "fecha_param": fecha_param,
            "status": "OK",
            "message": "Registros eliminados correctamente"
        }, 200

    except Exception as e:
        return {
            "table_name": table_name if 'table_name' in locals() else None,
            "fecha_columna": fecha_columna if 'fecha_columna' in locals() else None,
            "fecha_param": fecha_param if 'fecha_param' in locals() else None,
            "status": "ERROR",
            "error": str(e)
        }, 500