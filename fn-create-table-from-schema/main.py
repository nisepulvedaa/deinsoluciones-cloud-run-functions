import functions_framework
import json
from google.cloud import bigquery, storage

BUCKET_NAME = "deinsoluciones-serverless-config"

@functions_framework.http
def create_bigquery_table(request):
    request_json = request.get_json(silent=True)

    if not request_json:
        return {"error": "Se requiere un cuerpo JSON con schema_path, dataset_id y table_id."}, 400

    schema_path = request_json.get("schema_path")
    dataset_id = request_json.get("dataset_id")
    table_id = request_json.get("table_id")
    partition_column = request_json.get("partition_column")  # Opcional

    if not schema_path or not dataset_id or not table_id:
        return {"error": "Faltan campos requeridos: schema_path, dataset_id, table_id."}, 400

    try:
        # Leer esquema desde Cloud Storage
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(schema_path)
        schema_bytes = blob.download_as_bytes()
        schema_json = json.loads(schema_bytes)

        # Construcción del esquema de BigQuery
        schema = [bigquery.SchemaField(col["name"], col["type"], mode=col["mode"]) for col in schema_json]

        bq_client = bigquery.Client()
        table_ref = bq_client.dataset(dataset_id).table(table_id)


        # Validar si la tabla ya existe
        try:
            bq_client.get_table(table_ref)
            return {
                "message": f"La tabla '{table_id}' ya existe en el dataset '{dataset_id}'. No se creó nuevamente."
            }, 200
        except Exception:
            pass  # La tabla no existe

        # Crear nueva tabla
        table = bigquery.Table(table_ref, schema=schema)

        # Configurar partición si se especificó columna
        if partition_column:
            table.time_partitioning = bigquery.TimePartitioning(
                type_=bigquery.TimePartitioningType.DAY,
                field=partition_column,
                expiration_ms=None
            )

        # Crear tabla
        bq_client.create_table(table)

        return {
            "message": f"✅ Tabla '{table_id}' creada exitosamente en '{dataset_id}'"
                    + (f" con partición en '{partition_column}'." if partition_column else ".")
        }, 200

    except Exception as e:
        return {"error": str(e)}, 500