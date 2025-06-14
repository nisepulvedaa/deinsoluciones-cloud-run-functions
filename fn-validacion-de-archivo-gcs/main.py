import functions_framework
import json
import pyarrow.parquet as pq
from google.cloud import storage
from io import BytesIO
from datetime import datetime
import os

def check_parquet_records(bucket_name, file_name):
    """ Verifica si un archivo Parquet en Cloud Storage tiene registros. """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    # Verifica si el archivo existe
    if not blob.exists():
        return {"exists": False, "has_records": False}
    
    # Descarga el archivo en memoria
    file_stream = BytesIO()
    blob.download_to_file(file_stream)
    file_stream.seek(0)
    
    # Carga el archivo Parquet y verifica si tiene registros
    try:
        table = pq.read_table(file_stream)
        has_records = len(table) > 0
    except Exception as e:
        return {"exists": True, "has_records": False, "error": str(e)}
    
    return {"exists": True, "has_records": has_records}

@functions_framework.http
def validate_parquet(request):
    """ Cloud Function HTTP que valida la existencia y registros en un archivo Parquet en GCS. """
    if request.method == "GET":
        return json.dumps({"error": "Esta función solo acepta solicitudes POST con JSON."}), 400

    request_json = request.get_json(silent=True)
    
    if not request_json:
        return json.dumps({"error": "El cuerpo de la solicitud está vacío o mal formado. Se requiere un JSON válido."}), 400

    bucket_name = request_json.get("bucket_name")
    file_name = request_json.get("file_name")
    
    if not bucket_name or not file_name:
        return json.dumps({"error": "Se requieren 'bucket_name' y 'file_name'."}), 400
    
    # === Agregar sufijo con fecha actual ===
    nombre_base, extension = os.path.splitext(file_name)
    fecha_str = datetime.now().strftime("%Y-%m-%d")
    file_name_con_fecha = f"{nombre_base}_{fecha_str}{extension}"
    
    result = check_parquet_records(bucket_name, file_name_con_fecha)
    result["archivo_busqueda"] = f"gs://{bucket_name}/{file_name_con_fecha}"
    return json.dumps(result), 200

