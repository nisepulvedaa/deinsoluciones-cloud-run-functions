import json
import functions_framework
from datetime import datetime
from google.cloud import storage

# Configuración
BUCKET_NAME = "dev-deinsoluciones-ingestas"

def transformar_fecha(fecha, periodicidad):
    """Convierte la fecha a YYYY-MM-DD o YYYY-MM-01 según la periodicidad."""
    try:
        fecha_dt = datetime.strptime(fecha, "%d-%m-%Y")
        if periodicidad == "diaria":
            return fecha_dt.strftime("%Y-%m-%d")
        elif periodicidad == "mensual":
            return fecha_dt.strftime("%Y-%m-01")
        else:
            raise ValueError("Periodicidad inválida. Debe ser 'diaria' o 'mensual'.")
    except ValueError:
        raise ValueError("Formato de fecha inválido. Debe ser DD-MM-YYYY.")

def obtener_nombre_archivo(archivo):
    """Extrae el nombre base del archivo sin la extensión y lo estandariza."""
    nombre_archivo = archivo.rsplit('.', 1)[0]  # Asegura que solo elimine la última extensión
    nombre_archivo = nombre_archivo.replace("_", "-")  # Reemplaza underscores con guiones
    nombre_archivo = nombre_archivo.lower()  # Convierte todo a minúsculas
    return nombre_archivo

def generar_json(query, fecha_transformada, archivo):
    """Genera el JSON con la query y el path de salida."""
    nombre_archivo = obtener_nombre_archivo(archivo)  # Extraer el nombre sin extensión
    nombre_mayuscula = nombre_archivo.replace("-", "_").upper()
    output_path = f"gs://{BUCKET_NAME}/files/{nombre_mayuscula}/periodo_dia={fecha_transformada}/{archivo}"
    
    return {
        "query": query,
        "output_path": output_path
    }, f"files/{nombre_mayuscula}/periodo_dia={fecha_transformada}", f"jobs-params/{nombre_archivo}.json"


def subir_a_gcs(bucket_name, file_path, data):
    """Guarda el JSON en un bucket de Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(file_path)
    
    # Convertimos el JSON a string y lo subimos
    blob.upload_from_string(json.dumps(data, indent=4), content_type="application/json")
    print(f"✅ Archivo JSON subido exitosamente: gs://{bucket_name}/{file_path}")

@functions_framework.http
def generar_y_guardar_json(request):
    """Función HTTP que genera el JSON, lo guarda en GCS y crea la carpeta en GCS."""
    request_json = request.get_json(silent=True)

    print("Request: ", request_json)
    
    if not request_json:
        return {"error": "Debe enviar un JSON con los parámetros requeridos."}, 400

    # Validación de parámetros requeridos
    query = request_json.get("query")
    fecha = request_json.get("fecha")  # Formato esperado: DD-MM-YYYY
    archivo = request_json.get("archivo")  # Ejemplo: DIM_PRODUCT.parquet
    periodicidad = request_json.get("periodicidad")  # "diaria" o "mensual"

    if not query or not fecha or not archivo or not periodicidad:
        return {"error": "Faltan parámetros obligatorios (query, fecha, archivo, periodicidad)."}, 400

    try:
        # Convertir la fecha según la periodicidad
        fecha_transformada = transformar_fecha(fecha, periodicidad)
        
        # Generar JSON, obtener directorio y nombre de archivo JSON
        json_generado, directorio_gcs, json_file_path = generar_json(query, fecha_transformada, archivo)
        
        # Guardar JSON en GCS con el nombre correcto
        subir_a_gcs(BUCKET_NAME, json_file_path, json_generado)

        return {"message": f"JSON generado y guardado en gs://{BUCKET_NAME}/{json_file_path}, carpeta creada.", "json": json_generado}, 200

    except ValueError as e:
        return {"error": str(e)}, 400
