import json
import functions_framework
from google.cloud import storage

# Nombre del bucket y archivo JSON
BUCKET_NAME = "dev-deinsoluciones-global-config"
JSON_FILE = "calendarios/2025/dias_habiles.json"

def cargar_dias_habiles():
    """Carga el JSON desde Cloud Storage y lo convierte en un diccionario."""
    client = storage.Client()
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(JSON_FILE)

    try:
        contenido = blob.download_as_text()
        return json.loads(contenido)
    except Exception as e:
        print(f"Error al cargar el archivo JSON: {e}")
        return None

@functions_framework.http
def verificar_dia_habil(request):
    """Función que recibe una fecha y retorna si es hábil o no."""
    dias_habiles = cargar_dias_habiles()

    if dias_habiles is None:
        return {"error": "No se pudo cargar la lista de días hábiles"}, 500

    request_json = request.get_json(silent=True)
    
    if not request_json or "fecha" not in request_json:
        return {"error": "Debe enviar una fecha en formato DD-MM-YYYY"}, 400
    
    fecha = request_json["fecha"]

    # Buscar la fecha en la lista cargada
    resultado = next((d for d in dias_habiles if d["DIA"] == fecha), None)

    if resultado:
        return {"fecha": fecha, "habil": resultado["HABIL"]}, 200
    else:
        return {"error": "Fecha no encontrada en la lista de días hábiles"}, 404