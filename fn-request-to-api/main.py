import functions_framework
import requests
import json
import pandas as pd
from google.cloud import storage
from datetime import datetime
import os

# Configuración del bucket en GCS
BUCKET_NAME = "dev-deinsoluciones-ingestas"

@functions_framework.http
def fetch_and_store_mindicador(request):
    try:
        request_json = request.get_json()

        # Validamos que los parámetros existan
        fecha = request_json.get("fecha")  # Formato esperado: YYYY-MM-DD
        path_name = request_json.get("path_name")  # Nombre en el bucket
        indicador = request_json.get("indicador")  # Ejemplo: "uf", "dolar"
        filename = request_json.get("filename")  # Nombre personalizado del archivo, ej: "VALOR_UF.parquet"

        if not fecha or not path_name or not indicador or not filename:
            return {"error": "Faltan parámetros obligatorios: 'fecha', 'path_name', 'indicador' o 'filename'"}, 400

        # Convertimos la fecha de YYYY-MM-DD a DD-MM-YYYY
        try:
            fecha_dt = datetime.strptime(fecha, "%Y-%m-%d")
            fecha_para_api = fecha_dt.strftime("%d-%m-%Y")  # Para la API
        except ValueError:
            return {"error": "Formato de fecha inválido. Usa YYYY-MM-DD"}, 400

        # Construimos la URL de la API
        url = f"https://mindicador.cl/api/{indicador}/{fecha_para_api}"
        response = requests.get(url)

        if response.status_code != 200:
            return {"error": f"Error en la API: {response.status_code}, {response.text}"}, 500

        data = response.json()

        # Verificamos que la API devuelva datos
        if "serie" not in data or not data["serie"]:
            return {"error": f"No hay datos disponibles para {indicador} en {fecha_para_api}"}, 404

        # Convertimos los datos en un DataFrame de Pandas
        df = pd.DataFrame(data["serie"])

        if df.empty:
            return {"error": f"La API devolvió una serie vacía para {indicador}"}, 404

        # **Usar el nombre de archivo proporcionado**
        filename = filename if filename.endswith(".parquet") else f"{filename}.parquet"

        # **Definir la ruta en el bucket**
        carpeta = f"files/{path_name}/periodo_dia={fecha}/"
        destination_blob_name = carpeta + filename

        # **Guardar el archivo como Parquet localmente**
        temp_filename = f"/tmp/{filename}"
        df.to_parquet(temp_filename, engine="pyarrow")

        # **Subir a Cloud Storage**
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(temp_filename, content_type="application/octet-stream")

        # **Eliminar archivo local después de la subida**
        os.remove(temp_filename)

        return {
            "message": "Archivo guardado con éxito",
            "bucket": BUCKET_NAME,
            "file_path": f"gs://{BUCKET_NAME}/{destination_blob_name}"
        }, 200

    except Exception as e:
        return {"error": f"Error inesperado: {str(e)}"}, 500
