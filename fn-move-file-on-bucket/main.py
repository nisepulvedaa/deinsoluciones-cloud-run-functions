import os
import json
from datetime import datetime
import functions_framework
from google.cloud import storage
import pandas as pd
from io import BytesIO

@functions_framework.http
def mover_archivo_gcs(request):
    """Cloud Function HTTP que mueve un archivo entre rutas en GCS, renombrándolo según la periodicidad."""

    request_json = request.get_json(silent=True)

    # Validar parámetros obligatorios
    for key in ["bucket_origen", "path_origen", "bucket_destino", "path_destino", "nombre_archivo", "periodicidad"]:
        if key not in request_json:
            return (f"Falta parámetro requerido: {key}", 400)

    bucket_origen = request_json["bucket_origen"]
    path_origen = request_json["path_origen"].rstrip("/")
    bucket_destino = request_json["bucket_destino"]
    path_destino = request_json["path_destino"].rstrip("/")
    nombre_archivo = request_json["nombre_archivo"]
    periodicidad = request_json["periodicidad"].lower()

    # Inicializamos el cliente de GCS
    client = storage.Client()
    bucket_src = client.bucket(bucket_origen)
    full_path_origen = f"{path_origen}/{nombre_archivo}"
    blob_src = bucket_src.blob(full_path_origen)

    # Obtener nombre base y extensión
    nombre_base, extension = os.path.splitext(nombre_archivo)

    try:
        # Determinar la fecha según periodicidad
        if periodicidad == "espóradica":
            fecha_str = datetime.now().strftime("%Y-%m-%d")

        elif periodicidad == "diaria":
            parquet_data = blob_src.download_as_bytes()
            df = pd.read_parquet(BytesIO(parquet_data))
            if "periodo_dia" in df.columns:
                fecha_valor = df["periodo_dia"].unique()[0]
                fecha_str = fecha_valor if isinstance(fecha_valor, str) else fecha_valor.strftime("%Y-%m-%d")
            else:
                return ("El archivo no contiene el campo 'periodo_dia'", 400)

        elif periodicidad == "mensual":
            parquet_data = blob_src.download_as_bytes()
            df = pd.read_parquet(BytesIO(parquet_data))
            if "periodo_mes" in df.columns:
                fecha_valor = df["periodo_mes"].unique()[0]
                fecha_str = fecha_valor if isinstance(fecha_valor, str) else fecha_valor.strftime("%Y-%m-%d")
            else:
                return ("El archivo no contiene el campo 'periodo_mes'", 400)

        else:
            return ("Valor de 'periodicidad' no válido. Use: esporádica, diaria o mensual", 400)

        # Construir nuevo nombre y rutas
        nuevo_nombre = f"{nombre_base}_{fecha_str}{extension}"
        full_path_destino = f"{path_destino}/{nuevo_nombre}"

        # Copiar y eliminar
        bucket_dst = client.bucket(bucket_destino)
        bucket_src.copy_blob(blob_src, bucket_dst, full_path_destino)
        blob_src.delete()

        return (
            json.dumps({
                "status": "OK",
                "mensaje": f"Archivo '{nombre_archivo}' movido correctamente",
                "nuevo_path": f"gs://{bucket_destino}/{full_path_destino}"
            }),
            200,
            {"Content-Type": "application/json"}
        )

    except Exception as e:
        return (f"Error al mover archivo: {str(e)}", 500)
