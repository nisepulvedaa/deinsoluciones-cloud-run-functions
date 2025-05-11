import os
import json
import functions_framework
from google.cloud import storage

@functions_framework.http
def mover_archivo_gcs(request):
    """Cloud Function HTTP que mueve un archivo entre rutas en GCS"""
    request_json = request.get_json(silent=True)

    # Validar parámetros obligatorios
    for key in ["bucket_origen", "path_origen", "bucket_destino", "path_destino", "nombre_archivo"]:
        if key not in request_json:
            return (f"Falta parámetro requerido: {key}", 400)

    bucket_origen = request_json["bucket_origen"]
    path_origen = request_json["path_origen"].rstrip("/")  # elimina trailing slash si lo hay
    bucket_destino = request_json["bucket_destino"]
    path_destino = request_json["path_destino"].rstrip("/")
    nombre_archivo = request_json["nombre_archivo"]

    full_path_origen = f"{path_origen}/{nombre_archivo}"
    full_path_destino = f"{path_destino}/{nombre_archivo}"

    try:
        client = storage.Client()

        # Obtener blobs
        bucket_src = client.bucket(bucket_origen)
        blob_src = bucket_src.blob(full_path_origen)

        bucket_dst = client.bucket(bucket_destino)
        blob_dst = bucket_dst.blob(full_path_destino)

        # Copiar y borrar
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
