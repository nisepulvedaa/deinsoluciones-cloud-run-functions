import smtplib
import os
import json
import functions_framework
from email.message import EmailMessage
from google.cloud import secretmanager
from google.cloud import bigquery

def get_gmail_password():
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/182035274443/secrets/gmail-account-to-notify/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

def obtener_parametros_email(process_name):
    client = bigquery.Client()
    query = f"""
        SELECT params
        FROM `dev_config_zone.process_email`
        WHERE process_name = @process_name AND estatus = 1
    """
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("process_name", "STRING", process_name)
        ]
    )

    query_job = client.query(query, job_config=job_config)
    results = list(query_job.result())

    if not results:
        raise ValueError("No se encontraron parámetros para el proceso indicado.")

    # Parsear el campo JSON
    return json.loads(results[0].params)[0]

@functions_framework.http
def enviar_correo(request):
    request_json = request.get_json(silent=True)

    if not request_json or "process_name" not in request_json or "estado" not in request_json:
        return ("Error: Se requieren los campos 'process_name' y 'estado' ('OK' o 'ERROR')", 400)

    process_name = request_json["process_name"]
    estado = request_json["estado"].upper()

    try:
        params = obtener_parametros_email(process_name)

        to_list = [params.get("email_to_0")]
        if estado == "OK":
            subject = params.get("email_subj_ok")
            body = params.get("email_body_ok")
        else:
            subject = params.get("email_subj_err")
            body = params.get("email_body_err")

        gmail_user = "deinsolucionescl@gmail.com"
        gmail_password = get_gmail_password()

        message = EmailMessage()
        message.set_content(body)
        message['Subject'] = subject
        message['From'] = gmail_user
        message['To'] = ", ".join(to_list)

        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(gmail_user, gmail_password)
            smtp.send_message(message)

        return (f"Correo enviado correctamente a {to_list[0]}", 200)

    except Exception as e:
        return (f"Error al enviar correo: {str(e)}", 500)
