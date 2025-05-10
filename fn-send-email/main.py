import smtplib
import os
import json
import functions_framework
from email.message import EmailMessage
from google.cloud import secretmanager

def get_gmail_password():
    # Accede al secreto desde el proyecto remoto usando el ID
    client = secretmanager.SecretManagerServiceClient()
    name = "projects/182035274443/secrets/gmail-account-to-notify/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("UTF-8")

@functions_framework.http
def enviar_correo(request):
    request_json = request.get_json(silent=True)
    if not request_json or "to" not in request_json:
        return "Error: Se requiere una lista de destinatarios en el campo 'to'", 400

    to_list = request_json["to"]
    subject = request_json.get("subject", "Correo desde Cloud Function")
    body = request_json.get("body", "Este correo fue enviado desde una función en GCP.")

    gmail_user = "deinsolucionescl@gmail.com"  # Remplaza por tu correo real
    gmail_password = get_gmail_password()

    message = EmailMessage()
    message.set_content(body)
    message['Subject'] = subject
    message['From'] = gmail_user
    message['To'] = ", ".join(to_list)

    try:
        with smtplib.SMTP_SSL('smtp.gmail.com', 465) as smtp:
            smtp.login(gmail_user, gmail_password)
            smtp.send_message(message)
        return ("Correo enviado correctamente", 200)
    except Exception as e:
        return (f"Error al enviar correo: {str(e)}", 500)
