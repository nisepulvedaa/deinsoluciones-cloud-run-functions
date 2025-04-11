pipeline {
    agent any

    parameters {
        string(name: 'FUNCTION_FOLDER', defaultValue: 'fn-dias-habiles', description: 'Nombre del folder que contiene la función a desplegar')
    }

    environment {
        PROJECT_ID = 'deinsoluciones-serverless'
        REGION     = 'us-east4'
        BUCKET     = 'dev-deinsoluciones-run-sources'
        GIT_REPO   = 'git@github.com:nisepulvedaa/deinsoluciones-cloud-run-functions.git'
        SERVICE_ACCOUNT_EMAIL = '77134593518-compute@developer.gserviceaccount.com'
        GOOGLE_APPLICATION_CREDENTIALS = credentials('gcp-key')
    }

    stages {

        stage('Auth GCP') {
            steps {
                script {
                    sh """
                    echo 'Autenticando en GCP...'
                    gcloud auth activate-service-account jenkins-ci-cd-core@deinsoluciones-devops-ci-core.iam.gserviceaccount.com --key-file=$GOOGLE_APPLICATION_CREDENTIALS
                    """
                }
            }
        }

        stage('Checkout') {
            steps {
                git branch: 'main', credentialsId: 'github-ssh', url: "${env.GIT_REPO}"
            }
        }

        stage('Deploy Cloud Run Function: ${params.FUNCTION_FOLDER}') {
            steps {
                script {
                    sh """
                    echo 'Creando carpeta services/ en GCS (si no existe)...'
                    echo '' | gsutil cp - gs://${BUCKET}/services/.keep

                    echo 'Empaquetando ${params.FUNCTION_FOLDER}...'
                    zip -r ${params.FUNCTION_FOLDER}.zip ${params.FUNCTION_FOLDER}

                    echo 'Subiendo paquete a GCS...'
                    gsutil cp ${params.FUNCTION_FOLDER}.zip gs://${BUCKET}/services/${params.FUNCTION_FOLDER}.zip

                    echo 'Desplegando funcion ${params.FUNCTION_FOLDER}...'
                    gcloud run deploy ${params.FUNCTION_FOLDER} \
                        --source=gs://${BUCKET}/services/${params.FUNCTION_FOLDER}.zip \
                        --region=${REGION} \
                        --project=${PROJECT_ID} \
                        --service-account=${SERVICE_ACCOUNT_EMAIL} \
                        --platform=managed \
                        --allow-unauthenticated
                    """
                }
            }
        }
    }

    post {
        success {
            echo "¡Deploy de ${params.FUNCTION_FOLDER} exitoso!"
        }
        failure {
            echo "Falló el deploy de ${params.FUNCTION_FOLDER}. Revisar logs!"
        }
    }
}
