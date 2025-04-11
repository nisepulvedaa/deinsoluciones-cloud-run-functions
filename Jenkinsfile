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
        ENTRY_POINT = 'verificar_dia_habil'  // Cambiar este valor si el entry-point es otro
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

        stage('Deploy Cloud Function: ${params.FUNCTION_FOLDER}') {
            steps {
                script {
                    sh """
                    echo 'Subiendo fuente a GCS por buenas prácticas...'
                    echo '' | gsutil cp - gs://${BUCKET}/services/.keep
                    gsutil -m cp -r ${params.FUNCTION_FOLDER} gs://${BUCKET}/services/

                    echo 'Desplegando como Cloud Function Gen2 con IAM Authentication...'
                    gcloud functions deploy ${params.FUNCTION_FOLDER} \
                        --region=${REGION} \
                        --project=${PROJECT_ID} \
                        --runtime=python310 \
                        --trigger-http \
                        --entry-point=${ENTRY_POINT} \
                        --service-account=${SERVICE_ACCOUNT_EMAIL} \
                        --no-allow-unauthenticated \
                        --source=${params.FUNCTION_FOLDER}
                    """
                }
            }
        }
    }

    post {
        success {
            echo "¡Deploy de ${params.FUNCTION_FOLDER} exitoso como Cloud Function Gen2!"
        }
        failure {
            echo "Falló el deploy de ${params.FUNCTION_FOLDER}. Revisar logs!"
        }
    }
}
