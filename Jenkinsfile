pipeline {
    agent any

    parameters {
        string(name: 'FUNCTION_FOLDER', defaultValue: 'fn-dias-habiles', description: 'Nombre del folder que contiene la función a desplegar')
        string(name: 'ENTRY_POINT', defaultValue: 'verificar_dia_habil', description: 'Nombre de la función que será el entry point')
    }

    environment {
        PROJECT_ID = 'deinsoluciones-serverless'
        REGION     = 'us-east4'
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

        stage('Deploy Cloud Function: ${params.FUNCTION_FOLDER}') {
            steps {
                script {
                    sh """
                    echo 'Desplegando Cloud Function Gen2 desde código clonado...'
                    gcloud functions deploy ${params.FUNCTION_FOLDER} \
                        --region=${REGION} \
                        --project=${PROJECT_ID} \
                        --runtime=python310 \
                        --trigger-http \
                        --entry-point=${params.ENTRY_POINT} \
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
