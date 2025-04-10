pipeline {
    agent any

    environment {
        PROJECT_ID = 'deinsoluciones-serverless'
        REGION     = 'us-east4'
        BUCKET     = 'dev-deinsoluciones-run-sources'
        GIT_REPO   = 'git@github.com:nisepulvedaa/deinsoluciones-cloud-run-functions.git'
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

        stage('Deploy Cloud Run Functions') {
            steps {
                script {
                    sh """
                    echo 'Desplegando funciones Cloud Run...'

                    for dir in fn-*; do
                        if [ -d "\$dir" ]; then
                            echo "Empaquetando y subiendo \$dir a GCS..."
                            gsutil -m cp -r \$dir gs://${BUCKET}/services/

                            echo "Desplegando funcion \$dir..."
                            gcloud run deploy \$dir \
                                --source=gs://${BUCKET}/services/\$dir \
                                --region=${REGION} \
                                --project=${PROJECT_ID} \
                                --platform=managed \
                                --allow-unauthenticated
                        fi
                    done
                    """
                }
            }
        }
    }

    post {
        success {
            echo "¡Deploy de funciones Cloud Run exitoso!"
        }
        failure {
            echo "Falló el deploy de las funciones Cloud Run. Revisar logs!"
        }
    }
}
