pipeline {
    agent any

    stages {
        stage('Build') {
            steps {
                sh 'cd geo_wrapper;sbt build'
            }
        }
        stage('Test') {
            steps {
                echo 'Testing..'
            }
        }
        stage('Deploy') {
            steps {
                echo 'Deploying....'
            }
        }
    }
}