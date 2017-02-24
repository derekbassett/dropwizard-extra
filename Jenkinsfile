podTemplate(label: 'mypod', containers: [
    containerTemplate(name: 'maven', image: 'maven:3.3.9-jdk-8-alpine', ttyEnabled: true, command: 'cat'),
  ]) {

    node ('mypod') {
        stage('Get a Maven project') {
            checkout scm
        }
        stage('Build') {
            container('maven') {
                stage 'Build a Maven project'
                sh 'mvn clean package'
            }
        }
        stage('Results') {
            junit '**/target/surefire-reports/TEST-*.xml'
            archive 'target/*.jar'
        }
        stage('Publish') {
            echo 'I DO NOT WORK YET!'
        }
    }
}