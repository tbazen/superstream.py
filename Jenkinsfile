pipeline {

    agent {
            label 'memphis-jenkins-small-fleet-agent'
    }

    stages {
        stage('Install GoLang') {
            steps {
                script {
                    def branchName = env.BRANCH_NAME ?: ''
                    // Check if the branch is 'latest'
                    if (branchName == 'latest') {
                        // Read version from version-beta.conf
                        def version = readFile('version.conf').trim()
                        // Set the VERSION environment variable to the version from the file
                        env.versionTag = version
                        echo "Using version from version-beta.conf: ${env.VERSION}"
                    } else {
                        def version = readFile('version-beta.conf').trim()
                        env.versionTag = version
                        echo "Using version from version-beta.conf: ${env.versionTag}"                        
                    }
                }            
                sh 'wget -q https://go.dev/dl/go1.20.12.linux-amd64.tar.gz'
                sh 'sudo  tar -C /usr/local -xzf go1.20.12.linux-amd64.tar.gz'
            }
        }
        stage("Deploy Kafka.GO SDK") {
            steps {
                sh "git tag v$versionTag"
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                    sh "GIT_SSH_COMMAND='ssh -i $check' git push origin v$versionTag"
                }
                sh "GOPROXY=proxy.golang.org /usr/local/go/bin/go list -m github.com/memphisdev/superstream.go@v$versionTag"
                }
        }
      stage('Checkout to version branch'){
            when {
                expression { env.BRANCH_NAME == 'latest' }
            }        
            steps {
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                sh "git reset --hard origin/latest"
                sh "GIT_SSH_COMMAND='ssh -i $check'  git checkout -b $versionTag"
                sh "GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin $versionTag"
                }
            }
      }        
    }

    post {
        always {
            cleanWs()
        }
        success {
            notifySuccessful()
        }
        failure {
            notifyFailed()
        }
    }
}

def notifySuccessful() {
    emailext (
        subject: "SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """SUCCESSFUL: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output and connection attributes at ${env.BUILD_URL}""",
        recipientProviders: [requestor()]
    )
}
def notifyFailed() {
    emailext (
        subject: "FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]'",
        body: """FAILED: Job '${env.JOB_NAME} [${env.BUILD_NUMBER}]':
        Check console output at ${env.BUILD_URL}""",
        recipientProviders: [requestor()]
    )
}
