pipeline {

    agent {
            label 'memphis-jenkins-small-fleet-agent'
    }

    stages {
        stage('Install twine') {
            steps {
                script {
                    def branchName = env.BRANCH_NAME ?: ''
                    // Check if the branch is 'latest'
                    if (branchName == 'master') {
                        // Read version from version-beta.conf
                        def version = readFile('version-beta.conf').trim()
                        // Set the VERSION environment variable to the version from the file
                        env.versionTag = version
                        echo "Using version from version-beta.conf: ${env.versionTag}"
                    } else {
                        def version = readFile('version.conf').trim()
                        env.versionTag = version
                        echo "Using version from version.conf: ${env.versionTag}"                        
                    }
                }            
                sh """
                    pip3 install twine
                    python3 -m pip install urllib3==1.26.6
                """
            }
        }
        stage("Deploy to pypi") {
            steps {
                script {
                    if (env.BRANCH_NAME == 'master') {
                        sh """
                        sed -i -r "s/superstream-py/superstream-py-beta/g" pyproject.toml
                    """
                    }
                sh "sed -i -r 's/version = \"[0-9]+\\.[0-9]+\\.[0-9]+\"/version = \"$versionTag\"/g' pyproject.toml"
                sh "cat pyproject.toml" 
                // Install build dependencies
                sh 'pip install build'
                // Build your SDK
                sh 'python -m build'                                      
                }
                withCredentials([usernamePassword(credentialsId: 'python_sdk', usernameVariable: 'USR', passwordVariable: 'PSW')]) {
                    sh '~/.local/bin/twine upload -u $USR -p $PSW dist/*'
                }                
            }
        }
      stage('Checkout to version branch'){
            when {
                expression { env.BRANCH_NAME == 'latest' }
            }        
            steps {
                sh """
                   sudo dnf config-manager --add-repo https://cli.github.com/packages/rpm/gh-cli.repo -y
                   sudo dnf install gh -y
                """
                withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
                sh """
                   GIT_SSH_COMMAND='ssh -i $check' git checkout -b $versionTag
                   GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin $versionTag
                """
                }
                withCredentials([string(credentialsId: 'gh_token', variable: 'GH_TOKEN')]) {
                sh """
                   gh release create $versionTag --generate-notes
                """
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
