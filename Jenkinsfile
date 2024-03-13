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
                    if (branchName == 'test-pipeline') {
                        // Read version from version-beta.conf
                        def version = readFile('version-beta.conf').trim()
                        // Set the VERSION environment variable to the version from the file
                        env.versionTag = version
                        echo "Using version from version-beta.conf: ${env.VERSION}"
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
                    if (env.BRANCH_NAME == 'test-pipeline') {
                        sh """
                        sed -i -r "s/superstream/superstream-beta/g" setup.py
                    """
                    }
                sh """ 
                sed -i -r "s/version=\\"[0-9].[0-9].[0-9]/version=\\"$versionTag/g" pyproject.toml
                """
                sh "cat pyproject.toml" 
                // Install build dependencies
                sh 'pip install build'
                // Build your SDK
                sh 'python -m build'                                      
                }
            }
        }
    //   stage('Checkout to version branch'){
    //         when {
    //             expression { env.BRANCH_NAME == 'latest' }
    //         }        
    //         steps {
    //             withCredentials([sshUserPrivateKey(keyFileVariable:'check',credentialsId: 'main-github')]) {
    //             sh "git reset --hard origin/latest"
    //             sh "GIT_SSH_COMMAND='ssh -i $check'  git checkout -b $versionTag"
    //             sh "GIT_SSH_COMMAND='ssh -i $check' git push --set-upstream origin $versionTag"
    //             }
    //         }
    //   }        
    }

    post {
        always {
            cleanWs()
        }
        // success {
        //     notifySuccessful()
        // }
        // failure {
        //     notifyFailed()
        // }
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
