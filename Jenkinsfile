def jenkins_path = "/var/lib/jenkins/jobs/first_pipeline/builds/"

pipeline {
    agent any
    
    stages {
        
        stage('Build') {
            steps {
                sh """
        build_id='wget -qO- http://10.200.8.83:8080/job/first_pipeline/lastSuccessfulBuild/buildNumber'
        echo $build_id
        cd $jenkins_path/$build_id
        echo $ls
        """
            }
        }  
    }
    }
