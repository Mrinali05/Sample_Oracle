def jenkins_path = '/var/lib/jenkins'
   
    pipeline {
    agent any
    
    stages {
        
        stage('Build') {
            steps {
                sh """
        build_id=`wget -qO- $jenkins_path/job/job_name/lastSuccessfulBuild/buildNumber`
        echo $build_id
        """
            }
        }  
    }
