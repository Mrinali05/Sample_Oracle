def jenkins_path = '/var/lib/jenkins'
   
    pipeline {
    agent any
    
    stages {
        
        stage('Build') {
            steps {
                sh """
        build_id = build.getProject().getLastSuccessfulBuild()
        echo $build_id
        """
            }
        }  
    }
    }
