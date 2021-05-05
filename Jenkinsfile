
   
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
