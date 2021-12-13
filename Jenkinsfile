pipeline {
  
  agent any 
  
  stages {
    
    stage("Compile") {
      steps {
        echo "Compiling..."
        sh "sbt compile"
      }
    }
    
    stage("Test") {
      steps {
        echo "Testing..."
        sh "sbt test"
      }
    }
    
    stage("Package") {
      steps {
        echo "Packaging..."
        sh "sbt assembly"
      }
    }

    stage("Deploy") {
          steps {
            echo "Deploying..."
            sh "ansible -i ~/ansible/hosts all -m ping"
          }
        }

  }  
    
}
