pipeline {
  agent {
    docker {
      image 'hub.docker.alibaba-inc.com/aone-base-global/polardbx_engine:latest'
      args '-v /home/xiedao.yy/Software:/opt/Software \
            --cap-add=SYS_PTRACE \
            --security-opt seccomp=unconfined \
            --privileged'
    }
  }

  environment {
    CMAKE_BIN_PATH = '/opt/Software/cmake-3.28.0-linux-x86_64/bin/cmake'
    CTEST_BIN_PATH = '/opt/Software/cmake-3.28.0-linux-x86_64/bin/ctest'

    // relative path of project_root
    CICD_BUILD_ROOT = 'build'
    BOOST_PATH = 'extra/boost/boost_1_77_0.tar.bz2' 
    RESULT_PATH = "${CICD_BUILD_ROOT}/result"
    // CCACHE_DIR MUST USE ABSOLUTE PATH
    CCACHE_DIR = "${env.WORKSPACE}/${CICD_BUILD_ROOT}/.cache/ccache"

    CMAKE_C_FLAGS = ""
    CMAKE_CXX_FLAGS = ""
  }
  
  stages {
    stage('Configure') {
      steps {
        sh 'cicd/configure.sh'
      }
    }

    stage('Build') {
      steps {
        sh 'cicd/build.sh'
      }
    }

    stage('Test') {
      steps {
        sh 'cicd/unittest.sh || true'
        sh 'cicd/mtr.sh || true'
      }
    }
  }

  post {
    always {
      junit "${RESULT_PATH}/*.xml"
    }
  }
}