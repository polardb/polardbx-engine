pipeline {
  agent {
    docker {
      image 'reg.docker.alibaba-inc.com/polardb_x/mysql_dev:1.0-SNAPSHOT'
      args '-v /home/xiedao.yy/Software:/opt/Software \
            --cap-add=SYS_PTRACE \
            --security-opt seccomp=unconfined \
            --privileged'
    }
  }

  environment {
    CMAKE_BIN_PATH = '/opt/Software/cmake-3.28.0-linux-x86_64/bin/cmake'
    CTEST_BIN_PATH = '/opt/Software/cmake-3.28.0-linux-x86_64/bin/ctest'

    RELATIVE_CICD_BUILD_ROOT = 'build'
    CICD_BUILD_ROOT = "${env.WORKSPACE}/${RELATIVE_CICD_BUILD_ROOT}"
    BOOST_DIRECTORY = "${env.WORKSPACE}/extra/boost"
    BOOST_PATH = "${BOOST_DIRECTORY}/boost_1_77_0.tar.bz2"
    RESULT_PATH = "${CICD_BUILD_ROOT}/result"
    // CCACHE_DIR MUST USE ABSOLUTE PATH
    CCACHE_DIR = "${env.WORKSPACE}/${CICD_BUILD_ROOT}/.cache/ccache"

    CMAKE_C_FLAGS = ''
    CMAKE_CXX_FLAGS = ''

    DINGTALK_BOT_ID = '44281ff8-2953-4369-97f8-137e09cbc486'
  }

  stages {
    stage('Build') {
      steps {
        sh 'cicd/build.sh'
      }
    }

    stage('Test') {
      steps {
        sh 'cicd/unittest.sh'
        sh 'cicd/mtr.sh'
      }
    }

    stage('TestCoverage') {
      steps {
        sh 'cicd/test_coverage.sh'
      }
    }
  }

  post {
    cleanup {
      script {
        if (env.TEST_TYPE == 'DAILY_REGRESSION') {
          PostJunitResult()
        }
      }
      archiveArtifacts artifacts: "${RELATIVE_CICD_BUILD_ROOT}/result/**", allowEmptyArchive: true
    }
  }
}

def PostJunitResult() {
  def summary = junit allowEmptyResults: true, testResults: "${RELATIVE_CICD_BUILD_ROOT}/result/*.xml"
  dingtalk(
    robot: "${DINGTALK_BOT_ID}",
    type: 'MARKDOWN',
    title: '[PolarDB-X] DN 8032 Daily Regression',
    text: [
      "# [${env.JOB_NAME}](${env.JOB_URL})",
      '---',
      "- 任务: [${env.BUILD_DISPLAY_NAME}](${env.BUILD_URL})",
      "- 状态: ${currentBuild.currentResult}",
      "- 测试数量: ${summary.totalCount}",
      "- 失败数量: ${summary.failCount}",
      "- 跳过数量: ${summary.skipCount}",
      "- 成功数量: ${summary.passCount}",
      "- 持续时间: ${currentBuild.durationString}",
    ]
  )
}
