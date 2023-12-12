#!/usr/bin/bash

CORES=$(nproc)
WORKING_SPACE_PATH=$(pwd)
CMAKE_BIN=${CMAKE_BIN_PATH}
CTEST_BIN=${CTEST_BIN_PATH}

cd ${CICD_BUILD_ROOT} && \
${CTEST_BIN} --progress --parallel ${CORES} --output-on-failure \
  --timeout 300 --output-junit \
  ${WORKING_SPACE_PATH}/${RESULT_PATH}/test-unit.xml
