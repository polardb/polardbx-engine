#!/usr/bin/bash
source cicd/common.sh

CORES=$(nproc)
CTEST_BIN=${CTEST_BIN_PATH}

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ]; then
  cd "${CICD_BUILD_ROOT}" && \
  ${CTEST_BIN} --progress --parallel "${CORES}" --output-on-failure \
    --timeout 300 --output-junit \
    "${RESULT_PATH}"/unittest_result.xml
fi
