#!/usr/bin/bash
source cicd/common.sh

CORES=$(nproc)
CTEST_BIN=${CTEST_BIN_PATH}
TIME_OUT=3000

exclude_prefixes=(
    "ndb"
    "Ndb"
    "router"
    "gcs"
    "group_replication"
)

regex="("
for prefix in "${exclude_prefixes[@]}"; do
    regex+="$prefix|"
done
regex=${regex%|}
regex+=").*"

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${MANUAL}" ] ||
    [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ]; then
    cd "${CICD_BUILD_ROOT}" &&
        ${CTEST_BIN} --progress --parallel "${CORES}" --output-on-failure \
            --timeout "${TIME_OUT}" -E "${regex}" --output-junit \
            "${RESULT_PATH}"/unittest_result.xml
fi
