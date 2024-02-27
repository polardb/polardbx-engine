#!/usr/bin/bash
source cicd/common.sh

CORES=$(nproc)
RETRY=2
PARALLEL=$((CORES / 4))
PERL_BIN=perl
TIME_OUT=90

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ]; then
  ${PERL_BIN} "${CICD_BUILD_ROOT}"/mysql-test/mysql-test-run.pl \
    --report-unstable-tests \
    --timer \
    --force \
    --max-test-fail=100 \
    --retry="${RETRY}" \
    --skip-ndb \
    --skip-rpl \
    --skip-combinations \
    --nounit-tests \
    --parallel="${PARALLEL}" \
    --report-features \
    --unit-tests-report \
    --big-test \
    --testcase-timeout="${TIME_OUT}" \
    --xml-report="${RESULT_PATH}"/mtr_result.xml \
    --port-base=10000
fi
