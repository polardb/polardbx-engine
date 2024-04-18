#!/usr/bin/bash
source cicd/common.sh

PARALLEL=20
PERL_BIN=perl

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ]; then
  ${PERL_BIN} "${CICD_BUILD_ROOT}"/mysql-test/mysql-test-run.pl \
    --report-unstable-tests \
    --timer \
    --force \
    --max-test-fail=0 \
    --retry=2 \
    --skip-ndb \
    --skip-rpl \
    --skip-combinations \
    --nounit-tests \
    --parallel="${PARALLEL}" \
    --report-features \
    --unit-tests-report \
    --big-test \
    --testcase-timeout=90 \
    --suite-timeout=600 \
    --start-timeout=600 \
    --xml-report="${RESULT_PATH}"/mtr_result.xml \
    --port-base=10000
fi
