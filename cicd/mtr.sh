#!/usr/bin/bash
source cicd/common.sh

CORES=$(nproc)
RETRY=2
PARALLEL=${CORES}
PERL_BIN=perl

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ]; then
  ${PERL_BIN} "${CICD_BUILD_ROOT}"/mysql-test/mysql-test-run.pl \
    --report-unstable-tests \
    --sanitize \
    --timer \
    --force \
    --max-test-fail=0 \
    --retry="${RETRY}" \
    --skip-ndb \
    --skip-rpl \
    --skip-combinations \
    --nounit-tests \
    --parallel="${PARALLEL}" \
    --report-features \
    --unit-tests-report \
    --big-test \
    -testcase-timeout=45 \
    --xml-report="${RESULT_PATH}"/mtr_result.xml
fi