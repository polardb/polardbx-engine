#!/usr/bin/bash
source cicd/common.sh

CORES=32
RETRY=2
PARALLEL=${CORES}
PERL_BIN=perl

#--sanitize \
if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ]; then
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
    -testcase-timeout=45 \
    --xml-report="${RESULT_PATH}"/mtr_result.xml
fi