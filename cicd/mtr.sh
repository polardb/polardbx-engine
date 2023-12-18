#!/usr/bin/bash

WORKING_SPACE_PATH=$(pwd)
CORES=$(nproc)
RETRY=3
PARALLEL=${CORES}
PERL_BIN=perl

${PERL_BIN} ${CICD_BUILD_ROOT}/mysql-test/mysql-test-run.pl \
  --report-unstable-tests \
  --sanitize \
  --timer \
  --force \
  --max-test-fail=0 \
  --retry=${RETRY} \
  --skip-ndb \
  --skip-rpl \
  --skip-combinations \
  --nounit-tests \
  --parallel=${PARALLEL} \
  --report-features \
  --unit-tests-report \
  --big-test \
  --testcase-timeout=45 \
  --xml-report=${WORKING_SPACE_PATH}/${RESULT_PATH}/mtr_result.xml
