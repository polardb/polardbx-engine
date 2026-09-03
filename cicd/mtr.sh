#!/usr/bin/bash

# Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.
# 
# This program is free software; you can redistribute it and/or modify it under
# the terms of the GNU General Public License, version 2.0, as published by the
# Free Software Foundation.
# 
# This program is also distributed with certain software (including but not
# limited to OpenSSL) that is licensed under separate terms, as designated in a
# particular file or component or in included license documentation. The authors
# of MySQL hereby grant you an additional permission to link the program and
# your derivative works with the separately licensed software that they have
# included with MySQL.
# 
# This program is distributed in the hope that it will be useful, but WITHOUT
# ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
# FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
# for more details.
# 
# You should have received a copy of the GNU General Public License along with
# this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

source cicd/common.sh

PARALLEL=48
PERL_BIN=perl
return_code=0

# 解析命令行参数
if [ "$#" -gt 0 ]; then
  mtr_case="$*"
fi

# build.sh 在 DAILY_REGRESSION / MANUAL / MANUAL_ALL 时编译带 -DWITH_ASAN=1，
# MTR 需要 --sanitize 才能设置 $ASAN_OPTIONS，使测试脚本的 ASAN 超时适配生效。
SANITIZE_FLAGS=""
if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ]; then
  SANITIZE_FLAGS="--sanitize"
fi

if [ "${TEST_TYPE_ENUM}" -eq "${DAILY_REGRESSION}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_ALL}" ] ||
  [ "${TEST_TYPE_ENUM}" -eq "${MANUAL_COVERAGE}" ]; then
  set -x
  ${PERL_BIN} "${CICD_BUILD_ROOT}"/mysql-test/mysql-test-run.pl \
    --report-unstable-tests \
    --timer \
    --force \
    --max-test-fail=0 \
    --retry=2 \
    --skip-ndb \
    --skip-combinations \
    --nounit-tests \
    --parallel="${PARALLEL}" \
    --report-features \
    --unit-tests-report \
    --big-test \
    --testcase-timeout=1200 \
    --suite-timeout=4800 \
    --start-timeout=4800 \
    ${SANITIZE_FLAGS} \
    --xml-report="${RESULT_PATH}"/mtr_result.xml \
    --port-base=10000
  return_code=$?
fi
exit ${return_code}
