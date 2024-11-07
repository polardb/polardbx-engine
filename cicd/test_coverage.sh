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

if [ "${TEST_TYPE_ENUM}" -ne "${MERGE_TEST_COVERAGE}" ]; then
  exit 0
fi

if [ "${TEST_TYPE_ENUM}" -eq "${MERGE_TEST_COVERAGE}" ]; then
  echo "MERGE_ID: ${MERGE_ID}"
  git fetch origin ${TARGET_BRANCH}
  git fetch origin ${SOURCE_BRANCH}
  MERGE_COMMITS=$(git log origin/${TARGET_BRANCH}..origin/${SOURCE_BRANCH} --pretty=format:"%H")
  FIRST_COMMIT_ID=$(echo "${MERGE_COMMITS}" | tail -n 1)
  echo "FIRST_COMMIT_ID: ${FIRST_COMMIT_ID}"
  LAST_COMMID_ID=$(git log -n 1 --format=%H)
  LAST_COMMIT_TIME=$(git log -1 --format=%cd --date=format:'%Y-%m-%d %H:%M:%S')
  LAST_COMMIT_EMAIL=$(git log -1 --pretty=format:'%ce')

  cp cicd/cc_gen.sh ${CICD_BUILD_ROOT}
  chmod +x ${CICD_BUILD_ROOT}/cc_gen.sh

  cd ${CICD_BUILD_ROOT}
  ./cc_gen.sh -c "${FIRST_COMMIT_ID}"

else 
# todo: filter unused files
  ./cc_gen.sh
fi

source ./cc_gen.sh

# PUSH RESULT
result=$(
  cat <<EOF
'{
  "emp_email": "${LAST_COMMIT_EMAIL}",
  "source_branch": "${SOURCE_BRANCH}",
  "target_branch": "${TARGET_BRANCH}",
  "cr_last_commit_id": "${LAST_COMMID_ID}",
  "commit_time": "${LAST_COMMIT_TIME}",
  "incremental_coverage_rate": "${CODE_COVERAGE_DELTA}",
  "git_code_repo": "git@gitlab.alibaba-inc.com:polardbx/polardbx-engine.git",
  "new_line_count": "${NEW_LINE_CNT}",
  "ut_coverage_new_line_count": "${COV_NEW_LINE_CNT}",
}'
EOF
)

echo "curl -X PUT -H \"Content-Type: application/json\" -d ${result} https://qingzhou.aliyun-inc.com:5199/restapi/aliyun/git_code_coverage"

max_retries=6
retry_interval=10

attempt=1
while true; do
  response=$(curl -s -o /dev/null -w "%{http_code}" -X PUT -H "Content-Type: application/json" -d "${result}" https://qingzhou.aliyun-inc.com:5199/restapi/aliyun/git_code_coverage)

  if [[ $response =~ ^2 ]]; then
    echo "put delta success."
    break
  else
    echo "put delta failed, response: $response"
  fi

  if [ $attempt -ge $max_retries ]; then
    echo "retry up to $max_retries times"
    exit 1
  fi

  echo "wait $retry_interval seconds to retry... (attempt: $attempt)"
  sleep $retry_interval

  ((attempt++))
done
