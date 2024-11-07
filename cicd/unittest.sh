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
