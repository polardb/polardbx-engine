#!/bin/bash

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

readonly DAILY_REGRESSION=1
readonly MERGE_PRECHECK=2
readonly MERGE_TEST_COVERAGE=3
# skip mtr
readonly MANUAL=4
readonly MANUAL_ALL=5

GET_TEST_TYPE() {
  local res=0
  if [ "$1" = "DAILY_REGRESSION" ]; then
    res=$DAILY_REGRESSION
  elif [ "$1" = "MERGE_PRECHECK" ]; then
    res=$MERGE_PRECHECK
  elif [ "$1" = "MERGE_TEST_COVERAGE" ]; then
    res=$MERGE_TEST_COVERAGE
  elif [ "$1" = "MANUAL" ]; then
    res=$MANUAL
  elif [ "$1" = "MANUAL_ALL" ]; then
    res=$MANUAL_ALL
  else
    #use daily regression by default
    res=$DAILY_REGRESSION
  fi
  echo "$res"
}

export TEST_TYPE_ENUM=0
TEST_TYPE_ENUM=$(GET_TEST_TYPE "${TEST_TYPE}")

echo "TEST_TYPE_ENUM: $TEST_TYPE_ENUM"
