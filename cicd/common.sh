#!/bin/bash

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
