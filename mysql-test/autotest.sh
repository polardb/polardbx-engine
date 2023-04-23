#!/usr/bin/env bash

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t normal|big|all]
       Or
       $0 [-h | --help]
  -t                      regresstion test type, valid options are:
                          normal, don't run big testcases (default)
                          big, only run big testcases
                          all, run both normal and big testcases
                          tp, run all test cases with threadpool enabled

  -h, --help              Show this help message.

Note: this script is intended for internal use by MySQL developers.
EOF
}

parse_options()
{
  while test $# -gt 0
  do
    case "$1" in
    -t=*)
      test_type=`get_key_value "$1"`;;
    -t)
      shift
      test_type=`get_key_value "$1"`;;
    -h | --help)
      usage
      exit 0;;
    *)
      echo "Unknown option '$1'"
      exit 1;;
    esac
    shift
  done
}

test_type="normal"
parse_options "$@"

extra_mtr_option=""
if [ x"$test_type" = x"normal" ]; then
    extra_mtr_option=""
elif [ x"$test_type" = x"big" ]; then
    extra_mtr_option="--only-big-test --testcase-timeout=45"
elif [ x"$test_type" = x"all" ]; then
    extra_mtr_option="--big-test --testcase-timeout=45"
else
  echo "Invalid test type, it must be \"normal\", \"big\" or \"all\"."
  exit 1
fi

# Basic options
OPT="perl mysql-test-run.pl --report-unstable-tests --sanitize --timer --force"
OPT="$OPT --skip-ndb --parallel=32 --non-parallel-test"
OPT="$OPT --skip-test-list=collections/disabled-daily.list --nounit-tests"
OPT="$OPT $extra_mtr_option"

# Test without ps-protocol
RUN_MTR="$OPT --vardir=var_normal"
$RUN_MTR &>all.normal

# Test with ps-protocol
RUN_MTR="$OPT --vardir=var_normal_ps"
RUN_MTR="$RUN_MTR --ps-protocol"
$RUN_MTR &>all.normal_ps