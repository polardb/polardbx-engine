#!/usr/bin/env bash

get_key_value()
{
  echo "$1" | sed 's/^--[a-zA-Z_-]*=//'
}

usage()
{
cat <<EOF
Usage: $0 [-t normal|all|ps|push|daily|weekly|release]
       Or
       $0 [-h | --help]
  -t                      regresstion test type, valid options are:
                          normal, don't run big testcases (default)
                          all, run both normal and big testcases
                          ps, run all testcases under ps
                          push, run default.push
                          daily, run default.daily
                          weekly, run default.weekly
                          release, run default.release

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

# Basic options
OPT="perl mysql-test-run.pl --report-unstable-tests --sanitize --timer --force --max-test-fail=0 --retry=2 "
OPT="$OPT --skip-ndb --skip-rpl  --skip-combinations --nounit-tests  --parallel=32 --suite-timeout=600 --start-timeout=600 "
OPT="$OPT --report-features --unit-tests-report"

extra_mtr_option=""
if [ x"$test_type" = x"normal" ]; then
  extra_mtr_option=""
  $OPT $extra_mtr_option --vardir=var_normal  &>all.normal

elif [ x"$test_type" = x"all" ]; then
  extra_mtr_option="--big-test --testcase-timeout=45"
  $OPT $extra_mtr_option --vardir=var_all &>all.all

elif [ x"$test_type" = x"ps" ]; then
  extra_mtr_option="--big-test --testcase-timeout=45"
  $OPT $extra_mtr_option --vardir=var_ps --ps-protocol &>all.ps

elif [ x"$test_type" = x"push" ] || [ x"$test_type" = x"daily" ] || [ x"$test_type" = x"weekly" ] || [ x"$test_type" = x"release" ]; then
  export PB2WORKDIR=`pwd`
  export PRODUCT_ID=el7-x86-64bit
  export MTR_PARALLEL=32
  ./collections/default.$test_type &>all.$test_type

else
  echo "Invalid test type, it must be \"normal\" or \"ps\" or \"all\" or \"push\" or \"daily\" or \"weekly\" or \"release\"."
  exit 1
fi
