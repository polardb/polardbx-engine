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
    extra_mtr_option="--only-big-test --testcase-timeout=30"
elif [ x"$test_type" = x"all" ]; then
    extra_mtr_option="--big-test --testcase-timeout=30"
elif [ x"$test_type" = x"s1" ]; then
    extra_mtr_option="--suite=main,sys_vars,binlog,binlog_gtid,binlog_nogtid,service_sys_var_registration,service_status_var_registration,rpl_gtid"
elif [ x"$test_type" = x"s2" ]; then
    extra_mtr_option="--suite=rds,xengine,xengine_binlog,xengine_main,xengine_binlog_gtid,xengine_binlog_nogtid,xengine_sysvars,rpl_nogtid"
elif [ x"$test_type" = x"s3" ]; then
    extra_mtr_option="--suite=rpl,xengine_rpl,xengine_rpl_basic,xengine_rpl_gtid,xengine_rpl_nogtid"
else
  echo "Invalid test type, it must be \"normal\", \"big\" or \"all\"."
  exit 1
fi

para=`cat /proc/cpuinfo | grep processor| wc -l`
out_file=all.mtrresult.${test_type}
var_file=var_${test_type}

RUNMTR="./mysql-test-run.pl --report-unstable-tests --sanitize --timer --force --parallel=$para --comment=ps_row --ps-protocol --skip-ndb --skip-test-list=collections/disabled-daily.list --nounit-tests --max-connections=2048 --mysqld=--xengine=1 --mysqld=--recovery-inconsistency-check=off $extra_mtr_option"

# all test case
$RUNMTR 2>&1 | tee ${out_file}
rm -rf ${var_file}
mv var ${var_file}
