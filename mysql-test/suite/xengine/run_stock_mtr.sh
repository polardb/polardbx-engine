#!/bin/sh
# by beilou.wjy@alibaba-inc.com 2019.6.13



CURRENT_DIR="`pwd`"

if [ ! -d ${CURRENT_DIR}/mysql-test -o ! -f ${CURRENT_DIR}/build.sh ]; then
  echo "You should run this script in the root of source file"
fi

P=${1:-48}
MTR_TEST_DIR=${CURRENT_DIR}/mysql-test
MAX_FAIL=1000
SUITE_LIST=""

echo "We will run MTR cases in ${MTR_TEST_DIR}"

cd ${MTR_TEST_DIR}
#
RESULT=${MTR_TEST_DIR}/TOTAL_RESULT
./mysql-test-run.pl --mysqld=--xengine=1 \
                    --parallel=${P} \
                    --sanitize \
                    --force \
                    --skip-test-list=suite/xengine/disabled_stock.def \
                    --max-test-fail=${MAX_FAIL} > $RESULT 2>&1

# backup runtime environment for tracing
/bin/rm -rf stock_var_back
mv var stock_var_back

cat $RESULT|grep "Completed:"
echo "Test result in ${RESULT}"

FAIL_CASES=$(grep "Failing test(s)" ${RESULT} | sed  s'/Failing test(s)://g')
if [[ ! -z "${FAIL_CASES}" ]]; then
    echo "Following are failed cases, now run them one by one"
    echo "======================================================================"
    echo ${FAIL_CASES}
    echo "======================================================================"
    LOG_ONE=${MTR_TEST_DIR}/RUN_ONE_RESULT
    for c in ${FAIL_CASES}; do
        ./mysql-test-run.pl --mysqld=--xengine=1 --sanitize --force --quiet --retry=0 ${c} >> ${LOG_ONE}
    done
    FAIL_CASES=$(grep "Failing test(s)" ${LOG_ONE} | sed  s'/Failing test(s): //g')
    if [[ -f ${LOG_ONE} ]]; then
      echo -e "\n\nLog for running failed cases one by one" >> ${RESULT}
      cat ${LOG_ONE} >> ${RESULT}
      /bin/rm -f ${LOG_ONE}
    fi
    if [[ ! -z "${FAIL_CASES}" ]]; then
        echo "Following cases are failed again:"
        echo "=================================================================="
        echo ${FAIL_CASES}
        echo "=================================================================="
        echo "${FAIL_CASES}" > ${MTR_TEST_DIR}/FAIL_CASES
    fi
fi

