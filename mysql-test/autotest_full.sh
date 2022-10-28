#!/usr/bin/env bash

[ ! -f ./autotest_helper.sh ] && echo "Missing autotest_helper.sh" && exit 1
source ./autotest_helper.sh
init_mtr_cmd $@

# Test origin cases
# with thread pool and ps-protocol disabled
run_mtr "$CMD --mysqld=--thread-pool-enabled=0" var_normal all.normal_mtrresult

# with thread pool enabled
run_mtr "$CMD --mysqld=--thread-pool-enabled=1" var_tp all.tp_mtrresult

# with thread pool enabled on ps-protocol
run_mtr "$CMD --mysqld=--thread-pool-enabled=1 --ps-protocol" var_tp_ps all.tp_ps_mtrresult


# Test Xengine cases
CMD="$CMD --do-suite=xengine --mysqld=--xengine=1"
# --recovery-inconsistency-check doesn't support xengine
CMD="$CMD --mysqld=--recovery-inconsistency-check=off"

# with thread pool and ps-protocol disabled
run_mtr "$CMD --mysqld=--thread-pool-enabled=0" var_xengine xengine.mtrresult

# with ps-protocol
run_mtr "$CMD --mysqld=--thread-pool-enabled=0 --ps-protocol" var_xengine_ps xengine.ps_mtrresult

# with thread pool enabled
run_mtr "$CMD --mysqld=--thread-pool-enabled=1" var_xengine_tp xengine.tp_mtrresult

# with thread pool enabled on ps-protocol
run_mtr "$CMD --mysqld=--thread-pool-enabled=1 --ps-protocol" var_xengine_tp_ps xengine.tp_ps_mtrresult
