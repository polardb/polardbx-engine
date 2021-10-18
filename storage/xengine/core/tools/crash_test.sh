#!/bin/bash

# Copyright (c) 2020, Alibaba Group Holding Limited
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

# http://www.apache.org/licenses/LICENSE-2.0

# 3 Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

threads=80
update_threads=20
delete_threads=20
max_key_id=100000000
update_delete_count=1000000
duration=360000
dir2=/u01/rocksdb-test-1
resultfile=stress.txt

 args=" --sync=0
    --base_background_compactions=8
    --max_background_compactions=8
    --max_background_flushes=4
    --write_buffer_size=268435456
    --level0_slowdown_writes_trigger=64
    --level0_stop_writes_trigger=128
    --level_compaction_dynamic_level_bytes=true
    --target_file_size_base=134217728
    --max_bytes_for_level_base=1073741824
    --allow_concurrent_memtable_write=1
    --enable_write_thread_adaptive_yield=1
    --level0_file_num_compaction_trigger=2
    --transaction_db=1
    --num_column_families=5
    --cache_size=32212254720
    --num=$max_key_id
    --threads=$threads
    --num_deletion_threads=$delete_threads
    --num_update_threads=$update_threads
    --update_delete_count=$update_delete_count
    --duration=$duration"

prepare_cmd="./db_bench --benchmarks=stress_durability --use_existing_db=0 --db=$dir2 --wal_dir=$dir2"${args} 
update_cmd="./db_bench --benchmarks=stress_durability --use_existing_db=1 --db=$dir2 --wal_dir=$dir2"${args}
check_cmd="./db_bench --benchmarks=stress_check --use_existing_db=1 --db=$dir2 --wal_dir=$dir2"${args}

trap 'kill -9 $pid $stress_pid;exit' 2 15

check_fail_and_abort() {
    if grep -q "FAIL" $resultfile
    then exit 1
    fi
    if grep -q "Aborted" $resultfile
    then exit 1
    fi
    if grep -q "core dumped" $resultfile
    then exit 1
    fi
}

$prepare_cmd &
pid=$!
echo $pid
sleep 10s
kill -9 $pid
sleep 10s
check_fail_and_abort
$check_cmd
check_fail_and_abort
while true
do
  for k in $( seq 1 100 )
  do
    $update_cmd &
    pid=$!
    echo $pid
    sleep 10s
    kill -9 $pid
    sleep 10s
    check_fail_and_abort
    $check_cmd
    check_fail_and_abort
  done

  for k in $( seq 1 10 )
  do
    $update_cmd &
    pid=$!
    echo $pid
    sleep 100s
    kill -9 $pid
    sleep 10s 
    check_fail_and_abort
    $check_cmd
    check_fail_and_abort
  done

  $update_cmd &
  pid=$!
  echo $pid
  sleep 1000s
  kill -9 $pid
  sleep 10s
  check_fail_and_abort
  $check_cmd
  check_fail_and_abort
done

