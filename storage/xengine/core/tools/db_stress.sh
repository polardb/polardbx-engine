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

threads=5
update_threads=2
delete_threads=1
max_key_id=10000
update_delete_count=10000
duration=3600
dir1=/u01/rocksdb-test-1
dir1=/u01/rocksdb-test-2

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
    --num=$max_key_id
    --threads=$threads
    --num_deletion_threads=$delete_threads
    --num_update_threads=$update_threads
    --update_delete_count=$update_delete_count
    --duration=$duration"

normal_cmd="./db_bench --benchmarks=stress --use_existing_db=0 --db=$dir1 --wal_dir=$dir1"${args}
$normal_cmd &
stress_pid=$!

prepare_cmd="./db_bench --benchmarks=stress_durability --use_existing_db=0 --db=$dir2 --wal_dir=$dir2"${args}
update_cmd="./db_bench --benchmarks=stress_durability --use_existing_db=1 --db=$dir2 --wal_dir=$dir2"${args}
check_cmd="./db_bench --benchmarks=stress_check --use_existing_db=1 --db=$dir2 --wal_dir=$dir2"${args}

trap 'kill -9 $pid $stress_pid;exit' 2 15

$prepare_cmd &
pid=$!
echo $pid
sleep 10s
kill -9 $pid
$check_cmd
while true
do
  for k in $( seq 1 100 )
  do
    $prepare_cmd &
    pid=$!
    echo $pid
    sleep 10s
    kill -9 $pid
    $check_cmd
  done

  for k in $( seq 1 10 )
  do
    $prepare_cmd &
    pid=$!
    echo $pid
    sleep 100s
    kill -9 $pid
    $check_cmd
  done

  $prepare_cmd &
  pid=$!
  echo $pid
  sleep 1000s
  kill -9 $pid
  $check_cmd
done
