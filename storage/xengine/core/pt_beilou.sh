#!/bin/sh

bench_tool=./bu-RelWithDebInfo/db_bench
data_dir=/u01/beilou/workspace/xe_bench_data
DB_BENCH_BIN=./db_bench

function do_cmake() {
  dbug="$1"
  #gcc_flag="-g -lrt -ljemalloc "
  gcc_flag="-g -lrt"
  if [ "$dbug"  = "debug"  ];then
    gcc_flag="$gcc_flag -fPIC"
    bench_tool=./bu-Debug/db_bench
  else
    bench_tool=./bu-RelWithDebInfo/db_bench
    gcc_flag="$gcc_flag -O2 -DNDEBUG -fPIC"
  fi
  /bin/rm -rf ./CMakeFiles
  /bin/rm -f ./CMakeCache.txt
  /bin/rm -rf $DB_BENCH_BIN
  /bin/rm -rf $bench_tool
  sh build.sh -t $dbug
  cp -f $bench_tool $DB_BENCH_BIN
}

if [ "$1" = "b" ];
then
  do_cmake release
elif [ "$1" = "d" ];
then
  do_cmake debug
else 
  echo "use exits binary!"  
fi

table_size=1000000
run_time=25
threads=4
tables=32
wb_size=$((8 * 1024 * 1024 * 1024))
total_wb_size=$((512 * 1024 * 1024 * 1024))
db_wb_size=$((512 * 1024 * 1024 * 1024))
total_wal_size=$((1024 * 1024 * 1024 * 1024))
total_bc_size=$((512 * 1024 * 1024 * 1024))
row_cache_size=$((512 * 1024 * 1024 * 1024))
#row_cache_size=$((0))
skew_rate=0

if [ "$1" != "" ];
then
    thread=$1
fi

COMMON_ARGS="--compaction_mode=0 \
            --cpu_compaction_thread_num=9 \
            --fpga_compaction_thread_num=8 \
            --arena_block_size=32468 \
            --max_batch_group_slot_array_size=16 \
            --max_batch_group_size=16 \
            --max_batch_group_leader_wait_time_us=10 \
            --level0_file_num_compaction_trigger=32 \
            --level0_layer_num_compaction_trigger=1 \
            --level1_extents_major_compaction_trigger=2000 \
            --base_background_compactions=8 \
            --max_background_flushes=8 \
            --max_background_compactions=8 \
            --report_interval_seconds=1 \
            --report_file=$data_dir/rep.csv \
            --info_log_level=1 \
            --duration=$run_time \
            --statistics=0 \
            --perf_level=2 \
            --histogram=0 \
            --writes=0 \
            --block_size=16384 \
            --sync=0 \
            --transaction_db=0 \
            --async_mode=1 \
            --use_direct_write_for_wal=0 \
            --max_log_buffer_num=4 \
            --max_single_log_buffer_size=2000000 \
            --max_log_buffer_switch_limit=1000000 \
            --allow_concurrent_memtable_write=1 \
            --seed=100 \
            --transaction_lock_timeout=0 \
            --compression_type=none \
            --use_existing_db=0 \
            --db=$data_dir  \
            --wal_dir=$data_dir \
            --batch_size=1"

for threads in 32
do
 /bin/rm -rf $data_dir/*
 echo "run db_bench with threads=$threads duration=$run_time data_dir=$data_dir"
 export LD_PRELOAD=/u01/beilou/local/5730/lib/libjemalloc.so
 $DB_BENCH_BIN --benchmarks=fillseq,readseq,readrandom \
             --disable_wal=1 \
             --num=$table_size \
             --num_column_families=$tables \
             --threads=$threads \
             --value_size=8 \
             --pin_slice=false \
             --use_xcache=true \
             --read_random_exp_range=${skew_rate} \
             --write_buffer_size=$wb_size \
             --cache_size=$total_bc_size \
             --cache_numshardbits=14 \
             --table_cache_numshardbits=12 \
             --flush_write_buffer_after_test=true \
             --row_cache_size=$row_cache_size \
             --db_total_write_buffer_size=$total_wb_size \
             --db_write_buffer_size=$db_wb_size \
             --max_total_wal_size=$total_wal_size \
             --min_write_buffer_number_to_merge=10000 \
             --max_write_buffer_number_to_maintain=10000 \
             $COMMON_ARGS
done

