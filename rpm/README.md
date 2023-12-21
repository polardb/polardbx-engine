# 通过 RPM 部署 PolarDB-X 标准版

通过 RPM 部署 PolarDB-X 标准版，需要首先获取相应的 RPM 包，您可以手动编译生成该 RPM 包，也可以自行[下载](https://github.com/polardb/polardbx-engine/releases/)（请根据实际情况下载 x86 或 arm 对应的 RPM）。

下面先给出编译生成 RPM 的步骤，如果您已经下载好 RPM 包，则可以跳过这一步，直接[安装 RPM](#1)。
## 从源码编译生成 RPM
不同的操作系统，在编译时依赖的环境会略微不同，但编译出来的 RPM 包是通用的。
### 安装编译依赖
#### For Centos 7

```bash
# 安装必要依赖
yum remove -y cmake

yum install -y git make bison libarchive ncurses-devel libaio-devel cmake3 mysql rpm-build zlib-devel openssl-devel centos-release-scl

ln -s /usr/bin/cmake3 /usr/bin/cmake

yum install -y devtoolset-7-gcc devtoolset-7-gcc-c++ devtoolset-7-binutils

echo "source /opt/rh/devtoolset-7/enable" | sudo tee -a /etc/profile
source /etc/profile
```

#### For Centos 8

```bash
# 安装必要依赖
yum install -y git make bison libarchive ncurses-devel libaio-devel cmake3 mysql rpm-build zlib-devel

yum install -y libtirpc-devel dnf-plugins-core 

yum config-manager --set-enabled PowerTools

yum groupinstall -y "Development Tools"

yum install -y gcc gcc-c++
```

### 编译生成 RPM
```
# 拉取代码
git clone https://github.com/polardb/polardbx-engine.git --depth 1

# 编译生成 rpm
cd polardbx-engine/rpm && rpmbuild -bb t-polardbx-engine.spec
```
编译生成的 RPM 默认在 /root/rpmbuild/RPMS/x86_64/ 下。

## <div id='1'>安装 RPM</div>
```
yum install -y <您下载或编译的rpm>
```
安装后的二进制文件，会出现在 /opt/polardbx-engine/bin 中。

## 启动 DN
创建 polarx 用户（您也可以使用其他非 root 用户），准备一份 my.cnf（[参考模板](#mycnf)）和数据目录（如果改了 my.cnf，则下面的目录也要相应修改），就可以准备启动了。
```bash
# 创建并切换到 polarx 用户
useradd -ms /bin/bash polarx
echo "polarx:polarx" | chpasswd
echo "polarx    ALL=(ALL)    NOPASSWD: ALL" >> /etc/sudoers
su - polarx
# 创建必要目录
mkdir polardbx-engine
cd polardbx-engine && mkdir log mysql run data tmp

# 初始化
/opt/polardbx_engine/bin/mysqld --defaults-file=my.cnf --initialize-insecure
# 启动
/opt/polardbx_engine/bin/mysqld_safe --defaults-file=my.cnf &
```
稍等片刻，即可登录数据库。如果直接使用 my.cnf 精简模板，可以用`mysql -h127.0.0.1 -P4886 -uroot` 登录数据库。


## 体验高可用部署
如果一切顺利，到这里，您已经掌握了部署 PolarDB-X engine 的方式。接下来，让我们在 3 台机器上，部署一个完整的标准版集群，并验证高可用切换的能力。

假设我们的 3 台机器 IP 分别为：
```
192.168.6.183
192.168.6.184
192.168.6.185
```

我们在 3 台机器上，按前述步骤，安装 RPM 后，准备好 my.cnf 和目录（如果有任何步骤失败，请完全清理这些目录，重新创建）。然后在 3 个机器上，分别按如下方式启动：

``` bash
# 192.168.6.183 上执行
/opt/polardbx_engine/bin/mysqld --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@1' \
--initialize-insecure

/opt/polardbx_engine/bin/mysqld_safe --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@1' \
&

# 192.168.6.184 上执行
/opt/polardbx_engine/bin/mysqld --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@2' \
--initialize-insecure

/opt/polardbx_engine/bin/mysqld_safe --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@2' \
&

# 192.168.6.185 上执行
/opt/polardbx_engine/bin/mysqld --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@3' \
--initialize-insecure

/opt/polardbx_engine/bin/mysqld_safe --defaults-file=my.cnf \
--cluster-info='192.168.6.183:14886;192.168.6.184:14886;192.168.6.185:14886@3' \
&
```

注意到，我们在启动时修改了 `cluster-info` 的配置项，其中的格式为 `[host1]:[port1];[host2]:[port2];[host3]:[port3]@[idx]` ，不同的机器，只有 `[idx]` 不同，`[idx]` 也反映了该机器是第几个 `[host][port]`。请根据实际机器的 ip 修改该配置项。

稍等片刻，我们登录每个数据库，验证一下集群的状态，执行
```sql
SELECT * FROM INFORMATION_SCHEMA.ALISQL_CLUSTER_LOCAL
```
我们会看到有一个机器是 Leader：
```
          SERVER_ID: 1
       CURRENT_TERM: 20
     CURRENT_LEADER: 192.168.6.183:14886
       COMMIT_INDEX: 1
      LAST_LOG_TERM: 20
     LAST_LOG_INDEX: 1
               ROLE: Leader
          VOTED_FOR: 1
   LAST_APPLY_INDEX: 0
SERVER_READY_FOR_RW: Yes
      INSTANCE_TYPE: Normal
```
另外两个机器是 Follower：

```
          SERVER_ID: 2
       CURRENT_TERM: 20
     CURRENT_LEADER: 192.168.6.183:14886
       COMMIT_INDEX: 1
      LAST_LOG_TERM: 20
     LAST_LOG_INDEX: 1
               ROLE: Follower
          VOTED_FOR: 1
   LAST_APPLY_INDEX: 1
SERVER_READY_FOR_RW: No
      INSTANCE_TYPE: Normal
```

三节的 PolarDB-X engine 只有 Leader 节点可以写入数据。我们在 Leader 上建一个库表，写入一些简单的数据：
```sql
CREATE DATABASE db1;
USE db1;
CREATE TABLE tb1 (id int);
INSERT INTO tb1 VALUES (0), (1), (2);
```
然后我们可以在 Follower 上把数据查出来。

我们也可以在 Leader 上查询集群的状态：
```SQL
SELECT * FROM INFORMATION_SCHEMA.ALISQL_CLUSTER_GLOBAL;
```
结果形如：
```
***************** 1. row *****************
      SERVER_ID: 1
        IP_PORT: 192.168.6.183:14886
    MATCH_INDEX: 4
     NEXT_INDEX: 0
           ROLE: Leader
      HAS_VOTED: Yes
     FORCE_SYNC: No
ELECTION_WEIGHT: 5
 LEARNER_SOURCE: 0
  APPLIED_INDEX: 4
     PIPELINING: No
   SEND_APPLIED: No
***************** 2. row *****************
      SERVER_ID: 2
        IP_PORT: 192.168.6.184:14886
    MATCH_INDEX: 4
     NEXT_INDEX: 5
           ROLE: Follower
      HAS_VOTED: Yes
     FORCE_SYNC: No
ELECTION_WEIGHT: 5
 LEARNER_SOURCE: 0
  APPLIED_INDEX: 4
     PIPELINING: Yes
   SEND_APPLIED: No
**************** 3. row *****************
      SERVER_ID: 3
        IP_PORT: 192.168.6.185:14886
    MATCH_INDEX: 4
     NEXT_INDEX: 5
           ROLE: Follower
      HAS_VOTED: No
     FORCE_SYNC: No
ELECTION_WEIGHT: 5
 LEARNER_SOURCE: 0
  APPLIED_INDEX: 4
     PIPELINING: Yes
   SEND_APPLIED: No
```

`APPLIED_INDEX` 都是 4 ，说明数据目前在三节点上是完全一致的。

接下来，我们对 Leader 节点（192.168.6.183）进程 kill -9 ，让集群选出新 Leader。
```bash
kill -9 $(pgrep -x mysqld)
```
旧 Leader 被 kill 后，mysqld_safe 会立马重新拉起 mysqld 进程。

随后，我们看到，Leader 变成了 192.168.6.185 节点了

```
          SERVER_ID: 3
       CURRENT_TERM: 21
     CURRENT_LEADER: 192.168.6.185:14886
       COMMIT_INDEX: 5
      LAST_LOG_TERM: 21
     LAST_LOG_INDEX: 5
               ROLE: Leader
          VOTED_FOR: 3
   LAST_APPLY_INDEX: 4
SERVER_READY_FOR_RW: Yes
      INSTANCE_TYPE: Normal
```

通过以上步骤，我们在 3 个机器上部署了三节点的 PolarDB-X engine，并进行了简单的验证。您也可以在一个机器上部署三节点，但要保证使用不同的 my.cnf，且其中的 port、数据目录等参数不同，同时 cluster-info 的端口也要不同。

最后，上述过程仅仅是体验和测试，请不要直接用于生产。生产推荐使用 K8S 方式部署。如果确实要用 RPM 方式在生产上部署，业务需要注意自行感知 Leader 的切换，以使用正确的连接串访问数据库。

## <div id = 'mycnf'>my.cnf 参考模板</div>
请根据实际情况修改参数，仅验证功能和测试，推荐使用精简模板。
### 精简模版
```
[mysqld]
basedir = /opt/polardbx-engine
log_error_verbosity = 2
default_authentication_plugin = mysql_native_password
gtid_mode = ON
enforce_gtid_consistency = ON
log_bin = mysql-binlog
binlog_format = row
binlog_row_image = FULL
master_info_repository = TABLE
relay_log_info_repository = TABLE

# change me if needed
datadir = /home/polarx/polardbx-engine/data
tmpdir = /home/polarx/polardbx-engine/tmp
socket = /home/polarx/polardbx-engine/tmp.mysql.sock
log_error = /home/polarx/polardbx-engine/log/alert.log
port = 4886
cluster_id = 1234
cluster_info = 127.0.0.1:14886@1

[mysqld_safe]
pid_file = /home/polarx/polardbx-engine/run/mysql.pid
```

### 详细模板
```
[mysqld]
auto_increment_increment = 1
auto_increment_offset = 1
autocommit = ON
automatic_sp_privileges = ON
avoid_temporal_upgrade = OFF
back_log = 3000
binlog_cache_size = 1048576
binlog_checksum = CRC32
binlog_order_commits = OFF
binlog_row_image = full
binlog_rows_query_log_events = ON
binlog_stmt_cache_size = 32768
binlog_transaction_dependency_tracking = WRITESET
block_encryption_mode = "aes-128-ecb"
bulk_insert_buffer_size = 4194304
character_set_server = utf8
concurrent_insert = 2
connect_timeout = 10
datadir = /home/polarx/polardbx-engine/data
default_authentication_plugin = mysql_native_password
default_storage_engine = InnoDB
default_time_zone = +8:00
default_week_format = 0
delay_key_write = ON
delayed_insert_limit = 100
delayed_insert_timeout = 300
delayed_queue_size = 1000
disconnect_on_expired_password = ON
div_precision_increment = 4
end_markers_in_json = OFF
enforce_gtid_consistency = ON
eq_range_index_dive_limit = 200
event_scheduler = OFF
expire_logs_days = 0
explicit_defaults_for_timestamp = OFF
flush_time = 0
ft_max_word_len = 84
ft_min_word_len = 4
ft_query_expansion_limit = 20
general_log = OFF
general_log_file = /home/polarx/polardbx-engine/log/general.log
group_concat_max_len = 1024
gtid_mode = ON
host_cache_size = 644
init_connect = ''
innodb_adaptive_flushing = ON
innodb_adaptive_flushing_lwm = 10
innodb_adaptive_hash_index = OFF
innodb_adaptive_max_sleep_delay = 150000
innodb_autoextend_increment = 64
innodb_autoinc_lock_mode = 2
innodb_buffer_pool_chunk_size = 33554432
innodb_buffer_pool_dump_at_shutdown = ON
innodb_buffer_pool_dump_pct = 25
innodb_buffer_pool_instances = 8
innodb_buffer_pool_load_at_startup = ON
innodb_change_buffer_max_size = 25
innodb_change_buffering = none
innodb_checksum_algorithm = crc32
innodb_cmp_per_index_enabled = OFF
innodb_commit_concurrency = 0
innodb_compression_failure_threshold_pct = 5
innodb_compression_level = 6
innodb_compression_pad_pct_max = 50
innodb_concurrency_tickets = 5000
innodb_data_file_purge = ON
innodb_data_file_purge_interval = 100
innodb_data_file_purge_max_size = 128
innodb_data_home_dir = /home/polarx/polardbx-engine/mysql
innodb_deadlock_detect = ON
innodb_disable_sort_file_cache = ON
innodb_flush_log_at_trx_commit = 1
innodb_flush_method = O_DIRECT
innodb_flush_neighbors = 0
innodb_flush_sync = ON
innodb_ft_cache_size = 8000000
innodb_ft_enable_diag_print = OFF
innodb_ft_enable_stopword = ON
innodb_ft_max_token_size = 84
innodb_ft_min_token_size = 3
innodb_ft_num_word_optimize = 2000
innodb_ft_result_cache_limit = 2000000000
innodb_ft_sort_pll_degree = 2
innodb_ft_total_cache_size = 640000000
innodb_io_capacity = 20000
innodb_io_capacity_max = 40000
innodb_lock_wait_timeout = 50
innodb_log_buffer_size = 16777216
innodb_log_checksums = ON
innodb_log_file_size = 134217728
innodb_log_group_home_dir = /home/polarx/polardbx-engine/mysql
innodb_lru_scan_depth = 8192
innodb_max_dirty_pages_pct = 75
innodb_max_dirty_pages_pct_lwm = 0
innodb_max_purge_lag = 0
innodb_max_purge_lag_delay = 0
innodb_max_undo_log_size = 1073741824
innodb_monitor_disable =
innodb_monitor_enable =
innodb_old_blocks_pct = 37
innodb_old_blocks_time = 1000
innodb_online_alter_log_max_size = 134217728
innodb_open_files = 20000
innodb_optimize_fulltext_only = OFF
innodb_page_cleaners = 4
innodb_print_all_deadlocks = ON
innodb_purge_batch_size = 300
innodb_purge_rseg_truncate_frequency = 128
innodb_purge_threads = 4
innodb_random_read_ahead = OFF
innodb_read_ahead_threshold = 0
innodb_read_io_threads = 4
innodb_rollback_on_timeout = OFF
innodb_rollback_segments = 128
innodb_sort_buffer_size = 1048576
innodb_spin_wait_delay = 6
innodb_stats_auto_recalc = ON
innodb_stats_method = nulls_equal
innodb_stats_on_metadata = OFF
innodb_stats_persistent = ON
innodb_stats_persistent_sample_pages = 20
innodb_stats_transient_sample_pages = 8
innodb_status_output = OFF
innodb_status_output_locks = OFF
innodb_strict_mode = ON
innodb_sync_array_size = 16
innodb_sync_spin_loops = 30
innodb_table_locks = ON
innodb_tcn_cache_level = block
innodb_thread_concurrency = 0
innodb_thread_sleep_delay = 0
innodb_write_io_threads = 4
interactive_timeout = 7200
key_buffer_size = 16777216
key_cache_age_threshold = 300
key_cache_block_size = 1024
key_cache_division_limit = 100
lc_time_names = en_US
local_infile = OFF
lock_wait_timeout = 1800
log-bin-index = /home/polarx/polardbx-engine/mysql/mysql-bin.index
log_bin = /home/polarx/polardbx-engine/mysql/mysql-bin.log
log_bin_trust_function_creators = ON
log_bin_use_v1_row_events = 0
log_error = /home/polarx/polardbx-engine/log/alert.log
log_error_verbosity = 2
log_queries_not_using_indexes = OFF
log_slave_updates = 0
log_slow_admin_statements = ON
log_slow_slave_statements = ON
log_throttle_queries_not_using_indexes = 0
long_query_time = 1
loose_ccl_max_waiting_count = 0
loose_ccl_queue_bucket_count = 4
loose_ccl_queue_bucket_size = 64
loose_ccl_wait_timeout = 86400
loose_cluster-id = 1234
loose_cluster-info = 127.0.0.1:14886@1
loose_consensus_auto_leader_transfer = ON
loose_consensus_auto_reset_match_index = ON
loose_consensus_election_timeout = 10000
loose_consensus_io_thread_cnt = 8
loose_consensus_large_trx = ON
loose_consensus_log_cache_size = 536870912
loose_consensus_max_delay_index = 10000
loose_consensus_max_log_size = 20971520
loose_consensus_max_packet_size = 131072
loose_consensus_prefetch_cache_size = 268435456
loose_consensus_worker_thread_cnt = 8
loose_galaxyx_port = 32886
loose_implicit_primary_key = 1
loose_information_schema_stats_expiry = 86400
loose_innodb_buffer_pool_in_core_file = OFF
loose_innodb_commit_cleanout_max_rows = 9999999999
loose_innodb_doublewrite_pages = 64
loose_innodb_lizard_stat_enabled = OFF
loose_innodb_log_compressed_pages = ON
loose_innodb_log_optimize_ddl = OFF
loose_innodb_log_write_ahead_size = 4096
loose_innodb_multi_blocks_enabled = ON
loose_innodb_numa_interleave = OFF
loose_innodb_parallel_read_threads = 1
loose_innodb_undo_retention = 1800
loose_innodb_undo_space_reserved_size = 1024
loose_innodb_undo_space_supremum_size = 102400
loose_internal_tmp_mem_storage_engine = TempTable
loose_new_rpc = ON
loose_optimizer_switch = index_merge=on,index_merge_union=on,index_merge_sort_union=on,index_merge_intersection=on,engine_condition_pushdown=on,index_condition_pushdown=on,mrr=on,mrr_cost_based=on,block_nested_loop=on,batched_key_access=off,materialization=on,semijoin=on,loosescan=on,firstmatch=on,subquery_materialization_cost_based=on,use_index_extensions=on
loose_optimizer_trace = enabled=off,one_line=off
loose_optimizer_trace_features = greedy_search=on,range_optimizer=on,dynamic_range=on,repeated_subselect=on
loose_performance-schema_instrument = 'wait/lock/metadata/sql/mdl=ON'
loose_performance_point_lock_rwlock_enabled = ON
loose_performance_schema-instrument = 'memory/%%=COUNTED'
loose_performance_schema_accounts_size = 10000
loose_performance_schema_consumer_events_stages_current = ON
loose_performance_schema_consumer_events_stages_history = ON
loose_performance_schema_consumer_events_stages_history_long = ON
loose_performance_schema_consumer_events_statements_current = OFF
loose_performance_schema_consumer_events_statements_history = OFF
loose_performance_schema_consumer_events_statements_history_long = OFF
loose_performance_schema_consumer_events_transactions_current = OFF
loose_performance_schema_consumer_events_transactions_history = OFF
loose_performance_schema_consumer_events_transactions_history_long = OFF
loose_performance_schema_consumer_events_waits_current = OFF
loose_performance_schema_consumer_events_waits_history = OFF
loose_performance_schema_consumer_events_waits_history_long = OFF
loose_performance_schema_consumer_global_instrumentation = OFF
loose_performance_schema_consumer_statements_digest = OFF
loose_performance_schema_consumer_thread_instrumentation = OFF
loose_performance_schema_digests_size = 10000
loose_performance_schema_error_size = 0
loose_performance_schema_events_stages_history_long_size = 0
loose_performance_schema_events_stages_history_size = 0
loose_performance_schema_events_statements_history_long_size = 0
loose_performance_schema_events_statements_history_size = 0
loose_performance_schema_events_transactions_history_long_size = 0
loose_performance_schema_events_transactions_history_size = 0
loose_performance_schema_events_waits_history_long_size = 0
loose_performance_schema_events_waits_history_size = 0
loose_performance_schema_hosts_size = 10000
loose_performance_schema_instrument = '%%=OFF'
loose_performance_schema_max_cond_classes = 0
loose_performance_schema_max_cond_instances = 10000
loose_performance_schema_max_digest_length = 0
loose_performance_schema_max_digest_sample_age = 0
loose_performance_schema_max_file_classes = 0
loose_performance_schema_max_file_handles = 0
loose_performance_schema_max_file_instances = 1000
loose_performance_schema_max_index_stat = 10000
loose_performance_schema_max_memory_classes = 0
loose_performance_schema_max_metadata_locks = 10000
loose_performance_schema_max_mutex_classes = 0
loose_performance_schema_max_mutex_instances = 10000
loose_performance_schema_max_prepared_statements_instances = 1000
loose_performance_schema_max_program_instances = 10000
loose_performance_schema_max_rwlock_classes = 0
loose_performance_schema_max_rwlock_instances = 10000
loose_performance_schema_max_socket_classes = 0
loose_performance_schema_max_socket_instances = 1000
loose_performance_schema_max_sql_text_length = 0
loose_performance_schema_max_stage_classes = 0
loose_performance_schema_max_statement_classes = 0
loose_performance_schema_max_statement_stack = 1
loose_performance_schema_max_table_handles = 10000
loose_performance_schema_max_table_instances = 1000
loose_performance_schema_max_table_lock_stat = 10000
loose_performance_schema_max_thread_classes = 0
loose_performance_schema_max_thread_instances = 10000
loose_performance_schema_session_connect_attrs_size = 0
loose_performance_schema_setup_actors_size = 10000
loose_performance_schema_setup_objects_size = 10000
loose_performance_schema_users_size = 10000
loose_persist_binlog_to_redo = OFF
loose_persist_binlog_to_redo_size_limit = 1048576
loose_rds_audit_log_buffer_size = 16777216
loose_rds_audit_log_enabled = OFF
loose_rds_audit_log_event_buffer_size = 8192
loose_rds_audit_log_row_limit = 100000
loose_rds_audit_log_version = MYSQL_V1
loose_recovery_apply_binlog = OFF
loose_replica_read_timeout = 3000
loose_rpc_port = 34886
loose_session_track_system_variables = "*"
loose_session_track_transaction_info = OFF
loose_slave_parallel_workers = 32
low_priority_updates = 0
lower_case_table_names = 1
master_info_file = /home/polarx/polardbx-engine/mysql/master.info
master_info_repository = TABLE
master_verify_checksum = OFF
max_allowed_packet = 1073741824
max_binlog_cache_size = 18446744073709551615
max_binlog_stmt_cache_size = 18446744073709551615
max_connect_errors = 65536
max_connections = 5532
max_error_count = 1024
max_execution_time = 0
max_heap_table_size = 67108864
max_join_size = 18446744073709551615
max_length_for_sort_data = 4096
max_points_in_geometry = 65536
max_prepared_stmt_count = 16382
max_seeks_for_key = 18446744073709551615
max_sort_length = 1024
max_sp_recursion_depth = 0
max_user_connections = 5000
max_write_lock_count = 102400
min_examined_row_limit = 0
myisam_sort_buffer_size = 262144
mysql_native_password_proxy_users = OFF
net_buffer_length = 16384
net_read_timeout = 30
net_retry_count = 10
net_write_timeout = 60
ngram_token_size = 2
open_files_limit = 65535
opt_indexstat = ON
opt_tablestat = ON
optimizer_prune_level = 1
optimizer_search_depth = 62
optimizer_trace_limit = 1
optimizer_trace_max_mem_size = 1048576
optimizer_trace_offset = -1
performance_schema = ON
port = 4886
preload_buffer_size = 32768
query_alloc_block_size = 8192
query_prealloc_size = 8192
range_alloc_block_size = 4096
range_optimizer_max_mem_size = 8388608
read_rnd_buffer_size = 442368
relay_log = /home/polarx/polardbx-engine/mysql/slave-relay.log
relay_log_index = /home/polarx/polardbx-engine/mysql/slave-relay-log.index
relay_log_info_file = /home/polarx/polardbx-engine/mysql/slave-relay-log.info
relay_log_info_repository = TABLE
relay_log_purge = OFF
relay_log_recovery = OFF
replicate_same_server_id = OFF
loose_rotate_log_table_last_name =
server_id = 1234
session_track_gtids = OFF
session_track_schema = ON
session_track_state_change = OFF
sha256_password_proxy_users = OFF
show_old_temporals = OFF
skip_slave_start = OFF
skip_ssl = ON
slave_exec_mode = strict
slave_load_tmpdir = /home/polarx/polardbx-engine/tmp
slave_net_timeout = 4
slave_parallel_type = LOGICAL_CLOCK
slave_pending_jobs_size_max = 1073741824
slave_sql_verify_checksum = OFF
slave_type_conversions =
slow_launch_time = 2
slow_query_log = OFF
slow_query_log_file = /home/polarx/polardbx-engine/mysql/slow_query.log
socket = /home/polarx/polardbx-engine/run/mysql.sock
sort_buffer_size = 868352
sql_mode = NO_ENGINE_SUBSTITUTION
stored_program_cache = 256
sync_binlog = 1
sync_master_info = 10000
sync_relay_log = 1
sync_relay_log_info = 10000
table_open_cache_instances = 16
temptable_max_ram = 1073741824
thread_cache_size = 100
thread_stack = 262144
tls_version = TLSv1,TLSv1.1,TLSv1.2
tmp_table_size = 2097152
tmpdir = /home/polarx/polardbx-engine/tmp
transaction_alloc_block_size = 8192
transaction_isolation = REPEATABLE-READ
transaction_prealloc_size = 4096
transaction_write_set_extraction = XXHASH64
updatable_views_with_limit = YES
wait_timeout = 28800
innodb_buffer_pool_size = 644245094

[mysqld_safe]
pid_file = /home/polarx/polardbx-engine/run/mysql.pid
```
