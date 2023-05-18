
connect (conn1,localhost,root,,);
connection conn1;
--source suite/innodb_gcn/include/init_conn_base_seq.inc

connection default;
--source suite/innodb_gcn/include/init_conn_base_seq.inc

insert into t1 values (1, 10);

--let $commit_gcn = `select @base_seq + 100`
begin;
insert into t1 values (2, 20);
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
commit;

--source include/wait_innodb_all_purged.inc

--echo #########################################
--echo #  Case 1
--echo #########################################
--let $query_gcn = `select @base_seq`
begin;
--disable_query_log
--eval set innodb_snapshot_seq = $query_gcn
--enable_query_log
--error ER_SNAPSHOT_TOO_OLD
select count(0) from t1;

--disable_query_log
--error ER_SNAPSHOT_TOO_OLD
--eval select count(0) from t1 as of gcn $query_gcn
--enable_query_log
commit;

--echo #########################################
--echo #  Case 2
--echo #########################################
--error ER_SNAPSHOT_OUT_OF_RANGE
select count(0) from t1 as of timestamp date_add(now(), interval 3 second);

--error ER_SNAPSHOT_OUT_OF_RANGE
select count(0) from t1 as of timestamp '2020-06-04 00:00:00';

--echo #########################################
--echo #  Case 3
--echo #########################################
set global innodb_undo_retention = 900000;
connection default;
--let $commit_gcn = `select @base_seq + 200`
begin;
insert into t1 values (3, 30);
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
commit;

connection conn1;
--let $query_gcn = `select @base_seq + 201`
begin;
--disable_query_log
--eval set innodb_snapshot_seq = $query_gcn
--enable_query_log
select count(0) from t1;

connection default;
--let $commit_gcn = `select @base_seq + 300`
begin;
insert into t1 values (4, 40);
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
commit;

connection conn1;
select count(0) from t1;
commit;

--echo #########################################
--echo #  Case 4
--echo #########################################
connection default;
alter table t1 add column c2 int after id;

connection conn1;
--let $query_gcn = `select @base_seq + 201`
begin;
--disable_query_log
--eval set innodb_snapshot_seq = $query_gcn
--enable_query_log
--error ER_AS_OF_TABLE_DEF_CHANGED
select count(0) from t1;
commit;

--echo #########################################
--echo #  Case 5
--echo #########################################
connection default;
--let $commit_gcn = `select @base_seq + 400`
xa start '';
insert into t1 values (5, 50, 50);
xa end '';
xa prepare '';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

connection conn1;
--let $query_gcn = `select @base_seq + 501`
begin;
--disable_query_log
--eval set innodb_snapshot_seq = $query_gcn
--enable_query_log
set innodb_global_query_wait_timeout = 1;
--error ER_GP_WAIT_TIMEOUT
select count(0) from t1;
commit;

connection default;
xa commit '';

disconnect conn1;

--echo #########################################
--echo #  Cleanup
--echo #########################################
set global innodb_undo_retention = default;
drop table t1;

