--source include/rpl_connection_master.inc
--let $check_gcn = `select @@global.gcn_write_event`
create table tt (id int);

--echo ###############################
--echo # sync master's sys_gcn with slave's
--echo ###############################
--source include/rpl_connection_master.inc
--source suite/innodb_gcn/include/init_conn_base_seq.inc
--let $master_sys_gcn = `select @base_seq + 100`

--source suite/innodb_gcn/include/init_conn_base_seq.inc
--let $slave_sys_gcn = `select @base_seq + 100`

if ($master_sys_gcn > $slave_sys_gcn)
{
  --let $commit_gcn = $master_sys_gcn
}

if ($master_sys_gcn <= $slave_sys_gcn) {
  --let $commit_gcn = $slave_sys_gcn
}

begin;
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
insert into tt values (1);
commit;

--source include/sync_slave_sql_with_master.inc

if ($check_gcn) {
  --source include/rpl_connection_slave.inc
  --let $actual_sys_gcn = query_get_value("show status like 'Lizard_current_gcn'", Value, 1)
  --let $assert_text=actual_sys_gcn == expect_sys_gcn
  --let $assert_cond="$actual_sys_gcn" = "$commit_gcn"
  --source include/assert.inc
}

--source include/rpl_connection_master.inc
--source suite/innodb_gcn/include/init_conn_base_seq.inc

--echo ###############################
--echo # Case 1: Normal trx.
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 100`

begin;
insert into tt values (1);
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
commit;

--source include/sync_slave_sql_with_master.inc

if ($check_gcn) {
  --source include/rpl_connection_slave.inc
  --let $actual_sys_gcn = query_get_value("show status like 'Lizard_current_gcn'", Value, 1)
  --let $assert_text=actual_sys_gcn == expect_sys_gcn
  --let $assert_cond="$actual_sys_gcn" = "$commit_gcn"
  --source include/assert.inc
}

--source include/rpl_connection_master.inc
set global xa_detach_on_prepare = true;
--echo ###############################
--echo # Case 2: Detached XA + Commit
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 200`

eval xa begin '1_$xid_suffix';
insert into tt values (1);
eval xa end '1_$xid_suffix';
eval xa prepare '1_$xid_suffix';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
eval xa commit '1_$xid_suffix';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '1_$xid_suffix','',1
  --source suite/innodb_gcn/include/xa_info_verify.inc
}

--echo ###############################
--echo # Case 3: Detached XA + Rollback
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 300`

eval xa begin '2_$xid_suffix';
insert into tt values (2);
eval xa end '2_$xid_suffix';
eval xa prepare '2_$xid_suffix';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
eval xa rollback '2_$xid_suffix';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= ROLLBACK
  --let $my_xid= '2_$xid_suffix','',1
  --source suite/innodb_gcn/include/xa_info_verify.inc
}

--echo ###############################
--echo # Case 4: Detached XA One Phase
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 400`

eval xa begin '3_$xid_suffix';
insert into tt values (3);
eval xa end '3_$xid_suffix';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
eval xa commit '3_$xid_suffix' one phase;

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '3_$xid_suffix','',1
  --source suite/innodb_gcn/include/xa_info_verify.inc
}

--source include/rpl_connection_master.inc
set global xa_detach_on_prepare = false;
--echo ###############################
--echo # Case 5: Attached XA + Commit
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 500`

eval xa begin '4_$xid_suffix';
insert into tt values (4);
eval xa end '4_$xid_suffix';
eval xa prepare '4_$xid_suffix';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
eval xa commit '4_$xid_suffix';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '4_$xid_suffix','',1
  --source suite/innodb_gcn/include/xa_info_verify.inc
}

--echo ###############################
--echo # Case 6: Attached XA + Rollback
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 600`

eval xa begin '5_$xid_suffix';
insert into tt values (5);
eval xa end '5_$xid_suffix';
eval xa prepare '5_$xid_suffix';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
eval xa rollback '5_$xid_suffix';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= ROLLBACK
  --let $my_xid= '5_$xid_suffix','',1
  --source suite/innodb_gcn/include/xa_info_verify.inc
}

--echo ###############################
--echo # cleanup
--echo ###############################
--source include/rpl_connection_master.inc
set global xa_detach_on_prepare = ON;
drop table tt;
--source include/sync_slave_sql_with_master.inc
connection default;
