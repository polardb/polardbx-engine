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

xa begin '1';
insert into tt values (1);
xa end '1';
xa prepare '1';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
xa commit '1';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '1','',1
  --source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc
}

--echo ###############################
--echo # Case 3: Detached XA + Rollback
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 300`

xa begin '2';
insert into tt values (2);
xa end '2';
xa prepare '2';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
xa rollback '2';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= ROLLBACK
  --let $my_xid= '2','',1
  --source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc
}

--echo ###############################
--echo # Case 4: Detached XA One Phase
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 400`

xa begin '3';
insert into tt values (3);
xa end '3';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
xa commit '3' one phase;

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '3','',1
  --source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc
}

--source include/rpl_connection_master.inc
set global xa_detach_on_prepare = false;
--echo ###############################
--echo # Case 5: Attached XA + Commit
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 500`

xa begin '4';
insert into tt values (4);
xa end '4';
xa prepare '4';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
xa commit '4';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= COMMIT
  --let $my_xid= '4','',1
  --source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc
}

--echo ###############################
--echo # Case 6: Attached XA + Rollback
--echo ###############################
--source include/rpl_connection_master.inc
--let $commit_gcn = `select @base_seq + 600`

xa begin '5';
insert into tt values (5);
xa end '5';
xa prepare '5';
--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log
xa rollback '5';

--source include/sync_slave_sql_with_master.inc

--source include/rpl_connection_slave.inc

if ($check_gcn) {
  --let $expect_gcn= $commit_gcn
  --let $expect_state= ROLLBACK
  --let $my_xid= '5','',1
  --source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc
}

--echo ###############################
--echo # cleanup
--echo ###############################
--source include/rpl_connection_master.inc
set global xa_detach_on_prepare = default;
drop table tt;
--source include/sync_slave_sql_with_master.inc
connection default;
