
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--shutdown_server 0
--source include/wait_until_disconnected.inc
--exec echo "restart:--debug=+d,$debug_hook" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
--source include/wait_leader_apply_catchup.inc
-- exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--sleep 10

connect (conn1,localhost,root,,);
connection conn1;
set @@debug='';

--source include/flush_and_load_data.inc

flush logs;
--eval $sql0
select @@debug;
let $last_log_index= query_get_value(select LAST_LOG_INDEX from  information_schema.ALISQL_CLUSTER_LOCAL, LAST_LOG_INDEX, 1);
--disable_query_log
--error 0,2013
--eval call dbms_consensus.purge_log($last_log_index);
--enable_query_log
--sleep 2
--error 2013
show consensus logs;
disconnect conn1;
connection default;

--source include/wait_until_disconnected.inc
--enable_reconnect
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--source include/wait_until_connected_again.inc
--source include/wait_leader_apply_catchup.inc
--disable_reconnect

connect (conn1,localhost,root,,);
connection conn1;
xa recover;
xa commit 'xx';
disconnect conn1;
connection default;