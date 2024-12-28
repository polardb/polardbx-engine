
--connect (conn11,127.0.0.1,root,,test,$MYPORT_1)
--connection conn11

--echo =================================
--echo **** test xa commit in_leader_transfer failed
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect

delete from t1;

#xa prepare
xa start 'x1'; 
insert into t1 values(0);
xa end 'x1';
xa prepare 'x1';

#inject fault
xa recover;
SET SESSION debug="+d,force_in_leader_transfer";
select @@debug;

xa recover;
#check failed
--error ER_BINLOG_LOGGING_IMPOSSIBLE
xa commit 'x1';
--error 2013
xa recover;

--source include/wait_until_disconnected.inc
--enable_reconnect
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--source include/wait_until_connected_again.inc
--source include/wait_leader_apply_catchup.inc
--disable_reconnect

--disconnect conn11
--connect (conn11,127.0.0.1,root,,test,$MYPORT_1)
--connection conn11


#xa finish
xa commit 'x1';

#check result
select count(*) from t1;
xa recover;
select ROLE from information_schema.ALISQL_CLUSTER_local;


--echo =================================
--echo **** test xa commit after_leader_transfer failed
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect

#xa prepare
xa start 'x1'; 
insert into t1 values(0);
xa end 'x1';
xa prepare 'x1';

#inject fault
xa recover;
SET SESSION debug="+d,force_after_leader_transfer";
select @@debug;

xa recover;
#check failed
--error ER_BINLOG_LOGGING_IMPOSSIBLE
xa commit 'x1';
--error 2013
xa recover;

--source include/wait_until_disconnected.inc
--enable_reconnect
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--source include/wait_until_connected_again.inc
--source include/wait_leader_apply_catchup.inc
--disable_reconnect

--disconnect conn11
--connect (conn11,127.0.0.1,root,,test,$MYPORT_1)
--connection conn11

#xa finish
xa commit 'x1';

#check result
select count(*) from t1;
xa recover;
select ROLE from information_schema.ALISQL_CLUSTER_local;

--echo =================================
--echo **** test commit after_leader_transfer failed

#inject fault
SET SESSION debug="+d,force_after_leader_transfer";
select @@debug;

begin;
insert into t1 values(0);
--error ER_CONSENSUS_LEADERSHIP_IS_CHANGING
commit;

xa recover;

#check result
select count(*) from t1;
xa recover;
select ROLE from information_schema.ALISQL_CLUSTER_local;

SET SESSION debug="";
select @@debug;


--echo =================================
--echo **** test commit force_in_wait_commit failed
--exec echo "wait" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect

#inject fault
xa recover;
SET SESSION debug="+d,force_in_wait_commit";
select @@debug;

begin;
insert into t1 values(0);
--error ER_BINLOG_LOGGING_IMPOSSIBLE
commit;

--error 2013
select count(*) from t1;

--source include/wait_until_disconnected.inc
--enable_reconnect
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--source include/wait_until_connected_again.inc
--source include/wait_leader_apply_catchup.inc
--disable_reconnect

--disconnect conn11
--connect (conn11,127.0.0.1,root,,test,$MYPORT_1)
--connection conn11

#check result
#select count(*) from t1;
xa recover;
select ROLE from information_schema.ALISQL_CLUSTER_local;

--disconnect conn11
