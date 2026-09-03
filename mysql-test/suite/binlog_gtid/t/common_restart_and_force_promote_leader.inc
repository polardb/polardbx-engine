
--source include/wait_until_disconnected.inc
--sleep 2
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again_old.inc
connection default;
xa recover;
--disable_query_log
set global consensus_force_promote=1;
--enable_query_log
--source include/wait_follower_catchup.inc