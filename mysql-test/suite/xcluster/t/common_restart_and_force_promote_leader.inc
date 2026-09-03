
--source include/wait_until_disconnected.inc
#wait for other become leader
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect
--enable_reconnect
--source include/wait_until_connected_again.inc
connection default;
xa recover;