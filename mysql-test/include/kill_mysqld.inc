--let $_server_id= `SELECT @@server_id`
--let $_expect_file_name= $MYSQLTEST_VARDIR/tmp/mysqld.$_server_id.expect

--let $enable_gcov= query_get_value(SHOW VARIABLES LIKE "flush_gcov_enabled", Value, 1)
 
if($enable_gcov != "No such row") {
    --disable_query_log
    SET flush_gcov_enabled = on;
    --enable_query_log
}

--echo # Kill the server
--exec echo "wait" > $_expect_file_name
--shutdown_server 0
--source include/wait_until_disconnected.inc
