# ==== Purpose ====
# kill the learner created by 'include/stop_new_learner.inc'
#
# ==== usage ====
# [--let $check_connection=1]
# [--let $ori_conn= $conn1]
# [--let $learner_base_port= $port + 50]

if (!$ori_conn)
{
    --let $ori_conn= default
}

if (!$learner_base_port)
{
	--echo # learner_base_port not passed, exit
	--die must set learner_base_port
}

if ($check_connection)
{
	--connect (leaner_waiter_$learner_base_port, 127.0.0.1, root,, test, $learner_base_port)
	--connection leaner_waiter_$learner_base_port
	--disconnect leaner_waiter_$learner_base_port
	--connection $ori_conn
}

--let $learner_datadir= $MYSQL_TMP_DIR/learner_$learner_base_port
--let $learner_logdir= $MYSQL_TMP_DIR/learner_$learner_base_port.log
--let $learner_pid_file= $learner_datadir.pid
--let $learner_socket_path= $learner_datadir.sock
--let $learner_socket_lock_path= $learner_datadir.sock.lock

if ($check_connection)
{
	--exec kill -8 `cat $learner_pid_file`
}

--exec rm -rf $learner_datadir  $learner_logdir $learner_pid_file $learner_socket_path $learner_socket_lock_path
