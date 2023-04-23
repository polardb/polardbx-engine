# ==== Purpose ====
# kill the learner created by 'include/start_new_learner.inc'
#
# ==== usage ====
# [--let $learner_base_port= $port + 40]

if (!$learner_base_port)
{
	--echo # learner_base_port not passed, exit
	--die must set learner_base_port
}

--let $learner_datadir= $MYSQL_TMP_DIR/learner_$learner_base_port

--let $learner_datadir= $MYSQL_TMP_DIR/learner_$learner_base_port
--let $learner_logdir= $MYSQL_TMP_DIR/learner_$learner_base_port.log
--let $learner_pid_file= $learner_datadir.pid
--let $learner_socket_path= $learner_datadir.sock
--let $learner_socket_lock_path= $learner_datadir.sock.lock

--exec kill -8 `cat $learner_pid_file`
--exec rm -rf $learner_datadir
--exec rm -rf $learner_logdir
--exec rm -rf $learner_pid_file
--exec rm -rf $learner_socket_path
--exec rm -rf $learner_socket_lock_path
