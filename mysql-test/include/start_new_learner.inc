# ==== Purpose ====
# initialize a new learner node
#
# ==== usage ====
# [--let $learner_server_id= 6]
# [--let $learner_datadir= $MYSQL_TMP_DIR/learner1]
# [--let $learner_logdir= $MYSQL_TMP_DIR/server.log]
# [--let $learner_cluster_id= 1]
# [--let $learner_cluster_info= '127.0.0.1:13200']
# [--let $learner_port= 13009]
# --source include/start_new_learner.inc

--echo # start a new learner
if (!$learner_server_id)
{
	let $learner_server_id= 6;
}
if (!$learner_datadir)
{
	let $learner_datadir= $MYSQL_TMP_DIR/learner1;
}
if (!$learner_logdir)
{
	let $learner_logdir= $MYSQL_TMP_DIR/server.log;
}
if (!$learner_cluster_id)
{
	let $learner_cluster_id= 1;
}
if (!$learner_cluster_info)
{
	let $learner_cluster_info= '127.0.0.1:13200';
}
if (!$learner_port)
{
  let $learner_port= 13009;
}
--let $learner_socket_path= $learner_datadir.sock
--let BASEDIR= `select @@basedir`
--let DDIR= $learner_datadir
--let MYSQLD_LOG= $learner_logdir
--let extra_args=--defaults-file='$MYSQLTEST_VARDIR/my.cnf' --console --skip-ssl --basedir=$BASEDIR --lc-messages-dir=$MYSQL_SHAREDIR --socket=$learner_socket_path
--let init_args=--server_id=$learner_server_id --cluster-id=$learner_cluster_id --cluster-learner-node=ON --cluster-info=$learner_cluster_info
--exec if [ -d $DDIR ]; then rm -rf $DDIR; fi
--exec $MYSQLD $extra_args --initialize-insecure --datadir=$DDIR $init_args > $MYSQLD_LOG 2>&1
--exec $MYSQLD $extra_args --datadir=$DDIR $init_args --cluster-force-change-meta=ON --port=$learner_port >> $MYSQLD_LOG 2>&1
--exec $MYSQLD $extra_args --datadir=$DDIR $init_args --port=$learner_port >> $MYSQLD_LOG 2>&1 &
--sleep 10
