let $bootstrap_sql_file = $MYSQLTEST_VARDIR/tmp/bootstrap_tmp.sql;
#remove_file $bootstrap_sql_file;
write_file $bootstrap_sql_file;
CREATE DATABASE test;
EOF

--source include/make_consensus_args.inc

let $MYSQLD_ARGS = --pid-file=$MYSQLD_DATADIR/mysqld-in-test.pid --secure_file_priv=$MYSQLTEST_VARDIR --socket=$MYSQL_TMP_DIR/tmp.sock --skip-networking --skip-ssl --loose-skip-mysqlx --console --log-error-verbosity=3 --datadir=$MYSQLD_DATADIR $MYSQLD_EXTRA_ARGS > $MYSQLD_ERROR_LOG 2>&1 ;
--exec $MYSQLD --no-defaults $MYSQLD_ARGS --initialize-insecure $consensus_args --init-file=$bootstrap_sql_file
--remove_file $bootstrap_sql_file
