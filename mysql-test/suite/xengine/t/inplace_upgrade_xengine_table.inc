--file_exists $ZIP_FILE
--exec unzip -qo $ZIP_FILE -d $MYSQL_TMP_DIR

# upgrade xengine table when server restarts
--echo # restart server and check existing data
--let $shutdown_server_timeout= 10
--replace_result $MYSQL_TMP_DIR MYSQL_TMP_DIR
--let $restart_parameters= "restart: --xengine=1 --datadir=$MYSQL_TMP_DIR/data --innodb_data_file_path=ibdata1:12M:autoextend"
--source include/restart_mysqld.inc

SHOW DATABASES;
USE data;
SHOW TABLES;

SHOW CREATE TABLE t_hidden_pk;
SHOW CREATE TABLE t_hidden_pk_sk;
SHOW CREATE TABLE t_hidden_pk_uk;
SHOW CREATE TABLE t_pk;
SHOW CREATE TABLE t_pk_sk;
SHOW CREATE TABLE t_pk_uk;
SHOW CREATE TABLE t_pk_auto;

CHECK TABLE t_hidden_pk, t_hidden_pk_sk, t_hidden_pk_uk, t_pk, t_pk_sk, t_pk_uk, t_pk_auto;

SELECT * FROM t_hidden_pk;
SELECT * FROM t_hidden_pk_sk;
SELECT * FROM t_hidden_pk_uk;
SELECT * FROM t_pk;
SELECT * FROM t_pk_sk;
SELECT * FROM t_pk_uk;
SELECT * FROM t_pk_auto;
SELECT code FROM t_hidden_pk_uk;
SELECT code FROM t_pk_uk;

if ($DEBUG) {
  SET SESSION debug='+d,skip_dd_table_access_check';

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_hidden_pk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_hidden_pk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_hidden_pk_sk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_hidden_pk_sk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_hidden_pk_uk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_hidden_pk_uk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_pk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_pk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_pk_sk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_pk_sk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_pk_uk', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_pk_uk';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  let $t_id=query_get_value(SELECT id FROM mysql.tables WHERE name='t_pk_auto', id, 1);
  SELECT id, se_private_id, se_private_data FROM mysql.tables WHERE name='t_pk_auto';
  eval SELECT name, se_private_data FROM mysql.columns WHERE table_id=$t_id;
  eval SELECT name, se_private_data FROM mysql.indexes WHERE table_id=$t_id;

  SET SESSION debug='-d,skip_dd_table_access_check';
}

CREATE SCHEMA test2;
USE test2;
CREATE TABLE t1 LIKE data.t_pk_uk;
SHOW CREATE TABLE t1;
INSERT INTO t1 SELECT * FROM data.t_hidden_pk_uk;
SELECT * FROM t1;

DROP DATABASE test2;
DROP DATABASE data;

###########################################################################
# Cleanup
###########################################################################
--let $restart_parameters = restart:
--source include/restart_mysqld.inc

--force-rmdir $MYSQL_TMP_DIR/data

--source suite/xengine/include/check_xengine_log_error.inc

