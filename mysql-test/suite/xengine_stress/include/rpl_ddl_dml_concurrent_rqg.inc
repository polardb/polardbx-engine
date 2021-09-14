--echo =================================
--echo # Test with $cs and $coll
--echo =================================

eval CREATE TABLE $TARGET_DB.$TARGET_TABLE (
       col1 INT, col2 INT, col3 INT,
       col4 TEXT, col5 CHAR(80), col6 VARCHAR(1200)
     )CHARSET $cs COLLATE $coll;

--echo # Table in slave
--source include/rpl_sync.inc
--connection slave
--eval SHOW CREATE TABLE $TARGET_DB.$TARGET_TABLE

--echo # Table in master
--connection master
--eval SHOW CREATE TABLE $TARGET_DB.$TARGET_TABLE

--source rqg.inc

--source rpl_ddl_dml_concurrent_checksum.inc

--eval DROP TABLE $TARGET_DB.$TARGET_TABLE

