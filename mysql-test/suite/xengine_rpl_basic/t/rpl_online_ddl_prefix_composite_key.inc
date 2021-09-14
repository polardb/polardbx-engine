--echo ###################################################################
--echo case 1 non-unique secondary key
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b VARCHAR(10), c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
ALTER TABLE t1 ADD KEY pre_sk(b(6)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
INSERT INTO t1 VALUES(3, '3333333', 6, '3c');

--echo ####
--echo composite index
--echo ####
ALTER TABLE t1 ADD KEY com_sk(c, d), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
INSERT INTO t1 VALUES(4, '4444444', 8, '4d');

--echo ####
--echo composite index with prefix
--echo ####
ALTER TABLE t1 ADD KEY pre_com_sk(b(4), c), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
INSERT INTO t1 VALUES(5, '5555555', 10, '5e');

SELECT * FROM t1;
ALTER TABLE t1 DROP KEY pre_sk, DROP KEY com_sk, DROP KEY pre_com_sk, ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 2 unique secondary key
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b VARCHAR(10), c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
ALTER TABLE t1 ADD UNIQUE KEY pre_uk(b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(3, '2222333', 6, '3c');
INSERT INTO t1 VALUES(3, '3333333', 6, '3c');

--echo ####
--echo composite index
--echo ####
ALTER TABLE t1 ADD UNIQUE KEY com_uk(c, d), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(4, '4444444', 6, '3c');
INSERT INTO t1 VALUES(4, '4444444', 8, '4d');

--echo ####
--echo composite index with prefix
--echo ####
ALTER TABLE t1 ADD KEY pre_com_uk(b(6), c), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(5, '4444445', 8, '5e');
INSERT INTO t1 VALUES(5, '5555555', 10, '5e');

SELECT * FROM t1;
ALTER TABLE t1 DROP KEY pre_uk, DROP KEY com_uk, DROP KEY pre_com_uk, ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 3 unique secondary key with concurrent DML
--echo ###################################################################
--echo case 3.1 has unique conflict
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b VARCHAR(10), c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_uk(b(4)), ADD KEY pre_sk(d(6)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '3333333', 6, '3c');
  INSERT INTO t1 VALUES(4, '2222333', 8, '4d');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected failure of duplicate entry
--error 1062
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

--error 1062
INSERT INTO t1 VALUES(4, 'xxxxxxx', 8, '4d');
SELECT * FROM t1;

--echo ####
--echo composite index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY com_uk(c, d), ADD KEY com_sk(b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '5555555', 10, '5e');
  INSERT INTO t1 VALUES(6, '6666666', 10, '5e');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected failure of duplicate entry
--error 1062
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

--error 1062
INSERT INTO t1 VALUES(5, 'yyyyyyy', 10, '5eeee');
SELECT * FROM t1;

--echo ####
--echo composite index with prefix
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_com_uk(c, d(4)), ADD KEY pre_com_sk(b(4), c), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(7, '7777777', 14, '7e7e7e7e');
  INSERT INTO t1 VALUES(8, '8888888', 14, '7e7e8f8f');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected failure of duplicate entry
--error 1062
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

--error 1062
INSERT INTO t1 VALUES(5, 'yyyyyyy', 10, '5e');
SELECT * FROM t1;

DROP TABLE t1;

--echo ###################################################################
--echo case 3.2 no unique conflict
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b VARCHAR(10), c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_uk(b(4)), ADD KEY pre_sk(d(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '3333333', 6, '3c');
  --error 1062
  INSERT INTO t1 VALUES(2, '3333222', 8, '3c');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY successfully
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(4, '3333333', 8, '3c');
SELECT * FROM t1;

--echo ####
--echo composite index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY com_uk(c, d), ADD KEY com_sk(b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(4, '4444444', 8, '4d');
  --error 1062
  INSERT INTO t1 VALUES(5, '4444555', 8, '5d');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY successfully
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(4, 'yyyyyyy', 8, '8c');
SELECT * FROM t1;

ALTER TABLE t1 DROP KEY com_uk, DROP KEY com_sk, ALGORITHM=INPLACE, LOCK=DEFAULT;

--echo ####
--echo composite index with prefix
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_com_uk(c, d(4)), ADD KEY pre_com_sk(b(4), c), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '5555555', 10, '5e5e5e5e');
  --error 1062
  INSERT INTO t1 VALUES(6, '5555666', 10, '5e5e6f6f');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY successfully
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(5, 'yyyyyyy', 10, '8c');
INSERT INTO t1 VALUES(6, 'yyyyyyy', 10, '8c');
SELECT * FROM t1;

ALTER TABLE t1 DROP KEY pre_uk, DROP KEY pre_com_uk, DROP KEY pre_sk, DROP KEY pre_com_sk, ALGORITHM=INPLACE, LOCK=DEFAULT;
DROP TABLE t1;

--echo ###################################################################
--echo case 4 prefix key on blob field
--echo ###################################################################
--echo case 4.1 has unique conflict
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b TEXT, c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_uk(b(4)), ADD KEY pre_sk(d(6)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '3333333', 6, '3c');
  INSERT INTO t1 VALUES(4, '2222333', 8, '4d');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected failure of duplicate entry
--error 1062
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

--error 1062
INSERT INTO t1 VALUES(4, 'xxxxxxx', 8, '4d');
SELECT * FROM t1;

DROP TABLE t1;

--echo ###################################################################
--echo case 4.2 no unique conflict
--echo ###################################################################
eval CREATE TABLE t1(id INT PRIMARY KEY, b TEXT, c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2, '1a');
INSERT INTO t1 VALUES(2, '2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD UNIQUE KEY pre_uk(b(4)), ADD KEY pre_sk(d(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '3333333', 6, '3c');
  --error 1062
  INSERT INTO t1 VALUES(2, '3333222', 8, '3c');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY successfully
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
--error 1062
INSERT INTO t1 VALUES(4, '3333333', 8, '3c');
SELECT * FROM t1;

DROP TABLE t1;

--echo ###################################################################
--echo case 4.3 has unique conflict on PRIMARY KEY
--echo ###################################################################
eval CREATE TABLE t1(b TEXT NOT NULL, c int, d CHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('1111111', 2, '1a');
INSERT INTO t1 VALUES('2222222', 4, '2b');

--echo ####
--echo prefix index
--echo ####
SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
SEND ALTER TABLE t1 ADD PRIMARY KEY(b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('3333333', 6, '3c');
  INSERT INTO t1 VALUES('3333444', 8, '4d');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected failure of duplicate entry
--error 1062
reap;
CHECK TABLE t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

SELECT * FROM t1;

DROP TABLE t1;

