--echo ###################################################################
--echo case 1 Modify a primary key
--echo ###################################################################
--echo case 1.1 Change from an INT column to a TEXT column
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b TEXT NOT NULL, c int) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2);
INSERT INTO t1 VALUES(2, '2222222', 4);

ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc

DROP TABLE t1;

--echo ###################################################################
--echo case 1.2 Change from an INT column to a TEXT column with conflict in base
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b TEXT NOT NULL, c int) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '1111', 4);

--error 1062
ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;


--echo ###################################################################
--echo case 2 Modify a primary key with concurrent DML
--echo ###################################################################
--echo case 2.1 Change from an INT column to a TEXT column with concurrent DML
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b TEXT NOT NULL, c int) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2);
INSERT INTO t1 VALUES(2, '2222222', 4);
SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '3333333', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected modify pk successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 2.2 Change from an INT column to a TEXT column but has duplication conflict with concurrent DML
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b TEXT NOT NULL, c int) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111111', 2);
INSERT INTO t1 VALUES(2, '2222222', 4);
SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected modify pk failed
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 2.3 Change from an INT column to TEXT column but has duplication conflict with UPDATE from concurrent DML
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b TEXT NOT NULL) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '111111');
INSERT INTO t1 VALUES(2, '222222');
SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b='222222' WHERE a=1;
  UPDATE t1 SET b='111111' WHERE a=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';
--echo # Switch to connection master
connection master;
# receive the result of ALTER TABLE, expected modify pk failed
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;


--echo ###################################################################
--echo case 3 Add new primary key
--echo ###################################################################
--echo case 3.1: Add new primary key with a TEXT column
--echo ###################################################################
eval CREATE TABLE t1(a INT, b TEXT NOT NULL) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222');
INSERT INTO t1 VALUES(2, '2222');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1111');
INSERT INTO t1 VALUES(2, '2222');
ALTER TABLE t1 ADD PRIMARY KEY(b(10)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 3.2: Add new primary key with two TEXT columns
--echo ###################################################################
eval CREATE TABLE t1(a INT, b TEXT NOT NULL, c TEXT NOT NULL) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222', '3333');
INSERT INTO t1 VALUES(2, '2222', '3333');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b(4), c(6)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '2222', '3333');
INSERT INTO t1 VALUES(2, '4444', '6666');
ALTER TABLE t1 ADD PRIMARY KEY(b(4), c(6)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;


--echo ###################################################################
--echo case 4 Add new primary key with concurrent DML
--echo ###################################################################
--echo case 4.0: no conflict
--echo ###################################################################
eval CREATE TABLE t1(c1 INT, c2 TEXT NOT NULL, c3 INT, KEY k_c1(c1)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, 'abc', 111);
INSERT INTO t1 VALUES(2, 'def', 222);
INSERT INTO t1 VALUES(3, 'ghi', 333);
SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(c2(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;
  # wait the online DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(6, '6666', 6), (7, '777777', 7);
  SELECT * FROM t1;
  UPDATE t1 SET c2='aaaa' WHERE c1 > 6;
  SELECT * FROM t1;
  UPDATE t1 SET c1=99 WHERE c1 > 6;
  SELECT * FROM t1;
  DELETE FROM t1 WHERE c1=1;
  SELECT * FROM t1;

  # signal create pk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1: key1 in d1 duplicates with key2 in d2
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('2222333', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1.1: key1 in d1 duplicates with key2 in d2, but with a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 where b=2;
  INSERT INTO t1 VALUES('22223333', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1.2: key1 in d1 duplicates with key2 in d2, but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('22223333', 3);
  DELETE FROM t1 where b>=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1.3: key1 in d1 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='2222111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1.4: key1 in d1 duplicates with updated key2 in d2 but with pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b=2;
  UPDATE t1 SET a='2222111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.1.5: key1 in d1 duplicates with updated key2 in d2 but with post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='2222111' WHERE b=1;
  DELETE FROM t1 WHERE b=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2: key1 in d1 duplicates with key2 in d3
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('2222333', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2.1: key1 in d1 duplicates with key2 in d3, but with a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 where b=2;
  INSERT INTO t1 VALUES('2222222', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2.2: key1 in d1 duplicates with key2 in d3, but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('2222333', 3);
  DELETE FROM t1 where b=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2.3: key1 in d1 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='22221111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2.4: key1 in d1 duplicates with updated key2 in d3 but with pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b=2;
  UPDATE t1 SET a='22221111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2.5: key1 in d1 duplicates with updated key2 in d3 but with post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='22221111' WHERE b=1;
  DELETE FROM t1 WHERE b=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3: key1 in d1 duplicates with key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('2222222', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3.1: key1 in d1 duplicates with key2 in d4, but with a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 where b=2;
  INSERT INTO t1 VALUES('2222333', 3);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3.2: key1 in d1 duplicates with key2 in d4, but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES('2222333', 3);
  DELETE FROM t1 where b=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3.3: key1 in d1 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='22221111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3.4: key1 in d1 duplicates with updated key2 in d4 but with pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b=2;
  UPDATE t1 SET a='22221111' WHERE b=1;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.3.5: key1 in d1 duplicates with updated key2 in d4 but with post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET a='22221111' WHERE b=1;
  DELETE FROM t1 WHERE b=2;
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.4: key1 in d2 duplicates with key2 in d2
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.4.1: key1 in d2 duplicates with key2 in d2 but has a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  DELETE FROM t1 WHERE b=3;
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.4.2: key1 in d2 duplicates with key2 in d2 but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  INSERT INTO t1 VALUES('3333333', 4);
  DELETE FROM t1 WHERE a='3333333';
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.4.3: key1 in d2 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and update key2
  INSERT INTO t1 VALUES('3333333', 3);
  UPDATE t1 SET a='3333222' WHERE b=1;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.5: key1 in d2 duplicates with key2 in d3
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin      SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.5.1: key1 in d2 duplicates with key2 in d3 but has a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin      SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  DELETE FROM t1 WHERE b=3;
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.5.2: key1 in d2 duplicates with key2 in d3 but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin      SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333333', 4);
  DELETE FROM t1 WHERE a='3333333';
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.5.3: key1 in d2 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin      SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  # update key2
  UPDATE t1 SET a='3333222' WHERE b=2;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.6: key1 in d2 duplicates with key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin     SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.6.1: key1 in d2 duplicates with key2 in d4 but has a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin     SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  DELETE FROM t1 WHERE b=3;
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.6.2: key1 in d2 duplicates with key2 in d4 but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin     SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333444', 4);
  DELETE FROM t1 WHERE b>2;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.6.3: key1 in d2 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin     SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  # update key2
  UPDATE t1 SET a='2222333' WHERE b=3;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.7: key1 in d3 duplicates with key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done  SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.7.1: key1 in d3 duplicates with key2 in d4 but has a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done  SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  DELETE FROM t1 WHERE b=3;
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.7.2: key1 in d3 duplicates with key2 in d4 but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done  SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  INSERT INTO t1 VALUES('3333444', 4);
  DELETE FROM t1 WHERE b>2;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.7.3: key1 in d3 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done  SIGNAL start_dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml1';
  # insert key1
  INSERT INTO t1 VALUES('3333333', 3);
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR start_dml2';
  # update key2
  UPDATE t1 SET a='33331111' WHERE b=1;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.8: key1 in d4 duplicates with key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  INSERT INTO t1 VALUES('3333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.8.1: key1 in d4 duplicates with key2 in d4 but has a pre deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('33333333', 3);
  DELETE FROM t1 WHERE b=3;
  INSERT INTO t1 VALUES('33333444', 4);
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.8.2: key1 in d4 duplicates with key2 in d4 but with a post deletion
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  INSERT INTO t1 VALUES('3333444', 4);
  DELETE FROM t1 WHERE b>2;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo case 4.8.3: key1 in d4 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1(a TEXT NOT NULL, b INT) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES('11111', 1);
INSERT INTO t1 VALUES('22222', 2);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD PRIMARY KEY(a(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection dml
  connection dml;

  # wait the DDL enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  # insert key1 and key2
  INSERT INTO t1 VALUES('3333333', 3);
  # update key2
  UPDATE t1 SET a='3333222' WHERE b=2;
  select * from t1;
  # signal DDL that the key1 and key2 have been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

