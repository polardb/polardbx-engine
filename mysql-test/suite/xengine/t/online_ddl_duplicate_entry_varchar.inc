--echo ###################################################################
--echo case 4 duplicate entry on VARCHAR for DDL and online DML
--echo ###################################################################
--echo case 4.1 DDL to modify PRIMARY KEY to VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_copy_ddl_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_copy_ddl_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_unique_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 DROP PRIMARY KEY, ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the modify pk ddl enter the inplace_unique_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.2 DDL to ADD UNIQUE SECONDARY KEY on VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD UNIQUE KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the add uk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD UNIQUE KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the add uk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

eval CREATE TABLE t1(a INT PRIMARY KEY, b VARCHAR(10) NOT NULL, c int NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '2222', 4);

SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD UNIQUE KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to dml connection
  connection dml;
  # wait the add uk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(3, '2222', 6);
  SELECT * FROM t1;
  # signal modify pk DDL that DML finished
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
--error 1062
reap;
CHECK TABLE t1;
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

