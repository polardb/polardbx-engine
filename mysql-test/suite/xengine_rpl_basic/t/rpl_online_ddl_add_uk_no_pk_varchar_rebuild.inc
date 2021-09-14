--echo ###################################################################
--echo # case 1: key1 in d1 duplicates with key2 in d1
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 values(1, '1');
INSERT INTO t1 values(2, '1');
ALTER TABLE t1 ADD INDEX t1_b(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--error 1062
ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 1.2: key1 in d1 duplicates with key2 in d1, but with a deletion
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 values(1, '1');
DELETE FROM t1 WHERE b='1';
INSERT INTO t1 values(2, '1');
ALTER TABLE t1 ADD INDEX t1_b(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Add unique index successfully

SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;


--echo ###################################################################
--echo # case 2.1: key1 in d1 duplicates with inserted key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 2.2: key1 in d1 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# key2
INSERT INTO t1 VALUES(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # update key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b=1 WHERE a=2;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 2.3: key1 in d1 duplicates with inserted key2 in d2 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b='1';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 2.4: key1 in d1 duplicates with inserted key2 in d2 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  DELETE FROM t1 WHERE b=1;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 2.5: key1 in d1 duplicates with multiple DELETE-insert on key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b='1';
  INSERT INTO t1 VALUES(3, '1');
  DELETE FROM t1 WHERE b='1';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 3.1: key1 in d1 duplicates with inserted key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 3.2: key1 in d1 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# key2
INSERT INTO t1 VALUES(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b='1' WHERE a=2;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 3.3: key1 in d1 duplicates with inserted key2 in d3 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b='1';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 3.4: key1 in d1 duplicates with inserted key2 in d3 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  DELETE FROM t1 WHERE b='1';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 4.1: key1 in d1 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 4.2: key1 in d1 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# key2
INSERT INTO t1 VALUES(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b='1' WHERE a=2;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 4.3: key1 in d1 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  DELETE FROM t1 WHERE b='1';
  INSERT INTO t1 VALUES(5, '1');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
#--error 1062
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 4.4: key1 in d1 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '1');
  DELETE FROM t1 WHERE b='1';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 5.1: key1 in d2 duplicates with inserted key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(4, '2');
  INSERT INTO t1 VALUES(5, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 5.2: key1 in d2 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key1
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(5, '2');
  UPDATE t1 SET b='2' WHERE a=1;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 5.3: key1 in d2 duplicates with inserted key2 in d2 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(4, '2');
  DELETE FROM t1 WHERE b='2';
  INSERT INTO t1 VALUES(5, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 5.4: key1 in d2 duplicates with inserted key2 in d2 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  INSERT INTO t1 VALUES(4, '2');
  INSERT INTO t1 VALUES(5, '2');
  DELETE FROM t1 WHERE b='2';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 6.1: key1 in d2 duplicates with inserted key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 6.2: key1 in d2 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  UPDATE t1 set b='2' WHERE a=1;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 6.3: key1 in d2 duplicates with inserted key2 in d3 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  DELETE FROM t1 WHERE b='2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 6.4: key1 in d2 duplicates with inserted key2 in d3 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  DELETE FROM t1 WHERE b='2';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 7.1: key1 in d2 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 7.2: key1 in d2 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  UPDATE t1 set b='2' WHERE a=1;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 7.3: key1 in d2 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  DELETE FROM t1 WHERE b='2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 7.4: key1 in d2 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  DELETE FROM t1 WHERE b='2';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 8.1: key1 in d3 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 8.2: key1 in d3 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  UPDATE t1 set b='2' WHERE a=1;
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 8.3: key1 in d3 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  DELETE FROM t1 WHERE b='2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 8.4: key1 in d3 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  INSERT INTO t1 VALUES(3, '2');
  DELETE FROM t1 WHERE b='2';
  SELECT * FROM t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 9.1: key1 in d4 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # key2
  INSERT INTO t1 VALUES(3, '2');

  SELECT * FROM t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 9.2: key1 in d3 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # key2
  UPDATE t1 set b='2' WHERE a=1;

  SELECT * FROM t1;
  # signal create sk ddl that key1 has been inserted and key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 9.3: key1 in d4 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  DELETE FROM t1 WHERE b='2';
  # key2
  INSERT INTO t1 VALUES(3, '2');

  SELECT * FROM t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
SELECT * FROM t1;
--echo ADD UNIQUE INDEX successfully
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

--echo ###################################################################
--echo # case 9.4: key1 in d4 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send ALTER TABLE t1 ADD COLUMN c VARCHAR(20) DEFAULT 'Hello' AFTER a, ADD UNIQUE INDEX t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  INSERT INTO t1 VALUES(2, '2');
  # key2
  INSERT INTO t1 VALUES(3, '2');
  DELETE FROM t1 WHERE b='2';
  SELECT * FROM t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
SELECT * FROM t1;
--echo failed to ADD UNIQUE INDEX
SELECT * FROM t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
DROP TABLE t1;

