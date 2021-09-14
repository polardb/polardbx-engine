#if (!$coll) {
#  --echo Using default collation gbk_bin
#  --let $coll= gbk_bin
#}

--echo =====================================================
--echo # Test limitation of VARCHAR with gbk & $coll
--echo =====================================================

eval CREATE TABLE t(a CHAR(64)) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(REPEAT('abcd', 16));
SELECT * FROM t;
DROP TABLE t;

eval CREATE TABLE t(a CHAR(255)) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(REPEAT('abcde', 51));
SELECT * FROM t;
DROP TABLE t;

--error 1074
eval CREATE TABLE t(a CHAR(256)) CHARSET gbk COLLATE $coll;

--echo =====================================================
--echo # with hidden primary key
--echo =====================================================
--echo # with nullable varchar column
--echo # limit number of characters is 32766((65535 - 1(for nullbits) - 2(store var length))/2)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a VARCHAR(32767)) CHARSET gbk COLLATE $coll;

eval CREATE TABLE t(a VARCHAR(32766)) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(CONCAT('a', REPEAT('abcde', 6553)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(1537));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('b', REPEAT('abcde', 6553)));
  INSERT INTO t VALUES(CONCAT('c', REPEAT('abcde', 6553)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(1537));
ALTER TABLE t ADD UNIQUE KEY uka(a(1536));

--error 1062
INSERT INTO t VALUES(CONCAT('c', REPEAT('abcde', 6553)));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY uka2(a(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('d', REPEAT('abcde', 6553)));
  INSERT INTO t VALUES(CONCAT('e', REPEAT('abcde', 6553)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

DROP TABLE t;

--echo =====================================================
--echo # with non-nullable varchar column
--echo # limit number of characters is 32766((65535 - 2(store var length))/2)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a VARCHAR(32767) NOT NULL) CHARSET gbk COLLATE $coll;

eval CREATE TABLE t(a VARCHAR(32766) NOT NULL) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(CONCAT('a', REPEAT('abcde', 6553)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(1537));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('b', REPEAT('abcde', 6553)));
  INSERT INTO t VALUES(CONCAT('c', REPEAT('abcde', 6553)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(1537));
ALTER TABLE t ADD UNIQUE KEY uka(a(1536));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t DROP KEY uka, ADD PRIMARY KEY(a(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('d', REPEAT('abcde', 6553)));
  INSERT INTO t VALUES(CONCAT('e', REPEAT('abcde', 6553)));
  --error 1062
  INSERT INTO t VALUES(CONCAT('b', REPEAT('abcde', 6553)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

DROP TABLE t;

--echo =====================================================
--echo # with user defined primary key
--echo =====================================================
--echo # with nullable varchar column
--echo # limit number of characters is 32764((65535 - 4(pk) - 1(for nullbits) - 2(store var length))/2)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32765)) CHARSET gbk COLLATE $coll;

eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32764)) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(1, CONCAT('abcd', REPEAT('abcde', 6552)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(1537));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bcde', REPEAT('abcde', 6552)));
  INSERT INTO t VALUES(3, CONCAT('cdef', REPEAT('abcde', 6552)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(1537));

ALTER TABLE t ADD UNIQUE KEY(b(1536));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('defg', REPEAT('abcde', 6552)));
  INSERT INTO t VALUES(5, CONCAT('efgh', REPEAT('abcde', 6552)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bcde', REPEAT('abcde', 6552)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

DROP TABLE t;

--echo =====================================================
--echo # with non-nullable varchar column
--echo # limit number of characters is 32764((65535 - 4(pk) - 2(store var length))/2)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32765) NOT NULL) CHARSET gbk COLLATE $coll;

eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(32764) NOT NULL) CHARSET gbk COLLATE $coll;
INSERT INTO t VALUES(1, CONCAT('abcd', REPEAT('abcde', 6552)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(1537));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bcde', REPEAT('abcde', 6552)));
  INSERT INTO t VALUES(3, CONCAT('cdef', REPEAT('abcde', 6552)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(1537));

ALTER TABLE t ADD UNIQUE KEY(b(1536));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('defg', REPEAT('abcde', 6552)));
  INSERT INTO t VALUES(5, CONCAT('efgh', REPEAT('abcde', 6552)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bcde', REPEAT('abcde', 6552)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(b(1537));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t DROP KEY b, DROP KEY ukb, DROP PRIMARY KEY, ADD PRIMARY KEY(b(1536));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(6, CONCAT('fghi', REPEAT('abcde', 6552)));
  INSERT INTO t VALUES(7, CONCAT('ghij', REPEAT('abcde', 6552)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

DROP TABLE t;

--echo =====================================================

eval CREATE TABLE t1(id INT PRIMARY KEY, b char(10), c int, d CHAR(10))ENGINE=XENGINE CHARSET gbk COLLATE $coll;
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
  INSERT INTO t1 VALUES(4, '3333222', 8, '3c');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY failed
--error 1062
reap;
CHECK TABLE t1;
SHOW CREATE TABLE t1;
SELECT * FROM t1;
drop table t1;


eval CREATE TABLE t1(id INT PRIMARY KEY, b varchar(10), c int, d CHAR(10))ENGINE=XENGINE CHARSET gbk COLLATE $coll;
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
  INSERT INTO t1 VALUES(4, '3333222', 8, '3c');
  SELECT * FROM t1;
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection default
connection default;
# receive the result of ALTER TABLE, expected adding UNIQUE KEY failed
--error 1062
reap;
CHECK TABLE t1;
SHOW CREATE TABLE t1;
SELECT * FROM t1;

drop table t1;
