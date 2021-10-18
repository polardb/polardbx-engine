if (!$coll) {
  --echo Using default collation utf8mb4_0900_ai_ci
  --let $coll= utf8mb4_0900_ai_ci
}

--echo =====================================================
--echo # Test limitation of VARCHAR with utf8mb4 & $coll
--echo =====================================================

eval CREATE TABLE t(a CHAR(64)) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(REPEAT('abcd', 16));
SELECT * FROM t;
DROP TABLE t;

eval CREATE TABLE t(a CHAR(255)) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(REPEAT('abcde', 51));
SELECT * FROM t;
DROP TABLE t;

--error 1074
eval CREATE TABLE t(a CHAR(256)) CHARSET utf8mb4 COLLATE $coll;

--echo =====================================================
--echo # with hidden primary key
--echo =====================================================
--echo # with nullable varchar column
--echo # limit number of characters is 16383((65535 - 1(for nullbits) - 2(store var length))/4)
--echo =====================================================
--error 1074
eval CREATE TABLE t(a VARCHAR(16384)) CHARSET utf8mb4 COLLATE $coll;

eval CREATE TABLE t(a VARCHAR(16383)) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(CONCAT('abc', REPEAT('abcde', 3276)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(769));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('bcd', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(CONCAT('cde', REPEAT('abcde', 3276)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(769));

ALTER TABLE t ADD UNIQUE KEY uka(a(768));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY uka2(a(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('efg', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(CONCAT('fgh', REPEAT('abcde', 3276)));
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
--echo # limit number of characters is 16383((65535 - 2(store var length))/4)
--echo =====================================================
--error 1074
eval CREATE TABLE t(a VARCHAR(16384) NOT NULL) CHARSET utf8mb4 COLLATE $coll;

eval CREATE TABLE t(a VARCHAR(16383) NOT NULL) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(CONCAT('abc', REPEAT('abcde', 3276)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(769));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('bcd', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(CONCAT('cde', REPEAT('abcde', 3276)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;


CHECK TABLE t;
SHOW CREATE TABLE t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(769));

ALTER TABLE t ADD UNIQUE KEY uka(a(768));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t DROP KEY uka, ADD PRIMARY KEY(a(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('def', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(CONCAT('efg', REPEAT('abcde', 3276)));
  --error 1062
  INSERT INTO t VALUES(CONCAT('bcd', REPEAT('abcde', 3276)));
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
--echo # with non-nullable varchar column
--echo # limit number of characters is 16382((65535 - 4(pk) - 1(for nullbits) - 2(store var length))/4)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(16383)) CHARSET utf8mb4 COLLATE $coll;

eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(16382)) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(1, CONCAT('ab', REPEAT('abcde', 3276)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(769));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bc', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(3, CONCAT('cd', REPEAT('abcde', 3276)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(769));

ALTER TABLE t ADD UNIQUE KEY(b(768));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('de', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(5, CONCAT('ef', REPEAT('abcde', 3276)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bc', REPEAT('abcde', 3276)));
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
--echo # limit number of charactesr is 16382((65535 - 4(pk) - 2(store var length))/3 = 16382)
--echo =====================================================
--error 1118
eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(16383) NOT NULL) CHARSET utf8mb4 COLLATE $coll;

eval CREATE TABLE t(a INT PRIMARY KEY, b VARCHAR(16382) NOT NULL) CHARSET utf8mb4 COLLATE $coll;
INSERT INTO t VALUES(1, CONCAT('ab', REPEAT('abcde', 3276)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(769));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bc', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(3, CONCAT('cd', REPEAT('abcde', 3276)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(769));

ALTER TABLE t ADD UNIQUE KEY(b(768));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('de', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(5, CONCAT('ef', REPEAT('abcde', 3276)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bc', REPEAT('abcde', 3276)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t DROP PRIMARY KEY, ADD PRIMARY KEY(b(769));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t DROP KEY b, DROP KEY ukb, DROP PRIMARY KEY, ADD PRIMARY KEY(b(768));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(6, CONCAT('fg', REPEAT('abcde', 3276)));
  INSERT INTO t VALUES(7, CONCAT('gh', REPEAT('abcde', 3276)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

DROP TABLE t;

--echo =====================================================

eval CREATE TABLE t1(id INT PRIMARY KEY, b char(10), c int, d CHAR(10))ENGINE=XENGINE CHARSET utf8mb4 COLLATE $coll;
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


eval CREATE TABLE t1(id INT PRIMARY KEY, b varchar(10), c int, d CHAR(10))ENGINE=XENGINE CHARSET utf8mb4 COLLATE $coll;
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

--echo =====================================================

