--echo =====================================================
--echo # Test limitation of VARBINARY
--echo =====================================================

--error 1074
CREATE TABLE t(a BINARY(256));

CREATE TABLE t(a BINARY(64));
INSERT INTO t VALUES(REPEAT('abcd', 16));
SELECT * FROM t;
DROP TABLE t;

CREATE TABLE t(a BINARY(255));
INSERT INTO t VALUES(REPEAT('abcde', 51));
SELECT * FROM t;
DROP TABLE t;

--error 1074
CREATE TABLE t(a VARBINARY(65536));

--echo =====================================================
--echo # with hidden primary key
--echo =====================================================
--echo # with nullable varbinary column
--echo # limit number of binary character is 65532(65535 - 1(for nullbits) - 2(store var length))
--echo =====================================================
--error 1118
CREATE TABLE t(a VARBINARY(65533));

CREATE TABLE t(a VARBINARY(65532));
INSERT INTO t VALUES(CONCAT('ab', REPEAT('abcde', 13106)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(3073));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('cd', REPEAT('abcde', 13106)));
  INSERT INTO t VALUES(CONCAT('ef', REPEAT('abcde', 13106)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SELECT LENGTH(a) FROM t;
SHOW CREATE TABLE t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(3073));
ALTER TABLE t ADD UNIQUE KEY(a(3072));

DROP TABLE t;

--echo =====================================================
--echo # with non-nullable varbinary column
--echo # limit number of binary characters is 65533(65535 - 2(store var length))
--echo =====================================================
--error 1118
CREATE TABLE t(a VARBINARY(65534) NOT NULL);

CREATE TABLE t(a VARBINARY(65533) NOT NULL);
INSERT INTO t VALUES(CONCAT('abc', REPEAT('abcde', 13106)));
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD KEY(a);
--error 1071
ALTER TABLE t ADD KEY(a(3073));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY ka(a(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(CONCAT('bcd', REPEAT('abcde', 13106)));
  INSERT INTO t VALUES(CONCAT('cde', REPEAT('abcde', 13106)));
  SELECT LENGTH(a) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT LENGTH(a) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(a(3073));
ALTER TABLE t ADD UNIQUE KEY(a(3072));

CHECK TABLE t;
SHOW CREATE TABLE t;

DROP TABLE t;

--echo =====================================================
--echo # with user defined primary key
--echo =====================================================
--echo # with non-nullable varbinary column
--echo # limit number of binary characters is 65528(65535 - 4(pk) - 1(for nullbits) - 2(store var length))
--echo =====================================================
--error 1118
CREATE TABLE t(a INT PRIMARY KEY, b VARBINARY(65529));

CREATE TABLE t(a INT PRIMARY KEY, b VARBINARY(65528));
INSERT INTO t VALUES(1, CONCAT('abc', REPEAT('abcde', 13105)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(3073));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bcd', REPEAT('abcde', 13105)));
  INSERT INTO t VALUES(3, CONCAT('cde', REPEAT('abcde', 13105)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(3073));

ALTER TABLE t ADD UNIQUE KEY(b(3072));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('def', REPEAT('abcde', 13105)));
  INSERT INTO t VALUES(5, CONCAT('efg', REPEAT('abcde', 13105)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bcd', REPEAT('abcde', 13105)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

DROP TABLE t;

--echo =====================================================
--echo # with non-nullable varbinary column
--echo # limit number of binary characters is 65529(65535 - 4(pk) - 2(store var length))
--echo =====================================================
--error 1118
CREATE TABLE t(a INT PRIMARY KEY, b VARBINARY(65530) NOT NULL);

CREATE TABLE t(a INT PRIMARY KEY, b VARBINARY(65529) NOT NULL);
INSERT INTO t VALUES(1, CONCAT('abcd', REPEAT('abcde', 13105)));
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD KEY(b);
--error 1071
ALTER TABLE t ADD KEY(b(3073));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD KEY kb(b(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(2, CONCAT('bcde', REPEAT('abcde', 13105)));
  INSERT INTO t VALUES(3, CONCAT('cdef', REPEAT('abcde', 13105)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

--error 1071
ALTER TABLE t ADD UNIQUE KEY(b(3073));
ALTER TABLE t ADD UNIQUE KEY(b(3072));

SET DEBUG_SYNC='xengine.inplace_create_sk_scan_base_begin SIGNAL begin_dml WAIT_FOR dml_end';
send ALTER TABLE t ADD UNIQUE KEY ukb(b(3072));

  --echo # Switch dml connection
  connection dml;
  SET DEBUG_SYNC= 'now WAIT_FOR begin_dml';
  INSERT INTO t VALUES(4, CONCAT('defg', REPEAT('abcde', 13105)));
  INSERT INTO t VALUES(5, CONCAT('efgh', REPEAT('abcde', 13105)));
  --error 1062
  INSERT INTO t VALUES(6, CONCAT('bcde', REPEAT('abcde', 13105)));
  SELECT a, LENGTH(b) FROM t;
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

connection default;
reap;
CHECK TABLE t;
SHOW CREATE TABLE t;
SELECT a, LENGTH(b) FROM t;

DROP TABLE t;

--echo =====================================================

