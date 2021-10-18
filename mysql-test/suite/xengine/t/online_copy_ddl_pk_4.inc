--echo ###################################################################
--echo case 4.3: Add new primary key with a CHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT, b CHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2');
INSERT INTO t1 VALUES(2, '2');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1');
INSERT INTO t1 VALUES(2, '2');
ALTER TABLE t1 ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.4: Add new primary key with two CHAR columns
--echo ###################################################################
eval CREATE TABLE t1(a INT, b CHAR(10) NOT NULL, c CHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2', '2');
INSERT INTO t1 VALUES(2, '2', '2');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1', '2');
INSERT INTO t1 VALUES(2, '2', '4');
ALTER TABLE t1 ADD PRIMARY KEY(b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.5: Add new primary key with a VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT, b VARCHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222');
INSERT INTO t1 VALUES(2, '2222');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1111');
INSERT INTO t1 VALUES(2, '2222');
ALTER TABLE t1 ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.6: Add new primary key with two VARCHAR columns
--echo ###################################################################
eval CREATE TABLE t1(a INT, b VARCHAR(10) NOT NULL, c VARCHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222', '3333');
INSERT INTO t1 VALUES(2, '2222', '3333');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '2222', '3333');
INSERT INTO t1 VALUES(2, '4444', '6666');
ALTER TABLE t1 ADD PRIMARY KEY(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.7: Add new primary key with a part of a VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT, b VARCHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222111');
INSERT INTO t1 VALUES(2, '2222222');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1111111');
INSERT INTO t1 VALUES(2, '2222222');
ALTER TABLE t1 ADD PRIMARY KEY(b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.8: Add new primary key with a part of two VARCHAR columns
--echo ###################################################################
eval CREATE TABLE t1(a INT, b VARCHAR(10) NOT NULL, c VARCHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(1, '2222111', '2222333');
INSERT INTO t1 VALUES(2, '2222222', '2222666');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(b(4), c(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '2222222', '3333111');
INSERT INTO t1 VALUES(2, '4444444', '6666111');
ALTER TABLE t1 ADD PRIMARY KEY(b(4), c(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.9: Add new primary key with an INT column and a CHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT NOT NULL, b CHAR(10) NOT NULL)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(2, '2');
INSERT INTO t1 VALUES(2, '2');
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(a, b), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1');
INSERT INTO t1 VALUES(2, '2');
ALTER TABLE t1 ADD PRIMARY KEY(a, b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.10: Add new primary key with an INT column and a VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT NOT NULL, b VARCHAR(10) NOT NULL, c INT)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(2, '2222', 1);
INSERT INTO t1 VALUES(2, '2222', 2);
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(a, b), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE  FROM t1;
INSERT INTO t1 VALUES(1, '1111', 2);
INSERT INTO t1 VALUES(2, '1111', 4);
ALTER TABLE t1 ADD PRIMARY KEY(a, b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.11: Add new primary key with an INT column and a part of VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT NOT NULL, b VARCHAR(10) NOT NULL, c INT)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(2, '2222111', 2);
INSERT INTO t1 VALUES(2, '2222222', 4);
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(a, b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1111111', 2);
INSERT INTO t1 VALUES(2, '2222222', 4);
ALTER TABLE t1 ADD PRIMARY KEY(a, b(4)), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

--echo ###################################################################
--echo case 4.12: Add new primary key with an INT column, a CHAR column and a VARCHAR column
--echo ###################################################################
eval CREATE TABLE t1(a INT NOT NULL, b CHAR(10) NOT NULL, c VARCHAR(100) NOT NULL, d INT)CHARSET $cs COLLATE $coll;
INSERT INTO t1 VALUES(2, '2222', '222222', 1);
INSERT INTO t1 VALUES(2, '2222', '222222', 2);
--error 1062
ALTER TABLE t1 ADD PRIMARY KEY(a, b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;
SHOW INDEX FROM t1;
SELECT * FROM t1;

DELETE FROM t1;
INSERT INTO t1 VALUES(1, '1111', '111111', 1);
INSERT INTO t1 VALUES(2, '2222', '222222', 2);
ALTER TABLE t1 ADD PRIMARY KEY(a, b, c), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Adding a primary key successfully
SELECT * FROM t1;
SHOW CREATE TABLE t1;
DROP TABLE t1;

