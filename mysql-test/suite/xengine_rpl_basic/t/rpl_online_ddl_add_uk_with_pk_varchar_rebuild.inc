--echo ###################################################################
--echo # case 1: key1 in d1 duplicates with key2 in d1
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 values(1, '1');
INSERT INTO t1 values(2, '1');
alter table t1 add index t1_b(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--error 1062
alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 1.2: key1 in d1 duplicates with key2 in d1, but with a deletion
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
INSERT INTO t1 values(1, '1');
DELETE FROM t1 WHERE b='1';
INSERT INTO t1 values(2, '1');
alter table t1 add index t1_b(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;
--echo Add unique index successfully

select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;


--echo ###################################################################
--echo # case 2.1: key1 in d1 duplicates with inserted key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 2.2: key1 in d1 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# key2
insert into t1 values(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # update key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b=1 WHERE a=2;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 2.3: key1 in d1 duplicates with inserted key2 in d2 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  delete from t1 where b='1';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 2.4: key1 in d1 duplicates with inserted key2 in d2 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  delete from t1 where b=1;
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 2.5: key1 in d1 duplicates with multiple delete-insert on key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  delete from t1 where b='1';
  insert into t1 values(3, '1');
  delete from t1 where b='1';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 3.1: key1 in d1 duplicates with inserted key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 3.2: key1 in d1 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# key2
insert into t1 values(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b='1' WHERE a=2;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 3.3: key1 in d1 duplicates with inserted key2 in d3 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  delete from t1 where b='1';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 3.4: key1 in d1 duplicates with inserted key2 in d3 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  delete from t1 where b='1';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 4.1: key1 in d1 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 4.2: key1 in d1 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# key2
insert into t1 values(2, '2');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  UPDATE t1 SET b='1' WHERE a=2;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 4.3: key1 in d1 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  delete from t1 where b='1';
  insert into t1 values(5, '1');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
#--error 1062
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 4.4: key1 in d1 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_check_constraint_done point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '1');
  delete from t1 where b='1';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 5.1: key1 in d2 duplicates with inserted key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(4, '2');
  insert into t1 values(5, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 5.2: key1 in d2 duplicates with updated key2 in d2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key1
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(5, '2');
  UPDATE t1 SET b='2' WHERE a=1;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 5.3: key1 in d2 duplicates with inserted key2 in d2 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(4, '2');
  delete from t1 where b='2';
  insert into t1 values(5, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 5.4: key1 in d2 duplicates with inserted key2 in d2 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key1
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin SIGNAL start_dml WAIT_FOR dml_done';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  # insert key2
  # wait the create sk ddl enter the inplace_create_sk_scan_base_begin point
  SET DEBUG_SYNC= 'now WAIT_FOR start_dml';
  insert into t1 values(4, '2');
  insert into t1 values(5, '2');
  delete from t1 where b='2';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_done';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 6.1: key1 in d2 duplicates with inserted key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 6.2: key1 in d2 duplicates with updated key2 in d3
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  update t1 set b='2' where a=1;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 6.3: key1 in d2 duplicates with inserted key2 in d3 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  delete from t1 where b='2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 6.4: key1 in d2 duplicates with inserted key2 in d3 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  delete from t1 where b='2';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
# The duplicate entries are deleted, duplicate entry info will not be shown
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 7.1: key1 in d2 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 7.2: key1 in d2 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  update t1 set b='2' where a=1;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 7.3: key1 in d2 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  delete from t1 where b='2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 7.4: key1 in d2 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_scan_base_begin        SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  delete from t1 where b='2';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 8.1: key1 in d3 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 8.2: key1 in d3 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  update t1 set b='2' where a=1;
  select * from t1;
  # signal create sk ddl the key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 8.3: key1 in d3 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  delete from t1 where b='2';
  # key2
  insert into t1 values(3, '2');
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 8.4: key1 in d3 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_begin SIGNAL dml1 WAIT_FOR dml1_end';
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml2 WAIT_FOR dml2_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml1';
  # key1
  insert into t1 values(2, '2');
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml1_end';

  SET DEBUG_SYNC= 'now WAIT_FOR dml2';
  # key2
  insert into t1 values(3, '2');
  delete from t1 where b='2';
  select * from t1;
  # signal create sk ddl the key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml2_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 9.1: key1 in d4 duplicates with inserted key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  insert into t1 values(2, '2');
  # key2
  insert into t1 values(3, '2');

  select * from t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 9.2: key1 in d3 duplicates with updated key2 in d4
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
# key2
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  insert into t1 values(2, '2');
  # key2
  update t1 set b='2' where a=1;

  select * from t1;
  # signal create sk ddl that key1 has been inserted and key2 has been updated
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 9.3: key1 in d4 duplicates with inserted key2 in d4 but with a deletion before inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  insert into t1 values(2, '2');
  delete from t1 where b='2';
  # key2
  insert into t1 values(3, '2');

  select * from t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index successfully
reap;
select * from t1;
--echo add unique index successfully
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

--echo ###################################################################
--echo # case 9.4: key1 in d4 duplicates with inserted key2 in d4 but with a deletion after inserting key2
--echo ###################################################################
eval CREATE TABLE t1 (a INT PRIMARY KEY, b VARCHAR(10)) CHARSET $cs COLLATE $coll;
insert into t1 values(1, '1');
# set the wait point to insert key2
SET DEBUG_SYNC= 'xengine.inplace_create_sk_check_constraint_done  SIGNAL dml WAIT_FOR dml_end';
send alter table t1 add column c varchar(10) after a, add unique index t1_ub(b), ALGORITHM=INPLACE, LOCK=DEFAULT;

  --echo # Switch to connection con1
  connection con1;

  SET DEBUG_SYNC= 'now WAIT_FOR dml';
  # key1
  insert into t1 values(2, '2');
  # key2
  insert into t1 values(3, '2');
  delete from t1 where b='2';
  select * from t1;
  # signal create sk ddl the key1 and key2 has been inserted
  SET DEBUG_SYNC= 'now SIGNAL dml_end';

--echo # Switch to connection master
connection master;
# receive the result of alter table, expected create index fail
--error 1062
reap;
select * from t1;
--echo failed to add unique index
select * from t1;
--let $table_1= t1
--source ../include/online_ddl_checksum.inc
drop table t1;

