# Copyright (c) 2012 Oracle and/or its affiliates. All rights reserved.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; version 2 of the License.
#
# This program is distributed in the hope that it will be useful, but
# WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
# General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301
# USA
#

start_delay:
    # Avoid that worker threads cause a server crash before reporters are started.
    # This leads often to STATUS_ENVIRONMENT_ERROR though a crash happened.
    { sleep 2; return undef };

query_init:
    start_delay ; SET AUTOCOMMIT = 0; use rqg_ddl;

stage:
    STAGE 0; alter table t1 add column col5 int default 0; GOTO STAGE 1;         |
    STAGE 0; alter table t1 add column col6 char(255) default 'nothing'; GOTO STAGE 2; |
    STAGE 1; alter table t1 drop column col5; GOTO STAGE 0;    |
    STAGE 2; alter table t1 drop column col6; GOTO STAGE 0;

query:
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    START TRANSACTION;dml;COMMIT; |
    check;

dml:
    STAGE 0;INSERT INTO t_seq values(0);SET @id=last_insert_id();INSERT INTO t1 VALUES (@id, _int[invariant], _int[invariant] , _int[invariant] , { $str = "\"instage0 ".$prng->string(100)."\"" });INSERT INTO t2 VALUES (@id, _int[invariant] , _int[invariant] , _int[invariant] , $str);  |
    STAGE 1;INSERT INTO t_seq values(0);SET @id=last_insert_id();INSERT INTO t1 VALUES (@id, _int[invariant], _int[invariant] , _int[invariant] , { $str = "\"instage1 ".$prng->string(100)."\"" }, 0);INSERT INTO t2 VALUES (@id, _int[invariant] , _int[invariant] , _int[invariant] , $str); |
    STAGE 2;INSERT INTO t_seq values(0);SET @id=last_insert_id();INSERT INTO t1 VALUES (@id, _int[invariant], _int[invariant] , _int[invariant] , { $str = "\"instage2 ".$prng->string(100)."\"" }, 'nothing');INSERT INTO t2 VALUES (@id, _int[invariant] , _int[invariant] , _int[invariant] , $str);


check:
    CHECK_COLUMN_EQUAL 1;  select * from t1 where col1 > _int[invariant] limit 10; select * from t2 where col2 > _int[invariant] limit 10;  |
    CHECK_SELF_EQUAL 1 3; select * from t1 where col1 > _int[invariant] limit 100;                                                          |
    CHECK_COLUMN_EQUAL 2;  select * from t1 where col1 > _int[invariant] limit 10; select * from t2 where col2 > _int[invariant] limit 10;  |
    CHECK_ROWS_EQUAL;  select * from t1 where col1 > _int[invariant] limit 10; select * from t2 where col2 > _int[invariant] limit 10;      |
    CHECK_ROWS_ZERO; select * from t1  where t1.col1 != t1.col2;                                                                            |
    CHECK_IS_ORDERED_BY_COLUMN 0; select * from t1 where col1 > _int[invariant]  order by col1 limit 10;                                    |
    STAGE 1; CHECK_EQUAL; select col5 from t1 where col1 >_int[invariant] limit 33; select 0 from t1 where col1 >_int[invariant] limit 33;  |
    STAGE 2; CHECK_EQUAL; select col6 from t1 where col1 >_int[invariant] limit 33; select 'nothing' from t1 where col1 >_int[invariant] limit 33;

