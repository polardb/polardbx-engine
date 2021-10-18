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
# CHECK_IS_AUTO_INCREMENT 0; select id from t_fat1;                                                                                                |
# CHECK_IS_AUTO_INCREMENT 0; select id from t_thin1;


start_delay:
    # Avoid that worker threads cause a server crash before reporters are started.
    # This leads often to STATUS_ENVIRONMENT_ERROR though a crash happened.
    { sleep 2; return undef };

query_init:
    start_delay; SET AUTOCOMMIT = 0; use rqg_fat_thin;

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
    check;

dml:
    INSERT INTO t_seq values(0);SET @id=last_insert_id();INSERT INTO t_fat1 VALUES (@id, { $str1 = "\"".$prng->string(100)."\"" }, { $str2 = "\"".$prng->string(100)."\"" }, { $str3 = "\"".$prng->string(100)."\"" }, { $str4 = "\"".$prng->string(100)."\"" });INSERT INTO t_fat2 VALUES (@id, $str1, $str2, $str3, $str4);INSERT INTO t_thin1 VALUES(@id, $str1);INSERT INTO t_thin2 VALUES(@id, $str2);INSERT INTO t_thin3 VALUES(@id, $str3);INSERT INTO t_thin4 VALUES(@id, $str4);


check:
    CHECK_COLUMN_EQUAL 0; select col1 from t_fat1 where id > _int[invariant] limit 10; select col1 from t_thin1 where id > _int[invariant] limit 10; |
    CHECK_COLUMN_EQUAL 0; select col2 from t_fat1 where id > _int[invariant] limit 10; select col2 from t_thin2 where id > _int[invariant] limit 10; |
    CHECK_COLUMN_EQUAL 0; select col3 from t_fat1 where id > _int[invariant] limit 10; select col3 from t_thin3 where id > _int[invariant] limit 10; |
    CHECK_COLUMN_EQUAL 0; select col4 from t_fat1 where id > _int[invariant] limit 10; select col4 from t_thin4 where id > _int[invariant] limit 10; |
    CHECK_EQUAL;  select * from t_fat1 where id > _int[invariant] limit 10; select * from t_fat2 where id > _int[invariant] limit 10;                |
    CHECK_EQUAL;  select count(*) from t_fat1; select count(*) from t_seq;

