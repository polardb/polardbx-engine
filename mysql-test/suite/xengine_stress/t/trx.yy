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
    start_delay ; SET AUTOCOMMIT = 0; use rqg_trx;

query:
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "READ-COMMITTED"; START TRANSACTION; add_or_reduce; COMMIT; |
    SET SESSION transaction_isolation = "REPEATABLE-READ"; START TRANSACTION; trade; COMMIT; |
    SET SESSION transaction_isolation = "REPEATABLE-READ"; START TRANSACTION; trade; COMMIT; |
    SET SESSION transaction_isolation = "REPEATABLE-READ"; START TRANSACTION; trade; COMMIT; |
    check;

add_or_reduce:
   add;     |
   reduce;

add:
    INSERT INTO t_warehouse VALUES (0, _int[invariant], { $attr1 = "\"".$prng->string(100)."\"" }, { $attr2 = "\"".$prng->string(100)."\"" });UPDATE t_total SET count = count + _int[invariant];

reduce:
    SET @id=last_insert_id() div 2 * 2; SELECT COALESCE(sum(count),0) INTO @count FROM t_warehouse where id = @id FOR UPDATE;DELETE FROM t_warehouse WHERE id = @id;UPDATE t_total SET count = count-@count;

trade:
    UPDATE t_warehouse SET count = count - _digit[invariant] WHERE id > _int[invariant] limit 2; UPDATE t_warehouse SET count = count + _digit[invariant] WHERE id > _int[invariant] limit 2;

check:
    CHECK_EQUAL; select COALESCE(sum(count),0) from t_warehouse; select sum(count) from t_total;

