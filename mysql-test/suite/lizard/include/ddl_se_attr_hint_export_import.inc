--let $original_db = `SELECT DATABASE()`
--let $new_db_name=ddl_se_attr_hint_test_db_1

--disable_warnings
--eval DROP DATABASE IF EXISTS $new_db_name
--eval CREATE DATABASE $new_db_name
--enable_warnings

--echo # Step 1: Get 'SHOW CREATE TABLE $original_db.t1 FOR EXPORT'
--let $create_t1_export_stmt = query_get_value(SHOW CREATE TABLE $original_db.t1 FOR EXPORT, Create Table, 1)

--echo # Show captured CREATE statement for $original_db.t1 ---
--echo $create_t1_export_stmt
--eval SELECT * FROM INFORMATION_SCHEMA.INNODB_INDEX_STATUS WHERE SCHEMA_NAME='$original_db' AND TABLE_NAME='t1';

--echo # Step 2: Switch to new database and create t1 using the DDL from $original_db.t1
--eval USE $new_db_name
--eval $create_t1_export_stmt  # This creates t1 in $new_db_name

--echo # Step 3: Discard tablespace of t1 (in $new_db_name)
ALTER TABLE t1 DISCARD TABLESPACE;

--echo # Step 4: Flush $original_db.t1 for export
--eval FLUSH TABLE $original_db.t1 FOR EXPORT # This also locks $original_db.t1.

--echo # Step 5: Prepare file paths and copy $original_db.t1's data files to a temporary location
--let $MYSQLD_DATADIR = `SELECT @@datadir`

--let $t1_orig_ibd_src = $MYSQLD_DATADIR/$original_db/t1.ibd
--let $t1_orig_cfg_src = $MYSQLD_DATADIR/$original_db/t1.cfg

--let $t1_new_db_ibd_dst = $MYSQLD_DATADIR/$new_db_name/t1.ibd
--let $t1_new_db_cfg_dst = $MYSQLD_DATADIR/$new_db_name/t1.cfg

--let $tmp_ibd_path = $MYSQLTEST_VARDIR/tmp/ddl_se_attr_hint_export_orig_t1.ibd
--let $tmp_cfg_path = $MYSQLTEST_VARDIR/tmp/ddl_se_attr_hint_export_orig_t1.cfg

--echo # Copy $original_db.t1.ibd from datadir to temporary location
--copy_file $t1_orig_ibd_src $tmp_ibd_path
--file_exists $tmp_ibd_path
--copy_file $t1_orig_cfg_src $tmp_cfg_path
--file_exists $tmp_cfg_path

--echo # Step 6: Unlock tables (for $original_db.t1)
UNLOCK TABLES;

--echo # Step 7: Place $original_db.t1's files (from temporary location) as $new_db_name.t1's files
--copy_file $tmp_ibd_path $t1_new_db_ibd_dst
--file_exists $t1_new_db_ibd_dst
--copy_file $tmp_cfg_path $t1_new_db_cfg_dst
--file_exists $t1_new_db_cfg_dst

--echo # Step 8: Import tablespace for t1 (in $new_db_name)
# This is the main operation to test.
ALTER TABLE t1 IMPORT TABLESPACE; # This is $new_db_name.t1

--echo # Step 9: Verify t1 (in $new_db_name)
--echo # Performing CHECK TABLE t1 (in $new_db_name)
CHECK TABLE t1;

--echo # Verifying row counts between $original_db.t1 and $new_db_name.t1
--let $count_t1_orig = query_get_value(SELECT COUNT(*) FROM $original_db.t1, COUNT(*), 1)
--let $count_t1_new = query_get_value(SELECT COUNT(*) FROM $new_db_name.t1, COUNT(*), 1)

if ($count_t1_orig != $count_t1_new) {
  --die "Row count mismatch: $original_db.t1 has $count_t1_orig rows, $new_db_name.t1 has $count_t1_new rows after import."
}

--echo # Show create table t1 (in $new_db_name)
SHOW CREATE TABLE t1 FOR EXPORT;
--eval SELECT * FROM INFORMATION_SCHEMA.INNODB_INDEX_STATUS WHERE SCHEMA_NAME='$new_db_name' AND TABLE_NAME='t1';

--echo # Step 10: Cleanup
--eval USE $original_db # Switch back to original DB before dropping the new one
--disable_warnings
--eval DROP DATABASE IF EXISTS $new_db_name
--enable_warnings

--remove_file $tmp_ibd_path
--remove_file $tmp_cfg_path
--eval USE $original_db

--echo # End of ddl_se_attr_hint_export_import.inc 
