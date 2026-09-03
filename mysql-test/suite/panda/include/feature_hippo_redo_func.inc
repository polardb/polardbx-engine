### Purpose ###
#
# This inc file is invoked from panda.feature_hippo_redo test case.
#
# We check that HIPPO-specific redo logs are written and applied correctly:
# 1. MLOG_PAGE_CREATE_HIPPO (88) - Create a hippo index page
# 2. MLOG_COMP_PAGE_CREATE_HIPPO (89) - Create a hippo compact index page
# 3. MLOG_REC_SEC_TID_DELETE_MARK (90) - Mark secondary index record with tid column deleted
#
### Parameters ###
#
# $PREPARE_STATEMENTS       SQL for preparing.
# $PRODUCE_REDO_STATEMENTS  SQL for producing redo log.
# $CLEANUP_STATEMENTS       SQL for cleaning up.
# $EXTRA_ARGS               extra arguments for mysqld

SET opt_index_format_hippo_enabled=on;

--echo # 1. Prepare: create table with HIPPO index
--eval $PREPARE_STATEMENTS
SET GLOBAL innodb_log_checkpoint_now=1;

--echo # 2. Disable checkpoint and produce redo log
SET GLOBAL innodb_checkpoint_disabled=1;
--source include/no_checkpoint_start.inc
--eval $PRODUCE_REDO_STATEMENTS

--let CLEANUP_IF_CHECKPOINT=$CLEANUP_STATEMENTS;
--source include/no_checkpoint_end.inc

--echo # 3.1 Apply redo log when restarting and crash (test redo apply idempotency)
--error 137
--exec $MYSQLD_CMD $MYSQLD_ARGS $EXTRA_ARGS --debug=d,crash_if_hippo_in_redo

--echo # 3.2 Apply redo log when restarting and no crash
--let $restart_parameters=restart: $EXTRA_ARGS
--source include/start_mysqld.inc

--echo # 4. Clean up
SET DEBUG = '-d,crash_if_hippo_in_redo';
SET GLOBAL innodb_checkpoint_disabled=0;
--eval $CLEANUP_STATEMENTS

