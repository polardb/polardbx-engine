### Purpose ###
#
# This inc file is invoked from lizard.gpp_redo test case.
#
# We check that the redo logs are written and applied correctly 
# when gpp mode is enabled and disabled.
#
#
### Parameters ###
#
# $PREPARE_STATEMENTS       SQL for preparing.
# $PRODUCE_REDO_STATEMENTS  SQL for producing redo log.
# $CLEANUP_STATEMENTS       SQL for cleaning up.

--let $orig_opt_index_format_gpp_enabled = `SELECT @@global.opt_index_format_gpp_enabled`

--echo # 1. Test GPP mode enabled
SET opt_index_format_gpp_enabled = TRUE;
--eval $PREPARE_STATEMENTS
SET GLOBAL innodb_log_checkpoint_now=1;

--echo # 1.1 Produce redo log.
SET GLOBAL innodb_checkpoint_disabled=1;
--source include/no_checkpoint_start.inc
--eval $PRODUCE_REDO_STATEMENTS

--let CLEANUP_IF_CHECKPOINT=$CLEANUP_STATEMENTS;
--source include/no_checkpoint_end.inc

--echo # 1.2 Apply redo log within n_gfields when restarting and crash.
--error 137
--exec $MYSQLD_CMD $MYSQLD_ARGS --debug=d,crash_if_gpp_in_redo

--echo # 1.3 Apply redo log within n_gfields when restarting and no crash.
--let $restart_parameters=
--let $expect_crash_on_start=0
--source include/start_mysqld.inc

--echo # 1.4 Clean up
SET DEBUG = '-d,crash_if_gpp_in_redo';
SET GLOBAL innodb_checkpoint_disabled=0;
--eval SET opt_index_format_gpp_enabled= $orig_opt_index_format_gpp_enabled
--eval $CLEANUP_STATEMENTS

--echo # 2. Test GPP mode disabled
SET opt_index_format_gpp_enabled = FALSE;
--eval $PREPARE_STATEMENTS

# Ensure gpp_enabled redo logs will not be recovered.
SET GLOBAL innodb_log_checkpoint_now=1;
--echo # 2.1 Produce redo log.
SET GLOBAL innodb_checkpoint_disabled=1;
--source include/no_checkpoint_start.inc
--eval $PRODUCE_REDO_STATEMENTS

--let CLEANUP_IF_CHECKPOINT=$CLEANUP_STATEMENTS;
--source include/no_checkpoint_end.inc

--echo # 2.2 Apply redo log within n_gfields when restarting and no crash.
--let $restart_parameters = restart: --debug=d,crash_if_gpp_in_redo
--let $expect_crash_on_start=0
--source include/start_mysqld.inc

--echo # 2.3 Clean up
SET DEBUG = '-d,crash_if_gpp_in_redo';
SET GLOBAL innodb_checkpoint_disabled=0;
--eval SET opt_index_format_gpp_enabled= $orig_opt_index_format_gpp_enabled
--eval $CLEANUP_STATEMENTS

