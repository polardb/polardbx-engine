################################################################################
# WL#6969    Additional concurrency tests for BGC
#
# Auxiliary file for group commit deadlock
#
#################################################################################
--echo #
--echo # Verify it on AFTER_SYNC wait point
--echo # ==============================================================
--echo # Flush error happens before AFTER_SYNC wait point. So
--echo #
--source include/rpl_connection_master.inc
SET @rpl_semi_sync_master_wait_point_save= @@GLOBAL.RPL_SEMI_SYNC_MASTER_WAIT_POINT;
SET GLOBAL rpl_semi_sync_master_wait_point= AFTER_SYNC;

--echo # Enable diff_table test
--let $enable_diff_table= 1
--echo # Test tables with XEngine storage engine when enableing diff_table
--echo # and simulating flush error
--let $engine = XENGINE
--source extra/rpl_tests/rpl_stress_test.inc

--echo #
--echo # Verify it on AFTER_COMMIT wait point
--echo # ==============================================================
--echo #
--source include/rpl_connection_master.inc
SET GLOBAL rpl_semi_sync_master_wait_point= AFTER_COMMIT;

--echo # Enable diff_table test
--let $enable_diff_table= 1
--echo # Test tables with XEngine storage engine when enableing diff_table
--echo # and simulating flush error
--let $engine = XENGINE
--source extra/rpl_tests/rpl_stress_test.inc

# cleanup
--source include/rpl_connection_master.inc
SET GLOBAL rpl_semi_sync_master_wait_point= @rpl_semi_sync_master_wait_point_save;
