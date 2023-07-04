--source include/master-slave.inc

--echo ###############################
--echo # Non MTS mode
--echo ###############################
--source include/rpl_connection_slave.inc
--source include/stop_slave.inc
--disable_warnings
SET GLOBAL replica_parallel_workers = 0;
--enable_warnings
--source include/start_slave.inc

--source suite/innodb_gcn/t/feature_rpl_gcn.inc

--echo ###############################
--echo # MTS by DATABASE
--echo ###############################
--source include/rpl_connection_slave.inc
--source include/stop_slave.inc
SET GLOBAL replica_preserve_commit_order = 0;
--disable_warnings
SET GLOBAL replica_parallel_type = 'DATABASE';
SET GLOBAL replica_parallel_workers = 4;
--enable_warnings
--source include/start_slave.inc

--source suite/innodb_gcn/t/feature_rpl_gcn.inc

--echo ###############################
--echo # MTS by Logical_clock
--echo ###############################
--source include/rpl_connection_slave.inc
--source include/stop_slave.inc
SET GLOBAL replica_preserve_commit_order = default;
--disable_warnings
SET GLOBAL replica_parallel_type = 'LOGICAL_CLOCK';
SET GLOBAL replica_parallel_workers = 4;
--enable_warnings
--source include/start_slave.inc

--source suite/innodb_gcn/t/feature_rpl_gcn.inc

--echo ###############################
--echo # Restore variables
--echo ###############################
--source include/rpl_connection_slave.inc
--source include/stop_slave.inc
SET GLOBAL replica_preserve_commit_order = default;
--disable_warnings
SET GLOBAL replica_parallel_type = default;
SET GLOBAL replica_parallel_workers = default;
--enable_warnings
--source include/start_slave.inc

--source include/rpl_connection_master.inc
--source include/rpl_end.inc
