
--source include/have_polarx_rpc_plugin.inc
--source include/polarx_rpc_reset_and_wait.inc

--disable_query_log
--disable_result_log
TRUNCATE TABLE performance_schema.events_waits_history_long;
--enable_result_log
--enable_query_log
