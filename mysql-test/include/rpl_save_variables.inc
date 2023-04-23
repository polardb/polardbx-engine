--disable_query_log
set @old_rpl_semi_sync_master_wait_for_slave_count = @@global.rpl_semi_sync_master_wait_for_slave_count;
set @old_rpl_semi_sync_master_timeout = @@global.rpl_semi_sync_master_timeout;
set @old_rpl_semi_sync_master_wait_point = @@global.rpl_semi_sync_master_wait_point;
set @old_rpl_semi_sync_master_enabled = @@global.rpl_semi_sync_master_enabled;
--enable_query_log
