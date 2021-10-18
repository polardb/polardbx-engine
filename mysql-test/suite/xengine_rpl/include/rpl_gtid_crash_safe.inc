
-- let $engine = XENGINE 

call mtr.add_suppression("Recovery from master pos");

-- let $debug_option = crash_before_update_pos
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_after_update_pos_before_apply
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_before_writing_xid
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = half_binlogged_transaction
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_commit_before
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_commit_after_log
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_commit_after_prepare
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_reset.inc
-- let $debug_option = crash_commit_after
-- source suite/xengine_rpl/include/rpl_gtid_crash_safe_main.inc

-- source include/rpl_end.inc
