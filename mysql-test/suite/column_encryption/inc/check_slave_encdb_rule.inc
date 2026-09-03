--disable_warnings

connection conn_master;

--let $rpl_diff_statement= SELECT encdb_userauth_export()
--source include/rpl_diff.inc
--let $rpl_diff_statement= SELECT encdb_rule_export()
--source include/rpl_diff.inc

--enable_warnings
