--connection leader
--let $diff_userauth_prev = SELECT convert(encdb_userauth_export(), CHAR);
--let $diff_rule_prev = SELECT convert(encdb_rule_export(), CHAR);

--connection conn2
--source include/wait_apply_catchup.inc
--let $diff_userauth_after = SELECT convert(encdb_userauth_export(), CHAR);
--let $diff_rule_after = SELECT convert(encdb_rule_export(), CHAR);
--let $assert_text=userauth_prev == userauth_after
--let $assert_cond="$diff_userauth_prev" = "$diff_userauth_after"
--source include/assert.inc
--let $assert_text=rule_prev == rule_after
--let $assert_cond="$diff_rule_prev" = "$diff_rule_after"
--source include/assert.inc

--connection conn3
--source include/wait_apply_catchup.inc
--let $diff_userauth_after = SELECT convert(encdb_userauth_export(), CHAR);
--let $diff_rule_after = SELECT convert(encdb_rule_export(), CHAR);
--let $assert_text=userauth_prev == userauth_after
--let $assert_cond="$diff_userauth_prev" = "$diff_userauth_after"
--source include/assert.inc
--let $assert_text=rule_prev == rule_after
--let $assert_cond="$diff_rule_prev" = "$diff_rule_after"
--source include/assert.inc

--connection leader