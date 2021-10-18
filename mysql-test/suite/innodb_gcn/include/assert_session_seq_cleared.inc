# Check innodb_snapshot_seq and innodb_commit_seq are cleared

--let $invalid_seq= 18446744073709551615
--let $snapshot_seq= query_get_value(SELECT CAST(variable_value AS UNSIGNED) snapshot_seq FROM performance_schema.session_variables WHERE variable_name = 'innodb_snapshot_seq', snapshot_seq, 1)
--let $commit_seq= query_get_value(SELECT CAST(variable_value AS UNSIGNED) commit_seq FROM performance_schema.session_variables WHERE variable_name = 'innodb_commit_seq', commit_seq, 1)

--let $assert_text= Assert that innodb_snapshot_seq/innodb_commit_seq are cleared
--let $assert_cond= $snapshot_seq = $invalid_seq AND $commit_seq = $invalid_seq
--source include/assert.inc
