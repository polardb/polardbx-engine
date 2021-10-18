--source include/rpl_sync.inc

connection slave;
--let $sbm = query_get_value(SHOW SLAVE STATUS, Seconds_Behind_Master, 1)
while ($sbm) {
  --echo < Seconds_Behind_Master is $sbm in slave >
  --sleep 5
  --source include/rpl_sync.inc
  --let $sbm = query_get_value(SHOW SLAVE STATUS, Seconds_Behind_Master, 1)
}
connection master;

--echo # 1. due to unknonw issue, checksum may be different but it is same when
--echo #    doing manually checksum, so stop using checksum here
#connection slave;
# SHOW CREATE TABLE output varies because column orders
#--let $checksum_slave=query_get_value(checksum table $TARGET_DB.$TARGET_TABLE, Checksum, 1)

#connection master;
# SHOW CREATE TABLE output varies because column orders
#--let $checksum_master=query_get_value(checksum table $TARGET_DB.$TARGET_TABLE, Checksum, 1)

#--let $assert_text= checksum of $TARGET_DB.$TARGET_TABLE in master and slave is same
#--let $assert_cond=$checksum_master=$checksum_slave
#--source include/assert.inc

--echo # 2. use rpl_diff.inc to check query results on TARGET_TABLE, while
--echo #    records scan order may be different between master and slave,
--echo #    so order records with all columns
--let $rpl_diff_statement = SELECT * FROM $TARGET_TABLE ORDER BY col1, col2, col3, col4, col5, col6
--let $rpl_diff_database = $TARGET_DB
--source include/rpl_diff.inc

