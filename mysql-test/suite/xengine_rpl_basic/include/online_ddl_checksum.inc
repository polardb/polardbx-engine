
--source include/rpl_sync.inc
connection master;
eval show create table $table_1;
--let $checksum_master_table_1= query_get_value(checksum table $table_1, Checksum, 1)

connection slave;
eval show create table $table_1;
--let $checksum_slave_table_1= query_get_value(checksum table $table_1, Checksum, 1)

--let $assert_text= master-slave $table_1 is same
--let $assert_cond=$checksum_master_table_1=$checksum_slave_table_1
--source include/assert.inc

connection master;
