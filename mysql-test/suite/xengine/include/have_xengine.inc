--disable_query_log
SET default_storage_engine=xengine;
--enable_query_log
if (`SELECT COUNT(*) = 0 FROM INFORMATION_SCHEMA.ENGINES WHERE engine = 'xengine' AND support IN ('DEFAULT')`)
{
  --skip Test requires default engine XEngine
}

# comment temporarily before add slave_gtid_info
#if (`select count(*) = 0 from information_schema.tables where engine='xengine' and table_name='slave_gtid_info'`)
#{
#  --skip Test requires default engine XEngine
#}
--disable_query_log
# Table statistics can vary depending on when the memtables are flushed, so
# flush them at the beginning of the test to ensure the test runs consistently.
set global xengine_force_flush_memtable_now = true;
--enable_query_log

