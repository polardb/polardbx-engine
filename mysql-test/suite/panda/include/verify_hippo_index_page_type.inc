### Purpose ###
#
# Verify that the specified index has HIPPO_INDEX page type.
#
### Parameters ###
#
# $SCHEMA_NAME    Schema name (default: 'test')
# $TABLE_NAME     Table name
# $INDEX_NAME     Index name to verify
#

--disable_result_log
--disable_query_log

--let $tmp_schema = test
if($SCHEMA_NAME){
  --let $tmp_schema = $SCHEMA_NAME
}

--let $page_type = query_get_value(SELECT INDEX_PAGE_TYPE FROM information_schema.innodb_index_status WHERE schema_name='$tmp_schema' AND table_name='$TABLE_NAME' AND index_name='$INDEX_NAME', INDEX_PAGE_TYPE, 1)
--let $assert_pass = 0
if($page_type==HIPPO_INDEX){
  --let $assert_pass = 1
}
--let $assert_text = Index $INDEX_NAME page_type ($page_type) should be HIPPO_INDEX for table $TABLE_NAME
--let $assert_cond = $assert_pass = 1
--source include/assert.inc

--enable_result_log
--enable_query_log

