### Purpose ###
#
# Helper include: assert that a named index has (or does not have) SPC
# page type recorded in the DD (INNODB_INDEX_STATUS).
#
# Mirrors suite/panda/include/panda_dd_consistent.inc but for SPC.
#
### Parameters ###
#
# $SCHEMA_NAME   schema (database) name; defaults to 'test' if not set
# $TABLE_NAME    table name to check
# $INDEX_NAME    index name to check
# $IS_SPC        1 if the index should be SPC_PAGE, 0 otherwise
#

--disable_result_log
--disable_query_log

# Default schema to 'test' if the caller did not set it
--let $_spc_schema = test
if ($SCHEMA_NAME) {
  --let $_spc_schema = $SCHEMA_NAME
}

--let $dd_page_type = query_get_value(SELECT INDEX_PAGE_TYPE FROM INFORMATION_SCHEMA.INNODB_INDEX_STATUS WHERE SCHEMA_NAME = '$_spc_schema' AND TABLE_NAME = '$TABLE_NAME' AND INDEX_NAME = '$INDEX_NAME', INDEX_PAGE_TYPE, 1)

--let $dd_is_spc = 0
if ($dd_page_type == SPC_PAGE) {
  --let $dd_is_spc = 1
}

--let $assert_pass = 0
if ($IS_SPC == $dd_is_spc) {
  --let $assert_pass = 1
}

--let $assert_text = INDEX_PAGE_TYPE for $_spc_schema.$TABLE_NAME.$INDEX_NAME should match IS_SPC=$IS_SPC (got $dd_page_type)
--let $assert_cond = $assert_pass = 1
--source include/assert.inc

--enable_result_log
--enable_query_log
