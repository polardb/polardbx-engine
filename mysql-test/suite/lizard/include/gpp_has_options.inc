### Purpose ###
#
# We check that the options of index contain gpp or not.
#
### Parameters ###
#
# $SCHEMA_NAME
# $TABLE_NAME
# $FULL_TABLE_NAME
# $INDEX_NAME
# $IS_PARTITION
# $PK_COL
# $INDEX_COL          column name of the index
# $INDEX_COL_MIN_VAL  minimum value of the index column
# $INDEX_COL_MAX_VAL  maximum value of the index column
# $NON_INDEX_COL      column name which does not be included in the index
# $CONTAIN_GPP        1 or 0, the specified index should contain gpp or not

# Execute the query and store the result into a user variable
--disable_result_log
--disable_query_log
--let $tmp_table_name=$TABLE_NAME
if($FULL_TABLE_NAME){
  --let $tmp_table_name = $FULL_TABLE_NAME
}
--let $has_gpp = query_get_value(SELECT gpp_enabled FROM information_schema.innodb_index_status WHERE SCHEMA_NAME = '$SCHEMA_NAME' AND TABLE_NAME = '$tmp_table_name' AND INDEX_NAME = '$INDEX_NAME', gpp_enabled, 1)

# Assert that the gpp option is equal to CONTAIN_GPP
--let $assert_text = The options for $SCHEMA_NAME.$TABLE_NAME.$INDEX_NAME should match the expected IFT=$CONTAIN_GPP condition
--let $assert_cond = $has_gpp = $CONTAIN_GPP
--source include/assert.inc

# Test actual gpp.
if ($INDEX_COL!='') {
  call dbms_stat.flush_gpp();
  if ($IS_PARTITION==1) {
    --eval select $NON_INDEX_COL from $SCHEMA_NAME.$TABLE_NAME force index ($INDEX_NAME) where $PK_COL>=$INDEX_COL_MIN_VAL and $PK_COL<=$INDEX_COL_MAX_VAL and $INDEX_COL >= $INDEX_COL_MIN_VAL and $INDEX_COL <= $INDEX_COL_MAX_VAL;
  }
  if ($IS_PARTITION==0) {
    --eval select $NON_INDEX_COL from $SCHEMA_NAME.$TABLE_NAME force index ($INDEX_NAME) where $INDEX_COL >= $INDEX_COL_MIN_VAL and $INDEX_COL <= $INDEX_COL_MAX_VAL;
  }
 
  --let $guess_hit= query_get_value(show status like "Lizard_index_scan_guess_clust_hit", Value, 1)
  --let $guess_miss=query_get_value(show status like "Lizard_index_scan_guess_clust_miss", Value, 1)
  --let $actual_gpp=0
  if($guess_hit!=0){
    --let $actual_gpp = 1
  }
  if($guess_miss!=0){
    --let $actual_gpp = 1
  }

  --let $assert_pass=0
  if($actual_gpp == $CONTAIN_GPP){
    --let $assert_pass = 1
  }

  --let $assert_text = The actual gpp hit/miss for $SCHEMA_NAME.$TABLE_NAME.$INDEX_NAME should match the expected IFT=$CONTAIN_GPP condition
  --let $assert_cond = $assert_pass = 1
  --source include/assert.inc
}

# Clean up
--enable_result_log
--enable_query_log
