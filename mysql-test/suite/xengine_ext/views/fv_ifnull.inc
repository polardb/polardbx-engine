--disable_query_log
eval SET @my_select = 'SELECT IFNULL($col_type,''IS_NULL''),
$col_type, id FROM t1_values';
--source suite/xengine_ext/views/fv1.inc
