--disable_query_log
eval SET @my_select = 'SELECT IF($col_type, ''IS     TRUE'', ''IS NOT TRUE''),
$col_type, id FROM t1_values';
--source suite/xengine_ext/views/fv1.inc
