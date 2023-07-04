# set seesion variable 'base_seq' as current max_sequence
SET @base_seq = (SELECT CAST(variable_value AS UNSIGNED) FROM performance_schema.global_status WHERE variable_name = 'Lizard_current_gcn');
let $global_base_seq= query_get_value(select @base_seq as base_seq, base_seq, 1);
