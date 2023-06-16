# set seesion variable 'base_seq' as current max_sequence
SET @base_seq = (SELECT CAST(variable_value AS UNSIGNED) FROM performance_schema.global_status WHERE variable_name = 'Innodb_max_sequence');
