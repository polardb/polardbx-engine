
let $wait_condition=
  SELECT 1 FROM performance_schema.global_status WHERE
    VARIABLE_NAME like 'polarx_rpc_inited' AND
    VARIABLE_VALUE = '1';
--source include/wait_condition_or_abort.inc
