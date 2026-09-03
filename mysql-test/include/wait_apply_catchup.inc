# wait sync log
--sleep 1

let $enable_consensus = `SELECT @@enable_consensus`;
if ($enable_consensus)
{
  let $show_consensus_debug_info= 1
  # wait apply index = commit index
  let $wait_timeout= 120;
  let $wait_condition= select LAST_LOG_INDEX=LAST_APPLY_INDEX from information_schema.alisql_cluster_local limit 1;
  --source include/wait_condition.inc

  #LAST_APPLY_INDEX is not accurate, sleep 1s to ensure the condition is satisfied
  --sleep 1
}

