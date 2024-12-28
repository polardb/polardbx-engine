
# wait sync log
--sleep 1

let $enable_consensus = `SELECT @@enable_consensus`;
if ($enable_consensus)
{
  # wait applyindex catch up
  let $wait_timeout= 60;
  let $wait_condition= select count(distinct applied_index)=1 from information_schema.alisql_cluster_global;
  --source include/wait_condition.inc
}
