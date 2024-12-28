let $enable_consensus = `SELECT @@enable_consensus`;
if ($enable_consensus)
{
  # wait become leader
  let $wait_timeout= 60;
  let $wait_condition= select count(*)=1 from information_schema.alisql_cluster_local where role='leader';
  --source include/wait_condition.inc

  # wait applyindex catch up
  let $wait_timeout= 60;
  let $wait_condition= select count(distinct applied_index)=1 from information_schema.alisql_cluster_global;
  --source include/wait_condition.inc
}
