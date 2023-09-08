
# wait sync log
--sleep 1

# wait applyindex catch up
let $wait_timeout= 60;
let $wait_condition= select count(distinct applied_index)=1 from information_schema.alisql_cluster_global;
--source include/wait_condition.inc

#LAST_APPLY_INDEX is not accurate, sleep 1s to ensure the condition is satisfied
--sleep 1
