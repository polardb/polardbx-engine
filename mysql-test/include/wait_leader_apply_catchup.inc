
# wait sync log
--sleep 1

# wait applyindex catch up
let $wait_timeout= 60;
let $wait_condition= select count(distinct match_index)=1 && sum(match_index - applied_index) <= 1 && count(distinct applied_index)<=2 from information_schema.alisql_cluster_global;
--source include/wait_condition.inc

#LAST_APPLY_INDEX is not accurate, sleep 1s to ensure the condition is satisfied
--sleep 1
