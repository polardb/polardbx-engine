# wait become leader
let $wait_timeout= 60;
let $wait_condition= select count(*)=1 from information_schema.alisql_cluster_local where role='leader';
--source include/wait_condition.inc

# wait applyindex catch up
let $wait_timeout= 60;
let $wait_condition= select count(distinct match_index)=1 && count(distinct applied_index)=1 && sum(match_index - applied_index) = 0 from information_schema.alisql_cluster_global where role<>'Leader';
--source include/wait_condition.inc