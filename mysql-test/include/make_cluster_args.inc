# ==== Purpose ====
# initialize a new learner node
#
# --source include/make_cluster_args.inc



# --disable_query_log
let $myport_1=query_get_value(show variables like "port", Value, 1);
let $learner_consensus_port = query_get_value("select $myport_1+50+40 as c1", c1, 1);
# --enable_query_log
let $CLUSTER_ARGS=--cluster-id=1 --cluster-start-index=1 --cluster-info='127.0.0.1:$learner_consensus_port@1';