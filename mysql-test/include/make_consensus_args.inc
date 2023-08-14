# --source include/make_consensus_args.inc

if ($consensus_port == '')
{
	let $consensus_port = query_get_value("select @@port+40 as c1", c1, 1);
	--let $consensus_args= --cluster-start-index=1 --cluster-info='127.0.0.1:$consensus_port@1'
}

