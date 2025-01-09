
# ==== Usage ====
# --let $my_xid='1','',1
# --let $xa_conn=con1

# Parameters
# $my_xid
#   XID.

# Return
# $xa_prepare_uuid = `select col_uuid from temp_table`
# $xa_prepare_uba = `select col_uba from temp_table`

--echo call dbms_xa.prepare_with_trx_slot(#gtrid, #bqual, #formatID)
--disable_query_log
--output $MYSQLTEST_VARDIR/tmp/xa_proc_prepare.log
--eval call dbms_xa.prepare_with_trx_slot($my_xid)

--disable_connect_log
--connect(tmp_find_session, localhost, root,,)
connection tmp_find_session;

CREATE TEMPORARY TABLE temp_table (
    col_uuid VARCHAR(255),
    col_uba BIGINT
);

eval LOAD DATA INFILE '$MYSQLTEST_VARDIR/tmp/xa_proc_prepare.log'
INTO TABLE temp_table
FIELDS TERMINATED BY '\t'
LINES TERMINATED BY '\n'
IGNORE 1 LINES;

--let $xa_prepare_uuid = `select col_uuid from temp_table`
--let $xa_prepare_uba = `select col_uba from temp_table`
drop table temp_table;
--remove_file $MYSQLTEST_VARDIR/tmp/xa_proc_prepare.log
--enable_query_log

connection $xa_conn;

--disconnect tmp_find_session

--enable_connect_log
