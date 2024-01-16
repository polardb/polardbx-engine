--source suite/innodb_gcn/include/init_conn_base_seq.inc

create table tt (id int);

--echo ###############################
--echo # Case 1: commit detached XA
--echo ###############################
set xa_detach_on_prepare = on;
--let $commit_gcn = `select @base_seq + 100`

--eval xa start '1$postfix'
insert into tt values (1);
--eval xa end '1$postfix'
--eval xa prepare '1$postfix'

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

--eval xa commit '1$postfix'

--let $expect_gcn= $commit_gcn
--let $expect_state= COMMIT
--let $my_xid= '1$postfix','',1
--source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc

--echo ###############################
--echo # Case 2: rollback detached XA
--echo ###############################
set xa_detach_on_prepare = on;
--let $commit_gcn = `select @base_seq + 200`

--eval xa start '2$postfix'
insert into tt values (2);
--eval xa end '2$postfix'
--eval xa prepare '2$postfix'

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

--eval xa rollback '2$postfix'

--let $expect_gcn= $commit_gcn
--let $expect_state= ROLLBACK
--let $my_xid= '2$postfix','',1
--source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc

--echo ###############################
--echo # Case 3: commit attached XA
--echo ###############################
set xa_detach_on_prepare = off;
--let $commit_gcn = `select @base_seq + 300`

--eval xa start '3$postfix'
insert into tt values (3);
--eval xa end '3$postfix'
--eval xa prepare '3$postfix'

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

--eval xa commit '3$postfix'

--let $expect_gcn= $commit_gcn
--let $expect_state= COMMIT
--let $my_xid= '3$postfix','',1
--source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc

--echo ###############################
--echo # Case 4: rollback attached XA
--echo ###############################
set xa_detach_on_prepare = off;
--let $commit_gcn = `select @base_seq + 400`

--eval xa start '4$postfix'
insert into tt values (4);
--eval xa end '4$postfix'
--eval xa prepare '4$postfix'

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

--eval xa rollback '4$postfix'

--let $expect_gcn= $commit_gcn
--let $expect_state= ROLLBACK
--let $my_xid= '4$postfix','',1
--source suite/innodb_gcn/include/xa_proc_find_by_xid_verify.inc

--echo ###############################
--echo # cleanup
--echo ###############################
drop table tt;
set xa_detach_on_prepare = default;
