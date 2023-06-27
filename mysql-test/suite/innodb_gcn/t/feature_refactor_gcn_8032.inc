--source suite/innodb_gcn/include/init_conn_base_seq.inc

create table tt (id int);

--echo ###############################
--echo # Case 1: commit detached XA
--echo ###############################
set xa_detach_on_prepare = on;
--let $commit_gcn = `select @base_seq + 100`

xa start '1';
insert into tt values (1);
xa end '1';
xa prepare '1';

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

xa commit '1';

--let $expect_gcn= $commit_gcn
--let $expect_state= COMMIT
--let $gtrid= '1'
--source suite/innodb_gcn/include/xa_proc_find_by_gtrid_verify.inc

--echo ###############################
--echo # Case 2: rollback detached XA
--echo ###############################
set xa_detach_on_prepare = on;
--let $commit_gcn = `select @base_seq + 200`

xa start '2';
insert into tt values (2);
xa end '2';
xa prepare '2';

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

xa rollback '2';

--let $expect_gcn= $commit_gcn
--let $expect_state= ROLLBACK
--let $gtrid= '2'
--source suite/innodb_gcn/include/xa_proc_find_by_gtrid_verify.inc

--echo ###############################
--echo # Case 3: commit attached XA
--echo ###############################
set xa_detach_on_prepare = off;
--let $commit_gcn = `select @base_seq + 300`

xa start '3';
insert into tt values (3);
xa end '3';
xa prepare '3';

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

xa commit '3';

--let $expect_gcn= $commit_gcn
--let $expect_state= COMMIT
--let $gtrid= '3'
--source suite/innodb_gcn/include/xa_proc_find_by_gtrid_verify.inc

--echo ###############################
--echo # Case 4: rollback attached XA
--echo ###############################
set xa_detach_on_prepare = off;
--let $commit_gcn = `select @base_seq + 400`

xa start '4';
insert into tt values (4);
xa end '4';
xa prepare '4';

--disable_query_log
--eval set innodb_commit_seq = $commit_gcn
--enable_query_log

xa rollback '4';

--let $expect_gcn= $commit_gcn
--let $expect_state= ROLLBACK
--let $gtrid= '4'
--source suite/innodb_gcn/include/xa_proc_find_by_gtrid_verify.inc

--echo ###############################
--echo # cleanup
--echo ###############################
drop table tt;
set xa_detach_on_prepare = default;
