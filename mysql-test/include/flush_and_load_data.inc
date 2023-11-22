
flush logs;
xa recover;

--disable_result_log
--disable_query_log

--eval $sql

begin;
--eval $sql
commit;

begin;
--eval $sql
rollback;

xa start 'xx';
--eval $sql
xa end 'xx';
xa commit 'xx' one phase;

xa start 'xx';
--eval $sql
xa end 'xx';
xa prepare 'xx';
xa commit 'xx';


xa start 'xx';
--eval $sql
xa end 'xx';
xa prepare 'xx';
xa rollback 'xx';

xa start 'xx';
--eval $sql
xa end 'xx';
xa prepare 'xx';

--enable_result_log
--enable_query_log

xa recover;

--echo finish load
