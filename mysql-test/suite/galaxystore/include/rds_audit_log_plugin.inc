CREATE TABLE alog(rec varchar(4096));
SET GLOBAL local_infile = ON;
SET GLOBAL rds_audit_log_flush = ON;
let $audit_log_file= `select VARIABLE_VALUE from performance_schema.global_status where variable_name = 'rds_audit_log_filename'`;

## Test log skip
set rds_audit_log_skip = on;
select "You can not see me in audit log";
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%You can not see me in audit log%';
set rds_audit_log_skip = off;

## Test password is masked
create user test_pass@'%' identified by 'I_am_a_secret_password';
truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%I_am_a_secret_password%';
drop user test_pass@'%';

## Test row limit
set global rds_audit_log_row_limit = 0;
select 'Audit log is full';
truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%Audit log is full%';
set global rds_audit_log_row_limit = 100000;

## Test log policy
SET GLOBAL rds_audit_log_policy = "LOGINS";
select 'I am query and should not be logged';
truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%I am query and should not be logged%';

SET GLOBAL rds_audit_log_policy = "ALL";
SET GLOBAL rds_audit_log_statement_policy = "UPDATES";
select 'I am select query and should not be logged';
truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%I am select query and should not be logged%';

--error ER_NO_SUCH_TABLE
insert into non_exist_table values('I am update query and should be logged');
truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) != 0 from alog where rec like '%I am update query and should be logged%';

## Test event scheduler is not logged
create table event_test (id int) engine = innodb;
drop event if exists event1;
create event event1 on schedule every 1 second starts now() ends date_add(now(), interval 5 hour) DO insert into event_test values(1);
--sleep 2
drop table event_test;
drop event event1;

truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%insert into event_test values%';

## test proc is not logged
create table proc_test(id int) engine = innodb;
DELIMITER //;
CREATE PROCEDURE proc1()
BEGIN
insert into proc_test values(1);
END
//
DELIMITER ;//
call proc1();
drop procedure proc1;
drop table proc_test;

truncate alog;
--disable_warnings
--disable_query_log
eval load data local infile '$audit_Log_file' into table alog FIELDS TERMINATED BY ',';
--enable_query_log
--enable_warnings
select count(*) = 0 from alog where rec like '%insert into proc_test values%';

DROP TABLE alog;
SET GLOBAL local_infile = OFF;
