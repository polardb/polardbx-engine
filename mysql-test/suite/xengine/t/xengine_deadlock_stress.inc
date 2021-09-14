#
# Stress tests deadlock detection
#

--source include/big_test.inc
--source suite/xengine/include/have_xengine.inc

create user sbtest IDENTIFIED WITH mysql_native_password;
grant all privileges on *.* to 'sbtest'@'%';

create table t1 (a int primary key, b int) engine=xengine;

set @prior_xengine_lock_wait_timeout = @@xengine_lock_wait_timeout;
set @prior_xengine_deadlock_detect = @@xengine_deadlock_detect;
set global xengine_lock_wait_timeout = 100000;
set global xengine_deadlock_detect = ON;

exec python suite/xengine/t/xengine_deadlock_stress.py sbtest 127.0.0.1 $MASTER_MYPORT test t1 10000 10;

set global xengine_lock_wait_timeout = @prior_xengine_lock_wait_timeout;
set global xengine_deadlock_detect = @prior_xengine_deadlock_detect;

drop user 'sbtest'@'%';

drop table t1;
