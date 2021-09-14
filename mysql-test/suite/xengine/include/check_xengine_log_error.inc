# check error in xengine log files

--let $MYSQL_DATADIR = `select @@datadir`
# if no ERROR log in log file, grep will return 1
# if no such error log file, will return 2
--error 0,1,2
--exec grep ' ERROR ' $MYSQL_DATADIR/.xengine/Log*

