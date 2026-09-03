#
# wl#7277: InnoDB: Bulk Load for Create Index
#


# This test case needs to crash the server. Needs a debug server.
-- source include/have_debug.inc

# Don't test this under valgrind, memory leaks will occur.
-- source include/not_valgrind.inc

# Avoid CrashReporter popup on Mac
-- source include/not_crashrep.inc


# Create Insert Procedure
DELIMITER |;
CREATE PROCEDURE populate_spc_table()
BEGIN
	DECLARE i int DEFAULT 1;

	START TRANSACTION;
	WHILE (i <= 10000) DO
		INSERT INTO spc_table VALUES (i, CONCAT('name', i), CONCAT('address', i));
		SET i = i + 1;
	END WHILE;
	COMMIT;
END|
DELIMITER ;|

# Test scenarios:
# 1. Test restart;
# 2. Test crash recovery.

# Test Restart

eval CREATE TABLE spc_table(
  id      INT,
  name    VARCHAR(100),
  address VARCHAR(100),
  PRIMARY KEY (id)
) ENGINE=InnoDB ROW_FORMAT=$row_format;

-- disable_query_log
CALL populate_spc_table();
-- enable_query_log

SELECT COUNT(*) FROM spc_table;

CREATE INDEX idx_name ON spc_table(name) SECONDARY_ENGINE_ATTRIBUTE='{"spc": "true"}';

--source include/restart_mysqld.inc

CHECK TABLE spc_table;

SELECT * FROM spc_table WHERE name = 'name10';

SELECT * FROM spc_table WHERE name = 'name5000';

SELECT * FROM spc_table WHERE name = 'name10000';

SELECT * FROM spc_table WHERE name = 'name10010';

DROP TABLE spc_table;

# Test Crash Recovery

#
# The below part of test was disabled until InnoDB truly supports DDL
# atomicity/WL#9536 is implemented. Otherwise the test creates discrepancy
# between SQL-layer DD and InnoDB DD.
#

--disable_testcase BUG#0000

eval CREATE TABLE spc_table(
  id      INT,
  name    VARCHAR(100),
  address VARCHAR(100),
  PRIMARY KEY (id)
) ENGINE=InnoDB ROW_FORMAT=$row_format;


-- disable_query_log
CALL populate_spc_table();
-- enable_query_log

SET SESSION debug="+d,crash_after_index_create";

# Write file to make mysql-test-run.pl start up the server again
--exec echo "restart" > $MYSQLTEST_VARDIR/tmp/mysqld.1.expect

--error 2013
CREATE INDEX idx_name ON spc_table(name) SECONDARY_ENGINE_ATTRIBUTE='{"spc": "true"}';

--enable_reconnect
--source include/wait_until_connected_again.inc
--disable_reconnect

SELECT COUNT(*) FROM spc_table;

CHECK TABLE spc_table;

EXPLAIN SELECT * FROM spc_table WHERE name = 'name10';

SELECT * FROM spc_table WHERE name = 'name10';

SELECT * FROM spc_table WHERE name = 'name5000';

SELECT * FROM spc_table WHERE name = 'name10000';

SELECT * FROM spc_table WHERE name = 'name10010';

DROP TABLE spc_table;

--enable_testcase

DROP PROCEDURE populate_spc_table;
