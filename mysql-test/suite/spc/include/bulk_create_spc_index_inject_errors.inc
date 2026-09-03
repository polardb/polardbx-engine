--echo # ###########################################################
--echo # Prepare

# Create table in file-per-table tablespace.
eval CREATE TABLE spc_table
  (id INT, name VARCHAR(100), address VARCHAR(100))
  ENGINE=InnoDB
  TABLESPACE innodb_file_per_table;


# Insert some values (make sure more than one page would be used for records)
# Note: With SPC compression, records are heavily compressed. After compression,
# each record may only be a few bytes (storing only the difference from prefix).
# Calculation for 16KB page (16384 bytes):
#   - Page header: ~120 bytes
#   - Page dict (at least storing prefix once): ~100 bytes 
#   - Available space: 16384 - 120 - 100 = 15164 bytes
#   - Compressed record: ~5-10 bytes (difference suffix) + ~5 bytes (metadata) + 100 bytes(address) = ~110-120 bytes
#   - Records per page: 15164 / 120 ≈ 126 records
# To ensure multiple pages, we need at least 500+ records.
# Using 500 records to be safe (ensures at least 3 pages after compression).
DELIMITER |;
CREATE PROCEDURE populate_spc_table()
BEGIN
  DECLARE i INT DEFAULT 1;
  # Use long prefix to maximize compression benefit
  # Long prefix ensures good compression ratio in SPC
  DECLARE name_prefix VARCHAR(100) DEFAULT REPEAT('a', 90);
  DECLARE addr_prefix VARCHAR(100) DEFAULT REPEAT('b', 90);

  START TRANSACTION;
  WHILE (i <= 1000) DO
    INSERT INTO spc_table VALUES (i, CONCAT(name_prefix, i), CONCAT(addr_prefix, i));
    SET i = i + 1;
  END WHILE;
  COMMIT;
END|
DELIMITER ;|

--disable_query_log
CALL populate_spc_table();
--enable_query_log

DROP PROCEDURE populate_spc_table;

--echo # ###########################################################
--echo # Inject error during bulk reserve space

SET debug = '+d,ddl_btree_build_oom';

--error ER_OUT_OF_RESOURCES
CREATE INDEX idx_spc_table_name_address ON spc_table (name, address) SECONDARY_ENGINE_ATTRIBUTE='{"spc": "true"}';
SET debug = '-d,ddl_btree_build_oom';


--echo # ###########################################################
--echo # Inject error during bulk insert encoding for SPC index.

SET debug = '+d,ddl_btree_build_spc_encoder_fail';
--error ER_SPC_ENCODE_FAILED
CREATE INDEX idx_spc_table_name_address ON spc_table (name, address) SECONDARY_ENGINE_ATTRIBUTE='{"spc": "true"}';
SET debug = '-d,ddl_btree_build_spc_encoder_fail';


--echo # ###########################################################
--echo # No error - check for final success

CREATE INDEX idx_spc_table_name_address ON spc_table (name, address) SECONDARY_ENGINE_ATTRIBUTE='{"spc": "true"}';

--echo # ###########################################################
--echo # Cleanup

DROP TABLE spc_table;