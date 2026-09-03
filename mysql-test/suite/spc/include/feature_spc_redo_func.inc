### Purpose ###
#
# Helper for feature_spc_redo.inc.  Mirrors the pattern of
# suite/panda/include/feature_panda_redo_func.inc but for SPC.
#
# Steps:
#   1. Prepare: create table + pre-populate (if any)
#   2. Force a clean checkpoint
#   3. Disable further checkpoints (no_checkpoint_start)
#   4. Produce redo: DML operations that touch the SPC index
#   5. no_checkpoint_end: if a checkpoint slipped through, clean up and skip
#   6. Kill mysqld — redo is NOT checkpointed yet
#   7. Restart: redo is replayed by InnoDB recovery
#   8. Verify: SPC index checksum == PRIMARY KEY checksum
#   9. Clean up
#
### Parameters ###
#
# $PREPARE_STATEMENTS       SQL to prepare the table (CREATE + pre-INSERT).
# $PRODUCE_REDO_STATEMENTS  SQL that generates redo touching the SPC index.
# $CLEANUP_STATEMENTS       SQL to clean up after verification (DROP TABLE …).
#

--let $orig_opt_spc_enabled = `SELECT @@global.opt_index_format_spc_enabled`

SET opt_index_format_spc_enabled = ON;

--eval $PREPARE_STATEMENTS

# Force a clean checkpoint so the redo test starts from a known LSN.
SET GLOBAL innodb_log_checkpoint_now = 1;

--echo # Produce redo (checkpoint disabled)
SET GLOBAL innodb_checkpoint_disabled = 1;
--source include/no_checkpoint_start.inc
--eval $PRODUCE_REDO_STATEMENTS

--let CLEANUP_IF_CHECKPOINT = $CLEANUP_STATEMENTS
--source include/no_checkpoint_end.inc

--echo # mysqld was killed by no_checkpoint_end; restart to replay redo
--source include/start_mysqld.inc

--echo # After recovery: verify SPC index == PRIMARY KEY (checksum)

eval WITH
  ck_spc AS (SELECT BIT_XOR(CRC32(CONCAT_WS(',', $CHECKSUM_COLS))) AS chk
             FROM $CHECKSUM_TABLE FORCE INDEX ($CHECKSUM_INDEX)),
  ck_pk  AS (SELECT BIT_XOR(CRC32(CONCAT_WS(',', $CHECKSUM_COLS))) AS chk
             FROM $CHECKSUM_TABLE FORCE INDEX (PRIMARY))
SELECT IF(s.chk <=> p.chk, 'CHECKSUM_OK', 'CHECKSUM_FAILED') AS result
FROM ck_spc AS s, ck_pk AS p;

SET GLOBAL innodb_checkpoint_disabled = 0;
--eval SET opt_index_format_spc_enabled = $orig_opt_spc_enabled
--eval $CLEANUP_STATEMENTS
