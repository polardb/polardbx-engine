### Purpose ###
#
# Crash-recovery test for GCS/Lizard clustered-index cleanout redo on a
# ROW_FORMAT=COMPRESSED table.
#
# Cleanout of scn/uba/gcn on a compressed clustered page emits a
# MLOG_REC_CLUST_CLOVER_UPDATE redo record. On recovery this is replayed via
# lizard::row_upd_rec_clover_fields_in_recovery() -> page_zip_write_clover(),
# writing the fields both into the record and the uncompressed trailer slot.
#
# We produce the cleanout (and its redo) with checkpointing disabled, then
# force a crash recovery via innodb_fast_shutdown=2 (dirty pages are not
# flushed, so the change is recovered from redo). After restart the data and
# flashback (AS OF GCN) results must be correct and the page must pass
# CHECK TABLE.
#
### Parameters ###
#
# $kbs   KEY_BLOCK_SIZE used for the compressed table.
#

--echo # ============================================================
--echo # redo recovery, ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=$kbs
--echo # ============================================================

--source suite/innodb_gcn/include/init_conn_base_seq.inc

eval CREATE TABLE t1 (
  id INT PRIMARY KEY,
  c1 INT,
  c2 VARCHAR(200),
  KEY k_c1 (c1)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=$kbs;

--echo # Insert committed rows that are intentionally left un-cleaned.
SET GLOBAL innodb_commit_cleanout_max_rows = 0;
SET GLOBAL innodb_txn_cleanout_disable = 1;
SET GLOBAL innodb_lizard_stat_enabled = ON;
SET GLOBAL innodb_cleanout_write_redo = TRUE;

XA START 'zip_redo';
INSERT INTO t1 VALUES
  (1, 10,  REPEAT('a', 180)),
  (2, 20,  REPEAT('b', 180)),
  (3, 30,  REPEAT('c', 180)),
  (4, 40,  REPEAT('d', 180)),
  (5, 50,  REPEAT('e', 180)),
  (6, 60,  REPEAT('f', 180)),
  (7, 70,  REPEAT('g', 180)),
  (8, 80,  REPEAT('h', 180)),
  (9, 90,  REPEAT('i', 180)),
  (10, 100, REPEAT('j', 180));
XA END 'zip_redo';
XA PREPARE 'zip_redo';
SET innodb_commit_seq = @base_seq + 100;
XA COMMIT 'zip_redo';

--echo # Persist the inserts, then disable checkpointing so the cleanout change
--echo # can only be recovered from redo.
SET GLOBAL innodb_log_checkpoint_now = ON;
SET GLOBAL innodb_checkpoint_disabled = 1;

--echo # Commit cleanout deterministically back-fills scn/uba/gcn on the zip page
--echo # and writes MLOG_REC_CLUST_CLOVER_UPDATE redo.
SET GLOBAL innodb_commit_cleanout_max_rows = 1000;
call dbms_stat.flush_cleanout();
UPDATE t1 SET c1 = c1 + 1 WHERE id <= 6;

--let $v_commit = query_get_value(SHOW STATUS LIKE "Lizard_commit_cleanout_clean", Value, 1)
--let $assert_text = commit cleanout must produce redo on the compressed page (kbs=$kbs)
--let $assert_cond = $v_commit > 0
--source include/assert.inc

--echo # Crash recovery: dirty pages are not flushed, redo must be replayed.
SET GLOBAL innodb_fast_shutdown = 2;
--let $restart_parameters = restart:
--source include/restart_mysqld.inc

--echo # After recovery: data reflects the recovered update.
SELECT id, c1 FROM t1 ORDER BY id;

--echo # After recovery: the recovered gcn on the compressed record is correct.
--echo # (@base_seq is re-bound from the saved value; the connection was reset
--echo #  by the restart.)
--disable_query_log
eval SET @base_seq = $global_base_seq;
--enable_query_log
SELECT COUNT(*) AS visible_cnt FROM t1 AS OF GCN @base_seq + 200;
SELECT COUNT(*) AS invisible_cnt FROM t1 AS OF GCN @base_seq + 50;
--echo # pre-update snapshot reconstructed via undo on the compressed table
SELECT id, c1 FROM t1 AS OF GCN @base_seq + 100 ORDER BY id;

--echo # Page integrity after redo replay on the compressed page.
CHECK TABLE t1;

DROP TABLE t1;

SET GLOBAL innodb_commit_cleanout_max_rows = default;
SET GLOBAL innodb_txn_cleanout_disable = default;
SET GLOBAL innodb_cleanout_write_redo = default;
SET GLOBAL innodb_checkpoint_disabled = default;
SET GLOBAL innodb_fast_shutdown = default;
SET GLOBAL innodb_purge_run_now = ON;
