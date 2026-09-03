### Purpose ###
#
# Verify that the GCS/Lizard transaction fields (SCN / UNDO_PTR(UBA) / GCN),
# which live in the uncompressed trailer of a compressed clustered-index page,
# are cleaned out (back-filled) correctly on ROW_FORMAT=COMPRESSED tables.
#
# On a compressed clustered index the cleanout writes scn/uba/gcn both into the
# record on the page frame and into the per-record uncompressed storage slot via
# lizard::page_zip_write_clover(). This inc:
#   * exercises commit cleanout (deterministic: the committing transaction
#     back-fills its own just-modified clustered records) and asserts it ran;
#   * exercises the read/scan cleanout code path on the compressed page;
#   * proves correctness through data reads and flashback (AS OF GCN) queries,
#     which read the back-filled gcn directly from the compressed record.
#
### Parameters ###
#
# $kbs   KEY_BLOCK_SIZE used for the compressed table.
#
# The caller keeps purge stopped, lizard stats enabled and undo/txn retention
# high for the whole run.
#

--echo # ============================================================
--echo # ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=$kbs
--echo # ============================================================

--source suite/innodb_gcn/include/init_conn_base_seq.inc

eval CREATE TABLE t1 (
  id INT PRIMARY KEY,
  c1 INT,
  c2 VARCHAR(200),
  KEY k_c1 (c1)
) ENGINE=InnoDB ROW_FORMAT=COMPRESSED KEY_BLOCK_SIZE=$kbs;

--echo # Insert committed rows that are intentionally left un-cleaned
--echo # (commit cleanout disabled), at a controlled commit GCN (@base_seq+100).
SET GLOBAL innodb_commit_cleanout_max_rows = 0;
SET GLOBAL innodb_txn_cleanout_disable = 1;

XA START 'zip_scn';
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
XA END 'zip_scn';
XA PREPARE 'zip_scn';
SET innodb_commit_seq = @base_seq + 100;
XA COMMIT 'zip_scn';

--echo #
--echo # 1. Commit cleanout back-fills scn/uba/gcn on the compressed page.
--echo #    (flush_cleanout() first resets the counters to a 0 baseline)
--echo #
SET GLOBAL innodb_commit_cleanout_max_rows = 1000;
call dbms_stat.flush_cleanout();
UPDATE t1 SET c1 = c1 + 1 WHERE id <= 6;

--let $v_commit = query_get_value(SHOW STATUS LIKE "Lizard_commit_cleanout_clean", Value, 1)
--let $assert_text = commit cleanout must back-fill clustered records on the compressed page (kbs=$kbs)
--let $assert_cond = $v_commit > 0
--source include/assert.inc

--echo # data correctness after commit cleanout
SELECT id, c1 FROM t1 ORDER BY id;

--echo #
--echo # 2. Read/scan cleanout code path on the compressed page (coverage).
--echo #
SET GLOBAL innodb_txn_cleanout_disable = 0;
--disable_result_log
SELECT * FROM t1;
--enable_result_log
--echo # data unchanged by scan cleanout
SELECT id, c1 FROM t1 ORDER BY id;

--echo #
--echo # 3. Flashback correctness: gcn is read directly from the compressed
--echo #    record and reconstructs the pre-update snapshot via undo.
--echo #
--echo # visible at gcn >= insert commit gcn (@base_seq + 100)
SELECT COUNT(*) AS visible_cnt FROM t1 AS OF GCN @base_seq + 200;
--echo # invisible at gcn < insert commit gcn
SELECT COUNT(*) AS invisible_cnt FROM t1 AS OF GCN @base_seq + 50;
--echo # pre-update snapshot: original c1 values reconstructed from undo
SELECT id, c1 FROM t1 AS OF GCN @base_seq + 100 ORDER BY id;

DROP TABLE t1;

SET GLOBAL innodb_commit_cleanout_max_rows = default;
SET GLOBAL innodb_txn_cleanout_disable = default;
