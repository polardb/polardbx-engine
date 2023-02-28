# ==== Purpose ====
# Check that the result of the query from dbms_xa.find_by_gtrid() is as expected.

# ==== Usage ====
# --let $expect_gcn=10000
# --let $expect_state=COMMIT
# --let $gtrid='1'
# --source suite/rds/include/xa_proc_find_by_gtrid_verify.inc

# Parameters
# $expect_none
#   if set, it is expected that no corresponding transaction can be found.
# $expect_gcn
#   If set, gcn is checked to see if it is as expected.
# $expect_state
#   if set, state(commit/rollback) is checked to see if it is as expected.
# $gtrid
#   gtrid in XID.

--let $actual_gcn= query_get_value("call dbms_xa.find_by_gtrid($gtrid)", GCN, 1)
if ($actual_gcn == 'No such row') {
  if (!$expect_none) {
    --die Cannot find the transaction.
  }
}

if ($actual_gcn != 'No such row') {
  if ($expect_none) {
    --die expect cannot find transaction, but actually found.
  }

  if ($expect_gcn) {
    --let $assert_text=expect_gcn == actual_gcn
    --let $assert_cond=$expect_gcn = $actual_gcn
    --source include/assert.inc
  }

  if ($expect_state) {
    --let $actual_state = query_get_value("call dbms_xa.find_by_gtrid($gtrid)", State, 1)
    --let $assert_text=expect_state == actual_state
    --let $assert_cond="$expect_state" = "$actual_state"
    --source include/assert.inc
  }
}

--let $actual_gcn=
--let $actual_state=
--let $expect_gcn=
--let $expect_state=
--let $gtrid=
--let $expect_none=
