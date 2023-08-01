# ==== Purpose ====
# Check that the result of the query from dbms_xa.find_by_xid() is as expected.

# ==== Usage ====
# --let $expect_gcn=10000
# --let $expect_state= COMMIT
# --let $my_xid='1'
# --let $expect_csr= ASSIGNED_GCN
# --source suite/rds/include/xa_proc_find_by_xid_verify.inc

# Parameters
# $expect_gcn
#   If set, gcn is checked to see if it is as expected.
# $expect_state
#   if set, status(ATTACHED/DETACHED/COMMIT/ROLLBACK/NOTSTART_OR_FORGET/NOT_SUPPORT)
#   is checked to see if it is as expected.
# $expect_csr
#   if set, CSR(AUTOMATIC_GCN/ASSIGNED_GCN) is checked to see if it is as expected.
# $my_xid
#   XID.

--let $actual_gcn= query_get_value("call dbms_xa.find_by_xid($my_xid)", GCN, 1)
if ($actual_gcn != 'No such row') {

  if ($expect_gcn) {
    --let $assert_text=expect_gcn == actual_gcn
    --let $assert_cond=$expect_gcn = $actual_gcn
    --source include/assert.inc
  }

  if ($expect_state) {
    --let $actual_state = query_get_value("call dbms_xa.find_by_xid($my_xid)", Status, 1)
    --let $assert_text=expect_state == actual_state
    --let $assert_cond="$expect_state" = "$actual_state"
    --source include/assert.inc
  }

  if ($expect_csr) {
    --let $actual_csr = query_get_value("call dbms_xa.find_by_xid($my_xid)", CSR, 1)
    --let $assert_text=expect_csr == actual_csr
    --let $assert_cond="$expect_csr" = "$actual_csr"
    --source include/assert.inc
  }
}

--let $actual_gcn=
--let $actual_state=
--let $actual_csr=
--let $expect_gcn=
--let $expect_state=
--let $expect_csr=
--let $my_xid=
--let $expect_none=
