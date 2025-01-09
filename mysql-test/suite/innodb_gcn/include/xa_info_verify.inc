# ==== Purpose ====
# Verify XA info

# ==== Usage ====
# --let $my_xid='1','',1
# --let $has_slot_hint=1
# --let $slot_hint=2402850385
# --let $expect_status= COMMIT
# --let $expect_gcn= 62490
# --let $expect_csr= AUTOMATIC_GCN
# --let $expect_trx_id= 213540
# --let $expect_uba= 2402850385
# --let $expect_m_trx_id= 350350
# --let $expect_m_uba= 1204835035
# --let $expect_n_branch= 2
# --let $expect_n_local_branch= 2

# Parameters
# $expect_status
#   if set, status(ATTACHED/DETACHED_PREPARE/COMMIT/ROLLBACK/NOTSTART_OR_FORGET/NOT_SUPPORT)
#   is checked to see if it is as expected.
# $expect_gcn
#   If set, gcn is checked to see if it is as expected.
# $expect_csr
#   if set, CSR(AUTOMATIC_GCN/ASSIGNED_GCN) is checked to see if it is as expected.
# $expect_n_branch
#   if set, N_BRANCH is checked to see if it is as expected.
# $expect_n_local_branch
#   if set, N_LOCAL_BRANCH is checked to see if it is as expected.
# $my_xid
#   XID.

if ($has_slot_hint) {
  --source suite/innodb_gcn/include/load_xa_trx_info_with_hint.inc
}

if (!$has_slot_hint) {
  --source suite/innodb_gcn/include/load_xa_trx_info.inc
}

if ($expect_status) {
  --let $assert_text=expect_status == actual_status
  --let $assert_cond="$expect_status" = "$actual_status"
  --source include/assert.inc
}

if ($expect_gcn) {
  --let $assert_text=expect_gcn == actual_gcn
  --let $assert_cond="$expect_gcn" = "$actual_gcn"
  --source include/assert.inc
}

if ($expect_csr) {
  --let $assert_text=expect_csr == actual_csr
  --let $assert_cond="$expect_csr" = "$actual_csr"
  --source include/assert.inc
}

if ($expect_trx_id) {
  --let $assert_text=expect_trx_id == actual_trx_id
  --let $assert_cond="$expect_trx_id" = "$actual_trx_id"
  --source include/assert.inc
}

if ($expect_uba) {
  --let $assert_text=expect_uba == actual_uba
  --let $assert_cond="$expect_uba" = "$actual_uba"
  --source include/assert.inc
}

if ($expect_m_trx_id) {
  --let $assert_text=expect_m_trx_id == actual_m_trx_id
  --let $assert_cond="$expect_m_trx_id" = "$actual_m_trx_id"
  --source include/assert.inc
}

if ($expect_m_uba) {
  --let $assert_text=expect_m_uba == actual_m_uba
  --let $assert_cond="$expect_m_uba" = "$actual_m_uba"
  --source include/assert.inc
}

if ($expect_n_branch) {
  --let $assert_text=expect_n_branch == actual_n_branch
  --let $assert_cond="$expect_n_branch" = "$actual_n_branch"
  --source include/assert.inc
}

if ($expect_n_local_branch) {
  --let $assert_text=expect_n_local_branch == actual_n_local_branch
  --let $assert_cond="$expect_n_local_branch" = "$actual_n_local_branch"
  --source include/assert.inc
}

# reset
--let $my_xid=
--let $expect_status=
--let $expect_gcn=
--let $expect_csr=
--let $expect_trx_id=
--let $expect_uba=
--let $expect_m_trx_id=
--let $expect_m_uba=
--let $expect_n_branch=
--let $expect_n_local_branch=

--let $actual_status=
--let $actual_gcn=
--let $actual_csr=
--let $actual_trx_id=
--let $actual_uba=
--let $actual_m_trx_id=
--let $actual_m_uba=
--let $actual_n_branch=
--let $actual_n_local_branch=
