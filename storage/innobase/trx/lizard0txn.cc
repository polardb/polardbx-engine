/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0txn.cc
  Lizard transaction management.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#include "lizard0txn.h"
#include "lizard0scn.h"

ib_time_system_us_t server_start_time_for_txn = 0;

/** assemble undo ptr */
void txn_desc_t::assemble(const commit_mark_t &mark,
                          const slot_addr_t &slot_addr) {
  bool state = (mark.scn != SCN_NULL);
  if (state) {
    assert_commit_mark_allocated(mark);
  } else {
    assert_commit_mark_initial(mark);
  }
  cmmt = mark;
  undo_addr_t undo_addr(slot_addr, state, mark.csr);
  undo_encode_undo_addr(undo_addr, &this->undo_ptr);
}

/** assemble undo ptr */
void txn_desc_t::assemble_undo_ptr(const slot_addr_t &slot_addr) {
  bool state = (cmmt.scn != SCN_NULL);
  if (state) {
    assert_commit_mark_allocated(cmmt);
  } else {
    assert_commit_mark_initial(cmmt);
  }
  undo_addr_t undo_addr(slot_addr, state, cmmt.csr);
  undo_encode_undo_addr(undo_addr, &this->undo_ptr);
}

void txn_desc_t::resurrect_xa(const proposal_mark_t &txn_pmmt,
                              const xa_branch_t &txn_branch,
                              const xa_addr_t &txn_maddr) {
  pmmt = txn_pmmt;
  branch = txn_branch;
  maddr = txn_maddr;
}

void txn_desc_t::copy_xa_when_prepare(const MyGCN &xa_gcn,
                                      const xa_branch_t &xa_branch) {
  ut_ad(xa_gcn.is_pmmt_gcn());
  ut_ad(xa_gcn.decided());
  ut_ad(xa_gcn.pushed_up());
  pmmt = xa_gcn.clone_pmmt();

  ut_ad(!xa_branch.is_null());
  branch = xa_branch;
}

void txn_desc_t::copy_xa_when_commit(const MyGCN &xa_gcn,
                                     const xa_addr_t &xa_maddr) {
  ut_ad(xa_gcn.is_cmmt_gcn());
  ut_ad(xa_gcn.decided());
  ut_ad(xa_gcn.pushed_up());

  cmmt.copy_gcn(xa_gcn.clone_cmmt());
  maddr = xa_maddr;
}


namespace lizard {

slot_addr_t txn_sys_t::SLOT_ADDR_NO_REDO = {
    SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_NO_REDO};

slot_addr_t txn_sys_t::SLOT_ADDR_NULL = {0, 0, 0};

}  // namespace lizard
