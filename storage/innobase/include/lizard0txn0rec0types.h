/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0txn0rec0types.h
  Lizard transactional record management.

 Created 2024-10-15 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0rec0types_h
#define lizard0txn0rec0types_h

#include "lizard0undo0types.h"

/**
  Lizard transaction attributes in record (used by Vision)
   1) trx_id
   2) scn
   3) undo_ptr
*/
struct txn_rec_t {
 public:
  /* trx id */
  trx_id_t trx_id;
  /** scn number */
  scn_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;

  /**
    Although gcn isn't saved on record, but Global query still use gcn as
    visible judgement, and it can be retrieved by txn undo header, so defined
    gcn as txn record attribute.
  */
  /** Revision: Persist gcn into record */
  gcn_t gcn;

 public:
  txn_rec_t()
      : trx_id(0), scn(SCN_NULL), undo_ptr(UNDO_PTR_NULL), gcn(GCN_NULL) {}

  txn_rec_t(const trx_id_t trx_id_arg, const scn_t scn_arg,
            const undo_ptr_t undo_ptr_arg, const gcn_t gcn_arg)
      : trx_id(trx_id_arg),
        scn(scn_arg),
        undo_ptr(undo_ptr_arg),
        gcn(gcn_arg) {}

  /** Whether txn has committed through undo_ptr commit flag.
   *
   * Commit number value maybe are null if come from index row although
   * committed.
   *
   * @retval	true	commit
   * @retval	false	active */
  bool is_committed() const {
    if (!undo_ptr_is_active(undo_ptr)) {
      ut_ad(scn != SCN_NULL && gcn != GCN_NULL && trx_id != 0);
      return true;
    } else {
      /** Active trx didn't known Commit Info. */
      ut_ad(scn == SCN_NULL && gcn == GCN_NULL);
      /** assigned csr and slave are commit info. */
      ut_ad(csr() == CSR_AUTOMATIC && !is_slave());
      return false;
    }
  }

  /** Active or not. */
  bool is_active() const { return !is_committed(); }

  /** Whether txn has committed and fillup all valid commit number */
  bool is_whole_committed() const {
    if (is_committed() && scn != SCN_NULL && gcn != GCN_NULL) {
      return true;
    }
    return false;
  }

  void reset() {
    trx_id = 0;
    scn = SCN_NULL;
    undo_ptr = UNDO_PTR_NULL;
    gcn = GCN_NULL;
  }

  slot_ptr_t slot() const { return undo_ptr_get_slot(undo_ptr); }
  csr_t csr() const { return undo_ptr_get_csr(undo_ptr); }
  bool is_slave() const { return undo_ptr_is_slave(undo_ptr); }
  void clear_slave() { undo_ptr_clear_slave(&undo_ptr); }
};

#endif 
