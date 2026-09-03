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

/** @file include/lizard0txn0types.h
  Lizard transaction management.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0types_h
#define lizard0txn0types_h

#include "page0types.h"
#include "log0types.h"

#include "lizard0trx0purge0types.h"
#include "lizard0txn0mem.h"
#include "lizard0txn0rec.h"

/**
  The transaction description:

  It will be inited when allocate the first txn undo log
  header, and never change until transaction commit or rollback.
*/
struct txn_desc_t {
 public:
  /* Attention
   * undo_ptr will be protected by trx->mutex since there will be third
   * observer which is to judge transaction committed or not by read undo_ptr.
   */

  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** assign through gcs->new_commit() */
  commit_mark_t cmmt;
  /** assign through gcs->new_proposal() */
  proposal_mark_t pmmt;
  /** branch and maddr didn't have chance to be changed by gcs->new_xxxx(),
   * so reuse m_xa_branch, m_xa_maddr. */

  /**---------------------------------------------------------------------*/
  /** Those attributes come from external, must be accessed by xa_xxx()
   * function.
   */
 private:
  /** Global commit number. */
  gcn_tuple_t m_xa_gcn;
  /** proposal commit number. */
  proposal_mark_t m_xa_pmmt;
  /** branch info */
  xa_branch_t m_xa_branch;
  /** Master txn address for async commit. */
  xa_addr_t m_xa_maddr;

 public:
  txn_desc_t()
      : undo_ptr(UNDO_PTR_NULL),
        cmmt(),
        pmmt(),
        m_xa_gcn(),
        m_xa_pmmt(),
        m_xa_branch(),
        m_xa_maddr() {}

  void reset() {
    undo_ptr = UNDO_PTR_NULL;
    cmmt.reset();
    pmmt.reset();

    m_xa_gcn.reset();
    m_xa_pmmt.reset();
    m_xa_branch.reset();
    m_xa_maddr.reset();
  }

  /** assemble cmmt and undo ptr */
  void assemble(const commit_mark_t &mark, const slot_addr_t &slot_addr,
                bool is_slave_arg);

  /** assemble undo ptr */
  void assemble_undo_ptr(const slot_addr_t &slot_addr);

  void xa_resurrect(const proposal_mark_t &pmmt, const xa_branch_t &branch,
                    const xa_addr_t &maddr);

  void xa_copy_when_prepare(const gcn_tuple_t &xa_gcn,
                            const xa_branch_t &xa_branch);

  void xa_copy_when_commit(const gcn_tuple_t &xa_gcn,
                           const xa_addr_t &xa_maddr);

  // state committed
  bool is_whole_committed() const {
    return is_slot_committed() && is_cmmt_committed();
  }
  bool is_slot_committed() const { return !undo_addr_t::decode_active(undo_ptr); }
  bool is_cmmt_committed() const { return cmmt.is_whole_committed(); }

  // state allocated;
  bool is_slot_alloced() const { return undo_ptr != UNDO_PTR_NULL; }
  bool is_cmmt_alloced() const { return cmmt.is_alloced(); }

  bool is_whole_alloced() const {
    return is_slot_alloced() && is_cmmt_alloced();
  }
  // state initial;
  bool is_slot_initial() const { return undo_ptr == UNDO_PTR_NULL; }
  bool is_cmmt_initial() const { return cmmt.is_initial(); }
  bool is_whole_initial() const {
    return is_slot_initial() && is_cmmt_initial();
  }

  const gcn_tuple_t &xa_gcn() const { return m_xa_gcn; }
  const proposal_mark_t &xa_pmmt() const { return m_xa_pmmt; }
  const xa_addr_t &xa_maddr() const { return m_xa_maddr; }
  const xa_branch_t &xa_branch() const { return m_xa_branch; }
};

/**
  Lizard transaction attributes in index (used by Vision)
   1) scn
   2) undo_ptr
   3) gcn
*/
struct txn_index_t {
 public:
  /** scn number */
  std::atomic<scn_t> scn;
  /** undo log header address */
  std::atomic<undo_ptr_t> uba;
  /** gcn number */
  std::atomic<gcn_t> gcn;

  txn_index_t() : scn(SCN_NULL), uba(UNDO_PTR_NULL), gcn(GCN_NULL) {}

  void copy_from_txn_desc(const txn_desc_t &txn_desc) {
      scn = txn_desc.cmmt.scn;
      uba = txn_desc.undo_ptr;
      gcn = txn_desc.cmmt.gcn;
  }

  bool is_whole_committed() const {
    return !undo_addr_t::decode_active(uba.load()) && scn.load() != SCN_NULL &&
           gcn.load() != GCN_NULL;
  }
};

/**
  Lizard transaction attributes.
   1) scn
   2) undo_ptr
   3) gcn
*/
struct txn_info_t {
 public:
  /** scn number */
  scn_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** gcn number */
  gcn_t gcn;

  txn_info_t() : scn(SCN_NULL), undo_ptr(UNDO_PTR_NULL), gcn(GCN_NULL) {}

  txn_info_t(const scn_t scn_arg, const undo_ptr_t undo_ptr_arg,
             const gcn_t gcn_arg)
      : scn(scn_arg), undo_ptr(undo_ptr_arg), gcn(gcn_arg) {}

  txn_info_t(const txn_desc_t &txn_desc) {
    scn = txn_desc.cmmt.scn;
    undo_ptr = txn_desc.undo_ptr;
    gcn = txn_desc.cmmt.gcn;
  }

  txn_info_t(const txn_index_t &txn_index) {
    scn = txn_index.scn.load();
    undo_ptr = txn_index.uba.load();
    gcn = txn_index.gcn.load();
  }
};

namespace lizard {

/** Prepare some special transaction description. */
struct txn_sys_t {
 private:
  txn_sys_t() { assemble_txn_desc(); }

  void assemble_txn_desc() {
    /** Temporary table didn't have real UBA and scn. */
    txn_desc_temp.assemble(commit_mark_t::temporary(), slot_addr_t::temporary(),
                           false);

    /** Dynamic metadata table txn description */
    txn_desc_dm.assemble(commit_mark_t::dynamic_metadata(),
                         slot_addr_t::dynamic_metadata(), false);

    /** Log ddl table txn description */
    txn_desc_ld.assemble(commit_mark_t::log_ddl(), slot_addr_t::log_ddl(),
                         false);

    /** dd index txn for dd table. */
    txn_desc_dd.assemble(commit_mark_t::dd_dict(), slot_addr_t::dd_dict(),
                         false);

    /** dd index txn for dd table upgrade */
    txn_desc_dd_upgrade.assemble(commit_mark_t::index_upgrade(),
                                 slot_addr_t::index_upgrade(), false);
  }

 public:
  static struct txn_sys_t *instance() {
    static txn_sys_t txn_sys;
    return &txn_sys;
  }

  /** Whether scn and undo_ptr come from special temporary transaction
   * description.*/
  bool is_temporary(scn_t scn, undo_ptr_t undo_ptr) {
    if (scn == txn_desc_temp.cmmt.scn &&
        undo_ptr == txn_desc_temp.undo_ptr)
      return true;

    return false;
  }

  /** Whether undo address is for dm */
  bool is_dynamic_metadata(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_dm.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for temporary */
  bool is_temporary(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_temp.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for log ddl */
  bool is_log_ddl(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_ld.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_dd.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd_upgrade(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_dd_upgrade.undo_ptr) return true;

    return false;
  }

  bool is_special(const undo_addr_t &undo_addr) {
    return is_temporary(undo_addr) || is_log_ddl(undo_addr) ||
           is_dynamic_metadata(undo_addr) || is_dd_index_of_dd(undo_addr) ||
           is_dd_index_of_dd_upgrade(undo_addr);
  }

  bool is_special(const undo_ptr_t &undo_ptr) {
    undo_addr_t undo_addr(undo_ptr);
    return is_special(undo_addr);
  }

 public:
  /** Special for temporary table record. */
  txn_desc_t txn_desc_temp;
  /** Special for dynamic metadata table record. */
  txn_desc_t txn_desc_dm;
  /** Special for log ddl table record. */
  txn_desc_t txn_desc_ld;
  /** Sepcial for dd index for dd table. */
  txn_desc_t txn_desc_dd;
  /** Sepcial for dd index for dd table from upgrade */
  txn_desc_t txn_desc_dd_upgrade;
};

}  // namespace lizard


#endif  // lizard0txn_h
