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

/** @file include/lizard0txn.h
  Lizard transaction management.

 Created 2020-03-27 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn_h
#define lizard0txn_h

#include "page0types.h"
#include "log0types.h"

#include "lizard0purge0types.h"
#include "lizard0undo0types.h"
#include "lizard0ut.h"
#include "lizard0txn0rec.h"

struct trx_undo_ptr_t;

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
  /** scn number */
  commit_mark_t cmmt;
  /** proposal commit number. */
  proposal_mark_t pmmt;
  /** branch info */
  xa_branch_t branch;
  /** Master txn address for async commit. */
  xa_addr_t maddr;

 public:
  txn_desc_t() : undo_ptr(UNDO_PTR_NULL), cmmt(), pmmt(), branch(), maddr() {}

  void reset() {
    undo_ptr = UNDO_PTR_NULL;
    cmmt.reset();
    pmmt.reset();
    branch.reset();
    maddr.reset();
  }


  /** assemble cmmt and undo ptr */
  void assemble(const commit_mark_t &mark, const slot_addr_t &slot_addr,
                bool is_slave_arg);

  /** assemble undo ptr */
  void assemble_undo_ptr(const slot_addr_t &slot_addr);

  void resurrect_xa(const proposal_mark_t &pmmt, const xa_branch_t &branch,
                    const xa_addr_t &maddr);

  void copy_xa_when_prepare(const gcn_tuple_t &xa_gcn,
                            const xa_branch_t &xa_branch);

  void copy_xa_when_commit(const gcn_tuple_t &xa_gcn,
                           const xa_addr_t &xa_maddr);

  bool is_whole_committed() const {
    return !undo_ptr_is_active(undo_ptr) && cmmt.is_whole_committed();
  }

  /** Whether has allocated txn slot for active transaction. */
  bool alloced() const { return undo_ptr != UNDO_PTR_NULL; }
};

/** Transaction object if active. */
struct txn_rw_t {
 public:
  // referenced trx object if active.
  trx_t *trx;
  // fixed txn slot buf block object if active.
  buf_block_t *block;

  // Txn slot address and transaction state.
  undo_ptr_t undo_ptr;

 public:
  txn_rw_t() : trx(nullptr), block(nullptr), undo_ptr(UNDO_PTR_NULL) {}

  txn_rw_t(trx_t *trx_arg, buf_block_t *block_arg, undo_ptr_t undo_ptr_arg)
      : trx(trx_arg), block(block_arg), undo_ptr(undo_ptr_arg) {}

  void reset() {
    ut_a(block == nullptr);

    trx = nullptr;
    block = nullptr;
    undo_ptr = UNDO_PTR_NULL;
  }

  bool alloced() const { return undo_ptr != UNDO_PTR_NULL; }

  bool is_active() const {
    return trx != nullptr && undo_ptr != UNDO_PTR_NULL &&
           undo_ptr_is_active(undo_ptr);
  }

  /** release the txn slot buf block fix count. */
  void release_slot() {
    if (block) {
      ut_ad(was_slot_fixed());
      buf_block_unfix(block);
      block = nullptr;
    }
  }

  bool was_slot_fixed() const {
    return block != nullptr && block->page.buf_fix_count.load() > 0;
  }
};

/** transaction identity include trx_id and txn slot address.*/
struct txn_id_t {
 public:
  trx_id_t trx_id;
  undo_ptr_t undo_ptr;

 public:
  txn_id_t() : trx_id(0), undo_ptr(UNDO_PTR_NULL) {}

  txn_id_t(trx_id_t trx_id_arg, undo_ptr_t undo_ptr_arg)
      : trx_id(trx_id_arg), undo_ptr(undo_ptr_arg) {}

  bool alloced() const { return undo_ptr != UNDO_PTR_NULL; }
};

/**
  Lizard transaction attributes in index (used by Vision)
   1) scn
   2) undo_ptr
   3) gcn
*/
struct txn_index_t {
 public:
  /** undo log header address */
  std::atomic<undo_ptr_t> uba;
  /** scn number */
  std::atomic<scn_t> scn;
  /** gcn number */
  std::atomic<gcn_t> gcn;

  bool is_whole_committed() const {
    return !undo_ptr_is_active(uba.load()) && scn.load() != SCN_NULL &&
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
  /** scn number */
  scn_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** gcn number */
  gcn_t gcn;
};

namespace lizard {

/** Prepare some special transaction description. */
struct txn_sys_t {
 public:
  static slot_addr_t SLOT_ADDR_NO_REDO;
  static slot_addr_t SLOT_ADDR_NULL;

 private:
  txn_sys_t() { assemble_txn_desc(); }

  void assemble_txn_desc() {
    /** Temporary table didn't have real UBA and scn. */
    slot_addr_t slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
                             SLOT_OFFSET_TEMP_TAB_REC};

    commit_mark_t cmmt = {SCN_TEMP_TAB_REC, US_TEMP_TAB_REC, GCN_TEMP_TAB_REC,
                          CSR_AUTOMATIC};
    txn_desc_temp.assemble(cmmt, slot_addr, false);

    /** Dynamic metadata table txn description */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
                 SLOT_OFFSET_DYNAMIC_METADATA};
    cmmt = {SCN_DYNAMIC_METADATA, US_DYNAMIC_METADATA, GCN_DYNAMIC_METADATA,
            CSR_AUTOMATIC};
    txn_desc_dm.assemble(cmmt, slot_addr, false);

    /** Log ddl table txn description */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_LOG_DDL};

    cmmt = {SCN_LOG_DDL, US_LOG_DDL, GCN_LOG_DDL, CSR_AUTOMATIC};
    txn_desc_ld.assemble(cmmt, slot_addr, false);

    /** dd index txn for dd table. */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_DICT_REC};
    cmmt = {SCN_DICT_REC, US_DICT_REC, GCN_DICT_REC, CSR_AUTOMATIC};
    txn_desc_dd.assemble(cmmt, slot_addr, false);

    /** dd index txn for dd table upgrade */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
                 SLOT_OFFSET_INDEX_UPGRADE};
    cmmt = {SCN_INDEX_UPGRADE, US_INDEX_UPGRADE, GCN_INDEX_UPGRADE,
            CSR_AUTOMATIC};
    txn_desc_dd_upgrade.assemble(cmmt, slot_addr, false);
  }

 public:
  static struct txn_sys_t *instance() {
    static txn_sys_t txn_sys;
    return &txn_sys;
  }

  /** Whether scn and undo_ptr come from special temporary transaction
   * description.*/
  bool is_temporary(scn_t scn, undo_ptr_t undo_ptr) {
    if (scn == txn_desc_temp.cmmt.scn && undo_ptr == txn_desc_temp.undo_ptr)
      return true;

    return false;
  }

  /** Whether undo address is for dm */
  bool is_dynamic_metadata(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_dm.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for temporary */
  bool is_temporary(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_temp.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for log ddl */
  bool is_log_ddl(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_ld.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_ptr = undo_addr.encode();
    if (undo_ptr == txn_desc_dd.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd_upgrade(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_ptr = undo_addr.encode();
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

/**
 * prepare_in_tc is treated as first phase commit within 2PC, so we should
 * mark our transaction system like trx_commit_mark, but only proposal xa
 * transaction will really do something.
 *
 * @param[in]		trx	transaction context
 * @param[in/out]	undo	undo memory structure
 * @param[in]		hdr	undo log hdr
 * @param[in]		mtr
 *
 * @retval		proposal commit mark.
 * */
proposal_mark_t trx_prepare_mark(trx_t *trx, trx_undo_t *undo,
                                 trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Assign a new commit scn for the transaction when commit

  @param[in]      trx       current transaction
  @param[in/out]  scn_ptr   Commit scn which was generated only once
  @param[in]      undo      txn undo log
  @param[in]      undo page txn undo log header page
  @param[in]      offset    txn undo log header offset
  @param[in]      mtr       mini transaction
  @param[out]     serialised

  @retval         scn       commit scn struture
*/
commit_mark_t trx_commit_mark(trx_t *trx, commit_mark_t *scn_ptr,
                              trx_undo_t *undo, page_t *undo_hdr_page,
                              ulint hdr_offset, bool *serialised, mtr_t *mtr);

/** Commit txn memory structure after txn slot mini-transaction commit.
 *
 * @param[in/out]		trx
 * @param[in]			serialised */
void txn_commit_in_memory(trx_t *trx, bool serialised);

/** Get active transaction according to txn rec.
 *
 * @param[in/out]	txn rec
 * @param[in]		increment ref count
 * @param[in]		optional trx which is used to get local min active tid
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(txn_rec_t *txn_rec, bool do_ref_count,
                          const trx_t *optional_trx);

/** Get active transaction according to txn rw.
 *
 * @param[in]		txn rw
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_rw_t &txn_rw, bool do_ref_count);

/** Get active transaction according to txn identity.
 *
 * @param[in]		txn identity
 * @param[in]		increment ref count
 *
 * @retval	txn rw object.
 * */
txn_rw_t txn_rw_is_active(const txn_id_t &txn_id, bool do_ref_count);

/**
 * Judge transaction have committed through txn slot.
 *
 * @param[in]	txn rw object
 *
 * @retval	true	Committed
 * @retval	false	Active
 * */
bool txn_rw_is_committed_in_memory(const txn_rw_t &txn_rw);

/**
   Resurrect txn undo log segment,
   Maybe the trx didn't have m_redo update/insert undo log.
*/
void trx_resurrect_txn(trx_t *trx, trx_undo_t *undo, trx_rseg_t *rseg);

/** Prepares a transaction for given rollback segment. */
lsn_t txn_prepare_low(
    trx_t *trx,               /*!< in/out: transaction */
    txn_undo_ptr_t *undo_ptr, /*!< in/out: pointer to rollback
                              segment scheduled for prepare. */
    mtr_t *mtr);

/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx);
/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(const trx_t *trx);
/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx);

/** Load min active trx id which is cached within trx struct. */
extern trx_id_t trx_load_min_active_tid(const trx_t *trx);

/**
  Get a TXN rseg by XID.

  @retval     rollback segment
*/
trx_rseg_t *txn_rseg_assign_by_xid(const XID *xid);

/**
  Find transactions slot in the finalized state by XID.

  @param[in]  rseg         The rollseg where the transaction is being looked up.
  @params[in] xid          xid
  @param[out] txn_slot     Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool txn_rseg_find_txn_slot_by_xid(trx_rseg_t *rseg, const XID *xid,
                                   txn_slot_t *txn_slot);
/**
  If during an external XA, check whether the mapping relationship between xid
  and rollback segment is as expected.

  @param[in]        trx         current transaction

  @return           true        if success
*/
bool txn_rseg_check_xid_mapping(const XID *xid, const trx_rseg_t *expect_rseg);

}  // namespace lizard

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/* Assert the txn_desc is initial */
#define assert_txn_desc_initial(trx)                        \
  do {                                                      \
    ut_a((trx)->txn_desc.undo_ptr == UNDO_PTR_NULL &&       \
         lizard::commit_mark_state((trx)->txn_desc.cmmt) == \
             SCN_STATE_INITIAL);                            \
  } while (0)

/* Assert the txn_desc is allocated */
#define assert_txn_desc_allocated(trx)                      \
  do {                                                      \
    ut_a((trx)->txn_desc.undo_ptr != UNDO_PTR_NULL &&       \
         lizard::commit_mark_state((trx)->txn_desc.cmmt) == \
             SCN_STATE_INITIAL);                            \
  } while (0)

#define assert_trx_in_recovery(trx)                                            \
  do {                                                                         \
    if ((trx)->rsegs.m_txn.rseg != NULL && (trx)->rsegs.m_redo.rseg == NULL) { \
      ut_a((trx)->is_recovered);                                               \
    }                                                                          \
  } while (0)

#else

#define assert_txn_desc_initial(trx)
#define assert_txn_desc_allocated(trx)
#define assert_trx_in_recovery(trx)

#endif


#endif  // lizard0txn_h
