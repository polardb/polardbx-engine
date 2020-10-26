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

/** @file include/lizard0undo.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo_h
#define lizard0undo_h

#include "trx0types.h"
#include "trx0undo.h"

#include "lizard0dbg.h"
#include "lizard0scn.h"
#include "lizard0undo0types.h"
#include "ut0dbg.h"

#include <atomic>
#include <queue>
#include <set>
#include <vector>

struct trx_rseg_t;
struct trx_undo_t;
struct SYS_VAR;

/**
  Lizard transaction system undo format:

  At the end of undo log header history node:

  8 bytes     SCN number
  8 bytes     UTC time
  8 bytes     UBA address
  8 bytes     GCN

  Those three options will be included into all INSERT/UPDATE/TXN undo
  log header.


  Start from undo log old header, txn_undo will be different with trx_undo:

  1) txn undo : flag + reserved space

  2) trx undo : XA + GTID

  As the optional info, those will be controlled by TRX_UNDO_FLAGS.

     0x01 TRX_UNDO_FLAG_XID
     0x02 TRX_UNDO_FLAG_GTID
     0x80 TRX_UNDO_FLAG_TXN


  Attention:
    The UBA in undo log header only demonstrate the address, the state within UBA
    always is committed,  didn't use state to judge the transaction state.
*/

/** Those will only exist in txn undo log header*/
/*-------------------------------------------------------------*/
/* Random magic number */
#define TXN_UNDO_LOG_EXT_MAGIC (TRX_UNDO_LOG_XA_HDR_SIZE)
/* Previous scn of the trx who used the same TXN */
#define TXN_UNDO_PREV_SCN (TXN_UNDO_LOG_EXT_MAGIC + 4)
/* Previous utc of the trx who used the same TXN */
#define TXN_UNDO_PREV_UTC (TXN_UNDO_PREV_SCN + 8)
/* Previous gcn of the trx who used the same TXN */
#define TXN_UNDO_PREV_GCN (TXN_UNDO_PREV_UTC + 8)
/* Undo log state */
#define TXN_UNDO_LOG_STATE (TXN_UNDO_PREV_GCN + 8)
/* Flag how to use reserved space */
#define TXN_UNDO_LOG_EXT_FLAG (TXN_UNDO_LOG_STATE + 2)
/* Unused space */
#define TXN_UNDO_LOG_EXT_RESERVED (TXN_UNDO_LOG_EXT_FLAG + 1)
/* Unused space size */
#define TXN_UNDO_LOG_EXT_RESERVED_LEN 34
/* txn undo log header size */
#define TXN_UNDO_LOG_EXT_HDR_SIZE \
  (TXN_UNDO_LOG_EXT_RESERVED + TXN_UNDO_LOG_EXT_RESERVED_LEN)
/*-------------------------------------------------------------*/
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == TRX_UNDO_LOG_HDR_SIZE,
              "txn and trx undo log header size must be equal!");

/** Pls reuse the reserved space */
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == 275,
              "txn undo log header size cann't change!");
/** txn magic number */
#define TXN_MAGIC_N 91118498

/* States of an txn undo log header */
#define TXN_UNDO_LOG_ACTIVE   1
#define TXN_UNDO_LOG_COMMITED 2
#define TXN_UNDO_LOG_PURGED   3

namespace lizard {

/** The max percent of txn undo page that can be reused */
extern ulint txn_undo_page_reuse_max_percent;

#define TXN_UNDO_PAGE_REUSE_MAX_PERCENT \
  ((TRX_UNDO_PAGE_REUSE_LIMIT * 100) / UNIV_PAGE_SIZE)

/**------------------------------------------------------------------------*/
/** Initial value of undo ptr  */
constexpr undo_ptr_t UNDO_PTR_NULL = std::numeric_limits<undo_ptr_t>::min();

/** Fake UBA for internal table */
constexpr undo_ptr_t UNDO_PTR_FAKE = (undo_ptr_t)1 << UBA_POS_STATE;

/** Temporary table record UBA offset */
constexpr ulint UNDO_PTR_OFFSET_TEMP_TAB_REC = (ulint)0xFFFF;

/** Temporary table record UBA */
constexpr undo_ptr_t UNDO_PTR_TEMP_TAB_REC =
    (undo_ptr_t)1 << UBA_POS_STATE | (undo_ptr_t)UNDO_PTR_OFFSET_TEMP_TAB_REC;

/** Temporary table txn description */
constexpr txn_desc_t TXN_DESC_TEMP = {
    UNDO_PTR_TEMP_TAB_REC,
    {SCN_TEMP_TAB_REC, UTC_TEMP_TAB_REC, GCN_TEMP_TAB_REC}};

/** Dynamic metadata table record UBA offset */
constexpr ulint UNDO_PTR_OFFSET_DYNAMIC_METADATA = (ulint)0xFFFF - 1;

/** Temporary table record UBA */
constexpr undo_ptr_t UNDO_PTR_DYNAMIC_METADATA =
    (undo_ptr_t)1 << UBA_POS_STATE |
    (undo_ptr_t)UNDO_PTR_OFFSET_DYNAMIC_METADATA;

/** Dynamic metadata table txn description */
constexpr txn_desc_t TXN_DESC_DM = {
    UNDO_PTR_DYNAMIC_METADATA,
    {SCN_DYNAMIC_METADATA, UTC_DYNAMIC_METADATA, GCN_DYNAMIC_METADATA}};

/** Log_ddl table record UBA offset */
constexpr ulint UNDO_PTR_OFFSET_LOG_DDL = (ulint)0xFFFF - 2;

/** Log_ddl record UBA */
constexpr undo_ptr_t UNDO_PTR_LOG_DDL =
    (undo_ptr_t)1 << UBA_POS_STATE | (undo_ptr_t)UNDO_PTR_OFFSET_LOG_DDL;

/** Log ddl table txn description */
constexpr txn_desc_t TXN_DESC_LD = {UNDO_PTR_LOG_DDL,
                                    {SCN_LOG_DDL, UTC_LOG_DDL, GCN_LOG_DDL}};

/** Index UBA offset */
constexpr ulint UNDO_PTR_OFFSET_DICT_REC = (ulint)0xFFFF - 3;

/** Index UBA */
constexpr undo_ptr_t UNDO_PTR_DICT_REC =
    (undo_ptr_t)1 << UBA_POS_STATE | (undo_ptr_t)UNDO_PTR_OFFSET_DICT_REC;

/** UBA offset in undo log hdr */
constexpr ulint UNDO_PTR_OFFSET_UNDO_HDR = (ulint)0xFFFF - 4;

/** UBA in undo log hdr */
constexpr undo_ptr_t UNDO_PTR_UNDO_HDR =
    (undo_ptr_t)1 << UBA_POS_STATE | (undo_ptr_t)UNDO_PTR_OFFSET_UNDO_HDR;

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check the UBA validation */
bool undo_addr_validation(const undo_addr_t *undo_addr,
                          const dict_index_t *index);
/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validation(const page_t *page);

/** Confirm the consistent of scn, undo type, undo state. */
bool undo_scn_validation(const trx_undo_t *undo);

bool trx_undo_hdr_uba_validation(const trx_ulogf_t *log_hdr, mtr_t *mtr);

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool txn_undo_log_has_purged(const trx_rseg_t *rseg, const page_size_t &page_size);

#endif  // UNIV_DEBUG || LIZARD_DEBUG

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/

extern void trx_undo_hdr_init_scn(trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_scn    commit scn number
  @param[in]      mtr           current mtr context
*/
extern void trx_undo_hdr_write_scn(trx_ulogf_t *log_hdr, commit_scn_t &cmmt_scn,
                                   mtr_t *mtr);
/**
  Read UBA.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
undo_ptr_t trx_undo_hdr_read_uba(const trx_ulogf_t *log_hdr, mtr_t *mtr);
/**
  Write the UBA address into undo log header
  @param[in]      undo log header
  @param[in]      UBA
  @param[in]      mtr
*/
extern void trx_undo_hdr_write_uba(trx_ulogf_t *log_hdr,
                                   const undo_addr_t &undo_addr, mtr_t *mtr);
/**
  Write the UBA address into undo log header
  @param[in]      undo log header
  @param[in]      trx
  @param[in]      mtr
*/
extern void trx_undo_hdr_write_uba(trx_ulogf_t *log_hdr, const trx_t *trx,
                                   mtr_t *mtr);
/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
extern commit_scn_t trx_undo_hdr_read_scn(const trx_ulogf_t *log_hdr,
                                          mtr_t *mtr);

/**
  Check if the undo log header is reused.

  @param[in]      undo_page     undo log header page
  @param[out]     commit_scn    commit scn if have, otherwise 0
  @param[in]      mtr

  @return         bool          ture if the undo log header is reused
*/
bool txn_undo_header_reuse_if_need(
    const page_t *undo_page,
    commit_scn_t *commit_scn,
    mtr_t *mtr);

/**
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
extern void trx_undo_hdr_add_space_for_txn(page_t *undo_page,
                                           trx_ulogf_t *log_hdr, mtr_t *mtr);
/**
  Init the txn extension information.

  @param[in]      undo          undo memory struct
  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      prev_image    prev scn/utc if the undo log header is reused
  @param[in]      mtr
*/
void trx_undo_hdr_init_for_txn(trx_undo_t *undo, page_t *undo_page,
                               trx_ulogf_t *log_hdr,
                               const commit_scn_t &prev_image, mtr_t *mtr);

/**
  Read the txn undo log header extension information.

  @param[in]      undo page
  @param[in]      undo log hdr
  @param[in]      mtr
  @param[out]     txn_undo_ext
*/
void trx_undo_hdr_read_txn(const page_t *undo_page,
                           const trx_ulogf_t *undo_header, mtr_t *mtr,
                           txn_undo_hdr_t *txn_undo_hdr);
/**
  Read the scn, utc, gcn from prev image.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
commit_scn_t txn_undo_hdr_read_prev_scn(const trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the scn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_scn(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the scn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_scn(byte *ptr, scn_id_t scn);

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_undo_ptr(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the UBA into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        undo_ptr  UBA
*/
void trx_write_undo_ptr(byte *ptr, undo_ptr_t undo_ptr);

/**
  Read the scn
  @param[in]        ptr       buffer

  @return           scn_id_t  scn
*/
scn_id_t trx_read_scn(const byte *ptr);

/**
  Read the UBA
  @param[in]        ptr        buffer

  @return           undo_ptr_t undo_ptr
*/
undo_ptr_t trx_read_undo_ptr(const byte *ptr);

/**
  Decode the undo_ptr into UBA
  @param[in]      undo ptr
  @param[out]     undo addr
 */
void undo_decode_undo_ptr(undo_ptr_t undo_ptr, undo_addr_t *undo_addr);

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
void undo_encode_undo_addr(const undo_addr_t &undo_addr,
                           undo_ptr_t *undo_ptr);

/*-----------------------------------------------------------------------------*/

/* Lizard transaction rollback segment operation */
/*-----------------------------------------------------------------------------*/
/**
  Whether the txn rollback segment has been assigned
  @param[in]      trx
*/
bool trx_is_txn_rseg_assigned(trx_t *trx);
/**
  Whether the txn undo log has modified.
*/
bool trx_is_txn_rseg_updated(const trx_t *trx);
/**
  Always assign transaction rollback segment for trx
  @param[in]      trx
*/
void trx_assign_txn_rseg(trx_t *trx);
/**
  Always assign a txn undo log for transaction.

  @param[in]        trx         current transaction

  @return           DB_SUCCESS  Success
*/
dberr_t trx_always_assign_txn_undo(trx_t *trx);
/*-----------------------------------------------------------------------------*/
/**
  Init the txn description as NULL initial value.
  @param[in]      trx       current transaction
*/
void trx_init_txn_desc(trx_t *trx);
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
commit_scn_t trx_commit_scn(trx_t *trx, commit_scn_t *scn_ptr, trx_undo_t *undo,
                            page_t *undo_hdr_page, ulint hdr_offset,
                            bool *serialised, mtr_t *mtr);
/**
  Cleanup txn undo log segment when commit,

  It will :
    1) Add the UBA header into rseg->history
    2) Reinit the rseg->slot as FIL_NULL
    3) Destroy or reuse the undo mem object

  @param[in]      trx         trx owning the txn undo log
  @param[in/out]  undo_ptr    txn undo log structure
  @param[in]      undo_page   txn undo log header page, x-latched
  @param[in]      update_rseg_history_len
                              if true: update rseg history
                              len else skip updating it.
  @param[in]      n_added_logs
                              number of logs added
  @param[in]      mtr
*/
void trx_txn_undo_cleanup(trx_t *trx, txn_undo_ptr_t *undo_ptr,
                          page_t *undo_page, bool update_rseg_history_len,
                          ulint n_added_logs, mtr_t *mtr);
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
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_purge_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr);

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup info, nullptr if don't care.
  @param[in]      txn_mtr       Non-nullptr if use external mtr, the caller is
                                responsible for committing mtr;
                                If passing nullptr, it will use a temporary mtr.

  @return         bool       true if the record should be cleaned out.
*/
bool txn_undo_hdr_lookup_low(txn_rec_t *txn_rec,
                             txn_lookup_t *txn_lookup,
                             mtr_t *txn_mtr);

/**
  set txn_undo_hdr_t

  @param[in/out]  txn_rec    txn info of the records.

  @return         bool       true if the record should be cleaned out.
*/
inline void txn_lookup_t_set(txn_lookup_t *txn_lookup,
                             const txn_undo_hdr_t &txn_undo_hdr,
                             const commit_scn_t &real_image,
                             const txn_state_t &real_state) {
  if (txn_lookup == nullptr)  return;

  txn_lookup->txn_undo_hdr = txn_undo_hdr;
  txn_lookup->real_image = real_image;
  txn_lookup->real_state = real_state;
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  if (real_state == TXN_STATE_ACTIVE ||
      real_state == TXN_STATE_COMMITTED ||
      real_state == TXN_STATE_PURGED) {
    if (real_state != TXN_STATE_ACTIVE) {
      /** TXN reuse, current scn should be larger than prev scn */
      ut_a(txn_undo_hdr.image.scn > txn_undo_hdr.prev_image.scn);
    }
    ut_a(real_image == txn_undo_hdr.image);
  } else if (real_state == TXN_STATE_REUSE) {
    ut_a(real_image == txn_undo_hdr.prev_image);
  }
#endif /* UNIV_DEBUG || LIZARD_DEBUG */
}

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup info, nullptr if don't care.
  @param[in]      txn_mtr       Non-nullptr if use external mtr, the caller is
                                responsible for committing mtr;
                                If passing nullptr, it will use a temporary mtr.

  @return         bool       true if the record should be cleaned out.
*/
inline bool txn_undo_hdr_lookup(txn_rec_t *txn_rec,
                                txn_lookup_t *txn_lookup,
                                mtr_t *txn_mtr) {
  /* If txn_mtr != nullptr, it forces to hold txn header s-latch for
  a while. */
  if (!txn_lookup && !lizard_undo_ptr_is_active(txn_rec->undo_ptr)) {
    /** scn must allocated */
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn < SCN_MAX);
    return false;
  } else {
    return txn_undo_hdr_lookup_low(txn_rec, txn_lookup, txn_mtr);
  }
}

inline bool txn_lookup_rollptr_is_valid(const txn_lookup_t *txn_lookup) {
  return (txn_lookup->real_state < txn_state_t::TXN_STATE_PURGED);
}

/** Collect rsegs into the purge heap for the first time */
bool trx_collect_rsegs_for_purge(TxnUndoRsegs *elem,
                                 trx_undo_ptr_t *redo_rseg_undo_ptr,
                                 trx_undo_ptr_t *temp_rseg_undo_ptr,
                                 txn_undo_ptr_t *txn_rseg_undo_ptr);

/** Add the rseg into the purge queue heap */
void trx_add_rsegs_for_purge(commit_scn_t &scn, TxnUndoRsegs *elem);

/** Set txn undo log state.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction
@param[in]      state	  state */
inline void txn_undo_set_state(trx_ulogf_t *log_hdr, ulint state, mtr_t *mtr) {
  /* When creating a new page, hold SX latch, otherwise X latch */
  ut_ad(mtr_memo_contains_page(mtr, page_align(log_hdr),
        MTR_MEMO_PAGE_SX_FIX | MTR_MEMO_PAGE_X_FIX));

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  auto page = page_align(log_hdr);
  auto type = mach_read_from_2(page + TRX_UNDO_PAGE_HDR + TRX_UNDO_PAGE_TYPE);
  auto flag = mach_read_from_1(log_hdr + TRX_UNDO_FLAGS);
  ulint old_state = mach_read_from_2(log_hdr + TXN_UNDO_LOG_STATE);

  ut_a(type == TRX_UNDO_TXN);

  /* It must be TXN undo log, or be initializing */
  ut_a(state == TXN_UNDO_LOG_ACTIVE || (flag & TRX_UNDO_FLAG_TXN) != 0);

  if (state == TXN_UNDO_LOG_COMMITED)
    ut_a(old_state == TXN_UNDO_LOG_ACTIVE);
  else if (state == TXN_UNDO_LOG_PURGED)
    ut_a(old_state == TXN_UNDO_LOG_COMMITED || /* Normal case */
         old_state == TXN_UNDO_LOG_PURGED);    /* Crash recovery */
  else
    ut_a(state == TXN_UNDO_LOG_ACTIVE);
#endif

  mlog_write_ulint(log_hdr + TXN_UNDO_LOG_STATE, state, MLOG_2BYTES, mtr);
}

/** Set txn undo log state to purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     The corresponding commit_scn if it's TXN undo,
            or err if it's not TXN undo */
inline std::pair<commit_scn_t, bool> txn_undo_set_state_at_purge(
    const trx_rseg_t *rseg, const page_size_t &page_size) {
  commit_scn_t cmmt = COMMIT_SCN_NULL;

  if (fsp_is_txn_tablespace_by_id(rseg->space_id)) {
    mtr_t mtr;
    mtr_start(&mtr);

    const page_id_t page_id(rseg->space_id, rseg->last_page_no);
    page_t *undo_page = trx_undo_page_get(page_id, page_size, &mtr);
    trx_ulogf_t *undo_header = undo_page + rseg->last_offset;

    txn_undo_set_state(undo_header, TXN_UNDO_LOG_PURGED, &mtr);

    cmmt = trx_undo_hdr_read_scn(undo_header, &mtr);

    mtr_commit(&mtr);

    return std::make_pair(cmmt, false);
  }
  return std::make_pair(cmmt, true);
}

/** Set txn undo log state when commiting.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
inline void txn_undo_set_state_at_finish(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  txn_undo_set_state(log_hdr, TXN_UNDO_LOG_COMMITED, mtr);
}

/** Set txn undo log state when initializing.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
inline void txn_undo_set_state_at_init(trx_ulogf_t *log_hdr, mtr_t *mtr) {
  txn_undo_set_state(log_hdr, TXN_UNDO_LOG_ACTIVE, mtr);
}

/*
  Undo retention controller.
*/
class Undo_retention {
 public:
  // user configurations
  static ulint retention_time; // in seconds
  static ulint space_limit;    // in MiB
  static ulint space_reserve;  // in MiB
  // show status
  static char status[128];

  static int check_limit(THD *thd, SYS_VAR *var, void *save,
                         struct st_mysql_value *value);
  static int check_reserve(THD *thd, SYS_VAR *var, void *save,
                           struct st_mysql_value *value);
  static void on_update(THD *, SYS_VAR *, void *var_ptr, const void *save);

 protected:
  volatile bool m_stat_done;

  volatile ulint m_last_top_utc; // to output status

  std::atomic<ulint> m_total_used_size;

  Undo_retention () :
      m_stat_done(false),
      m_last_top_utc(0),
      m_total_used_size(0) {}

  Undo_retention &operator=(const Undo_retention&) = delete;
  Undo_retention(const Undo_retention&) = delete;

  static Undo_retention inst; // global instance

  static ulint current_utc() { return  ut_time_system_us() / 1000000; }

  static ulint mb_to_pages(ulint size) {
    return (ulint)(1024.0 * 1024.0 / univ_page_size.physical() * size);
  }

 public:
  static Undo_retention *instance() { return &inst; }

  /* Collect latest undo space sizes periodically */
  void refresh_stat_data();

  /* Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge */
  bool purge_advise();
};

}  // namespace lizard

/** Delcare the functions which were defined in other cc files.*/
/*=============================================================================*/
extern trx_undo_t *trx_undo_reuse_cached(trx_t *trx, trx_rseg_t *rseg,
                                         ulint type, trx_id_t trx_id,
                                         const XID *xid, bool is_gtid,
                                         mtr_t *mtr);

extern dberr_t trx_undo_create(trx_t *trx, trx_rseg_t *rseg, ulint type,
                               trx_id_t trx_id, const XID *xid, bool is_gtid,
                               trx_undo_t **undo, mtr_t *mtr);

void trx_resurrect_update_in_prepared_state(trx_t *trx, const trx_undo_t *undo);

void trx_purge_remove_log_hdr(trx_rsegf_t *rseg_hdr, trx_ulogf_t *log_hdr,
                              mtr_t *mtr);

trx_undo_t *trx_undo_mem_create(trx_rseg_t *rseg, ulint id, ulint type,
                                trx_id_t trx_id, const XID *xid,
                                page_no_t page_no, ulint offset);

void trx_undo_page_init(page_t *undo_page, /*!< in: undo log segment page */
                        ulint type,        /*!< in: undo log segment type */
                        mtr_t *mtr);       /*!< in: mtr */

ulint trx_undo_header_create(page_t *undo_page, /*!< in/out: undo log segment
                                                header page, x-latched; it is
                                                assumed that there is
                                                TRX_UNDO_LOG_HDR_SIZE bytes
                                                free space on it */
                             trx_id_t trx_id,   /*!< in: transaction id */
                             commit_scn_t *prev_image,
                                                /*!< out: previous scn/utc
                                                if have. Only used in TXN
                                                undo header. Pass in as NULL
                                                if don't care. */
                             mtr_t *mtr);       /*!< in: mtr */

/** Remove an rseg header from the history list.
@param[in,out]	rseg_hdr	rollback segment header
@param[in]	log_hdr		undo log segment header
@param[in,out]	mtr		mini transaction. */
void trx_purge_remove_log_hdr(trx_rsegf_t *rseg_hdr, trx_ulogf_t *log_hdr,
                              mtr_t *mtr);
/** Adds space for the XA XID after an undo log old-style header.
@param[in,out]	undo_page	undo log segment header page
@param[in,out]	log_hdr		undo log header
@param[in,out]	mtr		mini transaction
@param[in]	add_gtid	add space for GTID */
void trx_undo_header_add_space_for_xid(page_t *undo_page, trx_ulogf_t *log_hdr,
                                       mtr_t *mtr, bool add_gtid);

/*=============================================================================*/

#define TXN_DESC_NULL                                                          \
  { lizard::UNDO_PTR_NULL, COMMIT_SCN_NULL }

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/* Assert the txn_desc is initial */
#define assert_txn_desc_initial(trx)                                           \
  do {                                                                         \
    ut_a((trx)->txn_desc.undo_ptr == lizard::UNDO_PTR_NULL &&                  \
         lizard::commit_scn_state((trx)->txn_desc.cmmt) == SCN_STATE_INITIAL); \
  } while (0)

/* Assert the txn_desc is allocated */
#define assert_txn_desc_allocated(trx)                                         \
  do {                                                                         \
    ut_a((trx)->txn_desc.undo_ptr != lizard::UNDO_PTR_NULL &&                  \
         lizard::commit_scn_state((trx)->txn_desc.cmmt) == SCN_STATE_INITIAL); \
  } while (0)

#define assert_undo_ptr_initial(undo_ptr)                                      \
  do {                                                                         \
    ut_a((*(undo_ptr)) == lizard::UNDO_PTR_NULL);                              \
  } while (0)

#define assert_undo_ptr_allocated(undo_ptr)                                    \
  do {                                                                         \
    ut_a((undo_ptr) != lizard::UNDO_PTR_NULL);                                 \
  } while (0)

#define assert_trx_undo_ptr_initial(trx)                                       \
  assert_undo_ptr_initial((&(trx)->txn_desc.undo_ptr))

#define assert_trx_undo_ptr_allocated(trx)                                     \
  assert_undo_ptr_allocated((trx)->txn_desc.undo_ptr)

#define lizard_trx_undo_page_validation(page)                                  \
  do {                                                                         \
    ut_a(lizard::trx_undo_page_validation(page));                              \
  } while (0)

#define lizard_trx_undo_hdr_uba_validation(undo_hdr, mtr)     \
  do {                                                        \
    ut_a(lizard::trx_undo_hdr_uba_validation(undo_hdr, mtr)); \
  } while (0)

#define lizard_trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)           \
  do {                                                                         \
    txn_undo_hdr_t txn_undo_hdr;                                               \
    lizard::trx_undo_hdr_read_txn(undo_page, undo_hdr, mtr, &txn_undo_hdr);    \
    ut_a(txn_undo_hdr.magic_n == TXN_MAGIC_N && txn_undo_hdr.ext_flag == 0);   \
  } while (0)

#define lizard_undo_addr_validation(undo_addr, index)                          \
  do {                                                                         \
    ut_a(lizard::undo_addr_validation(undo_addr, index));                      \
  } while (0)

#define lizard_undo_scn_validation(undo)                                       \
  do {                                                                         \
    ut_a(lizard::undo_scn_validation(undo));                                   \
  } while (0)

#define assert_trx_in_recovery(trx)                                            \
  do {                                                                         \
    if ((trx)->rsegs.m_txn.rseg != NULL && (trx)->rsegs.m_redo.rseg == NULL) { \
      ut_a((trx)->is_recovered);                                               \
    }                                                                          \
  } while (0)

#define lizard_txn_undo_free_list_validate(rseg_hdr, undo_page, mtr)           \
  do {                                                                         \
    ut_a(lizard::txn_undo_free_list_validate(rseg_hdr, undo_page, mtr));       \
  } while (0)
#else

#define lizard_trx_undo_page_validation(page)
#define assert_txn_desc_initial(trx)
#define assert_txn_desc_allocated(trx)
#define assert_undo_ptr_initial(undo_ptr)
#define assert_undo_ptr_allocated(undo_ptr)
#define assert_trx_undo_ptr_initial(trx)
#define assert_trx_undo_ptr_allocated(trx)

#define lizard_trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)
#define lizard_undo_addr_validation(undo_addr, index)
#define lizard_undo_scn_validation(undo)
#define assert_trx_in_recovery(trx)
#define lizard_txn_undo_free_list_validate(rseg_hdr, undo_page, mtr)
#define lizard_trx_undo_hdr_uba_validation(undo_hdr, mtr)

#endif

#endif  // lizard0undo_h
