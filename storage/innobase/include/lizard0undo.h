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

#include "clone0repl.h"
#include "fut0lst.h"
#include "trx0types.h"
#include "trx0undo.h"

#include "lizard0dbg.h"
#include "lizard0scn.h"
#include "lizard0undo0types.h"
#include "lizard0ut.h"
#include "ut0dbg.h"

#include "sql/binlog/binlog_xa_specification.h"

#include <atomic>
#include <queue>
#include <set>
#include <vector>

#ifdef UNIV_PFS_MUTEX
/* Lizard undo retention start mutex PFS key */
extern mysql_pfs_key_t undo_retention_mutex_key;
#endif

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
    The UBA in undo log header only demonstrate the address, the state within
  UBA always is committed,  didn't use state to judge the transaction state.
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
#define TXN_UNDO_LOG_EXT_STORAGE (TXN_UNDO_LOG_STATE + 2)
/* New flags. */
#define TXN_UNDO_LOG_TAGS_1 (TXN_UNDO_LOG_EXT_STORAGE + 1)
/* Unused space */
#define TXN_UNDO_LOG_EXT_RESERVED (TXN_UNDO_LOG_TAGS_1 + 2)
/* Unused space size */
#define TXN_UNDO_LOG_EXT_RESERVED_LEN 32
/* txn undo log header size */
#define TXN_UNDO_LOG_EXT_HDR_SIZE \
  (TXN_UNDO_LOG_EXT_RESERVED + TXN_UNDO_LOG_EXT_RESERVED_LEN)
/*-------------------------------------------------------------*/
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == TRX_UNDO_LOG_GTID_HDR_SIZE,
              "txn and trx undo log header size must be equal!");

/** Pls reuse the reserved space */
static_assert(TXN_UNDO_LOG_EXT_HDR_SIZE == 275,
              "txn undo log header size cann't change!");
/** txn magic number */
#define TXN_MAGIC_N 91118498

/* States of an txn undo log header */
#define TXN_UNDO_LOG_ACTIVE 1
#define TXN_UNDO_LOG_COMMITED 2
#define TXN_UNDO_LOG_PURGED 3

/*****************************************
 *        TXN_UNDO_LOG_EXT_STORAGE           *
 *****************************************/
/** Empty TXN_UNDO_LOG_EXT_STORAGE */
#define TXN_EXT_STORAGE_NONE 0x00
/** bit_0: TXN have TXN_UNDO_LOG_TAGS_1. */
#define TXN_EXT_FLAG_HAVE_TAGS_1 0x01
/** Initial value of TXN_UNDO_LOG_EXT_STORAGE. */
#define TXN_EXT_STORAGE_V1 TXN_EXT_FLAG_HAVE_TAGS_1

/******************************************
 *           TXN_UNDO_LOG_TAGS_1       *
 ******************************************/
/** bit_0: Finish state of the transaction (true: ROLLBACK,
false: other state). */
#define TXN_NEW_TAGS_1_ROLLBACK 0x01

namespace lizard {

/** The max percent of txn undo page that can be reused */
extern ulint txn_undo_page_reuse_max_percent;

/** Max list size of txn_undo_cached of a rsegment. */
extern ulint srv_txn_cached_list_keep_size;

/** Retention time of txn undo data in seconds. */
extern ulong txn_retention_time;

#define TXN_UNDO_PAGE_REUSE_MAX_PCT_DEF 90
#define TXN_UNDO_PAGE_REUSE_LIMIT (9 * UNIV_PAGE_SIZE / 10)

#define TXN_UNDO_PAGE_REUSE_MAX_PERCENT \
  ((TXN_UNDO_PAGE_REUSE_LIMIT * 100) / UNIV_PAGE_SIZE)

/**------------------------------------------------------------------------*/
/** NULL value of slot ptr  */
constexpr undo_ptr_t UNDO_PTR_NULL = std::numeric_limits<undo_ptr_t>::min();

/** SLOT OFFSET:: Temporary table record */
constexpr ulint SLOT_OFFSET_TEMP_TAB_REC = (ulint)0xFFFF;

/** SLOT OFFSET:: Dynamic metadata table record */
constexpr ulint SLOT_OFFSET_DYNAMIC_METADATA = (ulint)0xFFFF - 1;

/** SLOT OFFSET:: Log_ddl table record */
constexpr ulint SLOT_OFFSET_LOG_DDL = (ulint)0xFFFF - 2;

/** SLOT OFFSET:: Index record */
constexpr ulint SLOT_OFFSET_DICT_REC = (ulint)0xFFFF - 3;

/** SLOT OFFSET:: UBA offset for no_redo insert/update undo. */
constexpr ulint SLOT_OFFSET_NO_REDO = (ulint)0xFFFF - 4;

/** SLOT OFFSET:: Index UBA that upgraded from old version. */
constexpr ulint SLOT_OFFSET_INDEX_UPGRADE = (ulint)0xFFFF - 5;

/** Lowest offset for all special cases. */
constexpr ulint SLOT_OFFSET_LIMIT = SLOT_OFFSET_INDEX_UPGRADE;

/** Please update limit value to minval from 0xFFFF. */
static_assert(SLOT_OFFSET_LIMIT + 5 == SLOT_OFFSET_TEMP_TAB_REC,
              "Please update limit.");

/** Special simulate page no for slot address. */
constexpr ulint SLOT_PAGE_NO_FAKE = 0;

/** Special simulate space id for slot address. */
constexpr ulint SLOT_SPACE_ID_FAKE = 0;

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
extern void undo_encode_undo_addr(const undo_addr_t &undo_addr,
                                  undo_ptr_t *undo_ptr);

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
    txn_desc_temp.assemble(cmmt, slot_addr);

    /** Dynamic metadata table txn description */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
                 SLOT_OFFSET_DYNAMIC_METADATA};
    cmmt = {SCN_DYNAMIC_METADATA, US_DYNAMIC_METADATA, GCN_DYNAMIC_METADATA,
            CSR_AUTOMATIC};
    txn_desc_dm.assemble(cmmt, slot_addr);

    /** Log ddl table txn description */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_LOG_DDL};

    cmmt = {SCN_LOG_DDL, US_LOG_DDL, GCN_LOG_DDL, CSR_AUTOMATIC};
    txn_desc_ld.assemble(cmmt, slot_addr);

    /** dd index txn for dd table. */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_DICT_REC};
    cmmt = {SCN_DICT_REC, US_DICT_REC, GCN_DICT_REC, CSR_AUTOMATIC};
    txn_desc_dd.assemble(cmmt, slot_addr);

    /** dd index txn for dd table upgrade */
    slot_addr = {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
                 SLOT_OFFSET_INDEX_UPGRADE};
    cmmt = {SCN_INDEX_UPGRADE, US_INDEX_UPGRADE, GCN_INDEX_UPGRADE,
            CSR_AUTOMATIC};
    txn_desc_dd_upgrade.assemble(cmmt, slot_addr);
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
    undo_encode_undo_addr(undo_addr, &undo_ptr);
    if (undo_ptr == txn_desc_dm.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for temporary */
  bool is_temporary(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_encode_undo_addr(undo_addr, &undo_ptr);
    if (undo_ptr == txn_desc_temp.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for log ddl */
  bool is_log_ddl(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_encode_undo_addr(undo_addr, &undo_ptr);
    if (undo_ptr == txn_desc_ld.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_encode_undo_addr(undo_addr, &undo_ptr);
    if (undo_ptr == txn_desc_dd.undo_ptr) return true;

    return false;
  }

  /** Whether undo address is for dd index of dd table. */
  bool is_dd_index_of_dd_upgrade(const undo_addr_t &undo_addr) {
    undo_ptr_t undo_ptr;
    undo_encode_undo_addr(undo_addr, &undo_ptr);
    if (undo_ptr == txn_desc_dd_upgrade.undo_ptr) return true;

    return false;
  }

  bool is_special(const undo_addr_t &undo_addr) {
    return is_temporary(undo_addr) || is_log_ddl(undo_addr) ||
           is_dynamic_metadata(undo_addr) || is_dd_index_of_dd(undo_addr) ||
           is_dd_index_of_dd_upgrade(undo_addr);
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

/* Lizard transaction undo header operation */
/*-----------------------------------------------------------------------------*/
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check the UBA validation */
bool undo_addr_validate(const undo_addr_t *undo_addr,
                        const dict_index_t *index);

bool slot_addr_validate(const slot_addr_t &slot_addr);
/**
  Validate the page is undo page

  @param[in]      page      undo page
  @return         true      it's undo page
*/
bool trx_undo_page_validate(const page_t *page);

/** Confirm the consistent of scn, undo type, undo state. */
bool undo_commit_mark_validate(const trx_undo_t *undo);

bool trx_undo_hdr_slot_validate(const trx_ulogf_t *log_hdr, mtr_t *mtr);

/** Check if an update undo log has been marked as purged.
@param[in]  rseg txn rseg
@param[in]  page_size
@return     true   if purged */
bool txn_undo_log_has_purged(const trx_rseg_t *rseg,
                             const page_size_t &page_size);

#endif  // UNIV_DEBUG || LIZARD_DEBUG

bool undo_slot_addr_equal(const slot_addr_t &slot_addr,
                          const undo_ptr_t undo_ptr);
/**
  Get txn undo state at trx finish.

  @param[in]      free_limit       space left on txn undo page
  @return  TRX_UNDO_TO_PURGE or TRX_UNDO_CACHED
*/
extern ulint decide_txn_undo_state_at_finish(ulint free_limit);

/**
  Initial the NULL value on SCN and UTC when create undo log header.
  include all kinds of undo log header type.
  The redo log logic is included in "MLOG_UNDO_HDR_CREATE";

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/

extern void trx_undo_hdr_init_cmmt(trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Write the scn and utc when commit.
  Include the redo log

  @param[in]      log_hdr       undo log header
  @param[in]      commit_mark    commit scn number
  @param[in]      mtr           current mtr context
*/
extern void trx_undo_hdr_write_cmmt(trx_ulogf_t *log_hdr,
                                    commit_mark_t &cmmt_scn, mtr_t *mtr);
/**
  Read Slot address.

  @param[in]      log_hdr       undo log header
  @param[out]     slot addr	decode from slot ptr.
  @param[in]      mtr           current mtr context
*/
slot_ptr_t trx_undo_hdr_read_slot(const trx_ulogf_t *log_hdr,
                                  slot_addr_t *slot_addr, mtr_t *mtr);
/**
  Write the slot address into undo log header
  @param[in]      undo log header
  @param[in]      slot
  @param[in]      mtr
*/
extern void trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr,
                                    const slot_addr_t &slot_addr, mtr_t *mtr);
/**
  Write the slot address into undo log header
  @param[in]      undo log header
  @param[in]      trx
  @param[in]      mtr
*/
extern slot_addr_t trx_undo_hdr_write_slot(trx_ulogf_t *log_hdr,
                                           const trx_t *trx, mtr_t *mtr);
/**
  Read the scn and utc.

  @param[in]      log_hdr       undo log header
  @param[in]      mtr           current mtr context
*/
extern commit_mark_t trx_undo_hdr_read_cmmt(const trx_ulogf_t *log_hdr,
                                            mtr_t *mtr);

/**
  Check if the undo log header is reused.

  @param[in]      undo_page     undo log header page
  @param[out]     commit_mark    commit scn if have, otherwise 0
  @param[in]      mtr

  @return         bool          ture if the undo log header is reused
*/
bool txn_undo_header_reuse_if_need(const page_t *undo_page,
                                   commit_mark_t *commit_mark, mtr_t *mtr);

/**
  Add the space for the txn especially.

  @param[in]      undo_page     undo log header page
  @param[in]      log_hdr       undo log hdr
  @param[in]      mtr
*/
extern void trx_undo_hdr_add_space_for_txn(page_t *undo_page,
                                           trx_ulogf_t *log_hdr, mtr_t *mtr);

/**
  Initialize the txn extension fields for the txn undo log header.

  @param[in]      undo_page         undo log header page
  @param[in]      log_hdr           undo log hdr
  @param[in]      prev_image        prev scn/utc if the undo log header is
  reused
  @param[in]      txn_ext_storage   txn extension storage flag
  @param[in]      mtr               mini transaction
*/
void trx_undo_hdr_txn_ext_init(page_t *undo_page, trx_ulogf_t *log_hdr,
                               const commit_mark_t &prev_image,
                               uint8 txn_ext_storage, mtr_t *mtr);

/**
  Add space for txn extension and initialize the fields.
  @param[in]      rseg              rollback segment
  @param[in]      undo_page         undo log header page
  @param[in]      mtr               mini transaction
  @param[in]      offset            txn header byte offset on page
  @param[in]      txn_ext_storage   txn extension storage flag
  @param[out]     slot_addr         slot address of created txn
  @param[out]     prev_image        prev scn/utc
*/
void trx_undo_header_add_space_for_txn(trx_rseg_t *rseg, page_t *undo_page,
                                       mtr_t *mtr, ulint offset,
                                       uint8 txn_ext_storage,
                                       slot_addr_t *slot_addr,
                                       commit_mark_t *prev_image);
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
commit_mark_t txn_undo_hdr_read_prev_cmmt(const trx_ulogf_t *log_hdr,
                                          mtr_t *mtr);

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
  Write the gcn into the buffer
  @param[in/out]    ptr       buffer
  @param[in]        txn_desc  txn description
*/
void trx_write_gcn(byte *ptr, const txn_desc_t *txn_desc);

/**
  Write the gcn into the buffer
  @param[in/out]    ptr     buffer
  @param[in]        scn     scn id
*/
void trx_write_gcn(byte *ptr, gcn_t gcn);

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
  Read the gcn
  @param[in]        ptr       buffer

  @return           gcn_t  scn
*/
gcn_t trx_read_gcn(const byte *ptr);

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
void undo_encode_undo_addr(const undo_addr_t &undo_addr, undo_ptr_t *undo_ptr);

/**
  Decode the slot_ptr into addr
  @param[in]      slot ptr
  @param[out]     slot addr
 */
void undo_decode_slot_ptr(slot_ptr_t slot_ptr, slot_addr_t *slot_addr);

/**
  Encode addr into slot_ptr that need to write undo header.
  @param[in]      slot addr
  @param[out]     slot ptr
*/
void undo_encode_slot_addr(const slot_addr_t &slot_addr, slot_ptr_t *slot_ptr);

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

/**
  If during an external XA, check whether the mapping relationship between xid
  and rollback segment is as expected.

  @param[in]        trx         current transaction

  @return           true        if success
*/
bool txn_check_xid_rseg_mapping(const XID *xid, const trx_rseg_t *expect_rseg);

/** Allocate txn undo and return transaction slot address.
 *
 * @param[in]	trx
 * @param[out]	Slot address
 *
 * @retval	innodb error code.
 **/
dberr_t trx_assign_txn_undo(trx_t *trx, slot_ptr_t *slot_ptr);

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
commit_mark_t trx_commit_mark(trx_t *trx, commit_mark_t *scn_ptr,
                              trx_undo_t *undo, page_t *undo_hdr_page,
                              ulint hdr_offset, bool *serialised, mtr_t *mtr);
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
  Recycle txn undo log segment
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_recycle_segment(trx_rseg_t *rseg, fil_addr_t hdr_addr);

/**
  Put the txn undo log segment into cached list after purge all.
  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
  @retval	    true	Not available slot
  @retval	    false	Success
*/
bool txn_purge_segment_to_cached_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                      mtr_t *mtr);

/**
  Put the txn undo log segment into free list after purge all.

  @param[in]        rseg        rollback segment
  @param[in]        hdr_addr    txn log hdr address
*/
void txn_purge_segment_to_free_list(trx_rseg_t *rseg, fil_addr_t hdr_addr,
                                    mtr_t *mtr);

/**
  Try to lookup the real scn of given records.

  @param[in/out]  txn_rec       txn info of the records.
  @param[out]     txn_lookup    txn lookup info, nullptr if don't care.
  @param[in]      txn_mtr       Non-nullptr if use external mtr, the caller is
                                responsible for committing mtr;
                                If passing nullptr, it will use a temporary mtr.

  @return         bool       true if the record should be cleaned out.
*/
bool txn_undo_hdr_lookup_low(txn_rec_t *txn_rec, txn_lookup_t *txn_lookup,
                             mtr_t *txn_mtr);

/**
  set txn_undo_hdr_t

  @param[in/out]  txn_rec    txn info of the records.

  @return         bool       true if the record should be cleaned out.
*/
inline void txn_lookup_t_set(txn_lookup_t *txn_lookup,
                             const txn_undo_hdr_t &txn_undo_hdr,
                             const commit_mark_t &real_image,
                             const txn_state_t &real_state) {
  if (txn_lookup == nullptr) return;

  txn_lookup->txn_undo_hdr = txn_undo_hdr;
  txn_lookup->real_image = real_image;
  txn_lookup->real_state = real_state;
#if defined UNIV_DEBUG || defined LIZARD_DEBUG
  if (real_state == TXN_STATE_ACTIVE || real_state == TXN_STATE_COMMITTED ||
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

inline bool txn_rec_lock_state_by_lookup(txn_rec_t *txn_rec,
                                         txn_lookup_t *txn_lookup, mtr_t *mtr) {
  ut_ad(txn_lookup && mtr);
  return txn_undo_hdr_lookup_low(txn_rec, txn_lookup, mtr);
}

/**
  Decide the real trx state.
    1) Search tcn cache
    2) Lookup txn undo

  Return the cleanout flag to decide yourself.

  @param[in/out]	txn record
  @param[in/out]	cleanout is needed?

  @retval	true		active
                false		committed
*/
inline bool txn_rec_real_state_by_misc(txn_rec_t *txn_rec,
                                       bool *cleanout = nullptr) {
  bool active = false;
  bool cache_hit = false;
  /** If record is not active, the trx must be committed. */
  if (!undo_ptr_is_active(txn_rec->undo_ptr)) {
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);
    /** The record has been cleaned out already. */
    if (cleanout) *cleanout = false;

    return false;
  }

  /** Pcur is nullptr, didn't support block level tcn cache. */
  cache_hit = trx_search_tcn(txn_rec, nullptr, nullptr);
  if (cache_hit) {
    ut_ad(!undo_ptr_is_active(txn_rec->undo_ptr));
    lizard_ut_ad(txn_rec->scn > 0 && txn_rec->scn <= SCN_MAX);
    lizard_ut_ad(txn_rec->gcn > 0 && txn_rec->gcn <= GCN_MAX);
    if (cleanout) *cleanout = true;
    return false;
  }

  /** Record is still active, lookup txn hdr to confirm it. */
  active = txn_undo_hdr_lookup_low(txn_rec, nullptr, nullptr);
  if (active) {
    return active;
  } else {
    /** cleanout when trx real state has committed. */
    if (cleanout) *cleanout = true;
    return false;
  }
}

/**
  Decide the real trx state when read current record.
    1) Search tcn cache
    2) Lookup txn undo

  and try to collect cursor to cache txn and cleanout record.


  @param[in/out]	txn record
  @param[in/out]	cleanout is needed?

  @retval	true		active
                false		committed
*/
extern bool txn_rec_cleanout_state_by_misc(txn_rec_t *txn_rec, btr_pcur_t *pcur,
                                           const rec_t *rec,
                                           const dict_index_t *index,
                                           const ulint *offsets);

inline bool txn_lookup_rollptr_is_valid(const txn_lookup_t *txn_lookup) {
  return (txn_lookup->real_state < txn_state_t::TXN_STATE_PURGED);
}

/** Collect rsegs into the purge heap for the first time */
bool trx_collect_rsegs_for_purge(TxnUndoRsegs *elem,
                                 trx_undo_ptr_t *redo_rseg_undo_ptr,
                                 trx_undo_ptr_t *temp_rseg_undo_ptr,
                                 txn_undo_ptr_t *txn_rseg_undo_ptr);

/** Add the rseg into the purge queue heap */
void trx_add_rsegs_for_purge(commit_mark_t &scn, TxnUndoRsegs *elem);

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
@return     The corresponding commit_mark if it's TXN undo,
            or err if it's not TXN undo */
inline std::pair<commit_mark_t, bool> txn_undo_set_state_at_purge(
    const trx_rseg_t *rseg, const page_size_t &page_size) {
  commit_mark_t cmmt = COMMIT_MARK_NULL;

  if (fsp_is_txn_tablespace_by_id(rseg->space_id)) {
    mtr_t mtr;
    mtr_start(&mtr);

    const page_id_t page_id(rseg->space_id, rseg->last_page_no);
    page_t *undo_page = trx_undo_page_get(page_id, page_size, &mtr);
    trx_ulogf_t *undo_header = undo_page + rseg->last_offset;

    txn_undo_set_state(undo_header, TXN_UNDO_LOG_PURGED, &mtr);

    cmmt = trx_undo_hdr_read_cmmt(undo_header, &mtr);

    mtr_commit(&mtr);

    return std::make_pair(cmmt, false);
  }
  return std::make_pair(cmmt, true);
}

/** Set txn undo log state when commiting.
@param[in,out]  log_hdr undo log header
@param[in,out]  mtr     mini transaction */
extern void txn_undo_set_state_at_finish(trx_t *trx, trx_ulogf_t *log_hdr,
                                         bool is_rollback, mtr_t *mtr);

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
  static ulint retention_time;  // in seconds
  static ulint space_limit;     // in MiB
  static ulint space_reserve;   // in MiB
  // show status
  static char status[128];

  static int check_limit(THD *thd, SYS_VAR *var, void *save,
                         struct st_mysql_value *value);
  static int check_reserve(THD *thd, SYS_VAR *var, void *save,
                           struct st_mysql_value *value);
  static void on_update(THD *, SYS_VAR *, void *var_ptr, const void *save);

  static void on_update_and_start(THD *thd, SYS_VAR *var, void *var_ptr,
                                  const void *save);

  ib_mutex_t m_mutex;

 protected:
  volatile bool m_stat_done;

  volatile ulint m_last_top_utc;  // to output status

  std::atomic<ulint> m_total_used_size;
  std::atomic<ulint> m_total_file_size;

  Undo_retention()
      : m_stat_done(false),
        m_last_top_utc(0),
        m_total_used_size(0),
        m_total_file_size(0) {}

  Undo_retention &operator=(const Undo_retention &) = delete;
  Undo_retention(const Undo_retention &) = delete;

  static Undo_retention inst;  // global instance

  static ulint current_utc() { return ut_time_system_us() / 1000000; }

  static ulint mb_to_pages(ulint size) {
    return (ulint)(1024.0 * 1024.0 / univ_page_size.physical() * size);
  }

  static ulint pages_to_mb(ulint n_pages) {
    return (ulint)(univ_page_size.physical() * n_pages / (1024.0 * 1024.0));
  }

 public:
  static Undo_retention *instance() { return &inst; }

  /* Collect latest undo space sizes periodically */
  void refresh_stat_data();

  /* Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge */
  bool purge_advise();

  /* Create the lizard undo retention mutex. */
  inline void init_mutex() { mutex_create(LATCH_ID_UNDO_RETENTION, &m_mutex); }

  /* Free the lizard undo retention mutex. */
  static inline void destroy() { mutex_free(&(instance()->m_mutex)); }

  void get_stat_data(ulint *used_size, ulint *file_size, ulint *retained_time);
};

/* Init undo_retention */
void undo_retention_init();

/** Design transaction strategy for recovering if has xa specification. */
class XA_specification_strategy {
 public:
  XA_specification_strategy(const trx_t *trx)
      : m_trx(trx), m_xa_spec(trx->xa_spec) {}

  virtual ~XA_specification_strategy() {}

  /**
   * Judge if has gtid when recovery trx.
   *
   * @retval	true
   * @retval	false
   */
  bool has_gtid();

  /**
   * Judge storage way for gtid according to gtid source.
   */
  trx_undo_t::Gtid_storage decide_gtid_storage();

  /**
   * Overwrite gtid storage type of trx_undo_t when recovery.
   */
  void overwrite_gtid_storage(trx_t *trx);

  /** Fill gtid info from xa spec. */
  void get_gtid_info(Gtid_desc &gtid_desc);

  /**
   * Judge if has gcn when commit detached XA
   *
   * @retval  true
   * @retval  false
   */
  bool has_gcn() const;

  /**
   * Overwrite commit gcn in trx when commit detached XA
   */
  void overwrite_gcn(trx_t *trx) const;

 private:
  const trx_t *m_trx;
  XA_specification *m_xa_spec;
};

class Guard_xa_specification {
 public:
  Guard_xa_specification(trx_t *trx, XA_specification *xa_spec, bool prepare);

  virtual ~Guard_xa_specification();

 private:
  trx_t *m_trx;
  XA_specification *m_xa_spec;
};

/**
  Get a TXN rseg by XID.

  @retval     rollback segment
*/
trx_rseg_t *get_txn_rseg_by_xid(const XID *xid);

/**
  Find transactions in the finalized state by XID.

  @param[in]  rseg         The rollseg where the transaction is being looked up.
  @params[in] xid          xid
  @param[out] txn_undo_hdr Corresponding txn undo header

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool txn_rseg_find_trx_info_by_xid(trx_rseg_t *rseg, const XID *xid,
                                   txn_undo_hdr_t *txn_undo_hdr);

/**
  Only write XID on the TXN.

  @params[in]     XID         xid info
  @params[in/out] undo        TXN undo
*/
void txn_undo_write_xid(const XID *xid, trx_undo_t *undo);

/**
  Initializes the part of TXN for an undo log memory object if it's TXN undo
  log. The memory object is inserted in the appropriate list in the rseg.

  @params[in] rseg        rollback segment memory object
  @params[in] undo        undo log memory object
  @params[in] undo_page   undo log page
  @params[in] undo_header undo log header
  @params[in] type        undo type, TRX_UNDO_TXN or others
  @params[in] flag        undo log flag, read from TRX_UNDO_FLAGS
  @params[in] state       undo log state, TRX_UNDO_CACHED, or others
  @params[in] mtr         mini transaction for write
*/
void trx_undo_mem_init_for_txn(trx_rseg_t *rseg, trx_undo_t *undo,
                               page_t *undo_page,
                               const trx_ulogf_t *undo_header, ulint type,
                               uint32_t flag, ulint state, mtr_t *mtr);

}  // namespace lizard

/** Delcare the functions which were defined in other cc files.*/
/*=============================================================================*/
extern trx_undo_t *trx_undo_reuse_cached(trx_t *trx, trx_rseg_t *rseg,
                                         ulint type, trx_id_t trx_id,
                                         const XID *xid,
                                         trx_undo_t::Gtid_storage gtid_storage,
                                         mtr_t *mtr);

[[nodiscard]] extern dberr_t trx_undo_create(
    trx_t *trx, trx_rseg_t *rseg, ulint type, trx_id_t trx_id, const XID *xid,
    trx_undo_t::Gtid_storage gtid_storage, trx_undo_t **undo, mtr_t *mtr);

void trx_resurrect_update_in_prepared_state(trx_t *trx, const trx_undo_t *undo);

void trx_undo_page_init(page_t *undo_page, /*!< in: undo log segment page */
                        ulint type,        /*!< in: undo log segment type */
                        mtr_t *mtr);       /*!< in: mtr */

ulint trx_undo_header_create(page_t *undo_page, /*!< in/out: undo log segment
                                                header page, x-latched; it is
                                                assumed that there is
                                                TRX_UNDO_LOG_HDR_SIZE bytes
                                                free space on it */
                             trx_id_t trx_id,   /*!< in: transaction id */
                             commit_mark_t *prev_image,
                             /*!< out: previous scn/utc
                             if have. Only used in TXN
                             undo header. Pass in as NULL
                             if don't care. */
                             mtr_t *mtr); /*!< in: mtr */

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
@param[in]	gtid_storage    GTID storage type */
void trx_undo_header_add_space_for_xid(page_t *undo_page, trx_ulogf_t *log_hdr,
                                       mtr_t *mtr,
                                       trx_undo_t::Gtid_storage gtid_storage);

/*=============================================================================*/

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/* Assert the txn_desc is initial */
#define assert_txn_desc_initial(trx)                          \
  do {                                                        \
    ut_a((trx)->txn_desc.undo_ptr == lizard::UNDO_PTR_NULL && \
         lizard::commit_mark_state((trx)->txn_desc.cmmt) ==   \
             SCN_STATE_INITIAL);                              \
  } while (0)

/* Assert the txn_desc is allocated */
#define assert_txn_desc_allocated(trx)                        \
  do {                                                        \
    ut_a((trx)->txn_desc.undo_ptr != lizard::UNDO_PTR_NULL && \
         lizard::commit_mark_state((trx)->txn_desc.cmmt) ==   \
             SCN_STATE_INITIAL);                              \
  } while (0)

#define assert_undo_ptr_initial(undo_ptr)         \
  do {                                            \
    ut_a((*(undo_ptr)) == lizard::UNDO_PTR_NULL); \
  } while (0)

#define assert_undo_ptr_allocated(undo_ptr)    \
  do {                                         \
    ut_a((undo_ptr) != lizard::UNDO_PTR_NULL); \
  } while (0)

#define assert_trx_undo_ptr_initial(trx) \
  assert_undo_ptr_initial((&(trx)->txn_desc.undo_ptr))

#define assert_trx_undo_ptr_allocated(trx) \
  assert_undo_ptr_allocated((trx)->txn_desc.undo_ptr)

#define trx_undo_page_validation(page)          \
  do {                                          \
    ut_a(lizard::trx_undo_page_validate(page)); \
  } while (0)

#define trx_undo_hdr_slot_validation(undo_hdr, mtr)          \
  do {                                                       \
    ut_a(lizard::trx_undo_hdr_slot_validate(undo_hdr, mtr)); \
  } while (0)

#define trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)               \
  do {                                                                      \
    txn_undo_hdr_t txn_undo_hdr;                                            \
    lizard::trx_undo_hdr_read_txn(undo_page, undo_hdr, mtr, &txn_undo_hdr); \
    ut_a(txn_undo_hdr.magic_n == TXN_MAGIC_N);                              \
  } while (0)

#define undo_addr_validation(undo_addr, index)          \
  do {                                                  \
    ut_a(lizard::undo_addr_validate(undo_addr, index)); \
  } while (0)

#define undo_commit_mark_validation(undo)          \
  do {                                             \
    ut_a(lizard::undo_commit_mark_validate(undo)); \
  } while (0)

#define assert_trx_in_recovery(trx)                                            \
  do {                                                                         \
    if ((trx)->rsegs.m_txn.rseg != NULL && (trx)->rsegs.m_redo.rseg == NULL) { \
      ut_a((trx)->is_recovered);                                               \
    }                                                                          \
  } while (0)

#define txn_undo_free_list_validation(rseg_hdr, undo_page, mtr)          \
  do {                                                                   \
    ut_a(lizard::txn_undo_free_list_validate(rseg_hdr, undo_page, mtr)); \
  } while (0)
#else

#define trx_undo_page_validation(page)
#define assert_txn_desc_initial(trx)
#define assert_txn_desc_allocated(trx)
#define assert_undo_ptr_initial(undo_ptr)
#define assert_undo_ptr_allocated(undo_ptr)
#define assert_trx_undo_ptr_initial(trx)
#define assert_trx_undo_ptr_allocated(trx)

#define trx_undo_hdr_txn_validation(undo_page, undo_hdr, mtr)
#define undo_addr_validation(undo_addr, index)
#define undo_commit_mark_validation(undo)
#define assert_trx_in_recovery(trx)
#define txn_undo_free_list_validation(rseg_hdr, undo_page, mtr)
#define trx_undo_hdr_slot_validation(undo_hdr, mtr)

#endif

#endif  // lizard0undo_h
