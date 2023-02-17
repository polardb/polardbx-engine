/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0undo0types.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo0types_h
#define lizard0undo0types_h

#include "trx0types.h"

#include "lizard0scn0types.h"
#include "lizard0txn.h"

struct trx_rseg_t;
struct trx_undo_t;

/**
  Lizard transaction system undo format:

  At the end of undo log header history node:

  8 bytes     SCN number
  8 bytes     UTC time

  Those two option will be included into all INSERT/UPDATE/TXN undo
  log header.


  Start from undo log old header, txn_undo will be different with trx_undo:

  1) txn undo : flag + reserved space

  2) trx undo : XA + GTID

  As the optional info, those will be controlled by TRX_UNDO_FLAGS.

     0x01 TRX_UNDO_FLAG_XID
     0x02 TRX_UNDO_FLAG_GTID
     0x80 TRX_UNDO_FLAG_TXN
*/


/** Those will exist all kinds of undo log header*/
/*-------------------------------------------------------------*/
/** Size of scn within undo log header */
#define TRX_UNDO_SCN_LEN 8

/** Size of UTC within undo log header */
#define TRX_UNDO_UTC_LEN 8

/** Size of UBA within undo log header */
#define TRX_UNDO_UBA_LEN 8
/*-------------------------------------------------------------*/

/** Flag determine that if it is active in UBA */
/*-------------------------------------------------------------*/
/**  */
#define UNDO_ADDR_T_ACTIVE   0
#define UNDO_ADDR_T_COMMITED 1
/*-------------------------------------------------------------*/

/** Undo block address (UBA) */
struct undo_addr_t {
  /* undo tablespace id */
  space_id_t space_id;
  /* undo log header page */
  page_no_t page_no;
  /* offset of undo log header */
  ulint offset;
  /* Commit scn number */
  scn_t scn;
  /* Active or Commit state */
  bool state;

  /** Commit gcn number */
  gcn_t gcn;
};

typedef struct undo_addr_t undo_addr_t;

/**
  New record format will include SCN and UBA:

  1) Format of scn in record:

   64 bit     scn number (8 bytes);

  2) Format of undo log address in record:

   1  bit     active/commit state (0:active 1:commit)
   8  bit     reserved unused
   7  bit     undo space number (1-127)
   32 bit     page no (4 bytes)
   16 bit     Offset of undo log header (2 bytes)
*/

#define UBA_POS_OFFSET 0
#define UBA_WIDTH_OFSET 16

#define UBA_POS_PAGE_NO (UBA_POS_OFFSET + UBA_WIDTH_OFSET)
#define UBA_WIDTH_PAGE_NO 32

#define UBA_POS_SPACE_ID (UBA_POS_PAGE_NO + UBA_WIDTH_PAGE_NO)
#define UBA_WIDTH_SPACE_ID 7

#define UBA_POS_UNUSED (UBA_POS_SPACE_ID + UBA_WIDTH_SPACE_ID)
#define UBA_WIDTH_UNUSED 8

#define UBA_POS_STATE (UBA_POS_UNUSED + UBA_WIDTH_UNUSED)
#define UBA_WIDTH_STATE 1

static_assert((UBA_POS_STATE + UBA_WIDTH_STATE) == 64,
              "UBA length must be 8 bytes");

static_assert(UBA_POS_PAGE_NO == 16, "UBA page no from 16th bits");

static_assert(UBA_POS_SPACE_ID == 48, "UBA space id from 48th bits");

/** Undo log header address in record */
typedef ib_id_t undo_ptr_t;

/** Scn in record */
typedef scn_t scn_id_t;

/**
  The transaction description:

  It will be inited when allocate the first txn undo log
  header, and never change until transaction commit or rollback.
*/
struct txn_desc_t {
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** scn number */
  commit_scn_t cmmt;
};

/**
  Lizard transaction attributes in record (used by Vision)
   1) trx_id
   2) scn
   3) undo_ptr
*/
struct txn_rec_t {
  /* trx id */
  trx_id_t trx_id;
  /** scn number */
  scn_id_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;

  /**
    Although gcn isn't saved on record, but Global query still use gcn as
    visible judgement, and it can be retrieved by txn undo header, so defined
    gcn as txn record attribute.
  */
  /** Revision: Persist gcn into record */
  gcn_t gcn;
};

/**
  Lizard transaction attributes in undo log record
   1) scn
   2) undo_ptr
   3) gcn
*/
struct txn_info_t {
  /** scn number */
  scn_id_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** gcn number */
  gcn_t gcn;
};

/**
  Lizard committed transaction txn information
  special for cleanout.
*/
typedef struct txn_rec_t txn_commit_t;
/**
  Lizard transaction attributes in index (used by Vision)
   1) scn
   2) undo_ptr
   3) gcn
*/
struct txn_index_t {
  /** undo log header address */
  undo_ptr_t uba;
  /** scn number */
  std::atomic<scn_id_t> scn;
  /** gcn number */
  std::atomic<gcn_t> gcn;
};

/** The struct of transaction undo for UBA */
struct txn_undo_ptr_t {
  /** Rollback segment in txn space */
  trx_rseg_t *rseg;
  /* transaction undo log segment */
  trx_undo_t *txn_undo;
};

/**
  Unlike normal UNDOs (insert undo / update undo), there are 5 kinds of states
  of TXN. Among them, TXN_STATE_ACTIVE, TXN_STATE_COMMITTED and TXN_STATE_PURGED
  are specified by TXN_UNDO_LOG_STATE flag (respectively, TXN_UNDO_LOG_ACTIVE,
  TXN_UNDO_LOG_COMMITED and TXN_UNDO_LOG_PURGED) in TXN header. And also, that's
  mean these TXN headers are existing.

  By contrast, TXN_STATE_REUSE / TXN_STATE_UNDO_CORRUPTED mean that the TXN headers
  are non-existing.

  * TXN_STATE_ACTIVE: A txn header is initialized as TXN_STATE_ACTIVE when the
  transaction begins.

  * TXN_STATE_COMMITTED: The state of txn header is set as TXN_STATE_COMMITTED
  at the moment that the transaction commits.

  * TXN_STATE_PURGED: At the moment that the purge sys start purging it. Notes
  that: Access to the binding normal UNDOs (insert undo / update undo) is not
  safe from then on.

  * TXN_STATE_REUSE: At the moment that the TXN headers are reused by another
  transactions. These TXN headers are reinited as TXN_STATE_ACTIVE, but for
  those UBAs who also pointed at them, are supposed to be TXN_STATE_REUSE.

  * TXN_STATE_UNDO_CORRUPTED: In fact, TXN_STATE_REUSE also lost their TXN headers,
  but TXN_STATE_UNDO_CORRUPTED is a abnormal state for some special cases, for
  example, page corrupt or TXN file unexpectedly removed.

  So the life cycle of TXN hedaer:

  TXN_STATE_ACTIVE (Trx_A) ==> TXN_STATE_COMMITTED (Trx_A) ==>
    TXN_STATE_PURGED (Trx_A) ==>
      * TXN_STATE_REUSE  (from Trx_A's point of view)
      * TXN_STATE_ACTIVE (from Trx_B's point of view)
*/
enum txn_state_t {
  TXN_STATE_ACTIVE,
  TXN_STATE_COMMITTED,
  TXN_STATE_PURGED,
  TXN_STATE_REUSE,
  TXN_STATE_UNDO_CORRUPTED
};

struct txn_undo_hdr_t {
  /** commit image in txn undo header */
  commit_scn_t image;
  /** undo log header address */
  undo_ptr_t undo_ptr;
  /** current trx who own the txn header */
  trx_id_t trx_id;
  /** A magic number, check if the page is corrupt */
  ulint magic_n;
  /* Previous scn/utc of the trx who used the same TXN */
  commit_scn_t prev_image;
  /** txn undo header state: TXN_UNDO_LOG_ACTIVE, TXN_UNDO_LOG_COMMITED,
  or TXN_UNDO_LOG_PURGED */
  ulint state;
  /** A flag determining how to explain the txn extension */
  ulint ext_flag;
};

struct txn_lookup_t {
  /** The raw data in txn header */
  txn_undo_hdr_t txn_undo_hdr;
  /**
    If the txn is still existing:
      * real_state: [TXN_STATE_ACTIVE, TXN_STATE_COMMITTED, TXN_STATE_PURGED]
      * real_image == txn_undo_hdr.image

    If the txn is non-existing:
      * real_state: [TXN_STATE_REUSE]
      * real_image == txn_undo_hdr.prev_image

    If the txn is unexpectedly lost:
      * real_state: [TXN_STATE_UNDO_CORRUPTED]
      * real_image == {SCN_UNDO_CORRUPTED, UTC_UNDO_CORRUPTED}
  */
  commit_scn_t real_image;
  txn_state_t real_state;
};

namespace lizard {

inline bool lizard_undo_ptr_is_active(undo_ptr_t undo_ptr) {
  return !(bool)(undo_ptr >> UBA_POS_STATE);
}

inline void lizard_undo_ptr_set_commit(undo_ptr_t *undo_ptr) {
  *undo_ptr |= (undo_ptr_t)1 << UBA_POS_STATE;
}

/**
  The element of minimum heap for the purge.
*/
class TxnUndoRsegs {
 public:
  explicit TxnUndoRsegs() {}

  explicit TxnUndoRsegs(scn_t scn) : m_scn(scn) {}

  scn_t get_scn() const { return m_scn; }

  void set_scn(scn_t scn) { m_scn = scn; }

  void push_back(trx_rseg_t *rseg) { m_rsegs.push_back(rseg); }

  void erase(Rseg_Iterator &it) { m_rsegs.erase(it); }

  ulint size() const { return m_rsegs.size(); }

  Rseg_Iterator begin() { return m_rsegs.begin(); }

  Rseg_Iterator end() { return m_rsegs.end(); }

  Rseg_Iterator arrange_txn_first() {
    ut_ad(m_rsegs.size() > 0);

    Rseg_Iterator iter = m_rsegs.begin();
    while (iter != m_rsegs.end()) {
      bool is_txn_rseg = fsp_is_txn_tablespace_by_id((*iter)->space_id);
      if (is_txn_rseg) {
        if (iter != m_rsegs.begin()) {
          /* Move txn rseg to position 0 */
          std::swap(*iter, m_rsegs.front());
        }
        break;
      }
      ++iter;
    }

    /* If no txn rseg, then only one temp rseg */
    ut_ad(iter != m_rsegs.end() || m_rsegs.size() == 1);

    return m_rsegs.begin();
  }

  void append(const TxnUndoRsegs &append_from) {
    ut_ad(get_scn() == append_from.get_scn());

    m_rsegs.insert(m_rsegs.end(), append_from.m_rsegs.begin(),
                   append_from.m_rsegs.end());
  }

  bool operator()(const TxnUndoRsegs &lhs, const TxnUndoRsegs &rhs) {
    return (lhs.m_scn > rhs.m_scn);
  }

 private:
  scn_t m_scn;
  Rsegs_Vector m_rsegs;
};

/**
  Use priority_queue as the minimum heap structure
  which is order by scn number */
typedef std::priority_queue<
    TxnUndoRsegs, std::vector<TxnUndoRsegs, ut_allocator<TxnUndoRsegs>>,
    TxnUndoRsegs>
    purge_heap_t;

} /* namespace lizard */

#endif  // lizard0undo0types_h
