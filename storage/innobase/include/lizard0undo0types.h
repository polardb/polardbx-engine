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

/** @file include/lizard0undo0types.h
  Lizard transaction undo and purge types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo0types_h
#define lizard0undo0types_h

#include "trx0types.h"

#include "lizard0scn0types.h"

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
  commit_scn_t scn;
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
};

/**
  Lizard transaction attributes in undo log record
   1) scn
   2) undo_ptr
*/
struct txn_info_t {
  /** scn number */
  scn_id_t scn;
  /** undo log header address */
  undo_ptr_t undo_ptr;
};

/**
  Lizard transaction attributes in index (used by Vision)
   1) scn
   2) undo_ptr
*/
struct txn_index_t {
  /** scn number */
  undo_ptr_t uba;
  /** undo log header address */
  std::atomic<scn_id_t> scn;
};


/** The struct of transaction undo for UBA */
struct txn_undo_ptr_t {
  /** Rollback segment in txn space */
  trx_rseg_t *rseg;
  /* transaction undo log segment */
  trx_undo_t *txn_undo;
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
  explicit TxnUndoRsegs(scn_t scn) : m_scn(scn) {
    for (auto &rseg : m_rsegs) {
      rseg = nullptr;
    }
  }

  /** Default constructor */
  TxnUndoRsegs() : TxnUndoRsegs(0) {}

  void set_scn(scn_t scn) { m_scn = scn; }

  scn_t get_scn() const { return m_scn; }

  /** Add rollback segment.
  @param rseg rollback segment to add. */
  void insert(trx_rseg_t *rseg) {
    for (size_t i = 0; i < m_rsegs_n; ++i) {
      if (m_rsegs[i] == rseg) {
        return;
      }
    }
    // ut_a(m_rsegs_n < 2);
    /* Lizard: one more txn rseg. */
    ut_a(m_rsegs_n < 2 + 1);
    m_rsegs[m_rsegs_n++] = rseg;
  }

  /** Number of registered rsegs.
  @return size of rseg list. */
  size_t size() const { return (m_rsegs_n); }

  /**
  @return an iterator to the first element */
  typename Rsegs_array<3>::iterator begin() { return m_rsegs.begin(); }

  /**
  @return an iterator to the end */
  typename Rsegs_array<3>::iterator end() {
    return m_rsegs.begin() + m_rsegs_n;
  }

  /** Append rollback segments from referred instance to current
  instance. */
  void insert(const TxnUndoRsegs &append_from) {
    ut_ad(get_scn() == append_from.get_scn());
    for (size_t i = 0; i < append_from.m_rsegs_n; ++i) {
      insert(append_from.m_rsegs[i]);
    }
  }

  /** Compare two TxnUndoRsegs based on scn.
  @param lhs first element to compare
  @param rhs second element to compare
  @return true if elem1 > elem2 else false.*/
  bool operator()(const TxnUndoRsegs &lhs, const TxnUndoRsegs &rhs) {
    return (lhs.m_scn > rhs.m_scn);
  }

  /** Compiler defined copy-constructor/assignment operator
  should be fine given that there is no reference to a memory
  object outside scope of class object.*/

 private:
  scn_t m_scn;

  size_t m_rsegs_n{};

  /** Rollback segments of a transaction, scheduled for purge. */
  // Rsegs_array<2> m_rsegs;
  /* Lizard: one more txn rseg. */
  Rsegs_array<3> m_rsegs;
};

/**
  Use priority_queue as the minimum heap structure
  which is order by scn number */
typedef std::priority_queue<
    TxnUndoRsegs, std::vector<TxnUndoRsegs, ut::allocator<TxnUndoRsegs>>,
    TxnUndoRsegs>
    purge_heap_t;

} /* namespace lizard */

#endif  // lizard0undo0types_h
