/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/lizard0txn0slot0types.h
  Lizard transactional slot structure.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0slot0types_h
#define lizard0txn0slot0types_h

#include "univ.i"

#include "lizard0gcs0service.h"
#include "lizard0txn0service.h"

/**
  Unlike normal UNDOs (insert undo / update undo), there are 5 kinds of states
  of TXN. Among them, txn_status_t::ACTIVE, txn_status_t::COMMITTED and
  txn_status_t::PURGED are specified by TXN_SLOT_STATE flag (respectively,
  txn_slot_t::ACTIVE, txn_slot_t::COMMITED and txn_slot_t::PURGED) in TXN
  header. And also, that's mean these TXN headers are existing.

  By contrast, txn_status_t::REUSE / txn_status_t::UNDO_INVALID mean that the
  TXN headers are non-existing.

  * txn_status_t::ACTIVE: A txn header is initialized as txn_status_t::ACTIVE
  when the transaction begins.

  * txn_status_t::COMMITTED: The state of txn header is set as
  txn_status_t::COMMITTED at the moment that the transaction commits.

  * txn_status_t::PURGED: At the moment that the purge sys start purging it.
  Notes that: Access to the binding normal UNDOs (insert undo / update undo) is
  not safe from then on.

  * txn_status_t::ERASED: At the moment that the erase sys start erasing it.
  Notes that: Access to the binding normal UNDOs (insert undo / update undo) is
  not safe for two phase purge tables from then on.

  * txn_status_t::REUSE: At the moment that the TXN headers are reused by
  another transactions. These TXN headers are reinited as txn_status_t::ACTIVE,
  but for those UBAs who also pointed at them, are supposed to be
  txn_status_t::REUSE.

  * txn_status_t::UNDO_INVALID: In fact, txn_status_t::REUSE also lost their TXN
  headers, but txn_status_t::UNDO_INVALID is a abnormal state for some special
  cases, for example, page corrupt or TXN file unexpectedly removed.

  So the life cycle of TXN hedaer:

  txn_status_t::ACTIVE (Trx_A) ==> txn_status_t::COMMITTED (Trx_A) ==>
    txn_status_t::PURGED (Trx_A) ==> { (txn_status_t::ERASED) (Trx_A) ==> }
      * txn_status_t::REUSE  (from Trx_A's point of view)
      * txn_status_t::ACTIVE (from Trx_B's point of view)
*/
enum class txn_status_t : char {
  ACTIVE,
  COMMITTED,
  PURGED,
  ERASED,
  REUSE,
  UNDO_INVALID,
};

/** XES storage format in transaction slot */
struct xes_storage_t {
 private:
  /******************************************
   *        TXN_SLOT_EXT_STORAGE (1bytes)
   *****************************************/

  /* Storage Format     1 (Bits: [TAG | AC_PMMT | AC_CMMT.....]) */

  /** Empty TXN_SLOT_EXT_STORAGE */
  static constexpr ulint XES_ALLOCATED_NONE = 0x00;
  /** bit_0: TXN have TXN_SLOT_XES_TAGS. */
  static constexpr ulint XES_ALLOCATED_TAGS = 0x01;
  /** bit_1: Prepare info of async commit transaction. */
  static constexpr ulint XES_ALLOCATED_AC_PREPARE = 0x02;
  /** bit_2: Commit info of async commit transaction. */
  static constexpr ulint XES_ALLOCATED_AC_COMMIT = 0x04;

  /** Initial value of TXN_SLOT_EXT_STORAGE. */
  static constexpr ulint XES_ALLOCATED_V1 = XES_ALLOCATED_TAGS;

 public:
  xes_storage_t(uint8_t storage) { decode(storage); }

  static uint8_t v0() { return XES_ALLOCATED_NONE; }
  static uint8_t v1() { return XES_ALLOCATED_V1; }

  static void allocate_tags(uint8_t &storage) { storage |= XES_ALLOCATED_TAGS; }
  static void allocate_ac_prepare(uint8_t &storage) {
    storage |= XES_ALLOCATED_AC_PREPARE;
  }
  static void allocate_ac_commit(uint8_t &storage) {
    storage |= XES_ALLOCATED_AC_COMMIT;
  }

  bool has_tags() const { return m_has_tags; }
  bool has_ac_prepare() const { return m_has_ac_prepare; }
  bool has_ac_commit() const { return m_has_ac_commit; }

 private:
  void decode(uint8_t storage) {
    m_has_tags = storage & XES_ALLOCATED_TAGS ? true : false;
    m_has_ac_prepare = storage & XES_ALLOCATED_AC_PREPARE ? true : false;
    m_has_ac_commit = storage & XES_ALLOCATED_AC_COMMIT ? true : false;
  }

 private:
  bool m_has_tags;
  bool m_has_ac_prepare;
  bool m_has_ac_commit;
};

/** Tags struture which is encoded into 1 bytes and stored in transaction slot
 * xes extension. */
struct xes_tags_t {
 private:
  /** tags encode format. */

  /******************************************
   *           TXN_SLOT_XES_TAGS(2bytes)
   ******************************************/
  /** bit_0: Finish state of the transaction (true: ROLLBACK,
  false: other state). */
  static constexpr ulint XES_TAGS_ROLLBACK = 0x01;
/**
  bit_1: true if it's a ASSIGNED GCN, false it's a AUTOMATIC GCN. This bit is
  only meaningful when doing async commit, and the state is prepare.

  NOTE:
  1. The commit gcn CSR is in the utc field of cmmt mark.
  2. However, the proposal gcn CSR is in the txn_tags_1.
*/
  static constexpr ulint XES_TAGS_AC_ASSIGNED = 0x02;

 public:
  xes_tags_t(uint16_t tags) { decode(tags); }

  static void mark_rollback(uint16_t &tags) { tags |= XES_TAGS_ROLLBACK; }
  static void mark_assigned(uint16_t &tags) { tags |= XES_TAGS_AC_ASSIGNED; }

  bool is_assigned() const { return m_csr == csr_t::CSR_ASSIGNED; }
  bool is_rollback() const { return m_is_rollback; }
  csr_t csr() const { return m_csr; }

 private:
  void decode(ulint tags) {
    m_is_rollback = tags & XES_TAGS_ROLLBACK ? true : false;
    m_csr = tags & XES_TAGS_AC_ASSIGNED ? csr_t::CSR_ASSIGNED
                                        : csr_t::CSR_AUTOMATIC;
  }

 private:
  bool m_is_rollback;
  csr_t m_csr;
};

/** NULL value of slot ptr  */
constexpr slot_ptr_t SLOT_PTR_NULL = std::numeric_limits<slot_ptr_t>::min();

/** Transaction slot address: */
class slot_addr_t {
 protected:
  /** Special simulate space id for slot address. */
  static constexpr ulint SLOT_SPACE_NUM_FAKE = 0;
  /** TXN can never asssign from TRX_SYS_SPACE. So SLOT_SPACE_ID_FAKE == 0 is
  considered as sepcial slot address. */
  static constexpr ulint SLOT_SPACE_ID_FAKE = 0;

  /**------------------------------------------------------------------------*/
  /** Special simulate page no for slot address. */
  static constexpr ulint SLOT_PAGE_NO_FAKE = 0;

  /**------------------------------------------------------------------------*/
  /** SLOT OFFSET:: Temporary table record */
  static constexpr ulint SLOT_OFFSET_TEMP_TAB_REC = (ulint)0xFFFF;

  /** SLOT OFFSET:: Dynamic metadata table record */
  static constexpr ulint SLOT_OFFSET_DYNAMIC_METADATA = (ulint)0xFFFF - 1;

  /** SLOT OFFSET:: Log_ddl table record */
  static constexpr ulint SLOT_OFFSET_LOG_DDL = (ulint)0xFFFF - 2;

  /** SLOT OFFSET:: Index record */
  static constexpr ulint SLOT_OFFSET_DICT_REC = (ulint)0xFFFF - 3;

  /** SLOT OFFSET:: UBA offset for no_redo insert/update undo. */
  static constexpr ulint SLOT_OFFSET_NO_REDO = (ulint)0xFFFF - 4;

  /** SLOT OFFSET:: Index UBA that upgraded from old version. */
  static constexpr ulint SLOT_OFFSET_INDEX_UPGRADE = (ulint)0xFFFF - 5;

  /** Lowest offset for all special cases. */
  static constexpr ulint SLOT_OFFSET_LIMIT = SLOT_OFFSET_INDEX_UPGRADE;

  /** Please update limit value to minval from 0xFFFF. */
  static_assert(SLOT_OFFSET_LIMIT + 5 == SLOT_OFFSET_TEMP_TAB_REC,
                "Please update limit.");
  /**
    slot_ptr_t
    ----------
    1) slot_ptr_t build as [rseg_id, page_no, offset]

    2) There are two kinds of slot_ptr_t:
    2.a) [rseg_id = 0,  page_no, offset].
         It's special slot_ptr_t, and never points to an actual storage location
    2.b) [rseg_id != 0, page_no, offset]
         It's regular slot_ptr_t, and always points to an actual storage
    location

    3) slot_ptr_t only represents the meaning of a TXN physical address and is
    only used for locating TXN.

    4) slot_ptr_t is typically persisted onto TRX_UNDO_SLOT of undo log header.

    undo_ptr_t
    ----------
    1) undo_ptr_t build as [Extra Flags, [slot_ptr_t]]

    2) In addition to representing the physical address expressed by slot_ptr_t,
       undo_ptr_t can also convey part of the transaction state information
       through extra flags.

    3) undo_ptr_t is typically persisted onto the index records.

    slot_addr_t
    -----------
    1) slot_ptr_t --(decode)--> slot_addr_t,
       decode will change rseg_id to space_id of slot_addr_t

    undo_addr_t
    -----------
    1) undo_ptr_t --(decode)--> slot_addr_t --(decode)--> undo_addr_t
       Extra Flags will be decoded from undo_ptr_t.
  */

  /**
    New record format will include SCN and UBA:

    1) Format of scn in record:

     64 bit     scn number (8 bytes);

    2) Format of undo log address in record:

     1  bit     active/commit state (0:active 1:commit)
     1  bit     commit source
     1  bit     share commit number
     6  bit     reserved unused
     7  bit     undo space number (1-127)
     32 bit     page no (4 bytes)
     16 bit     Offset of undo log header (2 bytes)
  */

 protected:
  /** Offset */
  static constexpr uint64_t UBA_POS_OFFSET = 0;
  static constexpr uint64_t UBA_WIDTH_OFFSET = 16;

  /** PageNO */
  static constexpr uint64_t UBA_POS_PAGE_NO = UBA_POS_OFFSET + UBA_WIDTH_OFFSET;
  static constexpr uint64_t UBA_WIDTH_PAGE_NO = 32;

  /** SPACE_ID (rseg id) */
  static constexpr uint64_t UBA_POS_SPACE_ID = (UBA_POS_PAGE_NO + UBA_WIDTH_PAGE_NO);
  static constexpr uint64_t UBA_WIDTH_SPACE_ID = 7;

  /** UNUSED */
  static constexpr uint64_t UBA_POS_UNUSED =
      (UBA_POS_SPACE_ID + UBA_WIDTH_SPACE_ID);
  static constexpr uint64_t UBA_WIDTH_UNUSED = 6;
  static constexpr uint64_t UBA_MASK_UNUSED =
      ((~(~0ULL << UBA_WIDTH_UNUSED)) << UBA_POS_UNUSED);

/** Address, include [offset, page_no, space_id] */
  static constexpr uint64_t UBA_POS_ADDR = 0;
  static constexpr uint64_t UBA_WIDTH_ADDR =
      (UBA_WIDTH_OFFSET + UBA_WIDTH_PAGE_NO + UBA_WIDTH_SPACE_ID);
  static constexpr uint64_t UBA_MASK_ADDR =
      ((~(~0ULL << UBA_WIDTH_ADDR)) << UBA_POS_ADDR);

  static_assert(UBA_POS_PAGE_NO == 16, "UBA page no from 16th bits");

  static_assert(UBA_POS_SPACE_ID == 48, "UBA space id from 48th bits");

 public:
  /* undo tablespace id */
  space_id_t space_id;
  /* undo log header page */
  page_no_t page_no;
  /* offset of undo log header */
  ulint offset;

 public:
  slot_addr_t(space_id_t space_id_arg, page_no_t page_no_arg, ulint offset_arg)
      : space_id(space_id_arg), page_no(page_no_arg), offset(offset_arg) {}

  explicit slot_addr_t(const slot_ptr_t slot_ptr)
      : space_id(0), page_no(0), offset(0) {
    decode(slot_ptr);
  }

  bool is_null() const;
  /** Normal txn undo allocated from txn undo space. */
  bool is_redo() const;
  /** Special fake address if didn't allocate txn undo */
  bool is_no_redo() const;

  bool is_validate() const;
  /**
    Check the slot address if is actually points at the real disk storage.
  */
  bool is_disk_mapped() const;

  bool equal_with(space_id_t space_id_arg, page_no_t page_no_arg,
                  ulint offset_arg) {
    return space_id == space_id_arg && page_no == page_no_arg &&
           offset == offset_arg;
  }

  /**
    Encode Slot_addr into slot_ptr
    @return slot_ptr_t
  */
  slot_ptr_t encode() const;

  /*
    Decode the slot_ptr into slot address
    @param[in]      slot ptr
  */
  void decode(slot_ptr_t slot_ptr);

  const std::string print() const {
    std::stringstream ss;
    ss << "Txn Slot Address:[space_id=" << space_id << ",page_no=" << page_no
       << ",offset=" << offset << "]";
    return ss.str();
  }

  /** Predefined slot address. */
  static const slot_addr_t temporary() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_TEMP_TAB_REC};
  }
  static const slot_addr_t dynamic_metadata() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE,
            SLOT_OFFSET_DYNAMIC_METADATA};
  }
  static const slot_addr_t log_ddl() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_LOG_DDL};
  }
  static const slot_addr_t dd_dict() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_DICT_REC};
  }
  static const slot_addr_t index_upgrade() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_INDEX_UPGRADE};
  }

  static const slot_addr_t no_redo() {
    return {SLOT_SPACE_ID_FAKE, SLOT_PAGE_NO_FAKE, SLOT_OFFSET_NO_REDO};
  }

  static const slot_addr_t null() { return {0, 0, 0}; }
};

/** Compare function */
inline bool operator==(const slot_addr_t &lhs, const slot_addr_t &rhs) {
  return (lhs.offset == rhs.offset && lhs.page_no == rhs.page_no &&
          lhs.space_id == rhs.space_id);
}


#endif
