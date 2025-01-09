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

#include "sql/handler.h"

#include "trx0types.h"

#include "lizard0scn0types.h"
#include "lizard0txn0service.h"

#include "sql/lizard/lizard_service.h"

struct trx_rseg_t;
struct trx_undo_t;
struct dict_index_t;

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
#define UNDO_ADDR_T_ACTIVE 0
#define UNDO_ADDR_T_COMMITED 1
/*-------------------------------------------------------------*/

/**
  slot_ptr_t
  ----------
  1) slot_ptr_t build as [rseg_id, page_no, offset]

  2) There are two kinds of slot_ptr_t:
  2.a) [rseg_id = 0,  page_no, offset].
       It's special slot_ptr_t, and never points to an actual storage location
  2.b) [rseg_id != 0, page_no, offset]
       It's regular slot_ptr_t, and always points to an actual storage location

  3) slot_ptr_t only represents the meaning of a TXN physical address and is only
     used for locating TXN.

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

constexpr uint64_t UBA_POS_OFFSET = 0;
constexpr uint64_t UBA_WIDTH_OFFSET = 16;

constexpr uint64_t UBA_POS_PAGE_NO = UBA_POS_OFFSET + UBA_WIDTH_OFFSET;
constexpr uint64_t UBA_WIDTH_PAGE_NO = 32;

constexpr uint64_t UBA_POS_SPACE_ID = (UBA_POS_PAGE_NO + UBA_WIDTH_PAGE_NO);
constexpr uint64_t UBA_WIDTH_SPACE_ID = 7;

constexpr uint64_t UBA_POS_UNUSED  = (UBA_POS_SPACE_ID + UBA_WIDTH_SPACE_ID);
constexpr uint64_t UBA_WIDTH_UNUSED  = 6;
constexpr uint64_t UBA_MASK_UNUSED = ((~(~0ULL << UBA_WIDTH_UNUSED)) << UBA_POS_UNUSED);

constexpr uint64_t UBA_POS_IS_SLAVE = (UBA_POS_UNUSED + UBA_WIDTH_UNUSED);
constexpr uint64_t UBA_WIDTH_IS_SLAVE = 1;
constexpr uint64_t UBA_MASK_IS_SLAVE = ((~(~0ULL << UBA_WIDTH_IS_SLAVE)) << UBA_POS_IS_SLAVE);

constexpr uint64_t UBA_POS_CSR = (UBA_POS_IS_SLAVE + UBA_WIDTH_IS_SLAVE);
constexpr uint64_t UBA_WIDTH_CSR  = 1;
constexpr uint64_t UBA_MASK_CSR = ((~(~0ULL << UBA_WIDTH_CSR)) << UBA_POS_CSR);

constexpr uint64_t UBA_POS_STATE = (UBA_POS_CSR + UBA_WIDTH_CSR);
constexpr uint64_t UBA_WIDTH_STATE = 1;
constexpr uint64_t UBA_MASK_STATE = ((~(~0ULL << UBA_WIDTH_STATE)) << UBA_POS_STATE);

/** Address, include [offset, page_no, space_id] */
constexpr uint64_t UBA_POS_ADDR = 0;
constexpr uint64_t UBA_WIDTH_ADDR =
    (UBA_WIDTH_OFFSET + UBA_WIDTH_PAGE_NO + UBA_WIDTH_SPACE_ID);
constexpr uint64_t UBA_MASK_ADDR =
    ((~(~0ULL << UBA_WIDTH_ADDR)) << UBA_POS_ADDR);

static_assert((UBA_POS_STATE + UBA_WIDTH_STATE) == 64,
              "UBA length must be 8 bytes");

static_assert(UBA_POS_PAGE_NO == 16, "UBA page no from 16th bits");

static_assert(UBA_POS_SPACE_ID == 48, "UBA space id from 48th bits");

/** Undo log header address in record */
typedef uint64_t slot_ptr_t;
typedef uint64_t undo_ptr_t;

/** NULL value of slot ptr  */
constexpr undo_ptr_t UNDO_PTR_NULL = std::numeric_limits<undo_ptr_t>::min();

inline bool undo_ptr_is_active(const undo_ptr_t &undo_ptr) {
  return !static_cast<bool>((undo_ptr & UBA_MASK_STATE) >> UBA_POS_STATE);
}

inline csr_t undo_ptr_get_csr(const undo_ptr_t &undo_ptr) {
  return static_cast<csr_t>((undo_ptr & UBA_MASK_CSR) >> UBA_POS_CSR);
}

inline bool undo_ptr_is_slave(const undo_ptr_t &undo_ptr) {
  return static_cast<bool>((undo_ptr & UBA_MASK_IS_SLAVE) >> UBA_POS_IS_SLAVE);
}

inline void undo_ptr_clear_slave(undo_ptr_t *undo_ptr) {
  *undo_ptr &= (~(((undo_ptr_t)(1)) << UBA_POS_IS_SLAVE));
}

inline void undo_ptr_clear_csr(undo_ptr_t *undo_ptr) {
  *undo_ptr &= (~(((undo_ptr_t)(1)) << UBA_POS_CSR));
}

inline void undo_ptr_set_commit(undo_ptr_t *undo_ptr, unsigned int csr,
                                bool is_slave) {
  *undo_ptr |= ((undo_ptr_t)1 << UBA_POS_STATE);

  undo_ptr_clear_csr(undo_ptr);
  undo_ptr_t value = static_cast<undo_ptr_t>(csr);
  *undo_ptr |= (value << UBA_POS_CSR);

  undo_ptr_clear_slave(undo_ptr);
  value = static_cast<undo_ptr_t>(is_slave);
  *undo_ptr |= (value << UBA_POS_IS_SLAVE);
}

/** Retrieve slot address from undo address */
inline slot_ptr_t undo_ptr_get_slot(const undo_ptr_t &undo_ptr) {
  return ((undo_ptr & UBA_MASK_ADDR) >> UBA_POS_ADDR);
}

inline bool undo_ptr_is_slot(const undo_ptr_t &undo_ptr) {
  return !(undo_ptr >> UBA_WIDTH_ADDR);
}

inline void slot_ptr_decode(slot_ptr_t slot_ptr, ulint *offset,
                            page_no_t *page_no, ulint *rseg_id) {
  *offset = (ulint)slot_ptr & 0xFFFF;
  slot_ptr >>= UBA_WIDTH_OFFSET;
  *page_no = (ulint)slot_ptr & 0xFFFFFFFF;
  slot_ptr >>= UBA_WIDTH_PAGE_NO;
  *rseg_id = (ulint)slot_ptr & 0x7F;
  slot_ptr >>= UBA_WIDTH_SPACE_ID;
}

/**
 * Transaction slot address:
 */
class slot_addr_t {
 public:
  /* undo tablespace id */
  space_id_t space_id;
  /* undo log header page */
  page_no_t page_no;
  /* offset of undo log header */
  ulint offset;

 public:
  slot_addr_t() : space_id(0), page_no(0), offset(0) {}

  slot_addr_t(space_id_t space_id_arg, page_no_t page_no_arg, ulint offset_arg)
      : space_id(space_id_arg), page_no(page_no_arg), offset(offset_arg) {}

  explicit slot_addr_t(slot_ptr_t slot_ptr) { decode(slot_ptr); }

  void reset() {
    space_id = 0;
    page_no = 0;
    offset = 0;
  }

  bool is_null() const;
  /** Normal txn undo allocated from txn undo space. */
  bool is_redo() const;
  /** Special fake address if didn't allocate txn undo */
  bool is_no_redo() const;

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
};

/** Compare function */
inline bool operator==(const slot_addr_t &lhs, const slot_addr_t &rhs) {
  return (lhs.offset == rhs.offset && lhs.page_no == rhs.page_no &&
          lhs.space_id == rhs.space_id);
}

/** Special simulate space id for slot address. */
constexpr ulint SLOT_SPACE_NUM_FAKE = 0;

/** TXN can never asssign from TRX_SYS_SPACE. So SLOT_SPACE_ID_FAKE == 0 is
considered as sepcial slot address. */
constexpr ulint SLOT_SPACE_ID_FAKE = 0;

/** Special simulate page no for slot address. */
constexpr ulint SLOT_PAGE_NO_FAKE = 0;

/**------------------------------------------------------------------------*/
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

/** Undo block address (UBA) */
class undo_addr_t : public slot_addr_t {
 public:
  /* Active or Commit state */
  bool state;
  /** Commit number source for gcn */
  csr_t csr;
  /** Whether xa branch is slave */
  bool is_slave;

 public:
  undo_addr_t() { reset(); }

  explicit undo_addr_t(const slot_addr_t &slot_addr)
      : slot_addr_t(slot_addr), state(0), csr(CSR_AUTOMATIC), is_slave(false) {}

  undo_addr_t(const slot_addr_t &slot_addr, bool state_arg, csr_t csr_arg,
              bool is_slave_arg)
      : slot_addr_t(slot_addr),
        state(state_arg),
        csr(csr_arg),
        is_slave(is_slave_arg) {}

  explicit undo_addr_t(undo_ptr_t undo_ptr) { decode(undo_ptr); }

  bool is_active() const { return state == 0; }

  bool is_null() const {
    return slot_addr_t::is_null() && state == 0 && csr == CSR_AUTOMATIC &&
           is_slave == false;
  }

  void reset() {
    slot_addr_t::reset();
    state = 0;
    csr = CSR_AUTOMATIC;
    is_slave = false;
  }

  /**
    Decode the undo_ptr into UBA
    @param[in]      undo ptr
  */
  void decode(undo_ptr_t undo_ptr);

  /**
    Encode UBA into undo_ptr that need to copy into record
    @return   undo_ptr
  */
  undo_ptr_t encode() const;

  const std::string print() const {
    std::stringstream ss;
    ss << "Undo Block Address:[space_id=" << space_id << ",page_no=" << page_no
       << ",offset=" << offset << ",state=" << state << ",csr=" << csr
       << ",slave=" << is_slave << "]";
    return ss.str();
  }
};

/**
  XA branch info structure:
  {n_globals, n_locals}
*/

struct xes_tags_t {
  bool is_rollback;
  csr_t csr;
};

extern xes_tags_t undo_decode_xes_tags(ulint tags);

/** The struct of transaction undo for UBA */
struct txn_undo_ptr_t {
  // XID will be actively and explicitly initialized
  txn_undo_ptr_t() : rseg(nullptr), txn_undo(nullptr), xid_for_hash() {}
  /** Rollback segment in txn space */
  trx_rseg_t *rseg;
  /* transaction undo log segment */
  trx_undo_t *txn_undo;
  /** XID that is used to map rseg, and also will be persisted in TXN undo */
  XID xid_for_hash;
};

struct txn_slot_t {
 public:
  txn_slot_t()
      : image(),
        slot_ptr(0),
        trx_id(0),
        magic_n(0),
        prev_image(),
        state(0),
        xes_storage(0),
        tags(0),
        is_2pp(false),
        pmmt(),
        branch(),
        maddr() {}

  txn_slot_t(commit_mark_t image_arg, slot_ptr_t slot_ptr_arg,
             trx_id_t trx_id_arg, ulint magic_n_arg,
             commit_mark_t prev_image_arg, ulint state_arg,
             ulint xes_storage_arg, ulint tags_arg, bool is_2pp_arg,
             proposal_mark_t pmmt_arg, xa_branch_t branch_arg,
             xa_addr_t addr_arg)
      : image(image_arg),
        slot_ptr(slot_ptr_arg),
        trx_id(trx_id_arg),
        magic_n(magic_n_arg),
        prev_image(prev_image_arg),
        state(state_arg),
        xes_storage(xes_storage_arg),
        tags(tags_arg),
        is_2pp(is_2pp_arg),
        pmmt(pmmt_arg),
        branch(branch_arg),
        maddr(addr_arg) {}

  /** commit image in txn undo header */
  commit_mark_t image;
  /** slot address */
  slot_ptr_t slot_ptr;
  /** current trx who own the txn header */
  trx_id_t trx_id;
  /** A magic number, check if the page is corrupt */
  ulint magic_n;
  /* Previous scn/utc of the trx who used the same TXN */
  commit_mark_t prev_image;
  /** txn undo header state: TXN_UNDO_LOG_ACTIVE, TXN_UNDO_LOG_COMMITED,
  or TXN_UNDO_LOG_PURGED */
  ulint state;
  /** A flag determining how to explain the txn extension */
  ulint xes_storage;
  /** flags of the TXN. For example: 0x01 means rollback. */
  ulint tags;
  /** true if the TXN is two phase purge. */
  bool is_2pp;
  /** AC XA PMMT info */
  proposal_mark_t pmmt;
  /** XA branch count info */
  xa_branch_t branch;
  /** XA master branch info */
  xa_addr_t maddr;
  /** Return true if the transaction was eventually rolled back. */
  bool is_rollback() const;
  /** Return true if the txn has new_flags. */
  bool tags_allocated() const;
  bool ac_prepare_allocated() const;
  bool ac_commit_allocated() const;
};

/**
  Check the slot address if is actually points at the real disk storage.
*/
extern bool slot_addr_disk_mapped(const slot_addr_t &slot_addr);

#if defined UNIV_DEBUG || defined LIZARD_DEBUG
/** Check the UBA validation */
bool undo_addr_validate(const undo_addr_t *undo_addr,
                        const dict_index_t *index);

bool slot_addr_validate(const slot_addr_t &slot_addr);

#define undo_addr_validation(undo_addr, index)  \
  do {                                          \
    ut_a(undo_addr_validate(undo_addr, index)); \
  } while (0)

#define assert_undo_ptr_initial(undo_ptr) \
  do {                                    \
    ut_a((*(undo_ptr)) == UNDO_PTR_NULL); \
  } while (0)

#define assert_undo_ptr_allocated(undo_ptr) \
  do {                                      \
    ut_a((undo_ptr) != UNDO_PTR_NULL);      \
  } while (0)

#else

#define undo_addr_validation(undo_addr, index)
#define assert_undo_ptr_initial(undo_ptr)
#define assert_undo_ptr_allocated(undo_ptr)

#endif  //

#endif  // lizard0undo0types_h
