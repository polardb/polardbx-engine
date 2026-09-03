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

/** @file include/lizard0txn0uba0types.h
  Lizard transactional slot uba types.

 Created 2025-07-03 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0txn0slot0uba0types_h
#define lizard0txn0slot0uba0types_h

#include "lizard0txn0slot0types.h"

struct dict_index_t;

/** Undo log header address in record */
typedef uint64_t undo_ptr_t;

/** NULL value of slot ptr  */
constexpr undo_ptr_t UNDO_PTR_NULL = std::numeric_limits<undo_ptr_t>::min();

/** Undo block address (UBA) */
class undo_addr_t : public slot_addr_t {
 private:
  /*
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
  /** Slave */
  static constexpr uint64_t UBA_POS_IS_SLAVE =
      (UBA_POS_UNUSED + UBA_WIDTH_UNUSED);
  static constexpr uint64_t UBA_WIDTH_IS_SLAVE = 1;
  static constexpr uint64_t UBA_MASK_IS_SLAVE =
      ((~(~0ULL << UBA_WIDTH_IS_SLAVE)) << UBA_POS_IS_SLAVE);

  /** CSR */
  static constexpr uint64_t UBA_POS_CSR = (UBA_POS_IS_SLAVE + UBA_WIDTH_IS_SLAVE);
  static constexpr uint64_t UBA_WIDTH_CSR = 1;
  static constexpr uint64_t UBA_MASK_CSR =
      ((~(~0ULL << UBA_WIDTH_CSR)) << UBA_POS_CSR);

  /** State */
  static constexpr uint64_t UBA_POS_STATE = (UBA_POS_CSR + UBA_WIDTH_CSR);
  static constexpr uint64_t UBA_WIDTH_STATE = 1;
  static constexpr uint64_t UBA_MASK_STATE =
      ((~(~0ULL << UBA_WIDTH_STATE)) << UBA_POS_STATE);

  static_assert((UBA_POS_STATE + UBA_WIDTH_STATE) == 64,
                "UBA length must be 8 bytes");

 public:
  /* Active or Commit state */
  bool state;
  /** Commit number source for gcn */
  csr_t csr;
  /** Whether xa branch is slave */
  bool is_slave;

 public:
  explicit undo_addr_t(const slot_addr_t &slot_addr)
      : slot_addr_t(slot_addr), state(0), csr(CSR_AUTOMATIC), is_slave(false) {}

  undo_addr_t(const slot_addr_t &slot_addr, bool state_arg, csr_t csr_arg,
              bool slave_arg)
      : slot_addr_t(slot_addr),
        state(state_arg),
        csr(csr_arg),
        is_slave(slave_arg) {}

  explicit undo_addr_t(const undo_ptr_t undo_ptr) : slot_addr_t(undo_ptr) {
    is_slave =
        static_cast<bool>((undo_ptr & UBA_MASK_IS_SLAVE) >> UBA_POS_IS_SLAVE);
    csr = static_cast<csr_t>((undo_ptr & UBA_MASK_CSR) >> UBA_POS_CSR);
    state = static_cast<bool>((undo_ptr & UBA_MASK_STATE) >> UBA_POS_STATE);
  }

  bool is_null() const {
    return slot_addr_t::is_null() && state == 0 && csr == CSR_AUTOMATIC &&
           is_slave == false;
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

  static bool decode_active(const undo_ptr_t &undo_ptr) {
    return !static_cast<bool>((undo_ptr & UBA_MASK_STATE) >> UBA_POS_STATE);
  }

  static csr_t decode_csr(const undo_ptr_t &undo_ptr) {
    return static_cast<csr_t>((undo_ptr & UBA_MASK_CSR) >> UBA_POS_CSR);
  }

  static bool decode_slave(const undo_ptr_t &undo_ptr) {
    return static_cast<bool>((undo_ptr & UBA_MASK_IS_SLAVE) >>
                             UBA_POS_IS_SLAVE);
  }

  static void clear_csr(undo_ptr_t *undo_ptr) {
    *undo_ptr &= (~(((undo_ptr_t)(1)) << UBA_POS_CSR));
  }

  static void clear_slave(undo_ptr_t *undo_ptr) {
    *undo_ptr &= (~(((undo_ptr_t)(1)) << UBA_POS_IS_SLAVE));
  }

  static void set_commit(undo_ptr_t *undo_ptr, unsigned int csr,
                         bool is_slave) {
    *undo_ptr |= ((undo_ptr_t)1 << UBA_POS_STATE);

    clear_csr(undo_ptr);
    undo_ptr_t value = static_cast<undo_ptr_t>(csr);
    *undo_ptr |= (value << UBA_POS_CSR);

    clear_slave(undo_ptr);
    value = static_cast<undo_ptr_t>(is_slave);
    *undo_ptr |= (value << UBA_POS_IS_SLAVE);
  }

/** Retrieve slot address from undo address */
  static slot_ptr_t decode_slot(const undo_ptr_t &undo_ptr) {
    return ((undo_ptr & UBA_MASK_ADDR) >> UBA_POS_ADDR);
  }

  static bool decode_is_slot(const undo_ptr_t &undo_ptr) {
    return !(undo_ptr >> UBA_WIDTH_ADDR);
  }

  static bool decode_is_allocated(const undo_ptr_t &undo_ptr) {
    return undo_ptr != UNDO_PTR_NULL;
  }

  bool is_validate(const dict_index_t *index);

  const std::string print() const {
    std::stringstream ss;
    ss << "Undo Block Address:[space_id=" << space_id << ",page_no=" << page_no
       << ",offset=" << offset << ",state=" << state << ",csr=" << csr
       << ",slave=" << is_slave << "]";
    return ss.str();
  }
};

#endif

