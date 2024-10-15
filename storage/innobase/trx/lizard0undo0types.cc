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

/** @file include/lizard0undo0types.cc
  Lizard transaction undo types.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#include "lizard0undo0types.h"
#include "lizard0txn.h"
#include "lizard0txn0space.h"
#include "lizard0undo.h"

#include "trx0rseg.h"

bool slot_addr_t::is_null() const {
  return *this == lizard::txn_sys_t::SLOT_ADDR_NULL;
}

bool slot_addr_t::is_no_redo() const {
  return *this == lizard::txn_sys_t::SLOT_ADDR_NO_REDO;
}

bool slot_addr_t::is_redo() const {
  return lizard::fsp_is_txn_tablespace_by_id(space_id);
}

/**
  Encode UBA into undo_ptr that need to copy into record
  @param[in]      undo addr
  @param[out]     undo ptr
*/
void undo_encode_undo_addr(const undo_addr_t &undo_addr, undo_ptr_t *undo_ptr) {
  ulint rseg_id = undo::id2num(undo_addr.space_id);

  *undo_ptr = (undo_ptr_t)(undo_addr.state) << UBA_POS_STATE |
              (undo_ptr_t)(undo_addr.csr) << UBA_POS_CSR |
              (undo_ptr_t)(undo_addr.is_slave) << UBA_POS_IS_SLAVE |
              (undo_ptr_t)rseg_id << UBA_POS_SPACE_ID |
              (undo_ptr_t)(undo_addr.page_no) << UBA_POS_PAGE_NO |
              undo_addr.offset;
}

/**
  Encode addr into slot_ptr that need to write undo header.
  @param[in]      slot addr
  @param[out]     slot ptr
*/
void undo_encode_slot_addr(const slot_addr_t &slot_addr, slot_ptr_t *slot_ptr) {
  ulint rseg_id = undo::id2num(slot_addr.space_id);
  /** Must be a valid txn undo slot address or no_redo special address. */
  lizard_ut_ad(slot_addr_validate(slot_addr));

  *slot_ptr = (slot_ptr_t)rseg_id << SLOT_POS_SPACE_ID |
              (slot_ptr_t)(slot_addr.page_no) << SLOT_POS_PAGE_NO |
              slot_addr.offset;
}
bool undo_slot_addr_equal(const slot_addr_t &slot_addr,
                          const undo_ptr_t undo_ptr) {
  undo_addr_t undo_addr;
  undo_decode_undo_ptr(undo_ptr, &undo_addr);
  if (undo_addr.offset == slot_addr.offset &&
      undo_addr.page_no == slot_addr.page_no &&
      undo_addr.space_id == slot_addr.space_id)
    return true;

  return false;
}

/**
  Decode the undo_ptr into UBA
  @param[in]      undo ptr
  @param[out]     undo addr
*/
void undo_decode_undo_ptr(const undo_ptr_t uba, undo_addr_t *undo_addr) {
  ulint rseg_id;
  undo_ptr_t undo_ptr = uba;
  ut_ad(undo_addr);

  undo_addr->offset = (ulint)undo_ptr & 0xFFFF;
  undo_ptr >>= UBA_WIDTH_OFFSET;
  undo_addr->page_no = (ulint)undo_ptr & 0xFFFFFFFF;
  undo_ptr >>= UBA_WIDTH_PAGE_NO;
  rseg_id = (ulint)undo_ptr & 0x7F;
  undo_ptr >>= UBA_WIDTH_SPACE_ID;

  /* Confirm the reserved bits */
  ut_ad(((ulint)undo_ptr & 0x3f) == 0);
  undo_ptr >>= UBA_WIDTH_UNUSED;
  undo_addr->is_slave = static_cast<bool>(undo_ptr & 0x1);

  undo_ptr >>= UBA_WIDTH_IS_SLAVE;
  undo_addr->csr = static_cast<csr_t>(undo_ptr & 0x1);

  undo_ptr >>= UBA_WIDTH_CSR;
  undo_addr->state = (bool)undo_ptr;

  /**
    It should not be trx_sys tablespace for normal table except
    of temporary table/LOG_DDL/DYNAMIC_METADATA/DDL in-process table */

  /**
    Revision:
    We give a fixed UBA in undo log header if didn't allocate txn undo
    for temporary table.
  */
  if (rseg_id == 0) {
    lizard_ut_ad(undo_addr->offset >= SLOT_OFFSET_LIMIT);
  }
  /** It's always redo txn undo log */
  undo_addr->space_id = trx_rseg_id_to_space_id(rseg_id, false);
}

/**
  Decode the slot_ptr into slot address
  @param[in]      slot ptr
  @param[out]     slot addr
*/
void undo_decode_slot_ptr(slot_ptr_t ptr_arg, slot_addr_t *slot_addr) {
  ulint rseg_id;
  slot_ptr_t slot_ptr = ptr_arg;
  ut_ad(slot_addr);

  slot_addr->offset = (ulint)slot_ptr & 0xFFFF;
  slot_ptr >>= SLOT_WIDTH_OFFSET;
  slot_addr->page_no = (ulint)slot_ptr & 0xFFFFFFFF;
  slot_ptr >>= SLOT_WIDTH_PAGE_NO;
  rseg_id = (ulint)slot_ptr & 0x7F;
  slot_ptr >>= SLOT_WIDTH_SPACE_ID;

  /* Confirm the reserved bits */
  ut_ad(((ulint)slot_ptr & 0x3f) == 0);

  if (!slot_addr->is_null() && rseg_id == 0) {
    lizard_ut_ad(slot_addr->is_no_redo());
  }
  /** It's redo txn slot or no_redo special txn slot */
  slot_addr->space_id = trx_rseg_id_to_space_id(rseg_id, false);
}

xes_tags_t undo_decode_xes_tags(ulint tags) {
  xes_tags_t xtt = {false, csr_t::CSR_AUTOMATIC};
  if (tags & XES_TAGS_ROLLBACK) {
    xtt.is_rollback = true;
  }
  if (tags & XES_TAGS_AC_ASSIGNED) {
    xtt.csr = csr_t::CSR_ASSIGNED;
  }
  return xtt;
}

/*-----------------------------------------------------------------------------*/
/* txn_slot_t related */
/*-----------------------------------------------------------------------------*/
bool txn_slot_t::tags_allocated() const {
  return xes_storage & XES_ALLOCATED_TAGS;
}

bool txn_slot_t::is_rollback() const {
  /** The TXN must be the new format. */
  ut_a(tags_allocated());

  switch (state) {
    case TXN_UNDO_LOG_COMMITED:
    case TXN_UNDO_LOG_PURGED:
      return undo_decode_xes_tags(tags).is_rollback;
    case TXN_UNDO_LOG_ACTIVE:
      ut_a(!(undo_decode_xes_tags(tags).is_rollback));
      return false;
    default:
      ut_error;
  }
}

bool txn_slot_t::ac_prepare_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_PREPARE;
}
bool txn_slot_t::ac_commit_allocated() const {
  return xes_storage & XES_ALLOCATED_AC_COMMIT;
}



#if defined UNIV_DEBUG || defined LIZARD_DEBUG

/** Check the UBA validation */
bool undo_addr_validate(const undo_addr_t *undo_addr,
                        const dict_index_t *index) {
  bool internal_dm_table = false;
  if (index) {
    internal_dm_table =
        (my_strcasecmp(system_charset_info, index->table->name.m_name,
                       "mysql/innodb_dynamic_metadata") == 0
             ? true
             : false);
  }

  if ((index && index->table->is_temporary())) {
    ut_a(lizard::txn_sys_t::instance()->is_temporary(*undo_addr));
  } else if (internal_dm_table) {
    ut_a(lizard::txn_sys_t::instance()->is_dynamic_metadata(*undo_addr));
  }

  /** If not special, must be normal txn undo address. */
  if (!lizard::txn_sys_t::instance()->is_special(*undo_addr)) {
    ut_a(lizard::fsp_is_txn_tablespace_by_id(undo_addr->space_id));
    ut_a(undo_addr->page_no > 0);
    /** TODO: offset must be align to TXN_UNDO_EXT */
    ut_a(undo_addr->offset >= (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
  }
  return true;
}

bool slot_addr_validate(const slot_addr_t &slot_addr) {
  /** no_redo insert/update undo */
  if (slot_addr.is_no_redo() || slot_addr.is_null()) {
    return true;
  } else {
    ut_a(lizard::fsp_is_txn_tablespace_by_id(slot_addr.space_id));
    ut_a(slot_addr.page_no > 0);
    /** TODO: offset must be align to TXN_UNDO_EXT */
    ut_a(slot_addr.offset >= (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE));
  }
  return true;
}

#endif
