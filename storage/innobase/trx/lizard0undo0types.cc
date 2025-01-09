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
  Decode the slot_ptr into slot address
  @param[in]      slot ptr
*/
void slot_addr_t::decode(slot_ptr_t slot_ptr) {
  ulint rseg_id;

  slot_ptr_decode(slot_ptr, &offset, &page_no, &rseg_id);

  /* Confirm the reserved bits */
  ut_ad(((ulint)slot_ptr & UBA_MASK_UNUSED) == 0);

  if (rseg_id == SLOT_SPACE_NUM_FAKE) {
    space_id = SLOT_SPACE_ID_FAKE;
  } else if (rseg_id > FSP_IMPLICIT_TXN_TABLESPACES) {
    space_id = SPACE_UNKNOWN;
  } else {
    space_id = trx_rseg_id_to_space_id(rseg_id, false);
  }
}

void undo_addr_t::decode(undo_ptr_t undo_ptr) {
  slot_addr_t::decode((slot_ptr_t)undo_ptr);

  is_slave = static_cast<bool>(undo_ptr & UBA_MASK_IS_SLAVE);

  csr = static_cast<csr_t>(undo_ptr & UBA_MASK_CSR);

  state = static_cast<bool>(undo_ptr & UBA_MASK_STATE);

  /**
    It should not be trx_sys tablespace for normal table except
    of temporary table/LOG_DDL/DYNAMIC_METADATA/DDL in-process table */

  /**
    Revision:
    We give a fixed UBA in undo log header if didn't allocate txn undo
    for temporary table.
  */
  if (space_id == SLOT_SPACE_ID_FAKE) {
    lizard_ut_ad(offset >= SLOT_OFFSET_LIMIT);
  }
}

slot_ptr_t slot_addr_t::encode() const {
  ulint rseg_id;

  ut_ad(!is_null());
  lizard_ut_ad(slot_addr_validate(*this));

  if (space_id == SLOT_SPACE_ID_FAKE) {
    rseg_id = SLOT_SPACE_NUM_FAKE;
  } else {
    rseg_id = undo::id2num(space_id);
  }

  return (slot_ptr_t)(rseg_id) << UBA_POS_SPACE_ID |
         (slot_ptr_t)(page_no) << UBA_POS_PAGE_NO | offset;
}

undo_ptr_t undo_addr_t::encode() const {
  return (undo_ptr_t)(state) << UBA_POS_STATE |
         (undo_ptr_t)(csr) << UBA_POS_CSR |
         (undo_ptr_t)(is_slave) << UBA_POS_IS_SLAVE |
         (undo_ptr_t)slot_addr_t::encode();
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

/**
  Check the slot address if is actually points at the real disk storage.
*/
bool slot_addr_disk_mapped(const slot_addr_t &slot_addr) {
  /* Space ID */
  if (!lizard::fsp_is_txn_tablespace_by_id(slot_addr.space_id)) {
    return false;
  }

  /* Offset */
  if (slot_addr.offset < TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE) {
    return false;
  }

  if ((slot_addr.offset - (TRX_UNDO_SEG_HDR + TRX_UNDO_SEG_HDR_SIZE)) %
      TXN_UNDO_LOG_EXT_HDR_SIZE) {
    return false;
  }

  return true;
}

#if defined UNIV_DEBUG || defined LIZARD_DEBUG

bool slot_addr_validate(const slot_addr_t &slot_addr) {
  /** no_redo insert/update undo */
  if (slot_addr.space_id == SLOT_SPACE_ID_FAKE) {
    return true;
  } else if (slot_addr.is_null()) {
    return true;
  } else {
    return slot_addr_disk_mapped(slot_addr);
  }
}

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
    ut_a(slot_addr_validate(*undo_addr));
  }
  return true;
}

#endif
