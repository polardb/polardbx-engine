/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file btr/lizard0btr0pcur.h
   Lizard persistent index tree cursor


 Created 2025-04-02 by Jianwei.zhao
 *******************************************************/

#include "lizard0btr0pcur.h"

#include "lizard0btr0cur.h"
#include "lizard0mon.h"

namespace lizard {

/** Whether to enable guess mode for btr_pcur_t::restore_position. */
bool index_pcur_guess_restore_enabled = true;

/** Whether to skip optimistic restore and go directly to guess+search path. */
bool inject_stress_test_for_guess = false;

/** Validate page cursor */
bool pcur_position_validate(btr_pcur_t *pcur, const rec_t *rec,
                            const dict_index_t *index) {
  ut_ad(rec && index);

  if (pcur) {
    return pcur->get_rec() == rec && pcur->index() == index;
  }
  return true;
}
}  // namespace lizard

/** Attempt to restore the persistent cursor position by directly latching the
previously known page and binary-searching within it, without a full B-tree
traversal from root. This is a "guess" strategy sitting between the optimistic
(modify_clock) path and the pessimistic (open_no_init) path.

Preconditions enforced by the caller:
  - latch_mode is BTR_SEARCH_LEAF or BTR_MODIFY_LEAF
  - m_rel_pos == BTR_PCUR_ON

@param[in]      index       clustered index
@param[in]      tuple       search tuple built from stored record prefix
@param[in]      mode        page cursor search mode (PAGE_CUR_LE, etc.)
@param[in]      latch_mode  BTR_SEARCH_LEAF or BTR_MODIFY_LEAF
@param[in,out]  mtr         mini-transaction
@param[in]      location    caller source location
@return true if successfully positioned on the same record, false otherwise */
bool btr_pcur_t::guess_position(dict_index_t *index, const dtuple_t *tuple,
                                page_cur_mode_t mode, ulint latch_mode,
                                mtr_t *mtr, ut::Location location) {
  ut_ad(is_positioned());
  ut_ad(latch_mode == BTR_SEARCH_LEAF || latch_mode == BTR_MODIFY_LEAF);
  ut_ad(m_rel_pos == BTR_PCUR_ON);

  if (!lizard::index_pcur_guess_restore_enabled) {
    return false;
  }

  /* Not supported for ibuf, spatial, temporary, or compressed tables. */
  if (dict_index_is_ibuf(index) || dict_index_is_spatial(index) ||
      index->table->is_temporary() || index->table->is_compressed()) {
    return false;
  }

  m_latch_mode = BTR_LATCH_MODE_WITHOUT_INTENTION(latch_mode);

  m_search_mode = mode;

  /* Use the page_no saved in m_block_when_stored at store() time. */
  page_no_t page_no = m_block_when_stored.page_id().page_no();

  /* Delegate to btr_cur_guess_clust_by_gpp_low for page fetch + search. */
  bool found = lizard::btr_cur_guess_by_gpp_low(
      page_no, index, tuple, get_btr_cur(), mode, latch_mode, mtr);

  if (found) {
    m_pos_state = BTR_PCUR_IS_POSITIONED;

    m_btr_cur.index = index;

    m_btr_cur.flag = BTR_CUR_BINARY;

    m_trx_if_known = nullptr;
  }

  lizard::index_pcur_guess_restore_stat(found);

  return found;
}
