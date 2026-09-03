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

/** @file row/lizard0row0undo.cc
 lizard row undo.

 Created 2025-01-07 by Yichang SONG
 *******************************************************/

#include "row0row.h"

#include "lizard0rem0slc.h"
#include "lizard0rem0slc0ctx.h"
#include "lizard0row.h"
#include "lizard0row0undo.h"

namespace lizard {

/** Repositions the pcur in the undo node on the panda index record,
 * if found. If the record is not found, close pcur.
 * @param[in]      mode       BTR_MODIFY_LEAF, ...
 * @param[in]      index      panda index
 * @param[in,out]  node       undo node
 * @param[in,out]  mtr     mini-transaction handle
 @return true if the record was found */
bool row_undo_reposition_panda_pcur(ulint mode, dict_index_t *index,
                                    undo_node_t *node, mtr_t *mtr) {
  bool found = node->found_panda;
  if (found) {
    ut_ad(undo_node_validate_panda_pcur(node, index));

    found = node->pcur.restore_position(mode, mtr, UT_LOCATION_HERE);
  } else {
    found = row_search_on_row_ref_for_panda(&node->pcur, mode, index, node->ref,
                                            mtr);
    if (found) {
      node->pcur.store_position(mtr);
    }
  }
  // pcursor will be closed collectively at the end of the rollback.
  // if (!found) {
  //   node->pcur.close();
  // }
  node->found_panda = found;
  return found;
}

/** Looks for the panda index record when node has the row reference.
 The pcur in node is used in the search. If found, stores the position
 of pcur, and detaches it. The pcur must be closed by the caller in any case.
 @param[in,out]  node             undo node
 @param[in]      panda_index      panda index
 @return true if found; NOTE the node->pcur must be closed by the
 caller, regardless of the return value */
bool row_undo_search_panda_to_pcur(undo_node_t *node,
                                   dict_index_t *panda_index) {
  bool found;
  mtr_t mtr;
  Slice_ctx sctx;
  Slice *slc = nullptr;

  ut_ad(!node->table->skip_alter_undo);
  ut_ad(node->layout == TL_BAMBOO && !node->is_rlog);
  ut_ad(dict_index_is_panda(panda_index));

  mtr_start(&mtr);
  dict_disable_redo_if_temporary(node->table, &mtr);

  found = row_search_on_row_ref_for_panda(&node->pcur, BTR_MODIFY_LEAF,
                                          panda_index, node->ref, &mtr);
  if (!found) {
    goto func_exit;
  }
  slc = sctx.get_offsets(node->pcur.get_reg(), panda_index, ULINT_UNDEFINED);

  found = (row_get_rec_roll_ptr(slc, panda_index) == node->roll_ptr);
  if (found) {
    ut_ad(row_get_rec_trx_id(slc, panda_index) == node->trx->id);
    node->pcur.store_position(&mtr);
  }

#ifdef UNIV_DEBUG
  if (!found) {
    lizard_info(ER_LIZARD) << "Ignore panda undo record due to not find a "
                              "record that matching roll_ptr, table_name="
                           << panda_index->table_name
                           << ",index_name=" << panda_index->name();
  }
#endif

func_exit:
  node->pcur.commit_specify_mtr(&mtr);
  node->found_panda = found;
  return (found);
}
#ifdef UNIV_DEBUG
/** Validate the persisent cursor. The purge node has two references
 to the clustered/panda index record - one via the ref member, and the
 other via the persistent cursor.  These two references must match
 each other if the found_clust flag is set.
 @return true if the stored copy of persistent cursor is consistent
 with the ref member.*/
bool undo_node_validate_panda_pcur(undo_node_t *node, dict_index_t *index) {
  ut_ad(lizard::dict_index_is_panda(index));

  if (!node->found_panda) {
    return (true);
  }

  if (!node->pcur.m_old_stored) {
    return (true);
  }
  ut_ad(node->pcur.get_btr_cur()->index == index);

  Slice_ctx sctx;
  Slice *slc = nullptr;

  slc = sctx.get_offsets(node->pcur.m_old_reg, index, ULINT_UNDEFINED);

  /* Here we are comparing the purge ref record and the stored initial
  part in persistent cursor. Both cases we store n_uniq fields of the
  cluster index and so it is fine to do the comparison. We note this
  dependency here as pcur and ref belong to different modules. */
  int st = cmp_dtuple_rec(node->ref, index, slc);

  if (st != 0) {
    ib::error(ER_IB_MSG_1010) << "Undo node panda pcur validation failed";
    ib::error(ER_IB_MSG_1011) << rec_printer(node->ref).str();
    ib::error(ER_IB_MSG_1012) << rec_printer(slc).str();
    return (false);
  }

  return (true);
}
#endif

}  // namespace lizard
