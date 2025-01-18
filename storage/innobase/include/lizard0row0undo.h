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

/** @file include/lizard0row0undo.h
 lizard row undo.

 Created 2025-01-07 by Yichang SONG
 *******************************************************/

#ifndef lizard0row0undo_h
#define lizard0row0undo_h

#include "row0undo.h"
namespace lizard {
/** Repositions the pcur in the undo node on the panda index record,
 * if found. If the record is not found, close pcur.
 * @param[in]      mode       BTR_MODIFY_LEAF, ...
 * @param[in]      index      panda index
 * @param[in,out]  node       undo node
 * @param[in,out]  mtr     mini-transaction handle
 @return true if the record was found */
bool row_undo_reposition_panda_pcur(ulint mode, dict_index_t *index,
                                    undo_node_t *node, mtr_t *mtr);

/** Looks for the panda index record when node has the row reference.
 The pcur in node is used in the search. If found, stores the position
 of pcur, and detaches it. The pcur must be closed by the caller in any case.
 @param[in,out]  node             undo node
 @param[in]      panda_index      panda index
 @return true if found; NOTE the node->pcur must be closed by the
 caller, regardless of the return value */
[[nodiscard]] bool row_undo_search_panda_to_pcur(undo_node_t *node,
                                                 dict_index_t *panda_index);

#ifdef UNIV_DEBUG
/** Validate the persistent cursor. The purge node has two references
    to the panda index record - one via the ref member, and the
    other via the persistent cursor.  These two references must match
    each other if the found_clust flag is set.
    @return true if the persistent cursor is consistent with
    the ref member.*/
bool undo_node_validate_panda_pcur(undo_node_t *node, dict_index_t *index);
#endif

}  // namespace lizard
#endif