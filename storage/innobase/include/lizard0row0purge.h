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

/** @file include/lizard0row0purge.h
 Lizard row purge implementation.

 Created 2020-03-27 by jiyang.zhang
 *******************************************************/
#ifndef lizard0row0purge_h
#define lizard0row0purge_h

#include "dict0mem.h"
struct purge_node_t;
class THD;

namespace lizard {

/** Purge phase for 2PC-Purge */
enum e_2pp_phase {
  /** Purge history list. */
  PURGE_HISTORY_LIST,
  /** Purge semi-purge list. */
  PURGE_SP_LIST,
};

/** Purges the parsed panda record.
 * @param[in,out]  node            row purge node
 * @param[in]      updated_extern  whether external columns were updated
 * @param[in,out]  thd             current thread
@return true if purged, false if skipped */
bool row_purge_record_for_panda(purge_node_t *node, bool updated_extern,
                                THD *thd);

/** Determines if it is possible to remove a secondary index entry.
Removal is possible if the secondary index entry does not refer to any
not delete marked version of a clustered index record where DB_TRX_ID
is newer than the purge view.

NOTE: This function should only be called by the purge thread, only
while holding a latch on the leaf page of the secondary index entry
(or keeping the buffer pool watch on the page).  It is possible that
this function first returns true and then false, if a user transaction
inserts a record that the secondary index entry would refer to.
However, in that case, the user transaction would also re-insert the
secondary index entry after purge has removed it and released the leaf
page latch.
@return true if the secondary index record can be purged */
[[nodiscard]] bool row_purge_poss_sec(purge_node_t *node, dict_index_t *index,
                                      const dtuple_t *entry, btr_cur_t *cur);

/**
 * Allocates and initializes the TRX_ID field in a partial row during purge
 * operations.
 *
 * During purge operations, trx_undo_rec_get_partial_row() creates a partial row
 * with only ordering fields initialized, leaving system columns (like TRX_ID)
 * as DATA_MISSING. This function properly initializes the TRX_ID field in the
 * clustered index row by:
 * 1. Setting the correct data type for the TRX_ID field
 * 2. Allocating memory for the field data
 * 3. Initializing the field with a zero transaction ID value
 *
 * Note that this does not fully initialize the entire row - only the TRX_ID
 * field is initialized. However, this is sufficient for building secondary
 * index entries for both hippo and regular secondary indexes during purge
 * operations. In particular, this TRX_ID initialization is critical for hippo
 * secondary indexes, which include the TID column in their index entries.
 *
 * @param[in] index  The clustered index (must be a clustered index)
 * @param[in,out] row  The partial row tuple to initialize
 * @param[in] heap  Memory heap for field data allocation
 */
void row_purge_alloc_trx_id_field(const dict_index_t *index, dtuple_t *row,
                                  mem_heap_t *heap);

}  // namespace lizard

#endif
