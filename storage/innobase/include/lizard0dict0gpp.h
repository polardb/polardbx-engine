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

/** @file include/lizard0dict0gpp.h
 GPP dictionary structure

 Created 2024-03-26 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0dict0gpp_h
#define lizard0dict0gpp_h

#include "dict0mem.h"

namespace lizard {

/** Add virtual GPP_NO column on index as virtual column.
 *
 * @param[in/out]	index
 * @param[in]		table
 * */
extern void dict_index_add_virtual_gcol(dict_index_t *index,
                                        const dict_table_t *table);
/**
 * Return prefined dict_table_t GPP_NO column.
 *
 * @return	always valid column. */
extern dict_col_t *dict_table_get_v_gcol(const dict_table_t *table);

/**
 * Copy column definition
 *
 * @param[in/out]	tuple
 * @Param[in]		dict_table_t */
extern void dict_table_copy_g_types(dtuple_t *tuple, const dict_table_t *table);

/** Build column definition for GPP_NO.
 *
 * @param[in]	table
 * @param[in]	heap
 *
 * @retval	GPP_NO column
 * */
extern dict_col_t *dict_mem_table_add_v_gcol(dict_table_t *table,
                                             mem_heap_t *heap);

/** Add stored GPP_NO column on secondary index following PK Columns.
 *
 * @param[in/out]	new index.
 * @param[in]		index.
 * @param[in]		dictionary table
 */
extern void dict_index_add_stored_gcol(dict_index_t *new_index,
                                       const dict_index_t *index,
                                       const dict_table_t *table);
}  // namespace lizard

#endif
