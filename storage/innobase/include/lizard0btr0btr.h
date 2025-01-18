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

/** @file btr/lizard0btr0btr.h
 *
 * Lizard btree upgrade version

 Created 2024-08-30 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0btr0btr_h
#define lizard0btr0btr_h

#include "dict0mem.h"
#include "fil0fil.h"

#include "lizard0btr0types.h"
#include "lizard0fil0types.h"

namespace lizard {

/** Read page type of btree root page.
 *
 * @param[in]	dict index
 *
 * @retval	persisted root page type.
 * */
extern page_type_t btr_page_read_root_type(const dict_index_t *index);

/** Whether upgrade index root page when btree create.
 *
 * @param[in]	index
 * @param[in]	index create type.
 * @param[in]   expected root page type.
 *
 * @retval	final root page create type. */
extern page_type_t btr_page_upgrade_root(const dict_index_t *index,
                                         page_type_t create_type,
                                         page_type_t expected_page_type);

/** Whether upgrade index not-root page when btree allocate page..
 *
 * @param[in]	root page type.
 * @param[in]	index create type.
 *
 * @retval	final page create type. */
extern page_type_t btr_page_upgrade_not_root(const page_type_t root_page_type,
                                             page_type_t create_type);

/**
 * Reset the index id to 0 if btr page freed.
 *
 * @param[in] block   Pointer to the page
 * @param[in] mtr     Mini-transaction, can be NULL if redo log is not needed
 */
extern void btr_page_reset_index_id(buf_block_t *block, mtr_t *mtr = nullptr);

}  // namespace lizard

#endif
