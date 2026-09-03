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

/** @file include/lizard0rem0rec.h
 Lizard Record manager.

 Created 2025-10-23 by jiyang.zhang
 *******************************************************/

#ifndef lizard0rem0rec_h
#define lizard0rem0rec_h
#include "rem0rec.h"

#include "lizard0rem0types.h"

struct dict_index_t;

/** Get next record on the page (const).
 *
 * @param[in]	reg	current record.
 * @param[in]	compact or not.
 *
 * @retval	const next record. */
[[nodiscard]] static inline creg_t reg_get_next_ptr_const(creg_t reg,
                                                          ulint comp) {
  ut_ad(reg.in_page());
  return {rec_get_next_ptr_const(reg.rec, comp), MLOC_IN_PAGE};
}

/** Get next record on the page (no-const).
 *
 * @param[in]	reg	current record.
 * @param[in]	compact or not.
 *
 * @retval	next record. */
[[nodiscard]] static inline reg_t reg_get_next_ptr(reg_t reg, ulint comp) {
  return {rec_get_next_ptr(reg.rec, comp), MLOC_IN_PAGE};
}

#endif
