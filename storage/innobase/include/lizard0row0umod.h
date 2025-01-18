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

/** @file include/lizard0row0umod.h
 Lizard row undo modify implementation.

 Created 2024-10-09 by Yichang Song
 *******************************************************/

#ifndef lizard0row0umod_h
#define lizard0row0umod_h

#include "row0umod.h"

namespace lizard {
/** Rollback the modified panda record.
 * @param[in,out]  node            row rollback node
 * @param[in]      thr             que thread
 * @param[in,out]  thd             current MySQL connection (for mdl)
 * @param[in,out]  mdl             MDL ticket
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
dberr_t row_undo_mod_record_for_panda(undo_node_t *node, que_thr_t *thr,
                                      THD *thd, MDL_ticket *mdl);

/** Rollback the modified panda record of row log.
 * @param[in,out]  node            row rollback node
 * @param[in]      thr             que thread
 * @param[in,out]  thd             current MySQL connection (for mdl)
 * @param[in,out]  mdl             MDL ticket
@return DB_SUCCESS or DB_OUT_OF_FILE_SPACE */
dberr_t row_undo_mod_record_for_rlog(undo_node_t *node, que_thr_t *thr,
                                     THD *thd, MDL_ticket *mdl);
}  // namespace lizard
#endif
