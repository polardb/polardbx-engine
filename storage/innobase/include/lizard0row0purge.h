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
}

#endif
