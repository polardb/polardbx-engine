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

/** @file include/lizard0row0ins.h
 Lizard insert into a table

 Created 2024-09-19 by jiyang.zhang
 *******************************************************/

#ifndef lizard0row0ins_h
#define lizard0row0ins_h

#include "btr0cur.h"
#include "data0data.h"

#include "lizard0dict.h"

namespace lizard {

extern dberr_t row_ins_panda_sec_index_entry_low(
    uint32_t flags, const txn_layout_t &layout, ulint mode, dict_index_t *index,
    mem_heap_t *offsets_heap, mem_heap_t *heap, dtuple_t *entry,
    trx_id_t trx_id, que_thr_t *thr, bool dup_chk_only);
}

#endif
