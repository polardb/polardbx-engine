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

/** @file include/lizard0row0upd.h
 Lizard update of a row

 Created 2024-09-19 by jiyang.zhang
 *******************************************************/

#ifndef lizard0row0upd_h
#define lizard0row0upd_h

#include "row0upd.h"

namespace lizard {

extern bool row_upd_panda_only_pk_changed(const upd_node_t *node,
                                          bool change_ord_field);

extern dberr_t row_upd_panda_only_pk(upd_node_t *node, que_thr_t *thr);
}

#endif
