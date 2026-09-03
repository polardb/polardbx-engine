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

/** @file include/lizard0gcs0snapshot0types.h
 Transaction Commit Number Snapshot

 Created 2020-09-22 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0gcs0snapshot0types_h
#define lizard0gcs0snapshot0types_h

#include "lizard0gcs0service.h"
#include "lizard0trx0service.h"

namespace lizard {
/**------------------------------------------------------------------------*/
/** Transaction Commit Number Snapshot */
/**------------------------------------------------------------------------*/
/**
 * Transaction Commit Number Snapshot:
 *
 * Snapshot system commit numbers include :
 *   0) up_limit_tid
 *   1) utc_sec
 *   2) scn
 *   3) gcn
 *
 *  to fill Commit Snapshot Buffer
 */
struct commit_snap_t {
  trx_id_t up_limit_tid;
  utc_t utc_sec;
  scn_t scn;
  gcn_t gcn;
};

}  // namespace lizard

#endif
