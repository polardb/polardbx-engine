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

/** @file include/lizard0undo0rec0types.h
  Lizard transaction undo record types.

 Created 2024-12-10 by Yichang SONG
 *******************************************************/

#ifndef lizard0undo0rec0types_h
#define lizard0undo0rec0types_h

#include "lizard0gcs0service.h"
#include "lizard0trx.h"
#include "lizard0txn0service.h"
#include "lizard0undo0types.h"
#include "trx0types.h"

/** Undo record transactional fields for reporting undo logs derived from row
 * log. */
struct urec_trx_t {
 public:
  /* trx id */
  trx_id_t trx_id{0};
  /* roll ptr */
  roll_ptr_t roll_ptr{0};
  /** scn number */
  scn_t scn{SCN_NULL};
  /** undo log header address */
  undo_ptr_t undo_ptr{UNDO_PTR_NULL};
  /* gcn number */
  gcn_t gcn{GCN_NULL};

  void build_for_rlog_ins() {
    trx_id = lizard::TRX_ID_FAKE_FOR_RLOG_INS;
    roll_ptr = 0;
    scn = SCN_NULL;
    undo_ptr = UNDO_PTR_NULL;
    gcn = GCN_NULL;
  }

  void reset() {
    trx_id = 0;
    roll_ptr = 0;
    scn = SCN_NULL;
    undo_ptr = UNDO_PTR_NULL;
    gcn = GCN_NULL;
  }
};

#endif  // lizard0undo0rec0types_h
