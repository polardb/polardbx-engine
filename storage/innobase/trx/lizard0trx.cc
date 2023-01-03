/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0trx.h
  Lizard transaction structure.

 Created 2020-08-27 by Jianwei.zhao
 *******************************************************/

#include "trx0trx.h"
#include "lizard0trx.h"
#include "lizard0row.h"
#include "lizard0cleanout.h"

namespace lizard {

void copy_to(TrxIdHash &h, TrxIdSet &s) {
  for (TrxIdHash::const_iterator it = h.cbegin(); it != h.cend(); it++) {
    s.insert(TrxTrack(it->first, it->second));
  }
}

void alloc_cleanout_cursors(trx_t *trx) {
  trx->cleanout_cursors = UT_NEW(Cleanout_cursors(), mem_key_row_cleanout);
  ut_ad(trx->cleanout_cursors != nullptr);
}

void release_cleanout_cursors(trx_t *trx) {
  UT_DELETE(trx->cleanout_cursors);
  trx->cleanout_cursors = nullptr;
}

void cleanout_rows_at_commit(trx_t *trx) {
  ut_ad(trx != nullptr);
  ut_ad(trx->cleanout_cursors != nullptr);

  if (trx->cleanout_cursors->cursor_count() == 0) {
    return;
  }

  auto undo_ptr = trx->txn_desc.undo_ptr;
  lizard_undo_ptr_set_commit(&undo_ptr);

  txn_rec_t txn_rec{trx->id, trx->txn_desc.cmmt.scn, undo_ptr,
                    trx->txn_desc.cmmt.gcn};

  commit_cleanout_do(trx, txn_rec);
}

}  // namespace lizard
