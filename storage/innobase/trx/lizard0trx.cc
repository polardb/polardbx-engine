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

/** @file trx/lizard0trx.cc
  Lizard transaction structure.

 Created 2020-08-27 by Jianwei.zhao
 *******************************************************/

#include "lizard0trx.h"
#include "lizard0cleanout.h"
#include "lizard0row.h"
#include "lizard0undo.h"
#include "trx0trx.h"

#ifndef UNIV_HOTBACKUP
#include <sql_class.h>
#endif

namespace lizard {

void alloc_commit_cleanout(trx_t *trx) {
  ut_ad(trx->cleanout == nullptr);

  trx->cleanout = ut::new_withkey<Commit_cleanout>(
      ut::make_psi_memory_key(mem_key_row_cleanout));
  ut_ad(trx->cleanout != nullptr);
}

void release_commit_cleanout(trx_t *trx) {
  if (trx->cleanout) {
    ut::delete_(trx->cleanout);
    trx->cleanout = nullptr;
  }
}

}  // namespace lizard
