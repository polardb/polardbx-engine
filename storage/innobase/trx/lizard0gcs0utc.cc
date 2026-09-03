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

/** @file trx/lizard0gcs0utc.cc
 Lizard utc number implementation.

 Created 2025-06-24 by Jianwei.zhao
 *******************************************************/

#include "lizard0gcs0utc.h"
#include "lizard0ut.h"

#include "trx0trx.h"

namespace lizard {

/** Generate a now utc for transaction commit.
 *
 * @param[in/out]	trx	rw transaction.
 *
 * @retval	utc */
utc_t UTC::new_us(trx_t *trx) const {
  utc_t us = ut_time_system_us();

  trx->txn_desc.cmmt.us = us;

  return us;
}

}  // namespace lizard
