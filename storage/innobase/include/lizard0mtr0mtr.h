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

/** @file include/lizard0mtr0mtr.h
  Lizard mtr utilities.

 Created 2020-05-28 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0mtr0mtr_h
#define lizard0mtr0mtr_h

#include "log0types.h"

struct mtr_t;

/** Lazily started mini-transaction.
 *
 * Hold a caller-owned mtr which is started on demand, so several
 * optional modification steps can share one single mtr while it's left
 * untouched if none of them happens. start() only takes effect on the
 * first call, and commit() is a no-op if the mtr was never started.
 *
 * NOTE: it's not a RAII guard, the caller must commit() explicitly,
 * which is asserted by the destructor. */
struct lazy_mtr_t {
  lazy_mtr_t(mtr_t *mtr);
  ~lazy_mtr_t();

  /** Start the mtr if it's not started yet. */
  void start();

  /** Commit the mtr if it was started.
   *
   * @retval	commit lsn of the mtr, or 0 if it was never started */
  lsn_t commit();

 private:
  /** The mini-transaction owned by the caller. */
  mtr_t *m_mtr;
  /** Whether the mtr is started and not committed yet. */
  bool m_started;
};

#endif  // lizard0mtr0mtr_h
