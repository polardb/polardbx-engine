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

/** @file mtr/lizard0mtr0mtr.cc
  Lizard mtr utilities.

 Created 2020-05-28 by Jianwei.zhao
 *******************************************************/

#include "mtr0mtr.h"

#include "lizard0mtr0mtr.h"

/** Constructor.
 *
 * @param[in]	mtr	mtr owned by the caller, left untouched until the
 * first start() */
lazy_mtr_t::lazy_mtr_t(mtr_t *mtr) : m_mtr(mtr), m_started(false) {}

/** Destructor. It never commits the mtr on behalf of the caller, so
 * assert that the mtr is not left started. */
lazy_mtr_t::~lazy_mtr_t() { ut_ad(m_started == false); }

/** Start the mtr if it's not started yet, so several optional
 * modification steps can call it unconditionally while only the first
 * one takes effect. */
void lazy_mtr_t::start() {
  if (m_started) return;

  mtr_start_sync(m_mtr);
  m_started = true;
}

/** Commit the mtr if it was started.
 *
 * @retval	commit lsn of the mtr, or 0 if it was never started, in
 * which case nothing was modified and there is nothing to flush */
lsn_t lazy_mtr_t::commit() {
  lsn_t lsn = 0;
  if (m_started) {
    mtr_commit(m_mtr);
    m_started = false;

    lsn = m_mtr->commit_lsn();
    ut_ad(lsn > 0 || !mtr_t::s_logging.is_enabled());
  }
  return lsn;
}
