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

/** @file include/lizard0mtr.h
  Lizard mtr wrapper.

 Created 2020-05-28 by Jianwei.zhao
 *******************************************************/

#include "mtr0mtr.h"

#include "lizard0mtr.h"

namespace lizard {

Mtr_wrapper::Mtr_wrapper(mtr_t *mtr) : m_mtr(mtr), m_started(false) {}

Mtr_wrapper::~Mtr_wrapper() { ut_ad(m_started == false); }

void Mtr_wrapper::start() {
  if (m_started) return;

  mtr_start_sync(m_mtr);
  m_started = true;
}

lsn_t Mtr_wrapper::commit() {
  lsn_t lsn = 0;
  if (m_started) {
    mtr_commit(m_mtr);
    m_started = false;

    lsn = m_mtr->commit_lsn();
    ut_ad(lsn > 0 || !mtr_t::s_logging.is_enabled());
    return lsn;
  } else {
    return lsn;
  }
}

}  // namespace lizard
