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

/** @file include/lizard0gcs0clean.cc
 Global Change System clean module advanced by purge sys and erase sys.

 Created 2020-03-23 by Jianwei.zhao
 *******************************************************/

#include "lizard0gcs0clean.h"
#include "lizard0gcs.h"

#include "lizard0trx0erase.h"

namespace lizard {

/** Read from page to init gcn value. */
void CClean::GcnCleaner::init() {
  ut_ad(m_inited == false);
  m_gcn = read();
  m_inited = true;
}

/** Read gcn value */
gcn_t CClean::GcnCleaner::read() {
  gcn_t gcn;
  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);

  gcn = mach_read_from_8(hdr + m_poffset);

  /** If lizard version is low, purge_gcn or erased_gcn has not been saved. */
  if (gcn == 0) {
    gcn = GCN_INITIAL;
  } else {
    ut_a(gcn >= GCN_INITIAL);
  }

  mtr_commit(&mtr);
  return gcn;
}

/** Persist gcn */
void CClean::GcnCleaner::write(gcn_t gcn) {
  ut_ad(m_inited == true);
  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + m_poffset, gcn, &mtr);
  mtr_commit(&mtr);
}

gcn_t CClean::GcnCleaner::get() const {
  ut_ad(m_inited == true);
  return m_gcn.load(std::memory_order_relaxed);
}

/**
  Flush the bigger commit number to lizard tbs,
  Only one single thread to persist.
*/
void CClean::GcnCleaner::advance(gcn_t gcn) {
  ut_ad(m_inited == true);
  if (gcn > m_gcn) {
    m_gcn.store(gcn, std::memory_order_relaxed);
    write(m_gcn);
  }
}

/** Init purge cleaner when purge system booting. */
void CClean::init_purge() {
  /** Init from purge heap since Purged scn didn't support persistent */
  scn_t purged_scn = trx_purge_boot_purged_scn();
  m_purged_scn.store(purged_scn, std::memory_order_relaxed);

  /** Unnecessary to boot utc from purge system. */
  m_purged_us = 0;

  /** init purged gcn from gcs header. */
  m_purged_gcn.init();

  lizard_system(ER_LIZARD) << "GCS Start Purged SCN: " << m_purged_scn
                           << ", GCN: " << m_purged_gcn.get();

  return;
}

/** Init erase cleaner when erase system booting. */
void CClean::init_erase() {
  scn_t erased_scn = trx_erase_boot_erased_scn();
  m_erased_scn.store(erased_scn, std::memory_order_relaxed);

  /** Unnecessary to boot utc from erase system. */
  m_erased_us = 0;

  m_erased_gcn.init();

  lizard_system(ER_LIZARD) << "GCS Start Erased SCN: " << m_erased_scn
                           << ", GCN: " << m_erased_gcn.get();
}

}  // namespace lizard
