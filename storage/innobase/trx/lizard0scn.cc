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


/** @file trx/lizard0scn.cc
 Lizard scn number implementation.

 Created 2020-03-24 by Jianwei.zhao
 *******************************************************/

#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0types.h"

#include "lizard0scn.h"
#include "lizard0sys.h"

#ifdef UNIV_PFS_MUTEX
/* Lizard scn mutex PFS key */
mysql_pfs_key_t lizard_scn_mutex_key;
#endif

namespace lizard {

bool srv_snapshot_update_gcn = false;

/** Constructor of SCN */
SCN::SCN()
    : m_scn(SCN_NULL),
      m_gcn(GCN_NULL),
      m_snapshot_gcn(GCN_NULL),
      m_inited(false) {
  mutex_create(LATCH_ID_LIZARD_SCN, &m_mutex);
}

/** Destructor of SCN */
SCN::~SCN() { mutex_free(&m_mutex); }

/** Assign the init value by reading from zesu tablespace */
void SCN::init() {
  ut_ad(!m_inited);
  ut_ad(m_scn == SCN_NULL);

  lizard_sysf_t *lzd_hdr;
  mtr_t mtr;
  mtr.start();

  lzd_hdr = lizard_sysf_get(&mtr);

  m_scn = 2 * LIZARD_SCN_NUMBER_MAGIN +
          ut_uint64_align_up(mach_read_from_8(lzd_hdr + LIZARD_SYS_SCN),
                             LIZARD_SCN_NUMBER_MAGIN);

  m_gcn = mach_read_from_8(lzd_hdr + LIZARD_SYS_GCN);
  m_snapshot_gcn = m_gcn.load();

  lizard_sys->min_safe_scn = m_scn.load();

  ut_a(m_scn > 0 && m_scn < SCN_NULL);
  mtr.commit();

  m_inited = true;
  return;
}

/** Flush the scn number to tablepace every ZEUS_SCN_NUMBER_MAGIN */
void SCN::flush_scn() {
  lizard_sysf_t *lzd_hdr;
  mtr_t mtr;

  ut_ad(m_mutex.is_owned());
  ut_ad(m_inited);
  ut_ad(m_scn != SCN_NULL);

  mtr_start(&mtr);
  lzd_hdr = lizard_sysf_get(&mtr);
  mlog_write_ull(lzd_hdr + LIZARD_SYS_SCN, m_scn, &mtr);
  mtr_commit(&mtr);
}

/** Flush the global commit number to system tablepace */
void SCN::flush_gcn() {
  lizard_sysf_t *lzd_hdr;
  mtr_t mtr;

  ut_ad(m_mutex.is_owned());
  ut_ad(m_inited);
  ut_ad(m_gcn != SCN_NULL);

  mtr_start(&mtr);
  lzd_hdr = lizard_sysf_get(&mtr);
  mlog_write_ull(lzd_hdr + LIZARD_SYS_GCN, m_gcn, &mtr);
  mtr_commit(&mtr);
}

/** Calucalte a new scn number
@return     scn */
scn_t SCN::new_scn() {
  scn_t num;
  ut_ad(m_inited);

  ut_ad(mutex_own(&m_mutex));

  /** flush scn every magin */
  if (!(m_scn % LIZARD_SCN_NUMBER_MAGIN)) flush_scn();

  num = ++m_scn;

  ut_a(num > 0 && num < SCN_NULL);

  return num;
}

/** Calculate a new scn number and consistent UTC time
@return   <SCN, UTC, GCN, Error> */
std::pair<commit_scn_t, bool> SCN::new_commit_scn(gcn_t gcn) {
  commit_scn_t cmmt = COMMIT_SCN_NULL;

  if (gcn != GCN_NULL)
    cmmt.gcn = gcn;
  else 
    cmmt.gcn = acquire_gcn(true);

  if (cmmt.gcn > m_gcn) {
    m_gcn = cmmt.gcn;
    flush_gcn();
  }

  cmmt.scn = new_scn();

  ut_ad(cmmt.scn > SCN_RESERVERD_MAX);

  return std::make_pair(cmmt, false);
}

/** Get current scn which is committed.
@param[in]  true if m_mutex is hold
@return     m_scn */
scn_t SCN::acquire_scn(bool mutex_hold) {
  scn_t ret;
  if (!mutex_hold) {
    mutex_enter(&m_mutex);
  }

  ret = m_scn.load();

  if (!mutex_hold) {
    mutex_exit(&m_mutex);
  }
  return ret;
}

/** Get current scn which is committed.
@param[in]  true if m_mutex is hold
@return     m_scn */
gcn_t SCN::acquire_gcn(bool mutex_hold) {
  gcn_t ret;
  if (!mutex_hold) {
    mutex_enter(&m_mutex);
  }

  if (srv_snapshot_update_gcn)
    ret = m_gcn > m_snapshot_gcn ? m_gcn.load() : m_snapshot_gcn.load();
  else 
    ret = m_gcn.load();

  if (!mutex_hold) {
    mutex_exit(&m_mutex);
  }
  return ret;
}

scn_t SCN::get_scn() { return m_scn.load(); }

gcn_t SCN::get_gcn() { return m_gcn.load(); }

void SCN::set_snapshot_gcn(gcn_t gcn, bool mutex_hold) { 

  if(gcn == GCN_NULL || gcn == GCN_INITIAL) 
    return;

  if (!mutex_hold) {
    mutex_enter(&m_mutex);
  }

  if( gcn > m_snapshot_gcn )
    m_snapshot_gcn = gcn;

  if (!mutex_hold) {
    mutex_exit(&m_mutex);
  }
}

gcn_t SCN::get_snapshot_gcn() { return m_snapshot_gcn.load(); }

/**
  Check the commit scn state

  @param[in]    scn       commit scn
  @return       scn state SCN_STATE_INITIAL, SCN_STATE_ALLOCATED or
                          SCN_STATE_INVALID
*/
enum scn_state_t commit_scn_state(const commit_scn_t &cmmt) {
  /** The init value */
  if (cmmt.scn == SCN_NULL && cmmt.utc == UTC_NULL)
    return SCN_STATE_INITIAL;

  /** The assigned commit scn value */
  if (cmmt.scn > 0 && cmmt.scn < SCN_MAX && cmmt.utc > 0 &&
      cmmt.utc < UTC_MAX && cmmt.gcn > 0 && cmmt.gcn < GCN_MAX) {
    /** TODO: Replace by real GCN in future */
    return SCN_STATE_ALLOCATED;
  }
  return SCN_STATE_INVALID;
}

}  // namespace lizard
