/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file trx/lizard0scn.cc
 Lizard scn number implementation.

 Created 2020-03-24 by Jianwei.zhao
 *******************************************************/

#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0types.h"

#include "lizard0ut.h"
#include "lizard0scn.h"
#include "lizard0gcs.h"

namespace lizard {

/** Write scn value into tablespace.

    @param[in]	metadata   scn metadata
 */
void ScnPersister::write(const PersistentGcsData *metadata) {
  ut_ad(metadata->get_scn() != SCN_NULL);

  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + GCS_DATA_SCN, metadata->get_scn(), &mtr);
  mtr_commit(&mtr);
}

/** Write scn value redo log.

    @param[in]	metadata   scn metadata
    @param[in]	mini transacton context

    @TODO
 */
void ScnPersister::write_log(const PersistentGcsData *, mtr_t *) { ut_a(0); }

/** Read scn value from tablespace and increase GCS_SCN_NUMBER_MAGIN
    to promise unique.

    @param[in/out]	metadata   scn metadata

 */
void ScnPersister::read(PersistentGcsData *metadata) {
  scn_t scn = SCN_NULL;
  gcs_sysf_t *hdr;
  mtr_t mtr;
  mtr.start();

  hdr = gcs_sysf_get(&mtr);

  scn = 2 * GCS_SCN_NUMBER_MAGIN +
        ut_uint64_align_up(mach_read_from_8(hdr + GCS_DATA_SCN),
                           GCS_SCN_NUMBER_MAGIN);

  ut_a(scn > 0 && scn < SCN_NULL);
  mtr.commit();

  metadata->set_scn(scn);
}

/**------------------------------------------------------------------------*/
/** GCN */
/**------------------------------------------------------------------------*/
/** Write gcn value into tablespace.

    @param[in]	metadata   gcn metadata
 */
void GcnPersister::write(const PersistentGcsData *metadata) {
  gcs_sysf_t *hdr;
  mtr_t mtr;

  ut_ad(metadata->get_gcn() != GCN_NULL);

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + GCS_DATA_GCN, metadata->get_gcn(), &mtr);
  mtr_commit(&mtr);
}

void GcnPersister::write_log(const PersistentGcsData *, mtr_t *) { ut_a(0); }

/** Read gcn value from tablespace

    @param[in/out]	metadata   gcn metadata

 */
void GcnPersister::read(PersistentGcsData *metadata) {
  gcn_t gcn = GCN_NULL;
  gcs_sysf_t *hdr;
  mtr_t mtr;
  mtr.start();

  hdr = gcs_sysf_get(&mtr);
  gcn = mach_read_from_8(hdr + GCS_DATA_GCN);

  mtr.commit();

  ut_a(gcn > 0 && gcn < GCN_NULL);
  metadata->set_gcn(gcn);
}

/** Constructor of SCN */
SCN::SCN() : m_scn(SCN_NULL), m_inited(false) {}

/** Destructor of SCN */
SCN::~SCN() { m_inited = false; }

/** Assign the init value by reading from tablespace */
void SCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_scn == SCN_NULL);

  PersistentGcsData meta;

  gcs->persisters.scn_persister()->read(&meta);
  m_scn = meta.get_scn();
  ut_a(m_scn > 0 && m_scn < SCN_NULL);

  m_inited = true;
  return;
}

/** Calucalte a new scn number
@return     scn */
scn_t SCN::new_scn() {
  scn_t num;
  ut_ad(m_inited);

  ut_ad(scn_list_mutex_own());

  /** flush scn every magin */
  if (!(m_scn % GCS_SCN_NUMBER_MAGIN)) {
    PersistentGcsData meta;
    meta.set_scn(m_scn.load());
    gcs->persisters.scn_persister()->write(&meta);
  }

  num = ++m_scn;

  ut_a(num > 0 && num < SCN_NULL);

  return num;
}

/** GCN constructor. */
GCN::GCN()
    : m_gcn(GCN_NULL),
      m_snapshot_gcn(GCN_NULL),
      m_persisted_gcn(GCN_NULL),
      m_inited(false) {}

/** Boot GCN module, read gcn value from tablespace,
 */
void GCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_gcn.load() == GCN_NULL);

  PersistentGcsData meta;
  gcs->persisters.gcn_persister()->read(&meta);

  m_gcn.store(meta.get_gcn());
  m_snapshot_gcn.store(meta.get_gcn());
  m_persisted_gcn.store(meta.get_gcn());

  ut_ad(m_gcn >= GCN_INITIAL && m_gcn < GCN_NULL);
  ut_ad(m_snapshot_gcn >= GCN_INITIAL && m_snapshot_gcn < GCN_NULL);
  ut_ad(m_persisted_gcn >= GCN_INITIAL && m_persisted_gcn < GCN_NULL);

  m_inited = true;
  return;
}

/** Flush the global commit number to system tablepace */
void GCN::flush_gcn(gcn_t gcn) {
  ut_ad(m_inited);
  ut_ad(gcn != GCN_NULL);
  ut_ad(gcn_order_mutex_own());

  PersistentGcsData meta;
  meta.set_gcn(gcn);
  gcs->persisters.gcn_persister()->write(&meta);
}

gcn_t GCN::new_gcn(gcn_t gcn) {
  gcn_t cmmt = GCN_NULL;
  ut_ad(gcn_order_mutex_own());

  if (gcn != GCN_NULL)
    cmmt = gcn;
  else
    cmmt = acquire_gcn();

  if (cmmt > m_gcn) {
    m_gcn = cmmt;
  }
  if (cmmt > m_persisted_gcn) {
    m_persisted_gcn = cmmt;
    flush_gcn(cmmt);
  }
  return cmmt;
}

/** Acquire bigger gcn between gcn and snapshot_gcn
@return     gcn */
gcn_t GCN::acquire_gcn() {
  gcn_t ret;
  ut_ad(gcn_order_mutex_own());

  ret = m_gcn > m_snapshot_gcn ? m_gcn.load() : m_snapshot_gcn.load();

  return ret;
}

void GCN::set_snapshot_gcn_if_bigger(gcn_t gcn) {
  ut_ad(!gcn_order_mutex_own());

  if(gcn == GCN_NULL || gcn == GCN_INITIAL) 
    return;

  gcn_order_mutex_enter();
  if (gcn > m_snapshot_gcn.load()) m_snapshot_gcn.store(gcn);
  gcn_order_mutex_exit();
}

/**
  Check the commit scn state

  @param[in]    scn       commit scn
  @return       scn state SCN_STATE_INITIAL, SCN_STATE_ALLOCATED or
                          SCN_STATE_INVALID
*/
enum scn_state_t commit_scn_state(const commit_scn_t &cmmt) {
  /** The init value */
  if (cmmt.scn == SCN_NULL && cmmt.utc == UTC_NULL && cmmt.gcn == GCN_NULL)
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
