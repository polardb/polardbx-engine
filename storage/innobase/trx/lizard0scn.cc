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

#include "lizard0gcs.h"
#include "lizard0scn.h"
#include "lizard0ut.h"

#include "sql/lizard/lizard_rpl_gcn.h"  // MyGCN...

namespace lizard {

/**
   Writes a log record about gcn rise.

   @param[in]		type	redo log record type
   @param[in,out]	log_ptr		current end of mini-transaction log
   @param[in/out]	mtr	mini-transaction

   @reval	end of mtr
 */
byte *mlog_write_initial_gcn_log_record(mlog_id_t type, byte *log_ptr,
                                        mtr_t *mtr) {
  ut_ad(type <= MLOG_BIGGEST_TYPE);
  ut_ad(type == MLOG_GCN_METADATA);

  mach_write_to_1(log_ptr, type);
  log_ptr++;

  mtr->added_rec();
  return (log_ptr);
}

/**
   Parses an initial log record written by mlog_write_initial_gcn_log_record.

   @param[in]	ptr		buffer
   @param[in]	end_ptr		buffer end
   @param[out]	type		log record type, should be
                                MLOG_GCN_METADATA
   @param[out]	gcn

   @return parsed record end, NULL if not a complete record
 */
byte *mlog_parse_initial_gcn_log_record(const byte *ptr, const byte *end_ptr,
                                        mlog_id_t *type, gcn_t *gcn) {
  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  *type = (mlog_id_t)((ulint)*ptr & ~MLOG_SINGLE_REC_FLAG);
  ut_ad(*type == MLOG_GCN_METADATA);

  ptr++;

  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  *gcn = mach_parse_u64_much_compressed(&ptr, end_ptr);

  return (const_cast<byte *>(ptr));
}

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

void GcnPersister::write_log(const PersistentGcsData *metadata, mtr_t *mtr) {
  ut_ad(metadata->get_gcn() != GCN_NULL);

  byte *log_ptr;

  /** 1 byte for MLOG_GCN_METADATA, 1...11 for gcn value. */
  static constexpr uint8_t write_size = 1 + 11;

  if (!mlog_open(mtr, write_size, log_ptr)) {
    /** Maybe redo disabled. */
    ut_ad(log_ptr == nullptr);
    return;
  }

  ut_ad(log_ptr != nullptr);

  log_ptr = mlog_write_initial_gcn_log_record(MLOG_GCN_METADATA, log_ptr, mtr);

  ulint consumed = mach_u64_write_much_compressed(log_ptr, metadata->get_gcn());

  log_ptr += consumed;

  mlog_close(mtr, log_ptr);
}

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
SCN::SCN(Persister *persister)
    : m_scn(SCN_NULL), m_persister(persister), m_inited(false) {}

/** Destructor of SCN */
SCN::~SCN() { m_inited = false; }

/** Assign the init value by reading from tablespace */
void SCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_scn == SCN_NULL);

  PersistentGcsData meta;

  m_persister->read(&meta);
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
    m_persister->write(&meta);
  }

  num = ++m_scn;

  ut_a(num > 0 && num < SCN_NULL);

  return num;
}

/** GCN constructor. */
GCN::GCN(Persister *persister)
    : m_gcn(GCN_NULL), m_persister(persister), m_inited(false) {}

/** Boot GCN module, read gcn value from tablespace,
 */
void GCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_gcn.load() == GCN_NULL);

  PersistentGcsData meta;
  m_persister->read(&meta);

  m_gcn.store(meta.get_gcn());

  ut_ad(m_gcn >= GCN_INITIAL && m_gcn < GCN_NULL);

  m_inited = true;
  return;
}

/**
  Prepare a gcn number according to source type when
  commit.

  2) GSR_AUTOMATIC
    -- Generate from current gcn early, use it directly.
    -- Use current gcn as commit gcn.
  3) GSR_ASSIGNED
    -- Come from 3-party component, use it directly,
       increase current gcn if bigger.

  @param[in]	gcn
  @param[in]	csr
  @param[in]	mini transaction

  @retval	gcn
*/
std::pair<gcn_t, csr_t> GCN::new_gcn(const gcn_t gcn, const csr_t csr,
                                     mtr_t *mtr) {
  gcn_t cmmt = GCN_NULL;
  ut_ad(!gcn_order_mutex_own());

  switch (csr) {
    case CSR_AUTOMATIC:
      /** 1. generate when binlog commit.
       *  2. didn't generate until now. */
      cmmt = (gcn == GCN_NULL ? m_gcn.load() : gcn);
      break;
    case CSR_ASSIGNED:
      ut_ad(gcn != GCN_NULL);
      cmmt = gcn;
      set_gcn_if_bigger(cmmt);
      break;
  }

  ut_ad(cmmt != GCN_NULL);

  PersistentGcsData meta;
  meta.set_gcn(cmmt);
  m_persister->write_log(&meta, mtr);
  return std::make_pair(cmmt, csr);
}

/**
   Push up current gcn if bigger.

   @param[in]	gcn
*/
void GCN::set_gcn_if_bigger(const gcn_t gcn) {
  ut_ad(!gcn_order_mutex_own());

  if (gcn == GCN_NULL) return;

  if (gcn > m_gcn.load()) {
    gcn_order_mutex_enter();

    if (gcn > m_gcn.load()) {
      m_gcn.store(gcn);
    }

    gcn_order_mutex_exit();
  }
}

/**
  Check the commit scn state

  @param[in]    scn       commit scn
  @return       scn state SCN_STATE_INITIAL, SCN_STATE_ALLOCATED or
                          SCN_STATE_INVALID
*/
enum scn_state_t commit_mark_state(const commit_mark_t &cmmt) {
  /** The init value */
  if (cmmt.scn == SCN_NULL && cmmt.us == US_NULL) return SCN_STATE_INITIAL;

  /** The assigned commit scn value */
  if (cmmt.scn > 0 && cmmt.scn <= SCN_MAX && cmmt.us > 0 && cmmt.us <= US_MAX &&
      cmmt.gcn > 0 && cmmt.gcn <= GCN_MAX) {
    /** TODO: Replace by real GCN in future */
    return SCN_STATE_ALLOCATED;
  }
  return SCN_STATE_INVALID;
}

}  // namespace lizard

/*****************************************
 *              commit_scn_t             *
 *****************************************/
void commit_mark_t::copy_from_my_gcn(const MyGCN *my_gcn) {
  if (!my_gcn->is_empty()) {
    gcn = my_gcn->get_gcn();
    csr = my_gcn->is_assigned() ? CSR_ASSIGNED : CSR_AUTOMATIC;
  } else {
    /** Automatic GCN. */
    gcn = lizard::GCN_NULL;
    csr = CSR_AUTOMATIC;
  }
}

void commit_mark_t::copy_to_my_gcn(MyGCN *my_gcn) {
  my_csr_t my_csr = my_csr_t::MYSQL_CSR_NONE;

  if (csr == csr_t::CSR_AUTOMATIC) {
    my_csr = my_csr_t::MYSQL_CSR_AUTOMATIC;
  } else {
    my_csr = my_csr_t::MYSQL_CSR_ASSIGNED;
  }

  my_gcn->set(gcn, my_csr);
}
