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

/** @file trx/lizard0gcs0gcn.cc
 Global Commit Number implementation.

 Created 2020-03-24 by Jianwei.zhao
 *******************************************************/

#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0types.h"

#include "lizard0gcs0gcn.h"
#include "lizard0gcs.h"


#ifdef UNIV_PFS_MUTEX
/* GCS gcn order mutex PFS key */
mysql_pfs_key_t gcn_order_mutex_key;
/* GCS gcn persist mutex PFS key */
mysql_pfs_key_t gcn_persist_mutex_key;

#endif

namespace lizard {
/**
   Writes a log record about gcn rise.

   @param[in]		type	redo log record type
   @param[in,out]	log_ptr		current end of mini-transaction log
   @param[in/out]	mtr	mini-transaction

   @reval	end of mtr
 */
byte *GCN::GcnPersister::mlog_write_initial_gcn_log_record(mlog_id_t type,
                                                           byte *log_ptr,
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
byte *GCN::GcnPersister::mlog_parse_initial_gcn_log_record(const byte *ptr,
                                                           const byte *end_ptr,
                                                           mlog_id_t *type,
                                                           meta_t *meta) {
  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  *type = (mlog_id_t)((ulint)*ptr & ~MLOG_SINGLE_REC_FLAG);
  ut_ad(*type == MLOG_GCN_METADATA);

  ptr++;

  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  meta->gcn = mach_parse_u64_much_compressed(&ptr, end_ptr);

  return (const_cast<byte *>(ptr));
}

/**------------------------------------------------------------------------*/
/** GCN */
/**------------------------------------------------------------------------*/
/** Write gcn value into tablespace.

    @param[in]	metadata   gcn metadata
 */
void GCN::GcnPersister::write(const meta_t *meta) {
  gcs_sysf_t *hdr;
  mtr_t mtr;

  ut_ad(meta->valid());

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + GCS_DATA_GCN, meta->gcn, &mtr);
  mtr_commit(&mtr);
}

/** Write gcn value redo log.

    @param[in]	metadata   gcn metadata
    @param[in]	mini transacton context
 */
void GCN::GcnPersister::write_log(const meta_t *meta, mtr_t *mtr) {
  ut_ad(meta->valid());

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

  ulint consumed = mach_u64_write_much_compressed(log_ptr, meta->gcn);

  log_ptr += consumed;

  mlog_close(mtr, log_ptr);
}

/** Read gcn value from tablespace

    @param[in/out]	metadata   gcn metadata

 */
void GCN::GcnPersister::read(meta_t *meta) {
  gcn_t gcn = GCN_NULL;
  gcs_sysf_t *hdr;
  mtr_t mtr;
  mtr.start();

  hdr = gcs_sysf_get(&mtr);
  gcn = mach_read_from_8(hdr + GCS_DATA_GCN);

  mtr.commit();

  meta->gcn = gcn;
  ut_a(meta->valid());
}

/** GCN constructor. */
GCN::GCN() : m_gcn(0), m_persister(), m_persisted_gcn(0), m_inited(false) {
  mutex_create(LATCH_ID_GCN_ORDER, &m_order_mutex);
  mutex_create(LATCH_ID_GCN_PERSIST, &m_persist_mutex);
}

GCN::~GCN() {
  mutex_free(&m_order_mutex);
  mutex_free(&m_persist_mutex);
  m_inited = false;
}

/** Boot GCN module, read gcn value from tablespace */
void GCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_gcn.load() == 0);

  meta_t meta;
  m_persister.read(&meta);

  m_gcn.store(meta.gcn);
  m_persisted_gcn.store(meta.gcn);

  ut_ad(m_gcn >= GCN_INSTALL_VALUE && m_gcn <= GCN_MAX);

  lizard_system(ER_LIZARD) << "GCS Read GCN: " << m_persisted_gcn.load();

  m_inited = true;
  return;
}

/**
  Prepare a gcn number according to source type when
  commit.

  1) GSR_AUTOMATIC
    -- Generate from current gcn early, use it directly.
    -- Use current gcn as commit gcn.
  2) GSR_ASSIGNED
    -- Come from 3-party component, use it directly,
       increase current gcn if bigger.

  @param[in]	gcn
  @param[in]	csr
  @param[in]	mini transaction

  @retval	gcn
*/
gcn_tuple_t GCN::new_gcn(const gcn_tuple_t &tuple, mtr_t *mtr) {
  gcn_t cmmt = GCN_NULL;
  ut_ad(!order_mutex_own());

  switch (tuple.csr) {
    case CSR_AUTOMATIC:
      /**
        1. GCN come from local SYS_GCN. For example signle-shard transaction.
        2. GCN come from other SYS_GCN of other DN. For example Asycn Commit
        transaction.
      */
      if (tuple.gcn == GCN_NULL) {
        cmmt = m_gcn.load();
      } else {
        /** Must already pushed up */
        ut_ad(tuple.gcn <= m_gcn.load());
        cmmt = tuple.gcn;
      }
      break;
    case CSR_ASSIGNED:
      ut_ad(tuple.gcn != GCN_NULL);
      ut_ad(tuple.gcn <= m_gcn.load());
      cmmt = tuple.gcn;
      break;
  }

  ut_ad(cmmt != GCN_NULL);

  meta_t meta;
  meta.gcn = cmmt;
  m_persister.write_log(&meta, mtr);

  return {cmmt, tuple.csr};
}

/** Generate a new proposal gcn when xa prepare in_tc according to user xa
 * proposal, then assign to trx->txn_desc.pmmt and will be written to undo
 * header.
 *
 * @param[in]		user proposal number
 * @param[in]		transaction
 * @param[in]		prepare_in_tc mini-transaction
 *
 * @retval		Proposed number */
gcn_tuple_t GCN::new_proposal_gcn(const proposal_mark_t &pmmt, trx_t *trx,
                                  mtr_t *mtr) {
  gcn_tuple_t gtuple = new_gcn({pmmt.gcn, pmmt.csr}, mtr);

  trx->txn_desc.pmmt = {gtuple.gcn, gtuple.csr};

  return gtuple;
}

/** Generate a new commit gcn when commit according to user commit sequence if
 * has, then assign to trx->txn_desc.cmmt and will be written to undo header.
 *
 * @param[in]		user commit sequence
 * @param[in]		transaction
 * @param[in]		commit mini-transaction
 *
 * @retval		commit number */
gcn_tuple_t GCN::new_commit_gcn(const gcn_tuple_t &tuple, trx_t *trx,
                                mtr_t *mtr) {
  gcn_tuple_t gtuple = new_gcn(tuple, mtr);
  /**Assign txn_desc. */
  trx->txn_desc.cmmt.gcn = gtuple.gcn;
  trx->txn_desc.cmmt.csr = gtuple.csr;

  return gtuple;
}

/**
   Push up current gcn if bigger when recovery.

   @param[in]	gcn
*/
void GCN::recover_gcn_if_bigger(const meta_t &meta) {
  ut_a(meta.valid());

  if (meta.gcn > m_gcn.load()) {
    m_gcn.store(meta.gcn);
  }
}

/**
   Push up current gcn if bigger.

   @param[in]	gcn
*/
void GCN::set_gcn_if_bigger(const gcn_t value) {
  ut_ad(!order_mutex_own());

  if (value == GCN_NULL) return;

  if (value > m_gcn.load()) {
    mutex_enter(&m_order_mutex);

    if (value > m_gcn.load()) {
      m_gcn.store(value);
    }

    mutex_exit(&m_order_mutex);
  }
}

/**
 * Persist gcn if current gcn > persisted gcn.
 *
 * @retval	true	if written */
bool GCN::persist_gcn() {
  bool written = false;

  mutex_enter(&m_persist_mutex);

  gcn_t current_gcn = m_gcn.load();
  if (current_gcn > m_persisted_gcn.load()) {
    /** Recover must be completed. */
    ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());

    m_persisted_gcn.store(current_gcn);

    meta_t meta;
    meta.gcn = m_persisted_gcn.load();
    m_persister.write(&meta);

    written = true;
    if (srv_gcs_print_process_debug) {
      lizard_info(ER_LIZARD) << "GCS persist GCN: " << current_gcn;
    }
  }

  mutex_exit(&m_persist_mutex);

  return written;
}

}  // namespace lizard
