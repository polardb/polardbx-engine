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

/** @file trx/lizard0gcs0scn.cc
 Lizard scn number implementation.

 Created 2020-03-24 by Jianwei.zhao
 *******************************************************/

#include "mtr0log.h"
#include "mtr0mtr.h"
#include "sync0types.h"
#include "ut0new.h"

#include "lizard0gcs.h"
#include "lizard0gcs0scn.h"

#ifdef UNIV_PFS_MUTEX
/* GCS scn list mutex PFS key */
mysql_pfs_key_t scn_list_mutex_key;
/* GCS scn persist mutex PFS key */
mysql_pfs_key_t scn_persist_mutex_key;

#endif

namespace lizard {

uint32_t srv_gcs_scn_list_shards = 4;

bool dbug_gcs_enable_traverse_scn_list = false;

/**
   Parses an initial log record written by mlog_write_initial_scn_log_record.

   @param[in]	ptr		buffer
   @param[in]	end_ptr		buffer end
   @param[out]	type		log record type, should be
                                MLOG_scn_METADATA
   @param[out]	scn

   @return parsed record end, NULL if not a complete record
 */
byte *SCN::ScnPersister::mlog_parse_initial_scn_log_record(const byte *ptr,
                                                           const byte *end_ptr,
                                                           mlog_id_t *type,
                                                           meta_t *meta) {
  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  *type = (mlog_id_t)((ulint)*ptr & ~MLOG_SINGLE_REC_FLAG);
  ut_ad(*type == MLOG_SCN_METADATA);

  ptr++;

  if (end_ptr < ptr + 1) {
    return nullptr;
  }

  meta->scn = mach_parse_u64_much_compressed(&ptr, end_ptr);

  return (const_cast<byte *>(ptr));
}

/**
   Writes a log record about scn rise.

   @param[in]		type	redo log record type
   @param[in,out]	log_ptr		current end of mini-transaction log
   @param[in/out]	mtr	mini-transaction

   @reval	end of mtr
 */
byte *SCN::ScnPersister::mlog_write_initial_scn_log_record(mlog_id_t type,
                                                           byte *log_ptr,
                                                           mtr_t *mtr) {
  ut_ad(type <= MLOG_BIGGEST_TYPE);
  ut_ad(type == MLOG_SCN_METADATA);

  mach_write_to_1(log_ptr, type);
  log_ptr++;

  mtr->added_rec();
  return (log_ptr);
}

/** Write scn value into tablespace.

    @param[in]	metadata   scn metadata
 */
void SCN::ScnPersister::write(const meta_t *meta) {
  ut_ad(meta->valid());

  gcs_sysf_t *hdr;
  mtr_t mtr;

  mtr_start(&mtr);
  hdr = gcs_sysf_get(&mtr);
  mlog_write_ull(hdr + GCS_DATA_SCN, meta->scn, &mtr);
  mtr_commit(&mtr);
}

/** Write scn value redo log.

    @param[in]	metadata   scn metadata
    @param[in]	mini transacton context

 */
void SCN::ScnPersister::write_log(const meta_t *meta, mtr_t *mtr) {
  ut_ad(meta->valid());

  byte *log_ptr;

  /** 1 byte for MLOG_SCN_METADATA, 1...11 for scn value. */
  static constexpr uint8_t write_size = 1 + 11;

  if (!mlog_open(mtr, write_size, log_ptr)) {
    /** Maybe redo disabled. */
    ut_ad(log_ptr == nullptr);
    return;
  }

  ut_ad(log_ptr != nullptr);

  log_ptr = mlog_write_initial_scn_log_record(MLOG_SCN_METADATA, log_ptr, mtr);

  ulint consumed = mach_u64_write_much_compressed(log_ptr, meta->scn);

  log_ptr += consumed;

  mlog_close(mtr, log_ptr);
}

/** Read scn value from tablespace and increase GCS_SCN_NUMBER_MAGIN
    to promise unique.

    @param[in/out]	metadata   scn metadata

 */
void SCN::ScnPersister::read(meta_t *meta) {
  scn_t scn = SCN_NULL;
  gcs_sysf_t *hdr;
  mtr_t mtr;
  mtr.start();

  hdr = gcs_sysf_get(&mtr);

  scn = 2 * SCN_NUMBER_MAGIN +
        ut_uint64_align_up(mach_read_from_8(hdr + GCS_DATA_SCN),
                           SCN_NUMBER_MAGIN);

  mtr.commit();

  meta->scn = scn;
  ut_a(meta->valid());
}

/** Constructor of SCN */
SCN::SCN()
    : m_scn(0),
      m_lists(nullptr),
      m_persister(),
      m_persisted_scn(0),
      m_shards(srv_gcs_scn_list_shards),
      m_inited(false) {
  m_lists = ut::new_arr_withkey<ut::Cacheline_padded<scn_list_t>>(
      UT_NEW_THIS_FILE_PSI_KEY, ut::Count{m_shards});

  // 初始化所有列表
  for (size_t i = 0; i < m_shards; i++) {
    m_lists[i].init(this);
  }

  mutex_create(LATCH_ID_SCN_PERSIST, &m_persist_mutex);
}

/** Destructor of SCN */
SCN::~SCN() {
  for (size_t i = 0; i < m_shards; i++) {
    m_lists[i].destroy();
  }

  ut::delete_arr(m_lists);

  mutex_free(&m_persist_mutex);
  m_inited = false;
}

/** Assign the init value by reading from tablespace */
void SCN::boot() {
  ut_ad(!m_inited);
  ut_ad(m_scn == 0);

  meta_t meta;
  m_persister.read(&meta);

  m_scn.store(meta.scn);
  m_persisted_scn.store(meta.scn);

  ut_a(m_scn > 0 && m_scn <= SCN_MAX);
  lizard_system(ER_LIZARD) << "GCS Read SCN: " << m_persisted_scn.load();
  m_inited = true;
  return;
}

/** Remove trx from list and modify min active scn if necessary. */
void SCN::scn_list_t::remove_list(trx_t *trx) {
  ut_ad(trx->id > 0);
  ut_ad(UT_LIST_GET_LEN(m_list) > 0);
  ut_ad(lock_own());

  UT_LIST_REMOVE(m_list, trx);

  auto min_active_trx = UT_LIST_GET_LAST(m_list);

  scn_t min_scn = min_active_trx == nullptr ? scn_mgr->load_next_scn()
                                            : min_active_trx->txn_desc.cmmt.scn;
  m_min_active_scn.store(min_scn);

  ut_a(min_scn != SCN_NULL);
  ut_ad(check_min_active_scn());
  return;
}

// Get min active scn by traversing list with lock.
scn_t SCN::scn_list_t::traverse_min_active_scn() const {
  ut_ad(lock_own());

  auto min_active_trx = UT_LIST_GET_LAST(m_list);
  scn_t min_scn = min_active_trx == nullptr ? scn_mgr->load_next_scn()
                                            : min_active_trx->txn_desc.cmmt.scn;
  return min_scn;
}

/** Check min active scn is valid. */
bool SCN::scn_list_t::check_min_active_scn() const {
  scn_t next_scn;
  trx_t *trx = nullptr;
  trx_t *prev_trx = nullptr;
  ut_ad(lock_own());

  next_scn = scn_mgr->load_next_scn();

  if (UT_LIST_GET_LEN(m_list) == 0) {
    ut_a(m_min_active_scn.load() <= next_scn);
  } else {
    trx = UT_LIST_GET_FIRST(m_list);
    ut_a(trx);
    ut_a(m_min_active_scn.load() <= trx->txn_desc.cmmt.scn);
    ut_a(m_min_active_scn <= next_scn);

    /** trx in serialisation_list_scn are ordered by scn */
    prev_trx = trx;
    for (trx = UT_LIST_GET_NEXT(scn_list, trx); trx != nullptr;
         trx = UT_LIST_GET_NEXT(scn_list, trx)) {
      ut_a(m_min_active_scn.load() <= trx->txn_desc.cmmt.scn);
      ut_a(prev_trx->txn_desc.cmmt.scn > trx->txn_desc.cmmt.scn);
      prev_trx = trx;
    }
  }
  return true;
}

/** Cleanup trx after commit in memory. */
void SCN::erase(trx_t *trx) {
  ut_ad(m_inited);
  ut_ad(trx->id > 0);
  ut_ad(trx != NULL && trx->state == TRX_STATE_COMMITTED_IN_MEMORY);
  ut_ad(trx->txn_desc.is_whole_committed());

  // Remove from scn list.
  latch_and_execute_list(map2shard(trx), [&trx](scn_list_t *list) -> bool {
    list->remove_list(trx);
    return false;
  });
}
/** Judge trx is still active by txn_desc.cmmt.scn.
 *  Attention: trx must be referenced or lock by trx_sys->shard.
 *
 *  @param[in]		trx
 *
 *  @revtal	true	mtr didn't commit
 * */
bool SCN::is_slot_active(const trx_t *trx) {
  bool active = false;
  ut_ad(trx->id > 0);
  latch_and_execute_list(map2shard(trx), [&](scn_list_t *list) -> bool {
    active = trx->txn_desc.cmmt.scn == SCN_NULL;
    return false;
  });

  return active;
}

/** Generate a new scn number for transaction commit.
 *
 * @param[in/out]	trx	rw transaction.
 * @param[in/out]	mtr	commit mini transaction.
 *
 * @retval	scn */
scn_t SCN::new_scn(trx_t *trx, mtr_t *mtr) {
  scn_t num = SCN_NULL;
  ut_ad(m_inited);
  ut_ad(trx->id > 0);
  ut_ad(trx->txn_desc.cmmt.scn == SCN_NULL);

  // Generate new scn and add into list.
  latch_and_execute_list(map2shard(trx), [&](scn_list_t *list) -> bool {
    num = ++m_scn;
    /** Assign txn_desc. */
    trx->txn_desc.cmmt.scn = num;

    list->add_list(trx);
    return false;
  });

  ut_ad(num != SCN_NULL);

  /** Write scn redo log. */
  meta_t meta;
  meta.scn = num;
  m_persister.write_log(&meta, mtr);

  ut_a(num > 0 && num <= SCN_MAX);
  return num;
}

/** Generate a unique scn number lonely */
scn_t SCN::new_scn() {
  scn_t num;
  ut_ad(m_inited);

  mtr_t mtr;
  mtr_start(&mtr);

  num = ++m_scn;

  meta_t meta;
  meta.scn = num;
  m_persister.write_log(&meta, &mtr);

  mtr_commit(&mtr);

  ut_a(meta.valid());
  return num;
}
/**
 * Persist scn if current scn > persisted scn.
 *
 * @retval	true	if written */
bool SCN::persist_scn() {
  bool written = false;
  scn_t current_scn;

  mutex_enter(&m_persist_mutex);

  current_scn = m_scn.load();
  if (current_scn > m_persisted_scn.load()) {
    /** Recover must be completed. */
    ut_ad(!recv_sys || !recv_sys->cn_recover->is_need_recovery());

    m_persisted_scn.store(current_scn);

    meta_t meta;
    meta.scn = m_persisted_scn.load();
    m_persister.write(&meta);

    written = true;

    if (srv_gcs_print_process_debug) {
      lizard_info(ER_LIZARD) << "GCS persist SCN: " << current_scn;
    }
  }

  mutex_exit(&m_persist_mutex);

  return written;
}

/**
   Push up current scn if bigger when recovery.

   @param[in]	scn
*/
void SCN::recover_scn_if_bigger(const meta_t &meta) {
  ut_a(meta.valid());

  if (meta.scn > m_scn.load()) {
    m_scn.store(meta.scn);
  }
}



}  // namespace lizard
