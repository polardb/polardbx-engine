/*****************************************************************************

Copyright (c) 2013, 2021, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file trx/lizard0xa.cc
  Lizard XA transaction structure.

 Created 2021-08-10 by Jianwei.zhao
 *******************************************************/

#include "ha_innodb.h"
#include "trx0sys.h"

#include "m_ctype.h"
#include "mysql/plugin.h"
#include "sql/sql_class.h"
#include "sql/xa.h"
#include "sql/sql_class.h"
#include "sql/sql_plugin_var.h"

#include "lizard0gcs.h"
#include "lizard0ha_innodb.h"
#include "lizard0read0types.h"
#include "lizard0undo.h"
#include "lizard0ut.h"
#include "lizard0xa.h"
#include "lizard0xa0types.h"

/** @{ */

/**
  check if the xid match the format v1.

  Requirement:
  1) Buqal length must be greater than XID_GROUP_SUFFIX_SIZE
  2) Split char must be right.
  3) The suffix must be a numober except split char

  @param[in]  xid   xid to be checked
  @return true if match, otherwise false
*/
bool xa_desc_t::check_if_match_format_v1(const XID *xid) {
  if (xid->is_null()) return false;

  int prefix_len =
      xid->gtrid_length + xid->bqual_length - XID_GROUP_SUFFIX_SIZE_V1;

  if (xid->bqual_length <= XID_GROUP_SUFFIX_SIZE_V1 ||
      xid->data[prefix_len] != XID_GROUP_SPLIT_CHAR_V1) {
    return false;
  }
  for (unsigned int i = 1; i < XID_GROUP_SUFFIX_SIZE_V1; i++) {
    if (!my_isdigit(&my_charset_latin1, xid->data[prefix_len + i])) {
      return false;
    }
  }
  return true;
}
/** @} */

/**
 * Before building the group id in format v1, we must check if the xid meet
 * requirements (call @check_if_match_format_v1). The trxs that meet the
 * following requirements are divided into a group in format v1:
 *
 * 1) gtrid must be equal
 * 2) bqual prefix must be equal
 * 3) formatID must be equal
 *
 * So we append bqual prefix and formatID to xa_desc_t::m_gid besides gtrid,
 * which is quite different from format v2. To specify the version, format
 * version is also appended.
 *
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid_v1() {
  ut_ad(m_gid.empty());

  /** No need to build the group id. */
  if (!check_if_match_format_v1(&m_xid)) {
    return false;
  }

  auto formatID = std::to_string(m_xid.get_format_id());

  int length = m_xid.get_gtrid_length() + m_xid.get_bqual_length() -
               XID_GROUP_SUFFIX_SIZE_V1 + formatID.size() + sizeof(FORMAT_V1) -
               1;

  m_gid.reserve(length);

  /** 1. append gtrid and bqual prefix */
  m_gid.append(m_xid.get_data(), length);

  /** 2. append formatID */
  m_gid.append(formatID);

  /** 3. append format version */
  m_gid.append(FORMAT_V1, sizeof(FORMAT_V1) - 1);

  return true;
}

/**
 * Build the group id in format v2. The trxs that meet the following
 * Requirements are divided into a group in format v2:
 *
 * 1) gtrid must be equal
 *
 * So we append gtrid to xa_desc_t::m_gid. To specify the version, format
 * version is also appended.
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid_v2() {
  ut_ad(m_gid.empty());
  m_gid.reserve(m_xid.get_gtrid_length() + sizeof(FORMAT_V2) - 1);

  /** 1. append gtrid */
  m_gid.append(m_xid.get_data(), m_xid.get_gtrid_length());

  /** 2. append format version */

  m_gid.append(FORMAT_V2, sizeof(FORMAT_V2) - 1);

  return true;
}

/**
 * Build the group id. For 0-FORMAT_V1_RANGE, use the format v1.
 * Otherwise, use the format v2.
 *
 * @return true if the group id is built successfully, otherwise false
 */
bool xa_desc_t::build_gid() {
  ut_ad(!m_xid.is_null());
  ut_ad(m_group == nullptr);

  ut_ad(m_xid.get_format_id() >= 0);

  if (m_xid.get_format_id() <= FORMAT_V1_RANGE) {
    return build_gid_v1();
  } else {
    return build_gid_v2();
  }
}

namespace lizard {

/**
 * Release the reference of the Xa Group for a given trx. Remove
 * it from trx_sys->xa_group_shards when the reference count is 0.
 * do nothing if xa_desc.is_group_null(), cause that the xa group might
 * have been released or even not exist(i.e. disabled).
 * @param[in]  trx  innodb transaction
 */
void trx_release_xa_group_if_need(trx_t *trx) {
  ut_ad(trx != nullptr);
  auto &xa_desc = trx->xa_desc;

  /* released or not exist(i.e diabled). */
  if (xa_desc.is_group_null()) return;

  /** If the trx is empty or read-only, the xa group can't be closed when
  use commit one phase. Just close it here. */
  xa_desc.group()->close();

  trx_sys->xa_group_shards[trx_get_xa_group_shard_no(xa_desc.gid())]
      .xa_groups.latch_and_execute(
          [&](Xa_group_by_id &xa_group_by_id) {
            /* As we own xa_group_by_id mutex, nobody can reference the
            xa_group at now. */
            if (xa_desc.group()->release()) {
              xa_group_by_id.erase(xa_desc.gid());
            }
          },
          UT_LOCATION_HERE);

  trx->xa_desc.set_group(nullptr);
}

/**
 * Adds the transaction to Xa group if need. If transaction group is
 * disabled, trx->xa_desc.group() would be nullptr and this trx should not
 * be added to any xa group.
 * @param[in]  trx   The transaction assumed to not be in the xa_group yet
 */
void trx_add_to_xa_group_if_need(trx_t *trx) {
  Xa_group *group = nullptr;
  ut_ad(trx != nullptr);
  group = trx->xa_desc.group();
  if (group) {
    group->insert(trx->id);
  }
}

XA_specification_strategy::XA_specification_strategy(const trx_t *trx)
    : m_trx(trx), m_xa_spec(trx->xa_spec) {}

/**
 * Judge if has gtid when recovery trx.
 *
 * @retval	true
 * @retval	false
 */
bool XA_specification_strategy::has_gtid() const {
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);

  if (binlog_xa_spec && binlog_xa_spec->has_gtid()) {
    return true;
  }

  return false;
}
/**
 * Judge storage way for gtid according to gtid source.
 */
trx_undo_t::Gtid_storage XA_specification_strategy::decide_gtid_storage() {
  trx_undo_t::Gtid_storage storage = trx_undo_t::Gtid_storage::NONE;
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);
  ut_ad(has_gtid());
  ut_ad(binlog_xa_spec->is_legal_source());

  switch (binlog_xa_spec->source()) {
    case binlog::Binlog_xa_specification::Source::NONE:
      ut_a(0);
      break;
    case binlog::Binlog_xa_specification::Source::COMMIT:
      storage = trx_undo_t::Gtid_storage::COMMIT;
      break;
    case binlog::Binlog_xa_specification::Source::XA_COMMIT_ONE_PHASE:
    case binlog::Binlog_xa_specification::Source::XA_PREPARE:
    case binlog::Binlog_xa_specification::Source::XA_COMMIT:
    case binlog::Binlog_xa_specification::Source::XA_ROLLBACK:
      storage = trx_undo_t::Gtid_storage::PREPARE_AND_COMMIT;
      break;
  }
  return storage;
}

/**
 * Overwrite gtid storage type of trx_undo_t when recovery.
 */
void XA_specification_strategy::overwrite_gtid_storage(trx_t *trx) {
  trx_undo_t *undo{nullptr};
  ut_ad(trx == m_trx);
  ut_ad(has_gtid());

  if (trx->rsegs.m_redo.rseg != nullptr && trx_is_redo_rseg_updated(trx)) {
    undo = trx->rsegs.m_redo.update_undo;
    if (undo) {
      undo->m_gtid_storage = decide_gtid_storage();
    }
  }
}

/** Fill gtid info from xa spec. */
void XA_specification_strategy::get_gtid_info(Gtid_desc *gtid_desc) {
  auto binlog_xa_spec =
      dynamic_cast<binlog::Binlog_xa_specification *>(m_xa_spec);
  ut_ad(has_gtid());

  gtid_desc->m_version = GTID_VERSION;

  auto &gtid = binlog_xa_spec->m_gtid;
  auto &sid = binlog_xa_spec->m_sid;

  gtid_desc->m_info.fill(0);
  auto char_buf = reinterpret_cast<char *>(&gtid_desc->m_info[0]);
  auto len = gtid.to_string(sid, char_buf);
  ut_a((size_t)len <= GTID_INFO_SIZE);
  gtid_desc->m_is_set = true;
}

/**
 * Judge if has gcn when recovering or commiting detached xa trxs.
 *
 * @return true if has gcn, false otherwise
 */
bool XA_specification_strategy::has_gcn() const {
  return m_xa_spec != nullptr && m_xa_spec->has_gcn();
}

/**
 * Overwite GCN info in trx when recovery, or commit detached XA.
 */
void XA_specification_strategy::overwrite_xa(trx_t *trx) const {
  if (trx_is_started(trx) && trx->rsegs.m_txn.rseg != nullptr &&
      trx_is_txn_rseg_updated(trx)) {
    m_xa_spec->copy_xa_to_trx(trx);
  }
}

Guard_xa_specification::Guard_xa_specification(trx_t *trx,
                                               XA_specification *xa_spec,
                                               bool prepare)
    : m_trx(trx), m_xa_spec(xa_spec) {
  ut_ad(trx);

  trx->xa_spec = m_xa_spec;
  XA_specification_strategy xss(trx);

  if (xss.has_gtid()) {
    xss.overwrite_gtid_storage(trx);
  }

  if (xss.has_gcn()) {
    xss.overwrite_xa(trx);
  }
}

Guard_xa_specification::~Guard_xa_specification() { m_trx->xa_spec = nullptr; }

template <class T>
struct my_hash {};

/** This one is based on splitmix64, which seems to be based on the blog article
Better Bit Mixing (mix 13) */
template <>
struct my_hash<uint64_t> {
  uint64_t operator()(const uint64_t key) {
    uint64_t x = key;
    x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
    x = x ^ (x >> 31);
    return x;
  }
};

/** See https://github.com/gcc-mirror/gcc/blob/master/intl/hash-string.h */
/* We assume to have `unsigned long int' value with at least 32 bits.  */
#define HASHWORDBITS 32
template <>
struct my_hash<char[XIDDATASIZE]> {
  uint64_t operator()(const char key[XIDDATASIZE]) {
    uint64_t hval, g;
    const char *str = key;
    const char *end = key + XIDDATASIZE;

    /* Compute the hash value for the given string.  */
    hval = 0;
    while (str < end && *str != '\0') {
      hval <<= 4;
      hval += (unsigned long int)*str++;
      g = hval & ((unsigned long int)0xf << (HASHWORDBITS - 4));
      if (g != 0) {
        hval ^= g >> (HASHWORDBITS - 8);
        hval ^= g;
      }
    }
    return hval;
  }
};

/** The following function, which is really good to hash different fields, is
copyed from boost::hash_combine. */
template <class T>
static inline void hash_combine(uint64_t &s, const T &v) {
  my_hash<T> h;
  s ^= h(v) + 0x9e3779b9 + (s << 6) + (s >> 2);
}

uint64_t hash_xid(const XID *xid) {
  uint64_t res = 0;
  char xid_data[XIDDATASIZE];
  size_t xid_data_len = xid->get_gtrid_length() + xid->get_bqual_length();
  size_t remain = XIDDATASIZE - xid_data_len;

  memcpy(xid_data, xid->get_data(),
         xid->get_gtrid_length() + xid->get_bqual_length());
  memset(xid_data + xid_data_len, 0, remain);

  hash_combine(res, xid_data);
  hash_combine(res, (uint64_t)xid->get_bqual_length());
  hash_combine(res, (uint64_t)xid->get_gtrid_length());
  hash_combine(res, (uint64_t)xid->get_format_id());

  return res;
}

struct TrxSysLockable {
  static TrxSysLockable &instance() {
    static TrxSysLockable m_instance;
    return m_instance;
  }
  void lock() { trx_sys_mutex_enter(); }
  void unlock() { trx_sys_mutex_exit(); }
};

const XID *get_external_xid_from_thd(THD *thd) {
  const XID *xid;

  if (!thd) {
    return nullptr;
  }

  if (thd == nullptr ||
      !thd->get_transaction()->xid_state()->check_in_xa(false)) {
    return nullptr;
  }

  xid = thd->get_transaction()->xid_state()->get_xid();

  /** Must be a valid and external XID. */
  ut_ad(!xid->is_null() && !xid->get_my_xid());

  return xid;
}
/**
  Search detached prepare XA transaction info by XID. NOTES:
  Assume holding xid_state lock, can't happen parallel rollback or commit.

  @param[in]  XID   xid
  @param[out] info  XA trx info

  @return true if found.
          false if not found.
*/
bool trx_search_detach_prepare_by_xid(const XID *xid, MyXAInfo *info) {
  trx_state_t state;

  std::lock_guard<TrxSysLockable> lock_guard(TrxSysLockable::instance());
  for (auto trx : trx_sys->rw_trx_list) {
    trx_mutex_enter(trx);
    state = trx->state.load(std::memory_order_relaxed);
    trx_mutex_exit(trx);

    if (state != TRX_STATE_PREPARED || !trx->xid->eq(xid)) {
      continue;
    }

    /**
      1. The transaction was detached once. So the undo state must be at
         least PREPARED_IN_TC.
      2. Holding the trx_sys mutex, so cannot become
         TRX_STATE_COMMITTED_IN_MEMORY.
      3. Holding the XID_STATE lock, so no concurrent commits or rollbacks are
         in progress.
    */
    ut_a(trx->mysql_thd == nullptr);
    ut_a(trx_is_prepared_in_tc(trx));

    info->status = XA_status::DETACHED_PREPARE;
    info->init_by_txn_undo(trx->id, trx_undo_get_txn(trx));
    return true;
  }

  return false;
}

/**
  Search rollbacking trx in background by XID. If found, such a transaction is
  considered as ATTACHED.

  @param[in]  XID   xid
  @param[out] info  XA trx info

  @return true if found.
          false if not found.
*/
bool trx_search_rollback_background_by_xid(const XID *xid, MyXAInfo *info) {
  bool is_recovered;
  trx_state_t state;

  std::lock_guard<TrxSysLockable> lock_guard(TrxSysLockable::instance());
  for (auto trx : trx_sys->rw_trx_list) {
    trx_mutex_enter(trx);
    is_recovered = trx->is_recovered;
    state = trx->state.load(std::memory_order_relaxed);
    trx_mutex_exit(trx);

    if (!is_recovered) continue;

    switch (state) {
      case TRX_STATE_COMMITTED_IN_MEMORY:
      case TRX_STATE_NOT_STARTED:
      case TRX_STATE_FORCED_ROLLBACK:
        /** recovered transaction can only be TRX_STATE_PREPARED or
        TRX_STATE_ACTIVE. See trx_lists_init_at_db_start. */
        ut_error;
        break;
      case TRX_STATE_PREPARED:
        if (trx->xid->eq(xid)) {
          /** In actual use, the transaction_cache will be searched first, and
          then the transaction information will be searched in the engine. So
          actually can't come into here. */
          return false;
        }
        continue;
      case TRX_STATE_ACTIVE:
        if (!trx->xid->eq(xid)) {
          continue;
        }
        break;
    }

    /**
      1. The trx that (is_recovered = 1 && state == TRX_STATE_ACTIVE) must being
         rollbacked backgroud.
      2. Holding trx_sys mutex, so can't be committed and can't be freed.
      3. NOTES: Assume that the trx must not in transaction cache. So no one
         can attach it.

      So trx->xid can be read safely.
    */
    ut_a(is_recovered && state == TRX_STATE_ACTIVE && trx->xid->eq(xid));

    /** Attached by background thread. */
    *info = MY_XA_INFO_ATTACH;

    return true;
  }

  return false;
}

static bool txn_find_slot_quick(const XID *xid, const slot_ptr_t slot_ptr_hint,
                                txn_slot_t *txn_slot) {
  txn_lookup_t txn_lookup;
  Txn_slot_reuse_by_xid_checker xid_checker(xid);

  bool found = txn_slot_read_guess(slot_ptr_hint, Cache_hint::KEEP_OLD,
                                   xid_checker, &txn_lookup);
  if (found) {
    *txn_slot = txn_lookup.txn_slot;
  }
  return found;
}

static bool txn_find_slot_slow(const XID *xid, txn_slot_t *txn_slot) {
  trx_rseg_t *rseg;

  rseg = txn_rseg_assign_by_xid(xid);

  ut_ad(rseg);

  return txn_rseg_find_txn_slot_by_xid(rseg, xid, txn_slot);
}

/**
  Find transactions in the finalized state by XID.

  @params[in] xid               XID
  @param[out] info              Corresponding transaction info

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool trx_search_history_by_xid(const XID *xid, MyXAInfo *info,
                               const slot_ptr_t slot_ptr_hint) {
  txn_slot_t txn_slot;
  bool found;

  found = txn_find_slot_quick(xid, slot_ptr_hint, &txn_slot);

  if (!found) {
    found = txn_find_slot_slow(xid, &txn_slot);
  }

  if (!found) {
    return false;
  }

  switch (txn_slot.state) {
    case TXN_UNDO_LOG_COMMITED:
    case TXN_UNDO_LOG_PURGED:
    case TXN_UNDO_LOG_ERASED:
      if (!txn_slot.tags_allocated()) {
        /** Found old format, not support. */
        *info = MY_XA_INFO_NOT_SUPPORT;
        break;
      }
      info->init_by_txn_slot(&txn_slot);
      break;
    case TXN_UNDO_LOG_ACTIVE:
      /** Skip txn in active state. */
      found = false;
      break;
    default:
      ut_error;
  }

  return found;
}

bool trx_slot_check_validity(const trx_t *trx) {
  THD *thd;
  const txn_undo_ptr_t *undo_ptr;

  thd = trx->mysql_thd;
  undo_ptr = &trx->rsegs.m_txn;

  ut_ad(mutex_own(&undo_ptr->rseg->mutex));

  /** 1. Check Transaction_ctx::m_xid_state::m_xid and xid_for_hash. */
  const XID *xid_in_thd = thd->get_transaction()->xid_state()->get_xid();
  if (thd->get_transaction()->xid_state()->check_in_xa(false)) {
    ut_a(xid_in_thd->eq(&undo_ptr->xid_for_hash));
  } else {
    ut_a(undo_ptr->xid_for_hash.is_null());
    return true;
  }

  /** 2. xid_for_hash must be a valid and external XID. */
  ut_ad(!undo_ptr->xid_for_hash.is_null() &&
        !undo_ptr->xid_for_hash.get_my_xid());

  /** 3. Check the rseg must be mapped by xid_for_hash. */
  ut_ad(trx_is_txn_rseg_updated(trx));
  ut_a(txn_rseg_check_xid_mapping(&undo_ptr->xid_for_hash, undo_ptr->rseg));

  /** 4. Check trx_t::xid and xid_for_hash. */
  if (!trx->xid->is_null()) {
    ut_a(trx->xid->eq(&undo_ptr->xid_for_hash));
  }

  /** 5. Check trx_undo_t::xid and xid_for_hash. */
  if ((undo_ptr->txn_undo->flag & TRX_UNDO_FLAG_XID)) {
    ut_a(undo_ptr->txn_undo->xid.eq(&undo_ptr->xid_for_hash));
  }

  return true;
}

}  // namespace lizard
