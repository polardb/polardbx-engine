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

#include "lizard0xa.h"
#include "lizard0read0types.h"
#include "lizard0xa0iface.h"
#include "lizard0undo.h"
#include "m_ctype.h"
#include "mysql/plugin.h"
#include "sql/xa.h"
#include "trx0sys.h"
#include "ha_innodb.h"
#include "lizard0ha_innodb.h"
#include "sql/sql_class.h"
#include "sql/mysqld.h" // innodb_hton
#include "lizard0ut.h"

#include "sql/sql_class.h"
#include "sql/sql_plugin_var.h"

/** Bqual format: 'xxx@nnnn' */
static unsigned int XID_GROUP_SUFFIX_SIZE = 5;

static char XID_GROUP_SPLIT_CHAR = '@';

/**
  Whether the XID group matched.

  Requirement:
   1) formatID must be equal
   2) gtrid must be equal
   3) bqual length must be equal
   4) bqual prefix must be equal
   5) bqual suffix must be number
*/
bool trx_group_match_by_xid(const XID *lhs, const XID *rhs) {
  /* Require within XA transaction */
  if (lhs->is_null() || rhs->is_null()) return false;

  /* Require formatID equal */
  if (lhs->formatID != rhs->formatID) return false;

  int prefix_len =
      lhs->gtrid_length + lhs->bqual_length - XID_GROUP_SUFFIX_SIZE;

  if (lhs->gtrid_length != rhs->gtrid_length ||
      lhs->bqual_length != rhs->bqual_length ||
      lhs->bqual_length <= XID_GROUP_SUFFIX_SIZE ||
      lhs->data[prefix_len] != XID_GROUP_SPLIT_CHAR ||
      memcmp(lhs->data, rhs->data, prefix_len + 1)) {
    return false;
  }

  for (unsigned int i = 1; i < XID_GROUP_SUFFIX_SIZE; i++) {
    if (!my_isdigit(&my_charset_latin1, lhs->data[prefix_len + i]) ||
        !my_isdigit(&my_charset_latin1, rhs->data[prefix_len + i])) {
      return false;
    }
  }

  return true;
}

/**
  Loop all the rw trxs to find xa transaction which belonged to the same group
  and push trx_id into group container.

  @param[in]    trx       current trx handler
  @param[in]    vision    current query view
*/
void vision_collect_trx_group_ids(const trx_t *my_trx, lizard::Vision *vision) {
  ut_ad(vision->group_ids.m_size == 0);
  ut_ad(vision->group_ids.m_ids.empty());

  /** Restrict only user client thread */
  if (my_trx->mysql_thd == nullptr ||
      my_trx->mysql_thd->system_thread != NON_SYSTEM_THREAD ||
      !thd_get_transaction_group(my_trx->mysql_thd))
    return;

  trx_sys_mutex_enter();

  for (auto trx = UT_LIST_GET_FIRST(trx_sys->rw_trx_list); trx != nullptr;
       trx = UT_LIST_GET_NEXT(trx_list, trx)) {
    trx_mutex_enter(trx);

    if (trx_group_match_by_xid(my_trx->xad.my_xid(), trx->xad.my_xid())) {
      vision->group_ids.push(trx->id);
    }

    trx_mutex_exit(trx);
  }

  trx_sys_mutex_exit();
}

namespace lizard {
namespace xa {
/** This one is based on splitmix64, which seems to be based on the blog article
Better Bit Mixing (mix 13) */
uint64_t hash_u64(uint64_t x) {
  x = (x ^ (x >> 30)) * UINT64_C(0xbf58476d1ce4e5b9);
  x = (x ^ (x >> 27)) * UINT64_C(0x94d049bb133111eb);
  x = x ^ (x >> 31);
  return x;
}

/** The following function, which is really good to hash different fields, is
copyed from boost::hash_combine. */
static inline void hash_combine(uint64_t &s, const uint64_t &v) {
  s ^= hash_u64(v) + 0x9e3779b9 + (s << 6) + (s >> 2);
}

uint64_t hash_gtrid(const char *in_gtrid, unsigned in_len) {
  uint64_t res = 0;
  char gtrid[MAXGTRIDSIZE];
  const char *gtrid_ptr;
  const char *end = gtrid + MAXGTRIDSIZE;

  memset(gtrid, 0, sizeof(gtrid));
  memcpy(gtrid, in_gtrid, in_len);

  for (gtrid_ptr = gtrid; gtrid_ptr < end; gtrid_ptr += sizeof(uint64_t)) {
    ut_ad(gtrid_ptr + sizeof(uint64_t) <= end);
    hash_combine(res, *(uint64_t *)gtrid_ptr);
  }

  return res;
}

const char *Transaction_state_str[] = {"COMMIT", "ROLLBACK"};

bool get_transaction_info_by_gtrid(const char *gtrid, unsigned len,
                                   Transaction_info *info) {
  trx_rseg_t *rseg;
  txn_undo_hdr_t txn_hdr;
  bool found;

  rseg = get_txn_rseg_by_gtrid(gtrid, len);

  ut_ad(rseg);

  found = txn_rseg_find_trx_info_by_gtrid(rseg, gtrid, len, &txn_hdr);

  if (found) {
    switch (txn_hdr.state) {
      case TXN_UNDO_LOG_COMMITED:
      case TXN_UNDO_LOG_PURGED:
        info->state = txn_hdr.is_rollback() ? TRANS_STATE_ROLLBACK
                                            : TRANS_STATE_COMMITTED;
        break;
      case TXN_UNDO_LOG_ACTIVE:
        /** Can't be active. */
        /** fall through */
      default:
        ut_error;
    }
    info->gcn = txn_hdr.image.gcn;
  }

  return found;
}

const char *transaction_state_to_str(const enum Transaction_state state) {
  return Transaction_state_str[state];
}

bool start_and_register_rw_trx_for_xa(THD *thd) {
  trx_t *trx = check_trx_exists(thd);

  /** check_trx_exists will create trx if no trx. */
  ut_ad(trx);

  trx_start_if_not_started(trx, true, UT_LOCATION_HERE);

  innobase_register_trx_only_trans(innodb_hton, thd, trx);

  thd->get_ha_data(innodb_hton->slot)->ha_info[1].set_trx_read_write();

  return false;
}

bool trx_slot_assign_for_xa(THD *thd, TSA *tsa) {
  bool error = false;

  trx_t *trx = check_trx_exists(thd);

  /** check_trx_exists will create trx if no trx. */
  ut_ad(trx);

  mutex_enter(&trx->undo_mutex);

  /** The trx must have been started as rw mode. */
  if (!trx_is_registered_for_2pc(trx) || !trx_is_started(trx) ||
      trx->id == 0 || trx->read_only || !trx_is_txn_rseg_assigned(trx)) {
    error = true;
    goto exit_func;
  }

  ut_ad(trx_is_txn_rseg_assigned(trx));

  /** If the TXN has been assigned, then just return. */
  if (trx_is_txn_rseg_updated(trx)) {
    if (tsa) {
      *tsa = static_cast<TSA>(trx->txn_desc.undo_ptr);
    }
    goto exit_func;
  }

  /** Force to assign a TXN. */
  if (lizard::trx_always_assign_txn_undo(trx) != DB_SUCCESS) {
    error = true;
    goto exit_func;
  }
  if (tsa) {
    *tsa = static_cast<TSA>(trx->txn_desc.undo_ptr);
  }

exit_func:
  mutex_exit(&trx->undo_mutex);

  return error;
}

void trx_slot_write_xid_for_one_phase_xa(THD *thd) {
  const XID *xid = thd->get_transaction()->xid_state()->get_xid();
  trx_t *trx = check_trx_exists(thd);

  ut_ad(trx_is_registered_for_2pc(trx));

  ut_ad(trx_is_started(trx));

  ut_ad(trx->id != 0 && !trx->read_only);

  ut_ad(trx_is_txn_rseg_assigned(trx));

  ut_ad(trx_is_txn_rseg_updated(trx));

  mutex_enter(&trx->rsegs.m_txn.rseg->mutex);

  txn_undo_write_xid(xid, trx->rsegs.m_txn.txn_undo);

  mutex_exit(&trx->rsegs.m_txn.rseg->mutex);
}

/*************************************************
*                Heartbeat Freezer              *
*************************************************/
bool srv_no_heartbeat_freeze;

ulint srv_no_heartbeat_freeze_timeout;

class Heartbeat_freezer {
 public:
  Heartbeat_freezer() : m_is_freeze(false) {}

  bool is_freeze() { return m_is_freeze; }

  void heartbeat() {
    std::lock_guard<std::mutex> guard(m_mutex);
    m_timer.update();
    m_is_freeze = false;
  }

  bool determine_freeze() {
    uint64_t diff_time;
    bool block;
    constexpr uint64_t PRINTER_INTERVAL_SECONDS = 180;
    static Lazy_printer printer(PRINTER_INTERVAL_SECONDS);

    std::lock_guard<std::mutex> guard(m_mutex);

    if (!srv_no_heartbeat_freeze) {
      block = false;
      goto exit_func;
    }

    diff_time = m_timer.since_last_update();

    block = (diff_time > srv_no_heartbeat_freeze_timeout);

  exit_func:
    if (block) {
      printer.print(
          "The purge sys is blocked because no heartbeat has been received "
          "for a long time. If you want to advance the purge sys, please call "
          "dbms_xa.send_heartbeat().");

      m_is_freeze = true;
    } else {
      m_is_freeze = false;
      printer.reset();
    }

    return block;
  }

 private:
  /** Timer for check timeout. */
  Simple_timer m_timer;

  /* No need to use std::atomic because no need to read the newest value
  immediately. */
  bool m_is_freeze;

  /* Mutex modification of m_is_freeze. */
  std::mutex m_mutex;
};

Heartbeat_freezer hb_freezer;

void hb_freezer_heartbeat() {
  hb_freezer.heartbeat();
}

bool hb_freezer_determine_freeze() {
  return hb_freezer.determine_freeze();
}

bool hb_freezer_is_freeze() {
  return srv_no_heartbeat_freeze && hb_freezer.is_freeze();
}

/**
  If enable the hb_freezer, pretend to send heartbeat before updating, so it
  won't be blocked because of timeout.
*/
void freeze_db_if_no_cn_heartbeat_enable_on_update(THD *, SYS_VAR *, void *var_ptr,
                                                  const void *save) {
  const bool is_enable = *static_cast<const bool *>(save);

  /** 1. Pretend to send heartbeat. */
  if (is_enable) {
    hb_freezer_heartbeat();
  }

  /** 2.Update the var */
  *static_cast<bool *>(var_ptr) = is_enable;
}

}  // namespace xa
}  // namespace lizard

