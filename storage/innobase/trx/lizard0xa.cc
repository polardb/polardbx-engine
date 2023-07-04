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
#include "lizard0undo.h"
#include "m_ctype.h"
#include "mysql/plugin.h"
#include "sql/xa.h"
#include "trx0sys.h"
#include "ha_innodb.h"
#include "lizard0ha_innodb.h"
#include "sql/sql_class.h"
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

/**
  Find transactions in the finalized state by GTRID.

  @params[in] in_gtrid          gtird
  @params[in] in_len            length
  @param[out] Transaction_info  Corresponding transaction info

  @retval     true if the corresponding transaction is found, false otherwise.
*/
bool trx_search_by_gtrid(const char *gtrid, unsigned len,
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
        if (!txn_hdr.have_tags_1()) {
          info->state = TRANS_STATE_UNKNOWN;
        } else {
          info->state = txn_hdr.is_rollback() ? TRANS_STATE_ROLLBACK
                                              : TRANS_STATE_COMMITTED;
        }
        break;
      case TXN_UNDO_LOG_ACTIVE:
        /** Can't be active. */
        /** fall through */
      default:
        ut_error;
    }
    txn_hdr.image.copy_to_my_gcn(&info->gcn);
  }
  return found;
}

const XID *trx_slot_get_xa_xid_from_thd(THD *thd) {
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
  ut_a(txn_check_gtrid_rseg_mapping(&undo_ptr->xid_for_hash, undo_ptr->rseg));

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

}  // namespace xa
}  // namespace lizard

