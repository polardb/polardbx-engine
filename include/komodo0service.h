/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file include/komodo0service.h
 Komodo service layer types.

 Created 2024-06-05 by zanye
 *******************************************************/

#ifndef komodo0service_h
#define komodo0service_h

#include "storage/innobase/include/lizard0gcs0service.h"
#include "storage/innobase/include/lizard0trx0service.h"
#include "storage/innobase/include/lizard0txn0service.h"

#include <string>

#include "my_dbug.h"

struct trx_undo_t;
struct txn_slot_t;

/*-----------------------------------------------------------------------------*/
/** GCN vision represent user query readview. */
struct MyVisionGCN {
 public:
  MyVisionGCN() { reset(); }

  MyVisionGCN(csr_t _csr, gcn_t _gcn, scn_t _scn) {
    csr = _csr;
    gcn = _gcn;
    current_scn = _scn;
  }

  MyVisionGCN &operator=(gcn_t _gcn) {
    csr = CSR_ASSIGNED;
    gcn = _gcn;
    current_scn = SCN_NULL;
    return *this;
  }

  void reset() {
    gcn = GCN_NULL;
    csr = CSR_AUTOMATIC;
    current_scn = SCN_NULL;
  }

  bool is_null() { return gcn == GCN_NULL; }

  gcn_t gcn;
  csr_t csr;
  scn_t current_scn;
};

enum XA_status {
  /** Another seesion has attached XA. */
  ATTACHED,
  /** Detached XA, the real state of the XA is PREPARE_IN_TC. */
  DETACHED_PREPARE,
  /** The XA has been erased from transaction cache, and also has been
  committed. */
  COMMIT,
  /** The XA has been erased from transaction cache, and also has been
  rollbacked. */
  ROLLBACK,
  /** Can't find such a XA in transaction cache and in transaction slots, it
  might never exist or has been forgotten. */
  NOTSTART_OR_FORGET,
  /** Found the XA in transaction slots, but the real state (commit/rollback)
  of the transaction can't be confirmed (using the old TXN format.). */
  NOT_SUPPORT,
};

struct MyXAInfo {
 public:
  MyXAInfo(XA_status s)
      : status(s), gcn(), is_proposal(false), slot(), branch(), maddr() {}

  XA_status status;

  /** Proposal GCN when prepare, or commit GCN after commit / rollback */
  gcn_tuple_t gcn;
  bool is_proposal;

  /* XA branch ID */
  xa_addr_t slot;

  /* The count branch info of the global transaction. */
  xa_branch_t branch;

  /* XA master branch ID */
  xa_addr_t maddr;

 public:
  bool is_null() {
    return gcn.is_null() && slot.is_null() && branch.is_null() &&
           maddr.is_null();
  }
  /** Init xa attributes from txn undo when active or prepare.
   *
   * @param[in]		trx id
   * @param[in]		txn undo if allocate */
  void init_by_txn_undo(const trx_id_t tid, const trx_undo_t *txn_undo);

  /** Init xa attributes from txn slot after transaction finished.
   *
   * @param[in]		txn slot */
  void init_by_txn_slot(const txn_slot_t *txn_slot);
};

const MyXAInfo MY_XA_INFO_ATTACH(XA_status::ATTACHED);

const MyXAInfo MY_XA_INFO_FORGET(XA_status::NOTSTART_OR_FORGET);

const MyXAInfo MY_XA_INFO_NOT_SUPPORT(XA_status::NOT_SUPPORT);

/**--------------------------------------------------------*/
/**--------------------------------------------------------*/
namespace komodo {

/** Rollback segment statistics */
struct rseg_stat_t {
 public:
  explicit rseg_stat_t()
      : rseg_pages(0),
        history_length(0),
        history_pages(0),
        secondary_length(0),
        secondary_pages(0) {}

 public:
  uint64_t rseg_pages;
  uint64_t history_length;
  uint64_t history_pages;
  uint64_t secondary_length;
  uint64_t secondary_pages;
};

/** Undo tablespace status include purge/erase state */
class trunc_status_t {
 public:
  trunc_status_t()
      : undo_name(),
        file_pages(0),
        rseg_stat(),
        oldest_history_cn(),
        oldest_secondary_cn() {}

  void aggregate(const rseg_stat_t &value) {
    rseg_stat.rseg_pages += value.rseg_pages;
    rseg_stat.history_length += value.history_length;
    rseg_stat.history_pages += value.history_pages;
    rseg_stat.secondary_length += value.secondary_length;
    rseg_stat.secondary_pages += value.secondary_pages;
  }

  std::string undo_name;
  uint64_t file_pages;
  rseg_stat_t rseg_stat;

  cn_order_t oldest_history_cn;
  cn_order_t oldest_secondary_cn;
};

class purge_status_t {
 public:
  purge_status_t()
      : history_length(0),
        current_cn(),
        purged_cn(),
        erased_cn(),
        txn_history_length(0),
        retention_gcn(0) {}

  uint64_t history_length;
  cn_order_t current_cn;
  cn_order_t purged_cn;
  cn_order_t erased_cn;
  cn_order_t cycled_cn;

  uint64_t txn_history_length;

  gcn_t retention_gcn;
};

}  // namespace komodo

#endif
