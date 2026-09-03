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

/** @file trx/lizard0undo0retent.cc
  Lizard undo retention.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#include "univ.i"

#include "sql/sql_error.h"
#include "sql/sql_plugin_var.h"
#include "sql/transaction/komodo0hb0freezer.h"

#include "srv0srv.h"
#include "trx0purge.h"

#include "lizard0undo0retent.h"
#include "lizard0txn0space.h"

#ifdef UNIV_PFS_MUTEX
/* Lizard undo retention start mutex PFS key */
mysql_pfs_key_t undo_retention_mutex_key;
#endif

namespace lizard {

/** Retention time of txn undo data in seconds. */
ulong txn_retention_time = 0;

/** TXN will be retained for a period of time after the database is restarted */
ib_time_system_us_t server_start_time_for_txn = 0;

/*********************************************************************/
/* Retention Parameters */
/*********************************************************************/
ulint srv_undo_retention_time_in_seconds = 0;             // in seconds
ulint srv_undo_retention_space_limit_in_mb = 100 * 1024;  // in MiB
ulint srv_undo_retention_space_reserve_in_mb = 0;         // in MiB

int undo_retention_check_space_limit(THD *thd, SYS_VAR *var, void *save,
                                     struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save < srv_undo_retention_space_reserve_in_mb) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_limit should more than"
                        " innodb_undo_space_reserve.");
    return 1;
  }

  return 0;
}

int undo_retention_check_space_reserve(THD *thd, SYS_VAR *var, void *save,
                                       struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save > srv_undo_retention_space_limit_in_mb) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_reserve should less than"
                        " innodb_undo_space_limit.");
    return 1;
  }

  return 0;
}

void undo_retention_parameter_on_update(THD *, SYS_VAR *, void *var_ptr,
                                        const void *save) {
  *static_cast<ulong *>(var_ptr) = *static_cast<const ulong *>(save);
  srv_purge_wakeup(); /* notify purge thread to try again */
}

/*********************************************************************/
/* Time_retention */
/*********************************************************************/
/** Block the purge while the undo is still within the retention time
 * window since its commit time. */
bool Time_retention::block_purge(const commit_order_t &ommt) {
  return ommt.us + std::chrono::duration_cast<std::chrono::microseconds>(
                       std::chrono::seconds(srv_undo_retention_time_in_seconds))
                       .count() >
         static_cast<ulint>(ut_time_system_us());
}

/*********************************************************************/
/* Space_retention */
/*********************************************************************/
/** Convert the page count into MiB size.
 *
 * @param[in]	n_pages	page number
 *
 * @retval	size in MiB */
static ulint pages_to_mb(ulint n_pages) {
  return (ulint)(univ_page_size.physical() * n_pages / (1024.0 * 1024.0));
}

/** Force the purge once the txn space usage exceeds the limit. */
bool Space_retention::advance_purge(const commit_order_t &ommt) {
  ulint limit_in_mb = srv_undo_retention_space_limit_in_mb;
  return (limit_in_mb > 0) && (m_txn_used_in_mb > limit_in_mb);
}

/** Block the purge while the txn space usage is below the reserve. */
bool Space_retention::block_purge(const commit_order_t &ommt) {
  ulint reserve_in_mb = srv_undo_retention_space_reserve_in_mb;
  return (reserve_in_mb > 0) && (m_txn_used_in_mb < reserve_in_mb);
}

/** Recollect the current txn undo space usage. */
void Space_retention::refresh() {
  ulint used_size = 0;

  /* TXN actual used size */
  for (auto undo_space : txn_spaces) {
    ulint size = 0;

    ut_ad(undo_space->is_txn());

    for (auto rseg : *undo_space->rsegs()) {
      size += rseg->get_curr_size();
    }

    used_size += size;
  }

  m_txn_used_in_mb = pages_to_mb(used_size);
}

/*********************************************************************/
/* GCN_retention */
/*********************************************************************/
void GCN_retention::refresh(gcn_t new_retention_gcn) {
  /** The monotonicity of the atomic variable `retention_gcn` is no longer
  guaranteed. If a caller requires monotonicity, it is their responsibility
  to handle this requirement correctly. */
  m_retention_gcn.store(new_retention_gcn);
  srv_purge_wakeup();
}

gcn_t GCN_retention::get() const {
  return m_retention_gcn.load(std::memory_order_relaxed);
}

bool GCN_retention::advance_purge(const commit_order_t &ommt) {
  /**
    (0, retention_gcn) will be purged.
    [retention_gcn, infinity) will be retention.
  */
  return ommt.gcn < m_retention_gcn.load();
}

/*********************************************************************/
/* Freezer_retention */
/*********************************************************************/
/** Block the purge if the heartbeat freezer requires freezing. */
bool Freezer_retention::block_purge(const commit_order_t &) {
  return komodo::hb_freezer_determine_freeze();
}

/*********************************************************************/
/* Undo_manager */
/*********************************************************************/
Undo_manager::Undo_manager()
    : m_retention_vec(), m_space_retention(nullptr), m_gcn_retention(nullptr) {}

Undo_manager::~Undo_manager() {
  if (!m_retention_vec.empty()) {
    destory();
  }
}

void Undo_manager::init() {
  m_retention_vec.push_back(new Time_retention());
  m_retention_vec.push_back((m_space_retention = new Space_retention()));
  m_retention_vec.push_back((m_gcn_retention = new GCN_retention()));
  m_retention_vec.push_back(new Freezer_retention());

  refresh_stat_data();
}

void Undo_manager::destory() {
  for (auto retention : m_retention_vec) {
    delete retention;
  }
  m_retention_vec.clear();
  m_space_retention = nullptr;
  m_gcn_retention = nullptr;
}

bool Undo_manager::can_purge(const commit_order_t &ommt) {
  /* Any advance vote forces the purge and overrides all the block votes. */
  for (auto retention : m_retention_vec) {
    if (retention->advance_purge(ommt)) return true;
  }

  /* Otherwise any block vote blocks the purge. */
  for (auto retention : m_retention_vec) {
    if (retention->block_purge(ommt)) return false;
  }

  return true;
}

void Undo_manager::refresh_stat_data() {
  m_space_retention->refresh();
}

void Undo_manager::refresh_retention_gcn(gcn_t retention_gcn) {
  m_gcn_retention->refresh(retention_gcn);
}

gcn_t Undo_manager::get_retention_gcn() const { return m_gcn_retention->get(); }

/** Check if the retention time has been satisfied since the server
 * start, so the txn undo is always retained for a whole retention
 * window after the restart.
 *
 * @retval	true if no retention is configured, or the retention window
 * since the server start has passed */
static bool txn_retention_satisfied_from_server_start() {
  ib_time_system_us_t cur_utc;

  /** Not set retention, can reuse txn */
  if (!txn_retention_time && !srv_undo_retention_time_in_seconds) {
    return true;
  }

  /** Not init, can not reuse txn. */
  if (unlikely(!server_start_time_for_txn)) {
    return false;
  }

  cur_utc = ut_time_system_us();

  std::chrono::microseconds retention_time = std::chrono::seconds(
      std::max(txn_retention_time, srv_undo_retention_time_in_seconds));

  if (cur_utc < server_start_time_for_txn + retention_time.count()) {
    return false;
  }

  return true;
}

/**
  Check if the txn retention time has been satisfied.
  If the retention time has been satisfied, the txn undo has been retained for
  the required period as defined by txn_retention_time.

  @param[in]  utc        utc on txn to be checked

  @retval     true if the txn retention satisfied
*/
bool txn_retention_satisfied(utc_t utc) {
  if (!txn_retention_satisfied_from_server_start()) {
    return false;
  }

  ut_ad(utc > 0);

  ib_time_system_us_t cur_utc = ut_time_system_us();
  std::chrono::microseconds retention_time =
      std::chrono::seconds(txn_retention_time);

  if (cur_utc < static_cast<int64_t>(utc) + retention_time.count()) {
    return false;
  }

  return true;
}

}  // namespace lizard


namespace komodo {
/** Record the server start time, from which the txn retention window
 * after the restart is measured. Must be called only once during the
 * server startup. */
void init_server_start_time_for_txn() {
  ut_ad(!lizard::server_start_time_for_txn);

  lizard::server_start_time_for_txn = ut_time_system_us();
}
}  // namespace komodo
