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

#include "srv0srv.h"
#include "trx0purge.h"

#include "lizard0undo0retent.h"

#ifdef UNIV_PFS_MUTEX
/* Lizard undo retention start mutex PFS key */
mysql_pfs_key_t undo_retention_mutex_key;
#endif

namespace lizard {

/** Retention time of txn undo data in seconds. */
ulong txn_retention_time = 0;

ib_time_system_us_t server_start_time_for_txn = 0;

/*
  static members.
*/
ulint Undo_retention::retention_time = 0;
ulint Undo_retention::space_limit = 100 * 1024;
ulint Undo_retention::space_reserve = 0;
char Undo_retention::status[128] = {0};
Undo_retention Undo_retention::inst;

/* Init undo_retention */
void undo_retention_init() {
  /* Init the lizard undo retention mutex. */
  Undo_retention::instance()->init_mutex();

  /* Force to refrese once at starting */
  Undo_retention::instance()->refresh_stat_data();
}

void init_server_start_time_for_txn() {
  ut_ad(!server_start_time_for_txn);

  server_start_time_for_txn = ut_time_system_us();
}

int Undo_retention::check_limit(THD *thd, SYS_VAR *var, void *save,
                                struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save < space_reserve) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_limit should more than"
                        " innodb_undo_space_reserve.");
    return 1;
  }

  return 0;
}

int Undo_retention::check_reserve(THD *thd, SYS_VAR *var, void *save,
                                  struct st_mysql_value *value) {
  if (check_func_long(thd, var, save, value)) return 1;

  if (*(ulong *)save > space_limit) {
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "InnoDB: innodb_undo_space_reserve should less than"
                        " innodb_undo_space_limit.");
    return 1;
  }

  return 0;
}

void Undo_retention::on_update(THD *, SYS_VAR *, void *var_ptr,
                               const void *save) {
  *static_cast<ulong *>(var_ptr) = *static_cast<const ulong *>(save);
  srv_purge_wakeup(); /* notify purge thread to try again */
}

void Undo_retention::on_update_and_start(THD *thd, SYS_VAR *var, void *var_ptr,
                                         const void *save) {
  ulong old_value = *static_cast<const ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);
  on_update(thd, var, var_ptr, save);

  /* If open the undo retention, refresh stat data synchronously. */
  if (new_value > 0 && old_value == 0) {
    instance()->refresh_stat_data();
  }
}

/*
  Collect latest undo space sizes periodically.
*/
void Undo_retention::refresh_stat_data() {
  mutex_enter(&m_mutex);
  ulint used_size = 0;
  ulint file_size = 0;
  std::vector<space_id_t> undo_spaces;

  if (retention_time == 0) {
    m_stat_done = false;
    mutex_exit(&m_mutex);
    return;
  }

  /* Actual used size */
  undo::spaces->s_lock();
  for (auto undo_space : undo::spaces->m_spaces) {
    ulint size = 0;

    for (auto rseg : *undo_space->rsegs()) {
      size += rseg->get_curr_size();
    }

    used_size += size;
    undo_spaces.push_back(undo_space->id());
  }
  undo::spaces->s_unlock();

  /* Physical file size */
  for (auto id : undo_spaces) {
    auto size = fil_space_get_size(id);
    file_size += size;
  }

  m_total_used_size = used_size;
  m_total_file_size = file_size;

  m_stat_done = true;

  mutex_exit(&m_mutex);
}


/*
  Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge
*/
bool Undo_retention::purge_advise(ulint us) {
  ut_a(us != 0);
  ulint utc = (ulint)(us / 1000000);

  /* Retention turned off or stating not done, can not advise */
  if (retention_time == 0 || !m_stat_done) return false;

  ulint used_size = m_total_used_size.load();

  if (space_limit > 0) {
    /* Rule_1: reach space limit, do purge */
    if (used_size > mb_to_pages(space_limit)) return false;
  }

  /* Rule_2: retention time not satisfied, block purge */
  auto cur_utc = current_utc();
  if ((utc + retention_time) > cur_utc) {
    return true;
  }

  /* Rule_3: below reserved size yet, can hold more history data */
  if (space_reserve > 0 && used_size < mb_to_pages(space_reserve)) {
    return true;
  }

  /* Rule_4: time satisfied and exceeded the reserved, just do purge */
  return false;
}



static bool txn_retention_satisfied_from_server_start() {
  ib_time_system_us_t cur_utc;

  /** Not set retention, can reuse txn */
  if (!txn_retention_time && !lizard::Undo_retention::retention_time) {
    return true;
  }

  /** Not init, can not reuse txn. */
  if (unlikely(!server_start_time_for_txn)) {
    return false;
  }

  cur_utc = ut_time_system_us();

  std::chrono::microseconds retention_time = std::chrono::seconds(
      std::max(txn_retention_time, lizard::Undo_retention::retention_time));

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

  auto cur_utc = ut_time_system_us();
  std::chrono::microseconds elapsed_time(cur_utc - utc);
  std::chrono::microseconds retention_time =
      std::chrono::seconds(txn_retention_time);
  ut_a(elapsed_time.count() > 0);

  if (elapsed_time > retention_time) {
    return true;
  }

  return false;
}

}  // namespace lizard
