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

/** @file include/lizard0undo0retent.h
  Lizard undo retention.

 Created 2020-04-02 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0undo0retent_h
#define lizard0undo0retent_h

#include "univ.i"
#include "ut0mutex.h"
#include "page0size.h"

#include "lizard0gcs0service.h"
#include "lizard0ut.h"

struct SYS_VAR;

#ifdef UNIV_PFS_MUTEX
/* Lizard undo retention start mutex PFS key */
extern mysql_pfs_key_t undo_retention_mutex_key;
#endif



namespace lizard {

/** Retention time of txn undo data in seconds. */
extern ulong txn_retention_time;

/** TXN will be retained for a period of time after the database is restarted */
extern ib_time_system_us_t server_start_time_for_txn;

/*
  Undo retention controller.
*/
class Undo_retention {
 public:
  // user configurations
  static ulint retention_time;  // in seconds
  static ulint space_limit;     // in MiB
  static ulint space_reserve;   // in MiB
  // show status
  static char status[128];

  static int check_limit(THD *thd, SYS_VAR *var, void *save,
                         struct st_mysql_value *value);
  static int check_reserve(THD *thd, SYS_VAR *var, void *save,
                           struct st_mysql_value *value);
  static void on_update(THD *, SYS_VAR *, void *var_ptr, const void *save);

  static void on_update_and_start(THD *thd, SYS_VAR *var, void *var_ptr,
                                  const void *save);

  ib_mutex_t m_mutex;

 protected:
  volatile bool m_stat_done;

  std::atomic<ulint> m_total_used_size;
  std::atomic<ulint> m_total_file_size;

  Undo_retention()
      : m_stat_done(false), m_total_used_size(0), m_total_file_size(0) {}

  Undo_retention &operator=(const Undo_retention &) = delete;
  Undo_retention(const Undo_retention &) = delete;

  static Undo_retention inst;  // global instance

  static ulint current_utc() { return ut_time_system_us() / 1000000; }

  static ulint mb_to_pages(ulint size) {
    return (ulint)(1024.0 * 1024.0 / univ_page_size.physical() * size);
  }

  static ulint pages_to_mb(ulint n_pages) {
    return (ulint)(univ_page_size.physical() * n_pages / (1024.0 * 1024.0));
  }

 public:
  static Undo_retention *instance() { return &inst; }

  /* Collect latest undo space sizes periodically */
  void refresh_stat_data();

  /* Decide whether to block purge or not based on the current
  undo tablespace size and retention configuration.

  @return     true     if blocking purge */
  bool purge_advise(ulint us);

  /* Create the lizard undo retention mutex. */
  inline void init_mutex() { mutex_create(LATCH_ID_UNDO_RETENTION, &m_mutex); }

  /* Free the lizard undo retention mutex. */
  static inline void destroy() { mutex_free(&(instance()->m_mutex)); }
};


/* Init undo_retention */
void undo_retention_init();

/**
  Check if the txn retention time has been satisfied.
  If the retention time has been satisfied, the txn undo has been retained for
  the required period as defined by txn_retention_time.

  @param[in]  utc        utc on txn to be checked

  @retval     true if the txn retention satisfied
*/
extern bool txn_retention_satisfied(utc_t utc);

}  // namespace lizard
   //
#endif

