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

#include "lizard0gcs0service.h"
#include "lizard0cmmt0types.h"
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

/** Minimum retention time of undo data, purge is blocked within the
 * window since the commit. */
extern ulint srv_undo_retention_time_in_seconds;      // in seconds
/** Space budget of txn undo data, purge is forced once the usage
 * exceeds it. */
extern ulint srv_undo_retention_space_limit_in_mb;    // in MiB
/** Reserved space of txn undo data, purge is blocked while the usage
 * is below it. */
extern ulint srv_undo_retention_space_reserve_in_mb;  // in MiB

/** Check function for the retention space limit parameter.
 *
 * Reject the new value if it's less than the current retention space
 * reserve.
 *
 * @param[in]	thd	user connection
 * @param[in]	var	system variable
 * @param[out]	save	validated value for the update function
 * @param[in]	value	new value from the user
 *
 * @retval	0 if the value is accepted, 1 if rejected */
extern int undo_retention_check_space_limit(THD *thd, SYS_VAR *var, void *save,
                                            struct st_mysql_value *value);

/** Check function for the retention space reserve parameter.
 *
 * Reject the new value if it's more than the current retention space
 * limit.
 *
 * @param[in]	thd	user connection
 * @param[in]	var	system variable
 * @param[out]	save	validated value for the update function
 * @param[in]	value	new value from the user
 *
 * @retval	0 if the value is accepted, 1 if rejected */
extern int undo_retention_check_space_reserve(THD *thd, SYS_VAR *var,
                                              void *save,
                                              struct st_mysql_value *value);

/** Update function shared by the retention parameters.
 *
 * Store the validated value and wake up the purge thread to reevaluate
 * the retention decision at once.
 *
 * @param[out]	var_ptr	destination variable
 * @param[in]	save	validated new value */
extern void undo_retention_parameter_on_update(THD *, SYS_VAR *, void *var_ptr,
                                               const void *save);

/** Undo retention policy interface.
 *
 * Each policy votes on whether the next undo log can be purged:
 * advance_purge() forces the purge regardless of any block vote, and
 * block_purge() vetoes it. See Undo_manager::can_purge() for the
 * precedence between the votes. */
class Retention {
 public:
  virtual ~Retention() {}

  /** Vote to force the purge.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge must be advanced regardless of any
   * block vote */
  virtual bool advance_purge(const commit_order_t &ommt) {
    return false;
  }

  /** Vote to block the purge.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge should be blocked */
  virtual bool block_purge(const commit_order_t &ommt) {
    return false;
  }
};

/** Time based retention.
 *
 * Retain the undo for at least srv_undo_retention_time_in_seconds
 * since its commit. */
class Time_retention : public Retention {
 public:
  /** Vote to block the purge if the undo is still within the retention
   * time window since its commit.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge should be blocked */
  virtual bool block_purge(const commit_order_t &ommt) override;
};

/** Space based retention.
 *
 * Retain the txn undo data as much as possible within the space
 * budget: purge is blocked while the usage is below the reserve, and
 * is forced once the usage exceeds the limit. */
class Space_retention : public Retention {
 public:
  Space_retention() : m_txn_used_in_mb(0) {}

  /** Vote to force the purge once the txn space usage exceeds the
   * retention space limit.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge must be advanced */
  virtual bool advance_purge(const commit_order_t &ommt) override;

  /** Vote to block the purge while the txn space usage is below the
   * retention space reserve.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge should be blocked */
  virtual bool block_purge(const commit_order_t &ommt) override;

  /** Recollect the current txn undo space usage. */
  void refresh();

 private:
  /** Latest collected txn undo space usage in MiB. */
  std::atomic<ulint> m_txn_used_in_mb;
};

/** GCN based retention.
 *
 * Only the undo committed within (0, retention_gcn) is allowed to be
 * purged, while [retention_gcn, infinity) is retained. */
class GCN_retention : public Retention {
 public:
  GCN_retention() : m_retention_gcn(0) {}

  /** Vote to force the purge if the commit gcn of the undo is behind
   * the retention gcn.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge must be advanced */
  virtual bool advance_purge(const commit_order_t &ommt) override;

  /** Refresh the retention gcn and wake up the purge thread.
   *
   * NOTE: the monotonicity of the retention gcn is not guaranteed
   * here, it's up to the caller if required.
   *
   * @param[in]	gcn	new retention gcn */
  void refresh(gcn_t);

  /** Get the current retention gcn.
   *
   * @retval	retention gcn */
  gcn_t get() const;

 private:
  /** Boundary gcn of the purge, see advance_purge(). */
  std::atomic<gcn_t> m_retention_gcn;
};

/** Heartbeat freezer based retention.
 *
 * Retain all the undo while the komodo heartbeat freezer decides to
 * freeze the purge. */
class Freezer_retention : public Retention {
 public:
  /** Vote to block the purge if the heartbeat freezer requires
   * freezing.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if the purge should be blocked */
  virtual bool block_purge(const commit_order_t &ommt) override;
};

/** Undo retention controller.
 *
 * Singleton which owns all the retention policies and combines their
 * votes into the final purge decision. */
class Undo_manager {
 public:
  /** Get the global singleton.
   *
   * @retval	the undo retention controller instance */
  static Undo_manager &instance() {
    static Undo_manager instance;
    return instance;
  }

  /** Register all the retention policies and collect the initial
   * statistics. */
  void init();

  /** Unregister and free all the retention policies. */
  void destory();

  /** Collect latest undo space sizes. */
  void refresh_stat_data();

  /** Refresh the retention gcn and wake up the purge thread.
   *
   * @param[in]	retention_gcn	new retention gcn */
  void refresh_retention_gcn(gcn_t retention_gcn);

  /** Get the current retention gcn.
   *
   * @retval	retention gcn */
  gcn_t get_retention_gcn() const;

  /** Decide whether the next undo can be purged.
   *
   * Any advance vote forces the purge and overrides all the block
   * votes, otherwise any block vote blocks the purge.
   *
   * @param[in]	ommt	commit order info of the next undo to be purged
   *
   * @retval	true if it can be purged */
  bool can_purge(const commit_order_t &ommt);

 private:
  Undo_manager &operator=(const Undo_manager &) = delete;
  Undo_manager (const Undo_manager &) = delete;
  Undo_manager();
  ~Undo_manager();

 private:
  /** All the registered retention policies. */
  std::vector<Retention *> m_retention_vec;

  /** Space retention, referenced by refresh_stat_data(). */
  Space_retention *m_space_retention;

  /** GCN retention, referenced by the retention gcn interfaces. */
  GCN_retention *m_gcn_retention;
};

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
