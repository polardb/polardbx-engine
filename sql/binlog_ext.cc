/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/binlog_ext.h"
#include "sql/consensus_log_event.h"
#include "sql/log_event_ext.h"
#include "sql/sql_class.h"
#include "sql/xa_ext.h"

Binlog_ext mysql_bin_log_ext;
uint opt_print_gtid_info_during_recovery = 1;
uint opt_recovery_apply_binlog_skip_counter = 0;
ulong opt_recovery_apply_binlog = RECOVERY_APPLY_BINLOG_SAME_AS_GTID;

/**
  Write a gtid set into error log.

  @param[in]   gtid_set_name   a name describes the gtid set.
  @param[in]   gtids           the gtid set will be logged
*/
void log_gtid_set(const char *gtid_set_name, const Gtid_set *gtids) {
  char *gtid_str = nullptr;
  if (gtids->to_string(&gtid_str, false) == -1) {
    LogErr(INFORMATION_LEVEL, ER_GTID_INFO, gtid_set_name,
           "Failed to allocate memory");
  } else {
    LogErr(WARNING_LEVEL, ER_GTID_INFO, gtid_set_name, gtid_str);
    my_free(gtid_str);
  }
}

Binlog_ext::Binlog_ext() {}

bool Binlog_ext::assign_gcn_to_flush_group(THD *first_seen) {
  uint64_t gcn = MYSQL_GCN_NULL;
  bool error = false;

  for (THD *head = first_seen; head; head = head->next_to_commit) {
    if (head->variables.innodb_commit_gcn != MYSQL_GCN_NULL) {
      gcn = head->variables.innodb_commit_gcn;
    } else if (get_xa_opt(head) != XA_ONE_PHASE) {
      error = ha_acquire_gcn(&gcn);
      DBUG_ASSERT(error || gcn != MYSQL_GCN_NULL);
    } else {
      gcn = MYSQL_GCN_NULL;
      DBUG_ASSERT(head->get_transaction()->m_flags.real_commit);
      DBUG_ASSERT(!head->get_transaction()->m_flags.commit_low);
    }

    head->m_extra_desc.m_commit_gcn = gcn;
  }

  return error;
}

/**
  Write the Gcn_log_event to the binary log (prior to writing the
  statement or transaction cache).

  @param thd Thread that is committing.
  @param cache_data The cache that is flushing.
  @param writer The event will be written to this Binlog_event_writer object.

  @retval false Success.
  @retval true Error.
*/
bool Binlog_ext::write_gcn(THD *thd, Binlog_event_writer *writer) {
  DBUG_TRACE;

  if (!opt_gcn_write_event) return false;

  Gcn_log_event gcn_evt(thd);
  bool ret = gcn_evt.write(writer);
  return ret;
}

/**
  XA transacitons don't have xid, but inc_prep_xids is() necessary for them
  to avoid binlog rotation happening before engine prepare/commit. It
  guarantees that XA transaction data lost happens only in the last binlog
  file when crash happens. Binlog crash recovery handles only the last
  binlog file.
*/
inline void Binlog_ext::xa_inc_prep_xids(THD *thd) {
  mysql_mutex_assert_owner(&mysql_bin_log.LOCK_log);

  if (thd->m_se_gtid_flags[THD::Se_GTID_flag::SE_GTID_PIN]) {
    mysql_bin_log.m_atomic_prep_xids++;
    thd->get_transaction()->xid_state()->m_ctx.set_increased_prep_xids();
  }
}

inline bool Binlog_ext::xa_delay_rotate(THD *thd, bool do_rotate) {
  if (thd->m_se_gtid_flags[THD::Se_GTID_flag::SE_GTID_PIN] && do_rotate) {
    thd->get_transaction()->xid_state()->m_ctx.set_do_binlog_rotate();
    return true;
  }

  return false;
}

Binlog_ext::XA_rotate_guard::~XA_rotate_guard() {
  THD *thd = m_thd;
  XID_STATE *xs = thd->get_transaction()->xid_state();

  // It has to decrease xids no matter error happens or not
  if (xs->m_ctx.increased_prep_xp_xids()) {
    mysql_bin_log.dec_prep_xids(thd);
    xs->m_ctx.reset_increased_prep_xids();
  }

  if (xs->m_ctx.do_binlog_rotate()) {
    xs->m_ctx.reset_do_binlog_rotate();
    if (*m_has_error) return;
    DEBUG_SYNC(thd, "ready_to_do_rotation");
    bool check_purge = false;
    mysql_mutex_lock(consensus_log_manager.get_sequence_stage1_lock());
    mysql_mutex_lock(&mysql_bin_log.LOCK_log);

    /*
      If rotate fails then depends on binlog_error_action variable
      appropriate action will be taken inside rotate call.
    */
    int error = mysql_bin_log.rotate(false, &check_purge);
    mysql_mutex_unlock(&mysql_bin_log.LOCK_log);
    mysql_mutex_unlock(consensus_log_manager.get_sequence_stage1_lock());

    if (!error && check_purge) mysql_bin_log.purge();
    // Since the XA action has succeeded, it will not return error to user.
    if (thd->is_error()) thd->clear_error();
  }
}

/* Dummy mts submode for applying binlog. */
class Dummy_mts_submode : public Mts_submode {
  int schedule_next_event(Relay_log_info *, Log_event *) override { return 0; }

  void attach_temp_tables(THD *, const Relay_log_info *,
                          Query_log_event *) override {}
  void detach_temp_tables(THD *, const Relay_log_info *,
                          Query_log_event *) override {}

  Slave_worker *get_least_occupied_worker(Relay_log_info *,
                                          Slave_worker_array *,
                                          Log_event *) override {
    return nullptr;
  }

  int wait_for_workers_to_finish(Relay_log_info *, Slave_worker *) override {
    return 0;
  }
};

template <typename T>
class Var_guard {
 public:
  Var_guard(T *var, T new_value) : m_old_value(*var), m_var(var) {
    *var = new_value;
  }
  ~Var_guard() { *m_var = m_old_value; }

 private:
  T m_old_value;
  T *m_var;
};

Binlog_recovery *Binlog_recovery::m_instance = nullptr;

Binlog_recovery *Binlog_recovery::instance() {
  if (m_instance == nullptr) {
    m_instance = new (std::nothrow) Binlog_recovery();
    if (m_instance == nullptr) LogErr(ERROR_LEVEL, ER_OOM);
  }
  return m_instance;
}

void Binlog_recovery::destroy() {
  if (m_instance) delete m_instance;
  m_instance = nullptr;
}

Binlog_recovery::Binlog_recovery()
    : m_gtids_in_last_file(global_sid_map, global_sid_lock),
      m_skip_counter_gtids(global_sid_map, global_sid_lock) {}

bool Binlog_recovery::begin() {
  // Any other 2pc storage is not supported
  if (total_ha_2pc > 2) {
    LogErr(ERROR_LEVEL, ER_RECOVERY_OTHER_ENGINES_NOT_SUPPORT);
    return true;
  }

  if (gtid_state->read_gtid_executed_from_table() == -1) return true;

  if (opt_print_gtid_info_during_recovery) {
    global_sid_lock->wrlock();
    log_gtid_set("[GTID INFO] Reading from table in Binlog_recovery::begin:",
                 gtid_state->get_executed_gtids());
    global_sid_lock->unlock();
  }

  return false;
}

bool Binlog_recovery::end() { return false; }

/**
   To guarantee crash safe, it has to update innodb's binlog offset to the
   transaction's binlog end pos at commit when applying the binlog events.

   To avoiding hacking MySQL code, Recovery_tc_log is used instead of normal
   tc_log(mysql_bin_log). Thus we can set the transaction's binlog end pos into
   thd just before commit.
*/
class Recovery_tc_log : public TC_LOG {
 public:
  char *binlog_name = nullptr;
  my_off_t binlog_pos = 0;

  int open(const char *) override { return 0; }
  void close() override {}

  enum_result commit(THD *thd, bool all) override {
    if (all) thd->set_trans_pos(binlog_name, binlog_pos);
    return ha_commit_low(thd, all) ? RESULT_ABORTED : RESULT_SUCCESS;
  }

  int rollback(THD *thd, bool all) override {
    return ha_rollback_low(thd, all);
  }
  int prepare(THD *thd, bool all) override { return ha_prepare_low(thd, all); }
};

bool Binlog_recovery::apply_binlog() {
  DBUG_TRACE;
  THD *thd = nullptr;
  bool ret = true;
  uint64 current_consensus_index = 0;

  sql_print_warning("Apply binlog from index %u (pos : %u) to index %u",
                    m_apply_start_index, m_apply_start_pos, m_apply_end_index);

  /*
    Engine has no data lost if m_gtids_in_last_file is subset of
    gtid_executed. In the case, applying binlog will be skipped.
  */
  bool is_subset = false;

  global_sid_lock->wrlock();

  if (m_gtids_in_last_file.is_empty()) {
    sql_print_warning(
        "Gtid_sets reading from last binlog is empty, ignore apply binlog");
    global_sid_lock->unlock();
    return false;
  }

  is_subset = m_gtids_in_last_file.is_subset(gtid_state->get_executed_gtids());

  /*
    previous_gtids_log_event is used by Clone_persist_gtid::write_other_gtids.
    Thus fill it with gtid_executed to avoid the warning:
    "The transaction owned GTID is already in the gtid_executed table."
  */
  const_cast<Gtid_set *>(gtid_state->get_previous_gtids_logged())
      ->add_gtid_set(gtid_state->get_executed_gtids());
  global_sid_lock->unlock();

  if (is_subset) {
    sql_print_warning(
        "Gtid_sets reading from last binlog is subset of excuted_gtids, ignore "
        "apply binlog");
    return false;
  }

  Binlog_file_reader reader(opt_master_verify_checksum);
  if (reader.open(m_apply_log_name, m_apply_start_pos)) {
    LogErr(ERROR_LEVEL, ER_BINLOG_FILE_OPEN_FAILED, reader.get_error_str());
    return true;
  }

  thd = new THD;
  THD_CHECK_SENTRY(thd);
  thd->thread_stack = (char *)&thd;
  thd->security_context()->skip_grants();
  thd->set_skip_readonly_check();
  thd->enable_slow_log = false;
  thd->store_globals();
  thd->set_new_thread_id();

  Recovery_tc_log recovery_tc_log;
  Dummy_mts_submode dummy_mts_submode;
  Rpl_filter dummy_filter;

  Var_guard<SHOW_COMP_OPTION> guard1(&binlog_hton->state, SHOW_OPTION_NO);
  Var_guard<bool> guard2(&opt_log_slave_updates, false);
  Var_guard<TC_LOG *> guard3(&tc_log, &recovery_tc_log);

  my_off_t current_group_end = 0;
  Gtid current_gtid;
  uint skip_counter = opt_recovery_apply_binlog_skip_counter;

  Relay_log_info *rli = Rpl_info_factory::create_rli(
      INFO_REPOSITORY_DUMMY, false, (const char *)"", true);
  if (!rli) {
    LogErr(ERROR_LEVEL, ER_OUTOFMEMORY);
    goto end;
  }

  // Set it as slave thread, thus resouce will be released automatically.
  thd->rli_slave = rli;
  thd->slave_thread = true;
  thd->system_thread = SYSTEM_THREAD_SLAVE_SQL;
  rli->info_thd = thd;
  rli->deferred_events_collecting = false;
  rli->current_mts_submode = &dummy_mts_submode;
  rli->set_filter(&dummy_filter);
  rli->set_rli_description_event(new Format_description_log_event());

  current_gtid.clear();
  while (1) {
    std::unique_ptr<Log_event> ev(reader.read_event_object());
    if (!ev) break;

    switch (ev->get_type_code()) {
      case binary_log::CONSENSUS_LOG_EVENT: {
        Consensus_log_event *consensus_log_ev =
            dynamic_cast<Consensus_log_event *>(ev.get());
        current_consensus_index = consensus_log_ev->get_index();
        break;
      }
      case binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT:
      case binary_log::CONSENSUS_CLUSTER_INFO_EVENT:
      case binary_log::CONSENSUS_EMPTY_EVENT:
        break;

      case binary_log::FORMAT_DESCRIPTION_EVENT:
      case binary_log::PREVIOUS_GTIDS_LOG_EVENT:
      case binary_log::ROWS_QUERY_LOG_EVENT:
      case binary_log::ROTATE_EVENT:
        break;
      case binary_log::ANONYMOUS_GTID_LOG_EVENT: {
        // Skip transaction without Gtid
        Gtid_log_event *gtid_ev = dynamic_cast<Gtid_log_event *>(ev.get());
        reader.seek(reader.event_start_pos() + gtid_ev->transaction_length);
        break;
      }
      default:
        if (ev->get_type_code() == binary_log::GTID_LOG_EVENT) {
          Gtid_log_event *gtid_ev = dynamic_cast<Gtid_log_event *>(ev.get());
          bool skip_trx = false;

          global_sid_lock->wrlock();
          skip_trx = gtid_state->get_executed_gtids()->contains_gtid(
              gtid_ev->get_sidno(false), gtid_ev->get_gno());
          current_gtid.set(gtid_ev->get_sidno(false), gtid_ev->get_gno());
          global_sid_lock->unlock();
          current_group_end =
              reader.event_start_pos() + gtid_ev->transaction_length;
          if (skip_trx) {
            reader.seek(current_group_end);
            /*GCN_LOG_EVENT has been applied before GTID_LOG_EVENT*/
            thd->m_extra_desc.reset();
            break;
          }
        }

        // It should not generate binary events again.
        thd->variables.option_bits &= ~OPTION_BIN_LOG;

        ev->thd = thd;
        if (ev->apply_event(rli)) {
          LogErr(ERROR_LEVEL, ER_RECOVERY_APPLY_BINLOG_ERR, m_apply_log_name,
                 reader.event_start_pos());

          if (skip_counter > 0 &&
              current_group_end > reader.event_start_pos()) {
            skip_counter--;
            reader.seek(current_group_end);
            thd->m_extra_desc.reset();

            if (!current_gtid.is_empty()) {
              global_sid_lock->wrlock();
              add_skip_counter_gtid(current_gtid);
              global_sid_lock->unlock();
            }
            LogErr(ERROR_LEVEL, ER_RECOVERY_APPLY_BINLOG_ERR_SKIP_COUNTER,
                   current_group_end, skip_counter);
            continue;
          }

          goto end;
        }

        /*
          Set the status that XA PREPARE is binlogged, thus their
          XA COMMIT/ROLLBACK later executed by users will be binlogged too.
        */
        if (ev->get_type_code() == binary_log::XA_PREPARE_LOG_EVENT) {
          XID xid;
          dynamic_cast<XA_prepare_log_event *>(ev.get())->get_xid(&xid);
          auto trx = transaction_cache_search(&xid);
          if (trx) trx->xid_state()->set_binlogged();
        }
    }

    if (current_consensus_index >= m_apply_end_index) 
      break;

  }

  if (reader.has_fatal_error()) {
    LogErr(ERROR_LEVEL, ER_RECOVERY_READ_BINLOG_ERR, m_apply_log_name,
           reader.event_start_pos());
    goto end;
  }

  /*
    The data must be persisted before removing IN_USE flag, because it
    will not be able to be recovered next crash after removing IN_USE flag.
  */
  ha_flush_logs(false);

  DBUG_EXECUTE_IF("crash_after_recover_apply_trx", { DBUG_SUICIDE(); });

  ret = false;
end:
  if (rli) {
    if (thd) {
      thd->clear_error();
      rli->cleanup_context(thd, true);
    }
    rli->end_info();
    rli->info_thd = nullptr;
  }
  delete thd;
  delete rli;

  /*
    Since Binlogging is disabled, the gtids of applying transactions will be
    added into purged_gtids. Thus it is cleared after binlog events are applied.
  */
  global_sid_lock->wrlock();
  const_cast<Gtid_set *>(gtid_state->get_lost_gtids())->clear();
  log_gtid_set("[GTID INFO] Apply binlog finished. executed_gtid : ",
               gtid_state->get_executed_gtids());
  global_sid_lock->unlock();
  return ret;
}

bool Binlog_recovery::add_gtid(const Gtid &gtid, my_off_t start_pos,
                               uint64 start_index) {
  if (gtid.is_empty()) return false;

  /*
    To reduce applying time, it starts from the first gtid which is not in
    gtid_executed.
  */
  if (m_apply_start_pos == 0 &&
      !gtid_state->get_executed_gtids()->contains_gtid(gtid)) {
    m_apply_start_pos = start_pos;
    m_apply_start_index = start_index;
  }

  if (m_gtids_in_last_file.ensure_sidno(gtid.sidno) != RETURN_STATUS_OK) {
    return true;
  }

  m_gtids_in_last_file._add_gtid(gtid);
  return false;
}

bool Binlog_recovery::add_skip_counter_gtid(const Gtid &gtid) {
  if (gtid.is_empty()) return false;

  if (m_skip_counter_gtids.ensure_sidno(gtid.sidno) != RETURN_STATUS_OK) {
    return true;
  }

  m_skip_counter_gtids._add_gtid(gtid);
  return false;
}

void Binlog_recovery::set_end_index(uint64 end_index) {
  m_apply_end_index = end_index;
}

void Binlog_recovery::set_apply_log_name(char *logname) {
  snprintf(m_apply_log_name, sizeof(m_apply_log_name), "%s", logname);
}
