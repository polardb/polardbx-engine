/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/recycle_bin/recycle_scheduler.h"
#include "mysql/components/services/log_builtins.h"
#include "mysql/plugin.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql/mysqld.h"              // my_localhost slave_net_timeout
#include "sql/mysqld_thd_manager.h"  // Global_THD_manager
#include "sql/protocol_classic.h"
#include "sql/recycle_bin/recycle_table.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sql_table.h"
#include "sql/sql_time.h"
#include "sql/thd_raii.h"
#include "sql/transaction.h"
#include "sql/tztime.h"
#include "thr_mutex.h"

namespace im {
namespace recycle_bin {

/* Recycle scheduler thread loop interval (seconds). */
ulonglong recycle_scheduler_interval = 1;
/* Whether run recycle scheduler thread */
bool opt_recycle_scheduler = true;
/* Whether print the detailed information when purge table */
bool recycle_scheduler_purge_table_print = false;
/** Record the recycle scheduler version changes if modified
    recycle_scheduler_rentention */
static ulonglong recycle_scheduler_loop_version = 1;
/**
  Log error message when purge table

  @param[in]      table_name  Dropping table name
  @param[in]      reason      Why failed
*/
static void log_purge_error(const char *table_name, const char *reason) {
  std::stringstream ss;
  ss << "Fail to purge table " << table_name << " since " << reason;
  LogErr(WARNING_LEVEL, ER_RECYCLE_BIN, ss.str().c_str());
}

/* Error handling constructor */
Recycle_scheduler_error_handler::Recycle_scheduler_error_handler(
    THD *thd, const char *table)
    : m_thd(thd), m_table(table), m_error(false) {
  thd->push_internal_handler(this);
}

/* Ignore those error when recycle scheduler */
bool Recycle_scheduler_error_handler::handle_condition(
    THD *, uint, const char *, Sql_condition::enum_severity_level *level,
    const char *message) {
  /* Log error */
  log_purge_error(m_table, message);
  if (*level == Sql_condition::SL_ERROR) m_error = true;
  return true;
}
/* Pop current error handling */
Recycle_scheduler_error_handler::~Recycle_scheduler_error_handler() {
  m_thd->pop_internal_handler();
}

Recycle_scheduler::Recycle_scheduler()
    : m_state(State::INITED), m_scheduler_thd(nullptr) {
  mysql_mutex_init(key_LOCK_state_recycle_scheduler, &m_state_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_state_recycle_scheduler, &m_state_cond);
  mysql_mutex_init(key_LOCK_sleep_recycle_scheduler, &m_sleep_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_sleep_recycle_scheduler, &m_sleep_cond);
}

Recycle_scheduler::~Recycle_scheduler() {
  DBUG_ENTER("Recycle_scheduler::~Recycle_scheduler");
  assert(m_state == State::INITED);
  mysql_mutex_destroy(&m_state_mutex);
  mysql_cond_destroy(&m_state_cond);
  mysql_mutex_destroy(&m_sleep_mutex);
  mysql_cond_destroy(&m_sleep_cond);
  m_scheduler_thd = nullptr;
  DBUG_VOID_RETURN;
}

void Recycle_scheduler::lock_state() { mysql_mutex_lock(&m_state_mutex); }
void Recycle_scheduler::unlock_state() { mysql_mutex_unlock(&m_state_mutex); }

void Recycle_scheduler::lock_sleep() { mysql_mutex_lock(&m_sleep_mutex); }
void Recycle_scheduler::unlock_sleep() { mysql_mutex_unlock(&m_sleep_mutex); }
/**
  Reset the state and m_scheduler_thd variable.
*/
void Recycle_scheduler::reset(bool need_lock) {
  DBUG_ENTER("Recycle_scheduler::reset");
  if (need_lock) lock_state();

  mysql_mutex_assert_owner(&m_state_mutex);
  m_state = State::INITED;
  m_scheduler_thd = nullptr;

  if (need_lock) unlock_state();

  DBUG_VOID_RETURN;
}

/**
  Really drop the recycled table.

  @param[in]        recycle_table       target table

  @retval           false               success
  @retval           true                failure
*/
bool Recycle_scheduler::purge_table(struct Recycle_show_result *recycle_table) {
  const char *table;
  THD *thd;
  DBUG_ENTER("Recycle_scheduler::purge_table");
  table = recycle_table->table.str;
  thd = m_scheduler_thd;

  /* Overide error handling */
  Recycle_scheduler_error_handler error_handler(thd, table);
  Disable_binlog_guard binlog_guard(thd);
  Disable_autocommit_guard autocommit_guard(thd);

  thd->set_query_id(next_query_id());

  mysql_reset_thd_for_next_command(thd);
  lex_start(thd);

  assert(!thd->in_sub_stmt);
  /* Statement transaction still should not be started. */
  assert(thd->get_transaction()->is_empty(Transaction_ctx::STMT));

  assert(!trans_check_state(thd));

  if (trans_commit_implicit(thd)) DBUG_RETURN(true);

  thd->mdl_context.release_transactional_locks();

  bool res = drop_base_recycle_table(thd, table);
  bool result;
  if (res || error_handler.is_error() || thd->killed) {
    result = trans_rollback_stmt(thd);
    result |= trans_rollback_implicit(thd);
  } else {
    result = trans_commit_stmt(thd);
    result |= trans_commit_implicit(thd);
    if (result) trans_rollback(thd);
  }
  close_thread_tables(thd);
  thd->mdl_context.release_transactional_locks();
  if (result) {
    log_purge_error(table, "transaction commit or rollback failed");
  }
  thd->end_statement();
  thd->cleanup_after_query();
  thd->mem_root->ClearForReuse();
  DBUG_RETURN(false);
}

/**
  Check whether recycle scheduler thread is running.

  @ret      true      Running
  @ret      false     Not running
*/
bool Recycle_scheduler::is_running() {
  lock_state();
  bool ret = (m_state == State::RUNNING);
  unlock_state();
  return ret;
}

/* Enter the condition wait stage */
void Recycle_scheduler::enter_cond(THD *thd, struct timespec *abstime,
                                   mysql_mutex_t *mutex, mysql_cond_t *cond,
                                   const PSI_stage_info *stage,
                                   const char *src_func, const char *src_file,
                                   uint src_line) {
  DBUG_ENTER("Recycle_scheduler::enter_cond");
  if (thd)
    thd->enter_cond(cond, mutex, stage, NULL, src_func, src_file, src_line);
  if (!abstime)
    mysql_cond_wait(cond, mutex);
  else
    mysql_cond_timedwait(cond, mutex, abstime);

  if (thd) {
    mysql_mutex_unlock(mutex);
    thd->exit_cond(NULL, src_func, src_file, src_line);
    mysql_mutex_lock(mutex);
  }
  DBUG_VOID_RETURN;
}
/**
   Stop the recycle scheduler task

   @retval       true          failure
   @retval       false         success
*/
bool Recycle_scheduler::stop() {
  THD *thd = current_thd;
  DBUG_ENTER("Recycle_scheduler::stop");
  struct timespec abs_timeout;

  lock_state();

  if (m_state == State::INITED) goto end;

  if (m_state == State::STOPPING) {
    while (m_state == State::STOPPING) {
      set_timespec(&abs_timeout, 5);
      enter_cond(thd, &abs_timeout, &m_state_mutex, &m_state_cond,
                 &waiting_for_recycle_scheduler_thread_stop, __func__, __FILE__,
                 __LINE__);
    }
    goto end;
  }
  do {
    m_state = State::STOPPING;

    /* Wake the recycle scheduler thread */
    mysql_mutex_lock(&m_scheduler_thd->LOCK_thd_data);
    m_scheduler_thd->awake(THD::KILL_CONNECTION);
    mysql_mutex_unlock(&m_scheduler_thd->LOCK_thd_data);

    enter_cond(thd, NULL, &m_state_mutex, &m_state_cond,
               &waiting_for_recycle_scheduler_thread_stop, __func__, __FILE__,
               __LINE__);
  } while (m_state == State::STOPPING);

end:
  unlock_state();
  DBUG_RETURN(false);
}

/**
  Init the context in new thread.

  @param[in]    thd     scheduler thread
*/
void static post_init_recycle_thread(THD *thd) {
  DBUG_ENTER("post_init_recycle_thread");
  thd->store_globals();
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd_manager->add_thd(thd);
  thd_manager->inc_thread_running();
  DBUG_VOID_RETURN;
}

/**
  Deinit the scheduler thread context when stop

  @param[in]    thd     scheduler thread
*/
void static deinit_recycle_thread(THD *thd) {
  DBUG_ENTER("deinit_recycle_thread");
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  thd->set_proc_info("Clearing");
  thd->get_protocol_classic()->end_net();
  thd->release_resources();
  thd_manager->remove_thd(thd);
  thd_manager->dec_thread_running();
  delete thd;
  DBUG_VOID_RETURN;
}

/**
  Convert the YYMMDDHHMMSS number to epoch time.
*/
static my_time_t get_epoch_time(ulonglong nr) {
  MYSQL_TIME t_mysql_time;
  bool not_used;

  /* Convert longlong to MYSQL_TIME (UTC) */
  my_longlong_to_datetime_with_warn(nr, &t_mysql_time, MYF(0));

  /* Convert MYSQL_TIME to epoch time */
  return my_tz_OFFSET0->TIME_to_gmt_sec(&t_mysql_time, &not_used);
}

/**
  Wakeup the sleep recycle scheduler.
*/
void Recycle_scheduler::wakeup() {
  recycle_scheduler_loop_version++;
  if (is_running()) {
    lock_sleep();
    mysql_cond_broadcast(&m_sleep_cond);
    unlock_sleep();
  }
}
/**
  Run the background thread to purge the recycled table.

  @param[in]    thd     scheduler thread
*/
void Recycle_scheduler::run(THD *thd) {
  my_time_t curr_time_sec;
  struct timespec abs_timeout;
  DBUG_ENTER("Recycle_scheduler::run");
  LogErr(SYSTEM_LEVEL, ER_RECYCLE_SCHEDULER, "background thread start");

  std::vector<Recycle_show_result *> container;
  MEM_ROOT mem_root(key_memory_recycle, MEM_ROOT_BLOCK_SIZE);
  while (is_running()) {
    thd_proc_info(thd, "Recycle scheduler collect tables");
    ulonglong lc_version = recycle_scheduler_loop_version;
    ulonglong lc_retention = recycle_bin_retention;
    curr_time_sec = my_micro_time() / 1000000;
    ulonglong min_future = ULLONG_MAX;

    /* Retrieve all the recycled tables */
    get_recycle_tables(thd, &mem_root, &container);

    /**
      Next round time calculation rule:
        1. Wait to minimum recycled time if any recycled table.
        2. Wait recycle_scheduler_interval if not any recycled table.
        3. Wake up if modify the recycle_bin_retention.
    */
    for (auto it = container.cbegin(); it != container.cend(); it++) {
      if (is_running()) {
        long long delta = curr_time_sec - get_epoch_time((*it)->recycled_time);

        /* If the keeped time has exceeded retention, then purge it. */
        if (delta >= 0 && static_cast<ulonglong>(delta) >= lc_retention) {
          if (recycle_scheduler_purge_table_print) {
            std::stringstream ss;
            ss << "purge table " << (*it)->table.str;
            LogErr(INFORMATION_LEVEL, ER_RECYCLE_BIN, ss.str().c_str());
          }
          purge_table(*it);
        } else {
          /* Get the next round least wait time */
          ulonglong lc_future = lc_retention - delta;
          min_future = min_future > lc_future ? lc_future : min_future;
        }
      } else {
        break;
      }
    }
    container.clear();
    mem_root.Clear();
    thd_proc_info(thd, "Recycle scheduler waiting for next round");

    /** Maybe awaked by stop command here, so instead of my_sleep() */
    if (min_future == ULLONG_MAX)
      set_timespec(&abs_timeout, recycle_scheduler_interval);
    else
      set_timespec(&abs_timeout, min_future);

    lock_sleep();
    if (!thd->is_killed() && (lc_version == recycle_scheduler_loop_version)) {
      enter_cond(thd, &abs_timeout, &m_sleep_mutex, &m_sleep_cond,
                 &stage_waiting_for_next_round, __func__, __FILE__, __LINE__);
    }
    unlock_sleep();
  }
  lock_state();
  deinit_recycle_thread(thd);
  reset(false);
  mysql_cond_broadcast(&m_state_cond);
  unlock_state();

  LogErr(SYSTEM_LEVEL, ER_RECYCLE_SCHEDULER, "background thread end");
  DBUG_VOID_RETURN;
}

/**
  Init the recycle scheduler thd context:

  1) Allow infinite access privileges
  2) Host is my_localhost
  3) Net is null (unnecessary for background thread)
*/
void static pre_init_recycle_thread(THD *thd) {
  DBUG_ENTER("pre_init_recycle_thread");
  thd->security_context()->set_master_access(~0ULL);
  thd->security_context()->set_host_or_ip_ptr((const char *)my_localhost,
                                              strlen(my_localhost));
  thd->get_protocol_classic()->init_net(NULL);
  thd->security_context()->set_user_ptr(C_STRING_WITH_LEN("recycle_scheduler"));
  thd->slave_thread = 0;
  thd->system_thread = SYSTEM_THREAD_RECYCLE_SCHEDULER;

  /* DAEMON didn't react to kill */
  thd->set_command(COM_DAEMON);
  thd->set_new_thread_id();
  thd->set_proc_info("Initialized");
  thd->set_time();

  thd->variables.lock_wait_timeout = LONG_TIMEOUT;

  thd->variables.transaction_read_only = false;
  thd->tx_read_only = false;
  DBUG_VOID_RETURN;
}
/**
  Recycle scheduler backgroud thread run logic.

  @param[in]      arg       recycle scheduler parameters
*/
static void *recycle_scheduler_thread(void *arg) {
  struct Recycle_scheduler_param *rs_param = nullptr;
  THD *thd = nullptr;
  Recycle_scheduler *scheduler = nullptr;
  DBUG_ENTER("recycle_scheduler_thread");

  rs_param = static_cast<struct Recycle_scheduler_param *>(arg);
  thd = rs_param->thd;
  scheduler = rs_param->scheduler;
  destroy_object<struct Recycle_scheduler_param>(rs_param);

  if (my_thread_init()) {
    LogErr(ERROR_LEVEL, ER_RECYCLE_SCHEDULER, "init new thread");
    thd->set_proc_info("Clearing");
    thd->get_protocol_classic()->end_net();
    scheduler->reset(true);
    delete thd;
    DBUG_RETURN(0);
  }

  thd->thread_stack = (char *)&thd;
  mysql_thread_set_psi_id(thd->thread_id());

  post_init_recycle_thread(thd);

  /* Run the recycle purge task */
  scheduler->run(thd);

  my_thread_end();
  DBUG_RETURN(0);
}
/**
  Start the recycle scheduler thread, Only log error message if failed.

  @retval       true          failure
  @retval       false         success
*/
bool Recycle_scheduler::start() {
  bool res = false;
  THD *new_thd = nullptr;
  my_thread_handle thread;
  struct Recycle_scheduler_param *rs_param = nullptr;
  DBUG_ENTER("Recycle_scheduler::start");
  lock_state();

  /* Maybe running or stopping already */
  if (m_state > State::INITED) goto end;

  if (!(new_thd = new THD)) {
    LogErr(ERROR_LEVEL, ER_RECYCLE_SCHEDULER, "create new thd");
    res = true;
    goto end;
  }

  pre_init_recycle_thread(new_thd);
  rs_param = allocate_recycle_object<struct Recycle_scheduler_param>();
  rs_param->thd = new_thd;
  rs_param->scheduler = this;
  m_scheduler_thd = new_thd;

  m_state = State::RUNNING;

  if (mysql_thread_create(key_thread_recycle_scheduler, &thread,
                          &connection_attrib, recycle_scheduler_thread,
                          (void *)rs_param)) {
    LogErr(ERROR_LEVEL, ER_RECYCLE_SCHEDULER, "create new thread");
    new_thd->set_proc_info("Clearing");
    new_thd->get_protocol_classic()->end_net();
    reset(false);
    delete new_thd;
    res = true;
    destroy_object<struct Recycle_scheduler_param>(rs_param);
  }

end:
  unlock_state();
  DBUG_RETURN(res);
}

} /* namespace recycle_bin */
} /* namespace im */
