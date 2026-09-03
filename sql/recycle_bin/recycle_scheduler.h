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

#ifndef SQL_RECYCLE_BIN_RECYCLE_SCHEDULER_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_SCHEDULER_INCLUDED

#include "mysql/components/services/bits/mysql_cond_bits.h"
#include "mysql/components/services/bits/mysql_mutex_bits.h"
#include "mysql/components/services/bits/psi_cond_bits.h"
#include "mysql/components/services/bits/psi_memory_bits.h"
#include "mysql/components/services/bits/psi_mutex_bits.h"
#include "mysql/components/services/bits/psi_thread_bits.h"
#include "sql/common/component.h"
#include "sql/error_handler.h"

class THD;

namespace im {
namespace recycle_bin {

extern PSI_mutex_key key_LOCK_state_recycle_scheduler;

extern PSI_cond_key key_COND_state_recycle_scheduler;

extern PSI_mutex_key key_LOCK_sleep_recycle_scheduler;

extern PSI_cond_key key_COND_sleep_recycle_scheduler;

extern PSI_memory_key key_memory_recycle;

extern PSI_thread_key key_thread_recycle_scheduler;

extern PSI_stage_info waiting_for_recycle_scheduler_thread_stop;

extern PSI_stage_info stage_waiting_for_next_round;

/* Recycle scheduler thread loop interval (seconds). */
extern ulonglong recycle_scheduler_interval;
/* Whether run recycle scheduler thread */
extern bool opt_recycle_scheduler;
/* Whether print the detailed information when purge table */
extern bool recycle_scheduler_purge_table_print;
/* Maximum number of consecutive skip rounds before forced execution */
extern ulonglong recycle_scheduler_max_skip_rounds;

/* recycle object allocator */
template <typename T, typename... Args>
T *allocate_recycle_object(Args &&...args) {
  return allocate_object<T, Args...>(key_memory_recycle,
                                     std::forward<Args>(args)...);
}

/** Recycle scheduler error handling. */
class Recycle_scheduler_error_handler : public Internal_error_handler {
 public:
  Recycle_scheduler_error_handler(THD *thd, const char *table);
  virtual bool handle_condition(THD *, uint sql_errno, const char *,
                                Sql_condition::enum_severity_level *,
                                const char *message) override;

  ~Recycle_scheduler_error_handler() override;

  bool is_error() { return m_error; }

 private:
  THD *m_thd;
  const char *m_table;
  bool m_error;
};

/**
  Init the recycle scheduler thd context:

  1) Allow infinite access privileges
  2) Host is my_localhost
  3) Net is null (unnecessary for background thread)
*/
void pre_init_recycle_scheduler(THD *thd);
/**
  Init the context in new thread.

  @param[in]    thd     scheduler thread
*/
bool post_init_recycle_scheduler(THD *thd);

/**
  Deinit the scheduler thread context when stop

  @param[in]    thd     scheduler thread
*/
void deinit_recycle_scheduler(THD *thd);

class Recycle_scheduler {
 public:
  Recycle_scheduler();

  virtual ~Recycle_scheduler();

  enum class State { INITED, RUNNING, STOPPING };

  void lock_state();
  void unlock_state();

  void lock_sleep();
  void unlock_sleep();
  /**
    Start the recycle scheduler thread, Only log error message if failed.

    @retval       true          failure
    @retval       false         success
  */
  bool start();
  /**
     Stop the recycle scheduler task

     @retval       true          failure
     @retval       false         success
  */
  bool stop();
  /**
    Run the background thread to purge the recycled table.
  */
  void run(THD *thd);

  void enter_cond(THD *thd, struct timespec *abstime, mysql_mutex_t *mutex,
                  mysql_cond_t *cond, const PSI_stage_info *stage,
                  const char *src_func, const char *src_file, uint src_line);

  State state() { return m_state; }
  /**
    Check whether recycle scheduler thread is running.

    @ret      true      Running
    @ret      false     Not running
  */
  bool is_running();

  /**
    Reset the state and scheduler_thd variable.
  */
  void reset(bool need_lock);

  /**
    Really drop the recycled table.

    @param[in]        recycle_table       target table

    @retval           false               success
    @retval           true                failure
  */
  bool purge_table(struct Recycle_show_result *recycle_table);

  /**
    Purge expired tables and oversized tables in the recycle bin.

    @param thd Thread handle for the current session
    @param mem_root Memory pool
    @param container Vector to store pointers to recycle table information

    @return The minimum time (in seconds) until the next check is needed:
            - If no tables exist, returns ULLONG_MAX
  */
  ulonglong detect_and_purge_recycle_table(THD *thd, MEM_ROOT *mem_root,
                                           std::vector<Recycle_show_result*> *container);

  /**
    Check if the current purge round should be skipped.

    @param[in]  curr_time_sec  Current time in seconds
    @retval     true       Should skip this round
    @retval     false      Should proceed with purge
  */
  bool should_skip_purge_round(ulonglong curr_time_sec);

  /**
    Wakeup the sleep recycle scheduler.
  */
  void wakeup();

  static Recycle_scheduler *m_recycle_scheduler;
  static Recycle_scheduler *instance() { return m_recycle_scheduler; }

 private:
  State m_state;
  THD *m_scheduler_thd;
  mysql_cond_t m_state_cond;
  mysql_mutex_t m_state_mutex;

  mysql_mutex_t m_sleep_mutex;
  mysql_cond_t m_sleep_cond;

  /* Local version number to track changes since last cleanup */
  ulonglong m_last_cleanup_version;

  /* Earliest expiration time (recycle_time + retention) of tables in recycle bin */
  /* This represents the earliest time when any table needs to be purged */
  ulonglong m_earliest_expiration_time;

  /* Count of consecutive skip rounds since last purge execution */
  ulonglong m_skip_count;
};

struct Recycle_scheduler_param {
  THD *thd;
  Recycle_scheduler *scheduler;
};

} /* namespace recycle_bin */
} /* namespace im */

#endif
