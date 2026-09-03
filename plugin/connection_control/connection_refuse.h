/* Copyright (c) 2016, 2022, Oracle and/or its affiliates.

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

#ifndef CONNECTION_REFUSE_H
#define CONNECTION_REFUSE_H

#include <atomic>

#include <chrono>
#include "my_hostname.h" /* HOSTNAME_LENGTH */
#include "my_inttypes.h"
#include "plugin/connection_control/connection_control_data.h" /* variables and status */
#include "plugin/connection_control/connection_control_interfaces.h" /* Observer interface */
#include "plugin/connection_control/connection_control_memory.h" /* Connection_control_alloc */
#include "plugin/connection_control/connection_refuse_api.h" /* Constants */
#include "sql/table.h"                                       /* Table_ref */

namespace connection_control {

static constexpr int64 MIN_TIME_STAMP =
    std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::time_point::min().time_since_epoch())
        .count();

static constexpr int64 MAX_TIME_STAMP =
    std::chrono::duration_cast<std::chrono::milliseconds>(
        std::chrono::system_clock::time_point::max().time_since_epoch())
        .count();

/** Group of ip addresses for the user@host */
class Connection_refuse_event_group {
  using Ip = std::string;

 private:
  class Ip_failed_info : public Connection_control_alloc {
   public:
    Ip_failed_info()
        : m_count(0),
          m_failed_time(MIN_TIME_STAMP),
          m_expired_time(MIN_TIME_STAMP) {}

    int64 get_count() const { return m_count; }
    int64 get_failed_time() const { return m_failed_time; }
    int64 get_expired_time() const { return m_expired_time; }

    void refresh(int64 threshold, int64 refuse_period, int64 current_time) {
      if (expired(current_time)) m_count = 0;
      m_count = m_count + 1;
      m_failed_time = current_time;
      m_expired_time = (m_count >= threshold) ? current_time + refuse_period
                                              : MIN_TIME_STAMP;
    }
    bool expired(int64 current_time) {
      return m_expired_time > MIN_TIME_STAMP && m_expired_time < current_time;
    }
    bool is_locked(int64 current_time) {
      return m_expired_time >= current_time;
    }

    ~Ip_failed_info() {}

   private:
    int64 m_count;      /** failed count */
    int64 m_failed_time; /** last failed time (ms) */
    int64 m_expired_time; /** expired time (ms) if not zero. */
  };

 public:
  Connection_refuse_event_group() {}
  Connection_refuse_event_group(const std::string &userhost);

  ~Connection_refuse_event_group();

  void reset_all();

  bool check_and_update_lock_status(const Sql_string &ip, int64 threshold,
                                    int64 refuse_period, bool successful_login);

  void fill_IS_table(Table_ref *tables);

 private:
  /** Hash map of <Ip, Ip_failed_info>. */
  std::unordered_map<Ip, Ip_failed_info> m_entries;
  /** The user@host */
  std::string m_userhost;

  mysql_rwlock_t m_lock;
};

/** A container of all connection event groups */
class Connection_refuse_event_group_by_userhost {
  using Userhost = Sql_string;

 public:
  Connection_refuse_event_group_by_userhost();

  ~Connection_refuse_event_group_by_userhost();

  bool check_and_update_lock_status(const Sql_string &userhost,
                                    const Sql_string &ip, int64 threshold,
                                    int64 refuse_period, bool successful_login);
  void reset_specified_user(const Sql_string &userhost);
  void reset_all();

  /** Fill the whole table. */
  void fill_IS_table(Table_ref *tables);
  /** Fill the table with specific user */
  void fill_IS_table_by_user(Table_ref *tables, const Sql_string &userhost);

 private:
  std::unordered_map<Userhost, Connection_refuse_event_group> m_groups;
  mysql_rwlock_t m_lock;
};

/**
  Connection event action to enforce max failed login constraint
*/

class Connection_refuse_action : public Connection_event_observer,
                                 public Connection_control_alloc {
 public:
  Connection_refuse_action(int64 threshold, int64 refuse_period,
                           opt_connection_control *sys_vars,
                           size_t sys_vars_size,
                           stats_connection_control *status_vars,
                           size_t status_vars_size, mysql_rwlock_t *lock,
                           bool enabled);

  /** Destructor */
  ~Connection_refuse_action() override {
    deinit();
    m_lock = nullptr;
  }

  void init(Connection_event_coordinator_services *coordinator);

  void set_threshold(int64 threshold) { m_threshold.store(threshold); }

  int64 get_threshold() { return m_threshold.load(); }

  void set_refuse_period(int64 refuse_period) {
    m_refuse_period.store(refuse_period);
  }

  int64 get_refuse_period() { return m_refuse_period.load(); }

  void fill_IS_table(THD *thd, Table_ref *tables, Item *cond);

  /** For Connection refuse action, we never handle with connection_event. */
  bool notify_event(MYSQL_THD, Connection_event_coordinator_services *,
                    const mysql_event_connection *, Error_handler *) override;

  bool notify_sys_var(Connection_event_coordinator_services *coordinator,
                      opt_connection_control variable, void *new_value,
                      Error_handler *error_handler) override;

  bool notify_event(Connection_event_coordinator_services *coordinator,
                    const mysql_event_polarx_connection_control *event,
                    Error_handler *error_handler) override;

 private:
  void deinit();

  void make_hash_key(const char *user, const char *host, Sql_string &key);

 private:
  /** Threshold value which triggers wait */
  std::atomic<int64> m_threshold;
  /** Period of time to refuse connections */
  std::atomic<int64> m_refuse_period;
  /** System variables */
  std::vector<opt_connection_control> m_sys_vars;
  /** Status variables */
  std::vector<stats_connection_control> m_stats_vars;
  /** Hash to store failed attempts from different ip for each userhost */
  Connection_refuse_event_group_by_userhost m_userhost_hash;
  /** RW lock */
  mysql_rwlock_t *m_lock;
};
}  // namespace connection_control
#endif /* !CONNECTION_DELAY_H */
