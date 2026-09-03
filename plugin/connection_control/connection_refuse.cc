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

#include "plugin/connection_control/connection_refuse.h"

#include <mysql/psi/mysql_thread.h>
#include <time.h>

#include "m_ctype.h" /* my_charset_bin */
#include "my_compiler.h"
#include "my_dbug.h"
#include "my_systime.h"
#include "mysqld_error.h"
#include "plugin/connection_control/connection_control.h"
#include "plugin/connection_control/security_context_wrapper.h"
#include "sql/current_thd.h" /* current_thd */
#include "sql/item_cmpfunc.h"
#include "sql/sql_class.h" /* THD, Security context */

/* Forward declaration */
bool schema_table_store_record(THD *thd, TABLE *table);

struct st_mysql_information_schema connection_control_refuse_statistic_view = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

/** Fields of information_schema.connection_control_refuse_statistic */
static ST_FIELD_INFO refuse_statistic_view_fields[] = {
    {"USERHOST", USERNAME_LENGTH + HOSTNAME_LENGTH + 6, MYSQL_TYPE_STRING, 0,
     MY_I_S_UNSIGNED, nullptr, 0},
    {"IP", HOSTNAME_LENGTH, MYSQL_TYPE_STRING, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {"FAILED_ATTEMPTS", 16, MYSQL_TYPE_LONG, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {"STATUS", 16, MYSQL_TYPE_STRING, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {"LAST_FAILED_TIME", 32, MYSQL_TYPE_STRING, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {"EXPIRED_TIME", 32, MYSQL_TYPE_STRING, 0, MY_I_S_UNSIGNED, nullptr, 0},
    {nullptr, 0, MYSQL_TYPE_NULL, 0, 0, nullptr, 0}};

static const LEX_CSTRING STATUS_STR[] = {{C_STRING_WITH_LEN("UNLOCKED")},
                                         {C_STRING_WITH_LEN("LOCKED")}};

namespace connection_control {
/** variables used by connection_refuse.cc */
static mysql_rwlock_t connection_event_refuse_lock;

static opt_connection_control opt_enums[] = {OPT_FAILED_CONNECTIONS_THRESHOLD,
                                             OPT_REFUSE_CONNECTION_PERIOD};

static size_t opt_enums_size = 2;

static stats_connection_control status_vars_enums[] = {
    STAT_CONNECTION_REFUSE_TRIGGERED};

static size_t status_vars_enums_size = 1;

static Connection_refuse_action *g_max_failed_connection_handler = nullptr;

Sql_string I_S_CONNECTION_CONTROL_REFUSE_STATISTIC_USERHOST(
    "information_schema.connection_control_refuse_statistic.userhost");

static int time_to_str(const int64 ms, char *str) {
  std::time_t time = ms / 1000;
  std::tm tm;
  MYSQL_TIME to;
  to.time_type = MYSQL_TIMESTAMP_DATETIME;

  /** Thread safe */
  localtime_r(&time, &tm);
  localtime_to_TIME(&to, &tm);
  return my_datetime_to_str(to, str, 0);
}

Connection_refuse_event_group::Connection_refuse_event_group(
    const std::string &userhost)
    : m_userhost(userhost) {
  mysql_rwlock_init(key_connection_event_refuse_lock, &m_lock);
}

Connection_refuse_event_group::~Connection_refuse_event_group() {
  reset_all();
  mysql_rwlock_destroy(&m_lock);
}

void Connection_refuse_event_group::reset_all() {
  DBUG_TRACE;

  WR_lock wr_lock(&m_lock);
  m_entries.clear();
}

bool Connection_refuse_event_group::check_and_update_lock_status(
    const Sql_string &ip, int64 threshold, int64 refuse_period,
    bool successful_login) {
  DBUG_TRACE;

  int64 current_time;

  /** Optimistic search, most of time should not block */
  {
    RD_lock lock(&m_lock);
    auto it = m_entries.find(ip);
    current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    if (likely(successful_login && it == m_entries.end())) {
      return false;
    }
    if (it != m_entries.end() && it->second.is_locked(current_time)) {
      return true;
    }
  }

  {
    WR_lock wr_lock(&m_lock);

    auto it = m_entries.find(ip);
    current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                       std::chrono::system_clock::now().time_since_epoch())
                       .count();

    /** exist and locked, do nothing */
    if (unlikely(it != m_entries.end() && it->second.is_locked(current_time))) {
      return true;
    }

    if (!successful_login) {
      m_entries[ip].refresh(threshold, refuse_period, current_time);
    } else if (it != m_entries.end()) {
      m_entries.erase(it);
    }
    return false;
  }
}

void Connection_refuse_event_group::fill_IS_table(Table_ref *tables) {
  DBUG_TRACE;
  THD *thd = current_thd;
  TABLE *table = tables->table;
  std::vector<Ip> expired_entries;
  int64 current_time;

  mysql_rwlock_rdlock(&m_lock);
  char time_buffer[128];
  current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                     std::chrono::system_clock::now().time_since_epoch())
                     .count();

  for (auto &e : m_entries) {
    if (e.second.expired(current_time)) {
      expired_entries.emplace_back(e.first);
      continue;
    }

    table->field[0]->store(m_userhost.c_str(), m_userhost.length(),
                           system_charset_info);
    table->field[1]->store(e.first.c_str(), e.first.length(),
                           system_charset_info);
    table->field[2]->store(e.second.get_count(), true);

    int locked = e.second.is_locked(current_time);
    table->field[3]->store(STATUS_STR[locked].str, STATUS_STR[locked].length,
                           system_charset_info);

    int len = time_to_str(e.second.get_failed_time(), time_buffer);
    table->field[4]->store(time_buffer, len, system_charset_info);

    len = locked ? time_to_str(e.second.get_expired_time(), time_buffer) : 0;
    table->field[5]->store(time_buffer, len, system_charset_info);

    /** If we fail to store the record, just break. */
    if (schema_table_store_record(thd, table)) break;
  }
  mysql_rwlock_unlock(&m_lock);

  if (unlikely(expired_entries.size() > 0)) {
    {
      WR_lock wr_lock(&m_lock);
      current_time = std::chrono::duration_cast<std::chrono::milliseconds>(
                         std::chrono::system_clock::now().time_since_epoch())
                         .count();
      for (auto &e : expired_entries) {
        auto it = m_entries.find(e);
        if (it != m_entries.end() && it->second.expired(current_time))
          m_entries.erase(it);
      }
    }
  }
}

Connection_refuse_event_group_by_userhost::
Connection_refuse_event_group_by_userhost() {
  mysql_rwlock_init(key_connection_event_refuse_lock, &m_lock);
}

Connection_refuse_event_group_by_userhost::~
Connection_refuse_event_group_by_userhost() {
  reset_all();
  mysql_rwlock_destroy(&m_lock);
}

bool Connection_refuse_event_group_by_userhost::check_and_update_lock_status(
    const Sql_string &userhost, const Sql_string &ip, int64 threshold,
    int64 refuse_period, bool successful_login) {
  RD_lock rd_lock(&m_lock);

  while (unlikely(m_groups.find(userhost) == m_groups.end())) {
    rd_lock.unlock();
    {
      WR_lock wr_lock(&m_lock);
      if (m_groups.find(userhost) == m_groups.end()) {
        m_groups.emplace(userhost, userhost);
      }
    }
    rd_lock.lock();
  }
  return m_groups[userhost].check_and_update_lock_status(
      ip, threshold, refuse_period, successful_login);
}

void Connection_refuse_event_group_by_userhost::reset_specified_user(
    const Sql_string &userhost) {
  RD_lock rd_lock(&m_lock);

  /** We never delete the group, we just clear the entries. */
  if (m_groups.find(userhost) != m_groups.end()) {
    m_groups[userhost].reset_all();
  }
}

void Connection_refuse_event_group_by_userhost::reset_all() {
  RD_lock rd_lock(&m_lock);

  /** We never delete the group, we just clear the entries. */
  for (auto &group : m_groups) {
    group.second.reset_all();
  }
}

void Connection_refuse_event_group_by_userhost::fill_IS_table(
    Table_ref *tables) {
  RD_lock rd_lock(&m_lock);
  for (auto &group : m_groups) {
    group.second.fill_IS_table(tables);
  }
}
void Connection_refuse_event_group_by_userhost::fill_IS_table_by_user(
    Table_ref *tables, const Sql_string &userhost) {
  RD_lock rd_lock(&m_lock);

  if (m_groups.find(userhost) != m_groups.end()) {
    m_groups[userhost].fill_IS_table(tables);
  }
}

/**
  Connection_refuse_action Constructor.

  @param [in] threshold         Defines a threshold after which wait is
  triggered
  @param [in] disable_period    period of refusing connections
  @param [in] sys_vars          System variables
  @param [in] sys_vars_size     Size of sys_vars array
  @param [in] status_vars       Status variables
  @param [in] status_vars_size  Size of status_vars array
  @param [in] lock              RW lock handle
*/

Connection_refuse_action::Connection_refuse_action(
    int64 threshold, int64 refuse_period, opt_connection_control *sys_vars,
    size_t sys_vars_size, stats_connection_control *status_vars,
    size_t status_vars_size, mysql_rwlock_t *lock, bool enabled)
    : Connection_event_observer(enabled),
      m_threshold(threshold),
      m_refuse_period(refuse_period),
      m_lock(lock) {
  if (sys_vars_size) {
    for (uint i = 0; i < sys_vars_size; ++i) m_sys_vars.push_back(sys_vars[i]);
  }

  if (status_vars_size) {
    for (uint i = 0; i < status_vars_size; ++i)
      m_stats_vars.push_back(status_vars[i]);
  }
}

void Connection_refuse_action::make_hash_key(const char *user, const char *host,
                                             Sql_string &key) {
  key.append("'");

  if (user) key.append(user);

  key.append("'@'");

  if (host) key.append(host);

  key.append("'");
}

bool Connection_refuse_action::notify_event(
    MYSQL_THD, Connection_event_coordinator_services *,
    const mysql_event_connection *, Error_handler *) {
  return false;
}

/**
  Notification of a change in system variable value

  @param [in] coordinator        Handle to coordinator
  @param [in] variable           Enum of variable
  @param [in] new_value          New value for variable
  @param [in] error_handler      Error handler object

  @returns processing status
    @retval false  Change in variable value processed successfully
    @retval true Error processing new value.
                  error_buffer may contain more details.
*/
bool Connection_refuse_action::notify_sys_var(
    Connection_event_coordinator_services *coordinator,
    opt_connection_control variable, void *new_value,
    Error_handler *error_handler) {
  DBUG_TRACE;
  bool error = true;
  Connection_event_observer *self = this;

  WR_lock wr_lock(m_lock);

  switch (variable) {
    case OPT_FAILED_CONNECTIONS_THRESHOLD: {
      int64 new_threshold = *(static_cast<int64 *>(new_value));
      assert(new_threshold >= DISABLE_THRESHOLD);
      set_threshold(new_threshold);

      if (new_threshold == DISABLE_THRESHOLD) m_userhost_hash.reset_all();

      if ((error = coordinator->notify_status_var(
               &self, STAT_CONNECTION_REFUSE_TRIGGERED, ACTION_RESET))) {
        error_handler->handle_error(
            ER_CONN_CONTROL_STAT_CONN_REFUSE_TRIGGERED_RESET_FAILED);
      }
      break;
    }
    case OPT_REFUSE_CONNECTION_PERIOD: {
      int64 new_period = *(static_cast<int64 *>(new_value));
      assert(new_period >= DEFAULT_REFUSE_PERIOD);
      set_refuse_period(new_period);

      if (new_period > DISABLE_REFUSE_PERIOD) {
        set_enabled(true);
      } else {
        set_enabled(false);
        m_userhost_hash.reset_all();
      }

      if ((error = coordinator->notify_status_var(
               &self, STAT_CONNECTION_REFUSE_TRIGGERED, ACTION_RESET))) {
        error_handler->handle_error(
            ER_CONN_CONTROL_STAT_CONN_REFUSE_TRIGGERED_RESET_FAILED);
      }

      break;
    }
    default:
      /* Should never reach here. */
      assert(false);
      error_handler->handle_error(ER_CONN_CONTROL_INVALID_CONN_REFUSE_TYPE);
  };
  return error;
}

/**
 *  Handle a connection control event, we only care about the unlock event.
 *
 * @param[in] coordinator Handle to coordinator
 * @param[in] reset_request Reset request
 * @param[in] error_handler Error handler
 */
bool Connection_refuse_action::notify_event(
    Connection_event_coordinator_services *coordinator,
    const mysql_event_polarx_connection_control *event, Error_handler *) {
  DBUG_TRACE;
  bool error = false;
  Connection_event_observer *self = this;
  Sql_string userhost;
  Sql_string ip(event->ip.str, event->ip.length);

  DBUG_EXECUTE_IF("simulate_connection_refuse_event_ip",
                  { ip = "192.168.1.1"; });

  make_hash_key(event->user.str, event->host.str, userhost);

  RD_lock rd_lock(m_lock);
  int threshold = this->get_threshold();
  int refuse_period = this->get_refuse_period();

  /** If feature is disabled, do nothing */
  if (threshold <= DISABLE_THRESHOLD) {
    return error;
  }

  if (event->event_subclass == MYSQL_AUDIT_POLARX_CONNECTION_CONTROL_CHECK) {
    if (m_userhost_hash.check_and_update_lock_status(
            userhost, ip, threshold, refuse_period, event->successful_login)) {
      coordinator->notify_status_var(&self, STAT_CONNECTION_REFUSE_TRIGGERED,
                                     ACTION_INC);
      my_error(ER_CONN_CONTROL_CONN_REFUSE_CONNECTION, MYF(0), userhost.c_str(),
               ip.c_str());
      error = true;
    }
  } else if (event->event_subclass ==
             MYSQL_AUDIT_POLARX_CONNECTION_CONTROL_UNLOCK) {
    m_userhost_hash.reset_specified_user(userhost);
  }
  return error;
}

/**
  Subscribe with coordinator for connection events

  @param [in] coordinator  Handle to Connection_event_coordinator_services
                           for registration
*/
void Connection_refuse_action::init(
    Connection_event_coordinator_services *coordinator) {
  DBUG_TRACE;
  assert(coordinator);
  bool retval;
  Connection_event_observer *subscriber = this;
  WR_lock wr_lock(m_lock);
  retval = coordinator->register_event_subscriber(&subscriber, &m_sys_vars,
                                                  &m_stats_vars);
  assert(!retval);
  if (retval) retval = false; /* Make compiler happy */
}

/**
  Clear data from Connection_refuse_action
*/

void Connection_refuse_action::deinit() {
  mysql_rwlock_wrlock(m_lock);
  m_userhost_hash.reset_all();
  m_sys_vars.clear();
  m_stats_vars.clear();
  m_threshold = DISABLE_THRESHOLD;
  mysql_rwlock_unlock(m_lock);
  m_lock = nullptr;
}

/**
  Get user information from "where userhost = <value>"

  @param [in] cond        Equality condition structure
  @param [out] eq_arg     Sql_string handle to store user information
  @param [in] field_name  userhost field

  @returns whether a value was found or not
    @retval false Found a value. Check eq_arg
    @retval true Error.
*/

static bool get_equal_condition_argument(Item *cond, Sql_string *eq_arg,
                                         const Sql_string &field_name) {
  if (cond != nullptr && cond->type() == Item::FUNC_ITEM) {
    Item_func *func = static_cast<Item_func *>(cond);
    if (func != nullptr && func->functype() == Item_func::EQ_FUNC) {
      Item_func_eq *eq_func = static_cast<Item_func_eq *>(func);
      if (eq_func->arguments()[0]->type() == Item::FIELD_ITEM &&
          my_strcasecmp(system_charset_info,
                        eq_func->arguments()[0]->full_name(),
                        field_name.c_str()) == 0) {
        char buff[1024];
        String *res;
        String filter(buff, sizeof(buff), system_charset_info);
        if (eq_func->arguments()[1] != nullptr &&
            (res = eq_func->arguments()[1]->val_str(&filter))) {
          eq_arg->append(res->c_ptr_safe(), res->length());
          return false;
        }
      }
    }
  }
  return true;
}

/**
  Function to fill information_schema.connection_control_refuse_statistic.

  Handles query with equality condition.
  For full scan, calls Connection_refuse_action::fill_IS_table()

  Permission : SUPER_ACL is required.

  @param [in] thd     THD handle.
  @param [in] tables  Handle to
                      information_schema.connection_control_refuse_statistic.
  @param [in] cond    Condition if any.
*/

void Connection_refuse_action::fill_IS_table(THD *thd, Table_ref *tables,
                                             Item *cond) {
  DBUG_TRACE;
  Security_context_wrapper sctx_wrapper(thd);
  if (!(sctx_wrapper.is_super_user() || sctx_wrapper.is_connection_admin()))
    return;
  RD_lock rd_lock(m_lock);
  Sql_string eq_arg;

  if (cond != nullptr &&
      !get_equal_condition_argument(
          cond, &eq_arg, I_S_CONNECTION_CONTROL_REFUSE_STATISTIC_USERHOST)) {
    m_userhost_hash.fill_IS_table_by_user(tables, eq_arg);
  } else {
    m_userhost_hash.fill_IS_table(tables);
  }
}

/**
  Initializes required objects for handling connection events.

  @param [in] coordinator    Connection_event_coordinator_services handle.
*/

bool init_connection_refuse_event(
    Connection_event_coordinator_services *coordinator,
    Error_handler *error_handler) {
  /*
    1. Initialize lock(s)
  */
  mysql_rwlock_init(key_connection_event_refuse_lock,
                    &connection_event_refuse_lock);
  g_max_failed_connection_handler = new Connection_refuse_action(
      g_variables.failed_connections_threshold,
      g_variables.refuse_connection_period, opt_enums, opt_enums_size,
      status_vars_enums, status_vars_enums_size, &connection_event_refuse_lock,
      g_variables.refuse_connection_period > DISABLE_REFUSE_PERIOD);
  if (!g_max_failed_connection_handler) {
    error_handler->handle_error(ER_CONN_CONTROL_REFUSE_ACTION_INIT_FAILED);
    return true;
  }
  g_max_failed_connection_handler->init(coordinator);

  return false;
}

/**
  Deinitializes objects and frees associated memory.
*/

void deinit_connection_refuse_event() {
  if (g_max_failed_connection_handler) delete g_max_failed_connection_handler;
  g_max_failed_connection_handler = nullptr;
  mysql_rwlock_destroy(&connection_event_refuse_lock);
  return;
}
}  // namespace connection_control

/**
  Function to fill information_schema.connection_control_refuse_statistic.

  @param [in] thd     THD handle.
  @param [in] tables  Handle to
                      information_schema.connection_control_refuse_stats.
  @param [in] cond    Condition if any.

  @returns Always returns false.
*/

int fill_refuse_statistic_view(THD *thd, Table_ref *tables, Item *cond) {
  if (connection_control::g_max_failed_connection_handler)
    connection_control::g_max_failed_connection_handler->fill_IS_table(
        thd, tables, cond);
  return false;
}

/**
  View init function

  @param [in] ptr    Handle to
                     information_schema.connection_control_refuse_statistic.

  @returns Always returns 0.
*/

int connection_control_refuse_statistic_view_init(void *ptr) {
  ST_SCHEMA_TABLE *schema_table = (ST_SCHEMA_TABLE *)ptr;

  schema_table->fields_info = refuse_statistic_view_fields;
  schema_table->fill_table = fill_refuse_statistic_view;
  return 0;
}
