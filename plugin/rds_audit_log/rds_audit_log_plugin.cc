/* Copyright (c) 2018, Alibaba Cloud and/or its affiliates. All rights reserved.

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

#include <mysql/plugin.h>
#include <mysql/plugin_audit.h>
#include <mysql/psi/mysql_cond.h>
#include <mysql/psi/mysql_file.h>
#include <mysql/psi/mysql_memory.h>
#include <mysql/psi/mysql_mutex.h>
#include <mysql/psi/mysql_rwlock.h>
#include <mysql/psi/mysql_thread.h>

#include <mysql/components/my_service.h>
#include <mysql/components/services/log_builtins.h>
#include <mysqld_error.h>

#include <sql/sql_class.h>

#include "audit_log.h"
#include "lex_string.h"
#include "typelib.h"

/* For logging service. Module plugin need to define these variables,
rds_audit_log is a statically compiled plugin, so we don't need these.*/
// static SERVICE_TYPE(registry) *reg_srv = nullptr;
// SERVICE_TYPE(log_builtins) *log_bi = nullptr;
// SERVICE_TYPE(log_builtins_string) *log_bs = nullptr;

/* Always define the memory key */
PSI_memory_key key_memory_audit_thd_buf;
PSI_memory_key key_memory_audit_log_buf;

/* clang-format off */
#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info rds_audit_all_memory[] = {
    {&key_memory_audit_thd_buf, "thd_event_buffer", 0, 0, PSI_DOCUMENT_ME},
    {&key_memory_audit_log_buf, "log_buffer", 0, 0, PSI_DOCUMENT_ME}};

PSI_rwlock_key key_rwlock_audit_lock_log;
PSI_rwlock_key key_rwlock_audit_lock_file;

static PSI_rwlock_info rds_audit_all_rwlocks[] = {
    {&key_rwlock_audit_lock_log, "LOCK_log", 0, 0, PSI_DOCUMENT_ME},
    {&key_rwlock_audit_lock_file, "LOCK_file", 0, 0, PSI_DOCUMENT_ME}};

PSI_mutex_key key_mutex_audit_flush_sleep;
PSI_mutex_key key_mutex_audit_buf_full_sleep;

static PSI_mutex_info rds_audit_all_mutexes[] = {
    {&key_mutex_audit_flush_sleep, "flush_sleep_mutex", 0, 0, PSI_DOCUMENT_ME},
    {&key_mutex_audit_buf_full_sleep, "buf_full_sleep_mutex", 0, 0,
     PSI_DOCUMENT_ME}};

PSI_cond_key key_cond_audit_flush_sleep;
PSI_cond_key key_cond_audit_buf_full_sleep;

static PSI_cond_info rds_audit_all_conds[] = {
    {&key_cond_audit_flush_sleep, "flush_sleep::cond", 0, 0, PSI_DOCUMENT_ME},
    {&key_cond_audit_buf_full_sleep, "buf_full_sleep::cond", 0, 0,
     PSI_DOCUMENT_ME}};

PSI_thread_key key_thread_audit_flush;

static PSI_thread_info rds_audit_all_threads[] = {
    {&key_thread_audit_flush, "flush_thread", "", 0, 0, PSI_DOCUMENT_ME}};

static void register_all_audit_psi_keys(void) {
  const char *category = "rds_audit";
  int count;

  count = static_cast<int>(array_elements(rds_audit_all_memory));
  mysql_memory_register(category, rds_audit_all_memory, count);

  count = static_cast<int>(array_elements(rds_audit_all_rwlocks));
  mysql_rwlock_register(category, rds_audit_all_rwlocks, count);

  count = static_cast<int>(array_elements(rds_audit_all_mutexes));
  mysql_mutex_register(category, rds_audit_all_mutexes, count);

  count = static_cast<int>(array_elements(rds_audit_all_conds));
  mysql_cond_register(category, rds_audit_all_conds, count);

  count = static_cast<int>(array_elements(rds_audit_all_threads));
  mysql_thread_register(category, rds_audit_all_threads, count);

}
#endif /* HAVE_PSI_INTERFACE */

static ulong rds_audit_log_event_buffer_size = 0;
static ulong rds_audit_log_buffer_size = 0;
static char *rds_audit_log_dir = nullptr;
static ulong rds_audit_log_format = 0;
static ulong rds_audit_log_version = 0;
static bool rds_audit_log_enabled = false;
static ulong rds_audit_log_row_limit = 0;
static ulong rds_audit_log_strategy = 0;
static bool rds_audit_log_flush = false;
static ulong rds_audit_log_policy = 0;
static ulong rds_audit_log_connection_policy = 0;
static ulong rds_audit_log_statement_policy = 0;

static MYSQL_THDVAR_BOOL(skip, PLUGIN_VAR_RQCMDARG,
    "Whether to skip log recording for current session.", nullptr, nullptr, 0);

static MYSQL_SYSVAR_ULONG(event_buffer_size,
    rds_audit_log_event_buffer_size,
    PLUGIN_VAR_RQCMDARG, "Buffer size for event buffer.",
    nullptr, nullptr, 2048, 0, ULONG_MAX, 1);

static void audit_log_buffer_size_update(THD *thd MY_ATTRIBUTE((unused)),
                                         SYS_VAR *var MY_ATTRIBUTE((unused)),
                                         void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->resize_log_buf(new_value);
}

static MYSQL_SYSVAR_ULONG(buffer_size,
    rds_audit_log_buffer_size, PLUGIN_VAR_RQCMDARG,
    "Buffer size for log buffer.",
    nullptr, audit_log_buffer_size_update, 8 * 1024 * 1024UL, 0, ULONG_MAX, 1);

static MYSQL_SYSVAR_STR(dir,
    rds_audit_log_dir,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY | PLUGIN_VAR_NOPERSIST,
    "Path to store RDS audit logs. Logs will be put under tmpdir is "
    "not specified.", nullptr, nullptr, nullptr);

static void audit_log_row_limit_update(THD *thd MY_ATTRIBUTE((unused)),
                                       SYS_VAR *var MY_ATTRIBUTE((unused)),
                                       void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_row_limit(new_value);
}

static MYSQL_SYSVAR_ULONG(row_limit,
    rds_audit_log_row_limit, PLUGIN_VAR_RQCMDARG,
    "Max number of rows in each audit log file. Log records will be discarded "
    "above this number.",
    nullptr, audit_log_row_limit_update, 100000, 0, ULONG_MAX, 1);

static const char *log_format_names[] = {"PLAIN", "JSON", NullS};
static TYPELIB audit_log_format_typelib = {array_elements(log_format_names) - 1,
                                           "", log_format_names, nullptr};

static MYSQL_SYSVAR_ENUM(format,
    rds_audit_log_format, PLUGIN_VAR_RQCMDARG,
    "The format of audit log, currently only support PLAIN (compatible with "
    "MySQL_V1 in RDS MySQL 5.6), JSON will be supported soon.",
    nullptr, nullptr, MYSQL_RDS_AUDIT_LOG::PLAIN, &audit_log_format_typelib);

/* PolorDB 8.0 supports MYSQL_V2, we don't */
const char *log_version_names[] = {"MYSQL_V1", "MYSQL_V3", NullS};
static TYPELIB audit_log_version_typelib = {array_elements(log_version_names) - 1,
                                           "", log_version_names, NULL};

static void audit_log_version_update(THD *thd MY_ATTRIBUTE((unused)),
                                    SYS_VAR *var MY_ATTRIBUTE((unused)),
                                    void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_log_version(
      (MYSQL_RDS_AUDIT_LOG::enum_log_version)new_value);
}

static MYSQL_SYSVAR_ENUM(version,
    rds_audit_log_version, PLUGIN_VAR_RQCMDARG,
    "The version of audit log, currently support MYSQL_V1 and MYSQL_V3.",
    NULL, audit_log_version_update,
    MYSQL_RDS_AUDIT_LOG::MYSQL_V1, &audit_log_version_typelib);

static void rds_audit_log_flush_update(THD *thd MY_ATTRIBUTE((unused)),
                                       SYS_VAR *var MY_ATTRIBUTE((unused)),
                                       void *var_ptr MY_ATTRIBUTE((unused)),
                                       const void *save) {
  /* This option is used as a flushing command, so the var_ptr is not updated
  and kept as OFF. */
  bool rotate = *static_cast<const bool *>(save);
  if (rotate) {
    rds_audit_log->rotate_log_file();
  }
}

static MYSQL_SYSVAR_BOOL(flush,
    rds_audit_log_flush,
    PLUGIN_VAR_NOPERSIST | PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_NODEFAULT,
    "When set to enabled, the current log file will be closed, and a new log "
    "file will be created.", nullptr, rds_audit_log_flush_update, 0);

static void rds_audit_log_enabled_update(THD *thd MY_ATTRIBUTE((unused)),
                                         SYS_VAR *var MY_ATTRIBUTE((unused)),
                                         void *var_ptr, const void *save) {
  bool old_value = *static_cast<bool *>(var_ptr);
  bool new_value = *static_cast<const bool *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<bool *>(var_ptr) = new_value;

  rds_audit_log->set_enable(new_value);
}

static MYSQL_SYSVAR_BOOL(enabled,
    rds_audit_log_enabled, PLUGIN_VAR_NOPERSIST | PLUGIN_VAR_RQCMDARG,
    "The option to enable or disable audit log.",
    nullptr, rds_audit_log_enabled_update, 0);

const char *log_strategy_names[] = {
    "ASYNCHRONOUS", "PERFORMANCE", "SEMISYNCHRONOUS", "SYNCHRONOUS", NullS};

static TYPELIB audit_log_strategy_typelib = {
    array_elements(log_strategy_names) - 1, "", log_strategy_names, nullptr};

static void audit_log_strategy_update(THD *thd MY_ATTRIBUTE((unused)),
                                      SYS_VAR *var MY_ATTRIBUTE((unused)),
                                      void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_log_strategy(
      (MYSQL_RDS_AUDIT_LOG::enum_log_strategy)new_value);
}

static MYSQL_SYSVAR_ENUM(strategy,
    rds_audit_log_strategy, PLUGIN_VAR_RQCMDARG,
    "The audit log strategy, currently support ASYNCHRONOUS, "
    "PERFORMANCE, SEMISYNCHRONOUS and SYNCHRONOUS.",
    nullptr, audit_log_strategy_update, MYSQL_RDS_AUDIT_LOG::ASYNCHRONOUS,
    &audit_log_strategy_typelib);

const char *log_policy_names[] = {
    "ALL", "LOGINS", "QUERIES", "NONE", NullS};

static TYPELIB audit_log_policy_typelib = {
    array_elements(log_policy_names) - 1, "", log_policy_names, nullptr};

static void audit_log_policy_update(THD *thd MY_ATTRIBUTE((unused)),
                                    SYS_VAR *var MY_ATTRIBUTE((unused)),
                                    void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_log_policy(
      (MYSQL_RDS_AUDIT_LOG::enum_log_policy)new_value);
}

static MYSQL_SYSVAR_ENUM(policy,
    rds_audit_log_policy, PLUGIN_VAR_RQCMDARG,
    "The policy controlling how the audit log plugin writes events to its "
    "log file. Supported values are 'ALL' (default), 'LOGINS', 'QUERIES' "
    "and 'NONE'.",
    nullptr, audit_log_policy_update, MYSQL_RDS_AUDIT_LOG::LOG_ALL,
    &audit_log_policy_typelib);

const char *log_connection_policy_names[] = {
    "ALL", "ERRORS", "NONE", NullS};

static TYPELIB audit_log_connection_policy_typelib = {
    array_elements(log_connection_policy_names) - 1, "",
    log_connection_policy_names, nullptr};

static void audit_log_connection_policy_update(THD *thd MY_ATTRIBUTE((unused)),
                                               SYS_VAR *var MY_ATTRIBUTE((unused)),
                                               void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_log_connection_policy(
      (MYSQL_RDS_AUDIT_LOG::enum_log_connection_policy)new_value);
}

static MYSQL_SYSVAR_ENUM(connection_policy,
    rds_audit_log_connection_policy, PLUGIN_VAR_RQCMDARG,
    "The policy controlling how the audit log plugin writes connection "
    "events to its log file. Supported values are 'ALL' (default), 'ERRORS' "
    "and 'NONE'.",
    nullptr, audit_log_connection_policy_update, MYSQL_RDS_AUDIT_LOG::CONN_ALL,
    &audit_log_connection_policy_typelib);

const char *log_statement_policy_names[] = {
    "ALL", "UPDATES", "UPDATES_OR_ERRORS", "ERRORS", "NONE", NullS};

static TYPELIB audit_log_statement_policy_typelib = {
    array_elements(log_statement_policy_names) - 1, "",
    log_statement_policy_names, nullptr};

static void audit_log_statement_policy_update(THD *thd MY_ATTRIBUTE((unused)),
                                              SYS_VAR *var MY_ATTRIBUTE((unused)),
                                              void *var_ptr, const void *save) {
  ulong old_value = *static_cast<ulong *>(var_ptr);
  ulong new_value = *static_cast<const ulong *>(save);

  if (old_value == new_value) {
    return;
  }

  *static_cast<ulong *>(var_ptr) = new_value;

  rds_audit_log->set_log_statement_policy(
      (MYSQL_RDS_AUDIT_LOG::enum_log_statement_policy)new_value);
}

static MYSQL_SYSVAR_ENUM(statement_policy,
    rds_audit_log_statement_policy, PLUGIN_VAR_RQCMDARG,
    "The policy controlling how the audit log plugin writes query "
    "events to its log file. Supported values are 'ALL' (default), 'UPDATES', "
    "'UPDATES_OR_ERRORS', 'ERRORS' and 'NONE'.",
    nullptr, audit_log_statement_policy_update, MYSQL_RDS_AUDIT_LOG::STMT_ALL,
    &audit_log_statement_policy_typelib);

static SYS_VAR *audit_log_system_vars[] = {
    MYSQL_SYSVAR(skip),
    MYSQL_SYSVAR(event_buffer_size),
    MYSQL_SYSVAR(buffer_size),
    MYSQL_SYSVAR(dir),
    MYSQL_SYSVAR(row_limit),
    MYSQL_SYSVAR(flush),
    MYSQL_SYSVAR(enabled),
    MYSQL_SYSVAR(format),
    MYSQL_SYSVAR(version),
    MYSQL_SYSVAR(strategy),
    MYSQL_SYSVAR(policy),
    MYSQL_SYSVAR(connection_policy),
    MYSQL_SYSVAR(statement_policy),
    nullptr};

static uint audit_log_get_filename(MYSQL_THD, SHOW_VAR *var, char *) {
  var->type = SHOW_CHAR;
  if (rds_audit_log)
    var->value = rds_audit_log->get_log_filename();
  else
    var->value = empty_c_string;

  return 0;
}

static uint audit_log_get_row_num(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(rds_audit_log->get_row_num());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_last_row_num(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(rds_audit_log->get_last_row_num());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_buffer_written_pos(MYSQL_THD, SHOW_VAR *var,
                                             char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(rds_audit_log->get_buffered_offset());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_buffer_flushed_pos(MYSQL_THD, SHOW_VAR *var,
                                             char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(rds_audit_log->get_flushed_offset());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_file_pos(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(rds_audit_log->get_file_pos());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_lost_row_buf(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_lost_row_num_by_buf_full());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_lost_row_buf_total(MYSQL_THD, SHOW_VAR *var,
                                             char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(
        rds_audit_log->get_lost_row_num_by_buf_full_total());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_lost_row_limit(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_lost_row_num_by_row_limit());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_lost_row_limit_total(MYSQL_THD, SHOW_VAR *var,
                                               char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value = static_cast<long long>(
        rds_audit_log->get_lost_row_num_by_row_limit_total());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_total_size(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_total_size());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_event_max_drop_size(MYSQL_THD,
                                                  SHOW_VAR *var,
                                                  char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_event_max_drop_size());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_events(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_events());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_events_filtered(MYSQL_THD, SHOW_VAR *var,
                                              char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_events_filtered());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_events_lost(MYSQL_THD, SHOW_VAR *var, char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_events_lost());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_events_written(MYSQL_THD, SHOW_VAR *var,
                                             char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_events_written());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_write_waits(MYSQL_THD, SHOW_VAR *var,
                                          char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_write_waits());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_file_writes(MYSQL_THD, SHOW_VAR *var,
                                          char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_file_writes());
  else
    *value = 0;

  return 0;
}

static uint audit_log_get_log_file_syncs(MYSQL_THD, SHOW_VAR *var,
                                         char *buf) {
  var->type = SHOW_LONGLONG;
  var->value = buf;
  long long *value = reinterpret_cast<long long *>(buf);
  if (rds_audit_log)
    *value =
        static_cast<long long>(rds_audit_log->get_log_file_syncs());
  else
    *value = 0;

  return 0;
}

static SHOW_VAR audit_log_status_vars[] = {
    {"rds_audit_log_filename", (char *)&audit_log_get_filename, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_curr_row", (char *)&audit_log_get_row_num, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_last_row", (char *)&audit_log_get_last_row_num, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_buffer_written_pos",
     (char *)&audit_log_get_buffer_written_pos, SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_buffer_flushed_pos",
     (char *)&audit_log_get_buffer_flushed_pos, SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_file_pos", (char *)&audit_log_get_file_pos, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_lost_row_by_buf_full", (char *)&audit_log_get_lost_row_buf,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_lost_row_by_buf_full_total",
     (char *)&audit_log_get_lost_row_buf_total, SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_lost_row_by_row_limit",
     (char *)&audit_log_get_lost_row_limit, SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_lost_row_by_row_limit_total",
     (char *)&audit_log_get_lost_row_limit_total, SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_current_size", (char *)&audit_log_get_file_pos,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_event_max_drop_size",
     (char *)&audit_log_get_log_event_max_drop_size,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_events", (char *)&audit_log_get_log_events,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_events_filtered",
     (char *)&audit_log_get_log_events_filtered,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_events_lost", (char *)&audit_log_get_log_events_lost,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_events_written", (char *)&audit_log_get_log_events_written,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_total_size", (char *)&audit_log_get_log_total_size,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_write_waits", (char *)&audit_log_get_log_write_waits,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_file_writes", (char *)&audit_log_get_log_file_writes,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {"rds_audit_log_file_syncs", (char *)&audit_log_get_log_file_syncs,
     SHOW_FUNC, SHOW_SCOPE_GLOBAL},

    {0, 0, SHOW_UNDEF, SHOW_SCOPE_GLOBAL}};
/* clang-format on */

/* Initialize the plugin at server start or plugin installation.
@return 0 success 1 failure */
static int audit_log_plugin_init(void *arg MY_ATTRIBUTE((unused))) {
  /* Initialize error logging service. */
  // if (init_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs)) return
  // (1);

#ifdef HAVE_PSI_INTERFACE
  register_all_audit_psi_keys();
#endif

  rds_audit_log = new MYSQL_RDS_AUDIT_LOG(
      rds_audit_log_dir, rds_audit_log_buffer_size, rds_audit_log_row_limit,
      rds_audit_log_format, rds_audit_log_strategy, rds_audit_log_policy,
      rds_audit_log_connection_policy, rds_audit_log_statement_policy,
      rds_audit_log_version, rds_audit_log_enabled);

  /* Failed to create flush thread */
  if (rds_audit_log->create_flush_thread()) {
    return (1);
  }

  return (0);
}

/* Terminate the plugin at server shutdown or plugin deinstallation. */
static int audit_log_plugin_deinit(void *arg MY_ATTRIBUTE((unused))) {
  // deinit_logging_service_for_plugin(&reg_srv, &log_bi, &log_bs);

  rds_audit_log->stop_flush_thread();
  delete rds_audit_log;
  rds_audit_log = nullptr;
  return (0);
}

/*
  @brief Plugin function handler.

  @param [in] thd         Connection context.
  @param [in] event_class Event class value.
  @param [in] event       Event data.

  @retval Value indicating, whether the server should abort continuation
          of the current operation.
*/
static int audit_log_notify(MYSQL_THD thd, mysql_event_class_t event_class,
                            const void *event) {
  /* Note: rds_audit_log->is_enabled() is a dirty of rds_audit_log->enabled */
  if (!rds_audit_log->is_enabled() || (bool)THDVAR(thd, skip)) {
    return 0;
  }

  /* This is the 1st time to write audit log for current THD, or the buffer
  is freed by release_thd(). */
  if (thd_get_rds_audit_event_buf(thd) == nullptr) {
    LEX_STRING *event_buf = (LEX_STRING *)my_malloc(
        key_memory_audit_thd_buf,
        sizeof(LEX_STRING) + rds_audit_log_event_buffer_size, MYF(MY_FAE));

    event_buf->length = rds_audit_log_event_buffer_size;
    event_buf->str = (char *)(event_buf + 1);
    thd_set_rds_audit_event_buf(thd, event_buf);
  }

  LEX_STRING *event_buf = thd_get_rds_audit_event_buf(thd);
  char *buf = event_buf->str;
  size_t buf_len = event_buf->length;

  rds_audit_log->process_event(event_class, event, buf, buf_len);

  return 0;
}

static void audit_log_release(MYSQL_THD thd) {
  LEX_STRING *event_buf = thd_get_rds_audit_event_buf(thd);
  if (event_buf) {
    my_free(event_buf);
    thd_set_rds_audit_event_buf(thd, nullptr);
  }
}

/*
  Plugin type-specific descriptor

  rds_audit_log plugin is intrested in these types of events:
  1. MYSQL_AUDIT_RDS_CONNECTION_CLASS
  2. MYSQL_AUDIT_RDS_QUERY_CLASS
*/
static struct st_mysql_audit audit_log_descriptor = {
    MYSQL_AUDIT_INTERFACE_VERSION, /* interface version    */
    audit_log_release,             /* release_thd function */
    audit_log_notify,              /* notify function      */
    {0,                            /* MYSQL_AUDIT_GENERAL_CLASS */
     0,                            /* MYSQL_AUDIT_CONNECTION_CLASS */
     0,                            /* MYSQL_AUDIT_PARSE_CLASS */
     0,                            /*
                                     MYSQL_AUDIT_AUTHORIZATION_CLASS, This event class
                                     is currently not supported.
                                   */
     0,                            /* MYSQL_AUDIT_TABLE_ACCESS_CLASS */
     0,                            /* MYSQL_AUDIT_GLOBAL_VARIABLE_CLASS */
     0,                            /* MYSQL_AUDIT_SERVER_STARTUP_CLASS */
     0,                            /* MYSQL_AUDIT_SERVER_SHUTDOWN_CLASS */
     0,                            /* MYSQL_AUDIT_COMMAND_CLASS */
     0,                            /* MYSQL_AUDIT_QUERY_CLASS */
     0,                            /* MYSQL_AUDIT_STORED_PROGRAM_CLASS */
     0,                            /* MYSQL_AUDIT_AUTHENTICATION_CLASS */
     0,                            /* MYSQL_AUDIT_MESSAGE_CLASS */
     /* MYSQL_AUDIT_RDS_CONNECTION_CLASS */
     (unsigned long)MYSQL_AUDIT_RDS_CONNECTION_ALL,
     /* MYSQL_AUDIT_RDS_QUERY_CLASS */
     (unsigned long)MYSQL_AUDIT_RDS_QUERY_ALL}};

/* RDS audit log plugin declaration */
mysql_declare_plugin(rds_audit_log){
    MYSQL_AUDIT_PLUGIN,      /* type                            */
    &audit_log_descriptor,   /* descriptor                      */
    "RDS_AUDIT_LOG",         /* name                            */
    "Fungo Wang",            /* author                          */
    "RDS audit log",         /* description                     */
    PLUGIN_LICENSE_GPL,      /* license                         */
    audit_log_plugin_init,   /* init function (when loaded)     */
    nullptr,                 /* check uninstall function        */
    audit_log_plugin_deinit, /* deinit function (when unloaded) */
    0x0010,                  /* version (0.1)                   */
    audit_log_status_vars,   /* status variables                */
    audit_log_system_vars,   /* system variables                */
    nullptr,                 /* reserved                        */
    0,                       /* flags                           */
} mysql_declare_plugin_end;
