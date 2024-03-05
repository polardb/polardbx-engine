/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "my_config.h"

#ifdef RDS_HAVE_JEMALLOC

#include "jemalloc/jemalloc.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/sql_jemalloc.h"
#include "sql/mysqld.h"       // stage_... mysql_tmpdir

namespace im {

bool opt_rds_active_memory_profiling = false;

const LEX_CSTRING JEMALLOC_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_jemalloc")};

Proc *Jemalloc_profile_proc::instance() {
  static Proc *proc = new Jemalloc_profile_proc();
  return proc;
}

Sql_cmd *Jemalloc_profile_proc::evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

bool Sql_cmd_jemalloc_profile::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_jemalloc_profile::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_jemalloc_profile::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  DBUG_ENTER("Sql_cmd_jemalloc_profile::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  /* check whether jemalloc memory profiling feature is enabled and active */
  if (!opt_rds_active_memory_profiling) {
    protocol->start_row();
    protocol->store_string(STRING_WITH_LEN("Fail"), system_charset_info);
    protocol->store_string(STRING_WITH_LEN("rds_active_memory_profiling is OFF, enable it at first"),
                    system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;

    my_eof(thd);
    DBUG_VOID_RETURN;
  }

  /* generate a memory profiling dump */
  char path[FN_REFLEN];
  const char *dst = path;
  sprintf(path, "%lu_%u_%llu_jeprof.heap", (ulong)getpid(), thd->thread_id(),
          my_micro_time());

  fn_format(path, path, mysql_tmpdir, "", MY_UNPACK_FILENAME);

  if (mallctl("prof.dump", NULL, NULL, &dst, sizeof(const char *))) {
    /* mallctl failed */
    protocol->start_row();
    protocol->store_string(STRING_WITH_LEN("Fail"), system_charset_info);
    protocol->store_string(STRING_WITH_LEN("Failed dumping memory profile"),
                    system_charset_info);

    if (protocol->end_row()) DBUG_VOID_RETURN;

    my_eof(thd);
    DBUG_VOID_RETURN;
  }

  char msg_buf[FN_REFLEN * 2];
  size_t msg_len;
  msg_len = snprintf(msg_buf, sizeof(msg_buf),
                     "A memory profile is dumped to"
                     " '%s'",
                     path);

  protocol->start_row();
  protocol->store_string(STRING_WITH_LEN("OK"), system_charset_info);
  protocol->store_string(msg_buf, msg_len, system_charset_info);

  if (protocol->end_row()) DBUG_VOID_RETURN;

  my_eof(thd);
  DBUG_VOID_RETURN;
}

void jemalloc_profiling_state() {
  char msg[1024];

  /* check whether profiling is enabled. */
  bool is_enabled = false;
  size_t var_len = sizeof(bool);

  mallctl("opt.prof", &is_enabled, &var_len, NULL, 0);

  /* check whether profiling is active */
  bool is_active = false;
  var_len = sizeof(bool);
  mallctl("prof.active", &is_active, &var_len, NULL, 0);

  if (is_enabled) {
    snprintf(msg, sizeof(msg), "%s %s", "enabled",
             is_active ? "and activated" : "but deactivated");
  } else {
    snprintf(msg, sizeof(msg), "%s", "disabled");
  }

  opt_rds_active_memory_profiling = (is_enabled && is_active);

  LogErr(INFORMATION_LEVEL, ER_JEMALLOC_PROFILING_STATE, msg);
}

bool check_active_memory_profiling(sys_var *, THD *thd, set_var *var) {
  /* check whether profiling is enabled */
  bool is_enabled = false;
  size_t var_len = sizeof(bool);
  mallctl("opt.prof", &is_enabled, &var_len, NULL, 0);

  /* check whether profiling is already actived */
  bool is_active = false;
  var_len = sizeof(bool);
  mallctl("prof.active", &is_active, &var_len, NULL, 0);

  /* can not proceed if jemalloc prof is not enabled */
  if (!is_enabled) {
    push_warning(
        thd, Sql_condition::SL_WARNING, ER_WRONG_VALUE_FOR_VAR,
        "Jemalloc is not enabled, set MALLOC_CONF ENV and restart mysqld "
        "(such as export MALLOC_CONF=\"prof:true,prof_active:true\")");
    return true;
  }

  /* active memory profiling */
  if (var->save_result.ulonglong_value) {
    if (is_active) {
      push_warning(thd, Sql_condition::SL_WARNING, ER_WRONG_VALUE_FOR_VAR,
                   "Jemalloc memory profiling is already activated.");
      return false;
    }

    bool active = true;
    if (mallctl("prof.active", NULL, NULL, &active, sizeof(bool))) {
      push_warning(thd, Sql_condition::SL_WARNING, ER_WRONG_VALUE_FOR_VAR,
                   "Failed activating jemalloc memory profiling.");
      return true;
    }
  } else {
    /* deactive jemalloc memory profiling */
    if (!is_active) {
      push_warning(thd, Sql_condition::SL_WARNING, ER_WRONG_VALUE_FOR_VAR,
                   "Jemalloc memory profiling is already deactivated.");

      return false;
    }

    bool active = false;
    if (mallctl("prof.active", NULL, NULL, &active, sizeof(bool))) {
      push_warning(thd, Sql_condition::SL_WARNING, ER_WRONG_VALUE_FOR_VAR,
                   "Failed deactivating jemalloc memory profiling.");
      return true;
    }
  }

  return false;
}

} /* namespace im */

#endif /* RDS_HAVE_JEMALLOC */


