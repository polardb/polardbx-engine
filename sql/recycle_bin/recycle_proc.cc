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

#include "sql/recycle_bin/recycle_proc.h"
#include "sql/auth/auth_acls.h"
#include "sql/recycle_bin/recycle.h"
#include "sql/recycle_bin/recycle_table.h"
#include "sql/sql_time.h"
#include "sql/tztime.h"

namespace im {

namespace recycle_bin {

/* Uniform schema name for recycle bin */
LEX_CSTRING RECYCLE_BIN_PROC_SCHEMA = {STRING_WITH_LEN("dbms_recycle")};


/* Singleton instance for show_tables */
Proc *Recycle_proc_show::instance() {
  static Proc *proc = new Recycle_proc_show(key_memory_recycle);

  return proc;
}

/* Singleton instance for purge_table */
Proc *Recycle_proc_purge::instance() {
  static Proc *proc = new Recycle_proc_purge(key_memory_recycle);

  return proc;
}

/**
  Evoke the sql_cmd object for show_tables() proc.
*/
Sql_cmd *Recycle_proc_show::evoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for purge_table() proc.
*/
Sql_cmd *Recycle_proc_purge::evoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}
/**
  Show the tables in recycle_bin

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_recycle_proc_show::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_recycle_proc_show::pc_execute");
  DBUG_RETURN(false);
}

/**
  Convert the YYMMDDHHMMSS number to MYSQL_TIME.
*/
static void get_local_time(THD *thd, ulonglong nr, MYSQL_TIME *timestamp) {
  MYSQL_TIME t_mysql_time;
  bool not_used;
  Time_zone *tz = thd->variables.time_zone;

  /* Convert longlong to MYSQL_TIME (UTC) */
  my_longlong_to_datetime_with_warn(nr, &t_mysql_time, MYF(0));

  my_time_t t_time;
  /* Convert MYSQL_TIME to epoch time */
  t_time = my_tz_OFFSET0->TIME_to_gmt_sec(&t_mysql_time, &not_used);

  /* Convert epoch time to local time */
  tz->gmt_sec_to_TIME(timestamp, t_time);
}

/**
  Calculate future time by adding xxx seconds.
*/
static bool get_future_time(THD *thd, MYSQL_TIME *timestamp,
                            ulonglong seconds) {
  Interval interval;
  memset(&interval, 0, sizeof(Interval));
  interval.second = seconds;
  return date_add_interval_with_warn(thd, timestamp, INTERVAL_SECOND, interval);
}

/**
  Query all recycle tables.
*/
void Sql_cmd_recycle_proc_show::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  std::vector<Recycle_show_result *> container;
  DBUG_ENTER("Sql_cmd_recycle_proc_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  if (get_recycle_tables(thd, thd->mem_root, &container)) DBUG_VOID_RETURN;

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto it = container.cbegin(); it != container.cend(); it++) {
    MYSQL_TIME timestamp;

    protocol->start_row();
    protocol->store_string((*it)->schema.str, (*it)->schema.length,
                           system_charset_info);
    protocol->store_string((*it)->table.str, (*it)->table.length,
                           system_charset_info);
    protocol->store_string((*it)->origin_schema.str,
                           (*it)->origin_schema.length,
                           system_charset_info);
    protocol->store_string((*it)->origin_table.str, (*it)->origin_table.length,
                           system_charset_info);

    get_local_time(thd, (*it)->recycled_time, &timestamp);
    protocol->store_datetime(timestamp, 0);

    if (get_future_time(thd, &timestamp, recycle_bin_retention))
      protocol->store_null();
    else
      protocol->store_datetime(timestamp, 0);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }
  my_eof(thd);
  DBUG_VOID_RETURN;
}

/* Override the default send result. */
void Sql_cmd_recycle_proc_purge::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_recycle_proc_purge::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }
  /** Ok protocal package has been set within mysql_rm_table(); */
  DBUG_VOID_RETURN;
}

/* Override the default check_access. */
bool Sql_cmd_recycle_proc_purge::check_access(THD *thd) {
  char buff[128];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  DBUG_ENTER("Sql_cmd_recycle_proc_purge::check_access");
  Recycle_process_context recycle_context(thd);
  /* Special privilege flag for ddl on recycle schema */
  thd->recycle_state->set_priv_relax();

  res = (*m_list)[0]->val_str(&str);
  const char *table = strmake_root(thd->mem_root, res->ptr(), res->length());

  Table_ref *table_list =
      build_table_list(thd, RECYCLE_BIN_SCHEMA.str, RECYCLE_BIN_SCHEMA.length,
                       table, strlen(table));
  if (check_table_access(thd, DROP_ACL, table_list, false, 1, false))
    DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Purge table in recycle_bin

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_recycle_proc_purge::pc_execute(THD *thd) {
  char buff[128];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  DBUG_ENTER("Sql_cmd_recycle_proc_purge::pc_execute");
  res = (*m_list)[0]->val_str(&str);
  const char *table = strmake_root(thd->mem_root, res->ptr(), res->length());

  DBUG_RETURN(recycle_purge_table(thd, table));
}

} /* namespace recycle_bin */

} /* namespace im */


