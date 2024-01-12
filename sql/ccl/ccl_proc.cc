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

#include "sql/ccl/ccl_proc.h"
#include "sql/ccl/ccl_common.h"
#include "sql/ccl/ccl_table.h"
#include "sql/ccl/ccl_table_common.h"

/**
  Concurrency control procedures (dbms_ccl)

*/
namespace im {

/* The uniform schema name for ccl */
const LEX_CSTRING CCL_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_ccl")};

/* Singleton instance for add_ccl_rule */
Proc *Ccl_proc_add::instance() {
  static Proc *proc = new Ccl_proc_add(key_memory_ccl);

  return proc;
}

/**
  Evoke the sql_cmd object for add_ccl_rule() proc.
*/
Sql_cmd *Ccl_proc_add::evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/* Singleton instance for flush_ccl_rule */
Proc *Ccl_proc_flush::instance() {
  static Proc *proc = new Ccl_proc_flush(key_memory_ccl);

  return proc;
}

/* Singleton instance for flush_ccl_queue */
Proc *Ccl_proc_flush_queue::instance() {
  static Proc *proc = new Ccl_proc_flush_queue(key_memory_ccl);

  return proc;
}

/**
  Evoke the sql_cmd object for flush_ccl_rule() proc.
*/
Sql_cmd *Ccl_proc_flush::evoke_cmd(THD *thd,
                                   mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for flush_ccl_queue() proc.
*/
Sql_cmd *Ccl_proc_flush_queue::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/* Singleton instance for del_ccl_rule */
Proc *Ccl_proc_del::instance() {
  static Proc *proc = new Ccl_proc_del(key_memory_ccl);

  return proc;
}

/**
  Evoke the sql_cmd object for del_ccl_rule() proc.
*/
Sql_cmd *Ccl_proc_del::evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/* Singleton instance for show_ccl_rule */
Proc *Ccl_proc_show::instance() {
  static Proc *proc = new Ccl_proc_show(key_memory_ccl);

  return proc;
}
/**
  Evoke the sql_cmd object for show_ccl_rule() proc.
*/
Sql_cmd *Ccl_proc_show::evoke_cmd(THD *thd,
                                  mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/* Singleton instance for show_ccl_queue */
Proc *Ccl_proc_show_queue::instance() {
  static Proc *proc = new Ccl_proc_show_queue(key_memory_ccl);

  return proc;
}
/**
  Evoke the sql_cmd object for show_ccl_rule() proc.
*/
Sql_cmd *Ccl_proc_show_queue::evoke_cmd(THD *thd,
                                        mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

static Ccl_record *get_record(THD *thd, mem_root_deque<Item *> *list) {
  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  Ccl_record *record = new (thd->mem_root) Ccl_record();
  /* rule type */
  res = (*list)[0]->val_str(&str);
  const char *type_str = strmake_root(thd->mem_root, res->ptr(), res->length());
  record->type = to_ccl_type(type_str);
  /* schema */
  res = (*list)[1]->val_str(&str);
  record->schema_name = strmake_root(thd->mem_root, res->ptr(), res->length());
  /* table */
  res = (*list)[2]->val_str(&str);
  record->table_name = strmake_root(thd->mem_root, res->ptr(), res->length());
  /* concurrency count */
  record->concurrency_count = (*list)[3]->val_int();
  /* keywords */
  res = (*list)[4]->val_str(&str);
  record->keywords = strmake_root(thd->mem_root, res->ptr(), res->length());

  return record;
}

class Skip_readonly_check_helper {
 public:
  Skip_readonly_check_helper(THD *thd) : m_thd(thd) {
    m_skip_readonly_check = m_thd->is_cmd_skip_readonly();
    m_thd->set_skip_readonly_check();
  }

  ~Skip_readonly_check_helper() {
    if (m_skip_readonly_check)
      m_thd->set_skip_readonly_check();
    else
      m_thd->reset_skip_readonly_check();
  }

 private:
  bool m_skip_readonly_check;
  THD *m_thd;
};

/**
  Add the rule into the concurrency_control table,
  and copy into ccl cache.

  First write into table, then update the ccl cache,
  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_add::pc_execute(THD *thd) {
  bool error = false;
  Conf_record *record;
  DBUG_ENTER("Sql_cmd_ccl_proc_add::pc_execute");
  Skip_readonly_check_helper helper(thd);
  record = get_record(thd, m_list);
  const char *msg = "";
  if (!record->check_valid(&msg)) {
    my_error(ER_CCL_INVALID_RULE, MYF(0), 0, "add rule");
    DBUG_RETURN(true);
  }
  error = add_ccl_rule(thd, record);
  DBUG_RETURN(error);
}

/**
  Clear the rule cache and read rules from mysql.concurrency_control table,
  and copy into ccl cache.

  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_flush::pc_execute(THD *thd) {
  Conf_error error;
  DBUG_ENTER("Sql_cmd_ccl_proc_flush::pc_execute");
  /* It should already report client error if failed. */
  if ((error = reload_ccl_rules(thd)) != Conf_error::CONF_OK) DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Clear the queue buckets and reinit it.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_flush_queue::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_ccl_proc_flush::pc_execute");

  System_ccl::instance()->get_queue_buckets()->init_queue_buckets(
      ccl_queue_bucket_count, ccl_queue_bucket_size,
      Ccl_error_level::CCL_WARNING);

  DBUG_RETURN(false);
}
/**
  Delete the rule from cache and table mysql.concurrency_control.

  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_del::pc_execute(THD *thd) {
  bool error;
  Conf_record *record;
  DBUG_ENTER("Sql_cmd_ccl_proc_flush::pc_execute");
  Skip_readonly_check_helper helper(thd);
  record = new (thd->mem_root) Ccl_record();
  record->set_id((*m_list)[0]->val_int());
  if ((error = del_ccl_rule(thd, record))) DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Show the rule in cache

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_show::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_ccl_proc_show::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_ccl_proc_show::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  std::vector<Ccl_show_result *> results;
  DBUG_ENTER("Sql_cmd_ccl_proc_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  System_ccl::instance()->collect_rules(thd->mem_root, results);

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto it = results.cbegin(); it != results.cend(); it++) {
    Ccl_show_result *result = *it;
    protocol->start_row();
    protocol->store(result->id);
    protocol->store_string(result->type.str, result->type.length,
                           system_charset_info);
    protocol->store_string(result->schema.str, result->schema.length,
                           system_charset_info);
    protocol->store_string(result->table.str, result->table.length,
                           system_charset_info);
    protocol->store_string(result->state.str, result->state.length,
                           system_charset_info);
    protocol->store_string(result->ordered.str, result->ordered.length,
                           system_charset_info);
    protocol->store(result->concurrency_count);
    protocol->store(result->matched);
    protocol->store(result->running);
    protocol->store(result->waiting);
    protocol->store_string(result->keywords.str, result->keywords.length,
                           system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/**
  Show the queue buckets in cache

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_ccl_proc_show_queue::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_ccl_proc_show_queue::pc_execute");
  DBUG_RETURN(false);
}

void Sql_cmd_ccl_proc_show_queue::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  std::vector<Ccl_show_result *> results;
  DBUG_ENTER("Sql_cmd_ccl_proc_show_queue::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  System_ccl::instance()->collect_queue_buckets(thd->mem_root, results);

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto it = results.cbegin(); it != results.cend(); it++) {
    Ccl_show_result *result = *it;
    protocol->start_row();
    protocol->store(result->id);
    protocol->store_string(result->type.str, result->type.length,
                           system_charset_info);
    protocol->store(result->concurrency_count);
    protocol->store(result->matched);
    protocol->store(result->running);
    protocol->store(result->waiting);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}
} /* namespace im */
