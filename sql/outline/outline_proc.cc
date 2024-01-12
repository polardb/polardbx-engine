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

#include "sql/outline/outline_proc.h"
#include "sql/derror.h"  // ER_THD
#include "sql/outline/outline.h"
#include "sql/outline/outline_digest.h"
#include "sql/outline/outline_table.h"
#include "sql/outline/outline_table_common.h"

namespace im {

LEX_CSTRING OUTLINE_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_outln")};

/* Singleton instance for add_optimizer_outline */
Proc *Outline_optimizer_proc_add::instance() {
  static Proc *proc = new Outline_optimizer_proc_add(key_memory_outline);

  return proc;
}

/* Singleton instance for add_index_outline */
Proc *Outline_index_proc_add::instance() {
  static Proc *proc = new Outline_index_proc_add(key_memory_outline);

  return proc;
}

/* Singleton instance for del_outline */
Proc *Outline_proc_del::instance() {
  static Proc *proc = new Outline_proc_del(key_memory_outline);

  return proc;
}

/* Singleton instance for flush_outline */
Proc *Outline_proc_flush::instance() {
  static Proc *proc = new Outline_proc_flush(key_memory_outline);

  return proc;
}

/* Singleton instance for show_outline */
Proc *Outline_proc_show::instance() {
  static Proc *proc = new Outline_proc_show(key_memory_outline);

  return proc;
}

/* Singleton instance for preview_outline */
Proc *Outline_proc_preview::instance() {
  static Proc *proc = new Outline_proc_preview(key_memory_outline);

  return proc;
}

/**
  Evoke the sql_cmd object for add_optimizer_outline() proc.
*/
Sql_cmd *Outline_optimizer_proc_add::evoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for add_index_outline() proc.
*/
Sql_cmd *Outline_index_proc_add::evoke_cmd(THD *thd,
                                           mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for del_outline() proc.
*/
Sql_cmd *Outline_proc_del::evoke_cmd(THD *thd,
                                     mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for flush_outline() proc.
*/
Sql_cmd *Outline_proc_flush::evoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for show_outline() proc.
*/
Sql_cmd *Outline_proc_show::evoke_cmd(THD *thd,
                                      mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Evoke the sql_cmd object for preview_outline() proc.
*/
Sql_cmd *Outline_proc_preview::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}
/**
  Create record from parameters.

  @param[in]      thd       Thread context
  @param[in]      list      Parameters

  @retval         record    Outline record
*/
Outline_record *Sql_cmd_optimizer_outline_proc_add::get_record(THD *thd) {
  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  Outline_record *record = new (thd->mem_root) Outline_record();

  /* This outline type must be 'OPTIMIZER' */
  record->type = Outline_type::OPTIMIZER;
  record->scope = outline_scopes[0];

  /* schema */
  res = (*m_list)[0]->val_str(&str);
  record->schema =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* digest */
  res = (*m_list)[1]->val_str(&str);
  record->digest =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* position */
  record->pos = (*m_list)[2]->val_int();

  /* hint */
  res = (*m_list)[3]->val_str(&str);
  record->hint =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* sql */
  res = (*m_list)[4]->val_str(&str);
  alloc_query(thd, res->ptr(), res->length(), record->query);

  return record;
}

/**
  Create record from parameters.

  @param[in]      thd       Thread context
  @param[in]      list      Parameters

  @retval         record    Outline record
*/
Outline_record *Sql_cmd_index_outline_proc_add::get_record(THD *thd) {
  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  Outline_record *record = new (thd->mem_root) Outline_record();

  /* schema */
  res = (*m_list)[0]->val_str(&str);
  record->schema =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* digest */
  res = (*m_list)[1]->val_str(&str);
  record->digest =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* position */
  record->pos = (*m_list)[2]->val_int();

  /* type */
  res = (*m_list)[3]->val_str(&str);
  const char *type_str = strmake_root(thd->mem_root, res->ptr(), res->length());
  record->type = to_outline_type(type_str);

  /* index */
  res = (*m_list)[4]->val_str(&str);
  record->hint =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));

  /* scope */
  res = (*m_list)[5]->val_str(&str);
  const char *scope_str =
      strmake_root(thd->mem_root, res->ptr(), res->length());
  record->scope = to_outline_scope(scope_str);

  /* sql */
  res = (*m_list)[6]->val_str(&str);
  alloc_query(thd, res->ptr(), res->length(), record->query);

  return record;
}

/**
  Get the parameters from proc and create new record object

  @param[in]        thd         thread context

  @retval           record      Outline attributes object
*/
Outline_record *Sql_cmd_outline_proc_preview::get_record(THD *thd) {
  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  Outline_record *record = new (thd->mem_root) Outline_record();
  /* schema */
  res = (*m_list)[0]->val_str(&str);
  record->schema =
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length()));
  /* sql */
  res = (*m_list)[1]->val_str(&str);
  alloc_query(thd, res->ptr(), res->length(), record->query);

  return record;
}
/**
  Compute digest according to query statement.

  1) Compute digest if supply sql text.
  2) Confirm the generated digest and digest parameter.
  3) Use the digest directly if not supply sql text.

  Report error if failed.

  @param[in]      thd               Thread context
  @param[in]      record            Outline object
  @param[out]     digest_truncated  Whether truncate sql text when generate
                                    digest.

  @retval         true             Failure
  @retval         false            Success
*/
static bool outline_compute_digest(THD *thd, Outline_record *record,
                                   bool *digest_truncated) {
  char buff1[64];
  char buff2[1024];
  DBUG_ENTER("outline_compute_digest");
  assert(!blank(record->query));

  String digest(buff1, sizeof(buff1), system_charset_info);
  String digest_text(buff2, sizeof(buff2), system_charset_info);

  /* Compute the digest and digest text by sql text. */
  bool error =
      generate_statement_digest(thd, record->schema, record->query, &digest,
                                &digest_text, digest_truncated);

  if (error) {
    if (!thd->is_error()) {
      my_error(ER_OUTLINE_DIGEST_COMPUTE, MYF(0), record->query);
    }
    DBUG_RETURN(true);
  }

  /* Comfirm the generated digest is consistent with parameter if supplied */
  char *gen_digest = strmake_root(thd->mem_root, digest.ptr(), digest.length());
  if (!blank(record->digest) && strcmp(record->digest.str, gen_digest)) {
    my_error(ER_OUTLINE_DIGEST_MISMATCH, MYF(0), record->digest.str,
             gen_digest);
    DBUG_RETURN(true);
  }
  /* Save the digest and digest text */
  record->digest = to_lex_cstring(gen_digest);
  record->digest_text = to_lex_cstring(
      strmake_root(thd->mem_root, digest_text.ptr(), digest_text.length()));
  DBUG_RETURN(false);
}

/**
  Add the optimizer outline into the mysql.outline table,
  and copy into outline cache.

  First write into table, then update the outline cache,
  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_optimizer_outline_proc_add::pc_execute(THD *thd) {
  bool error = false;
  Outline_record *record;
  DBUG_ENTER("Sql_cmd_optimizer_outline_proc_add::pc_execute");
  record = get_record(thd);
  /* Parse the sql and compute sql digest */
  if (!blank(record->query)) {
    bool digest_truncated = false;
    if ((error = outline_compute_digest(thd, record, &digest_truncated)))
      DBUG_RETURN(true);
    /** IF truncated sql text when generate digest,
        report warning or error. */
    if (digest_truncated) {
      if (thd->variables.outline_allowed_sql_digest_truncate) {
        push_warning_printf(
            thd, Sql_condition::SL_WARNING, ER_OUTLINE_SQL_DIGEST_TRUNCATED,
            ER_THD(thd, ER_OUTLINE_SQL_DIGEST_TRUNCATED), record->query);
      } else {
        my_error(ER_OUTLINE_SQL_DIGEST_TRUNCATED, MYF(0), record->query);
        DBUG_RETURN(true);
      }
    }
  }

  /* Parse the optimizer hint and check validation */
  LEX_CSTRING hint =
      to_lex_cstring(thd->strmake(record->hint.str, record->hint.length));

  PT_hint_list *hint_list = parse_optimizer_hint(thd, hint);

  if (hint_list == nullptr) {
    my_error(ER_OUTLINE_OPTIMIZER_HINT_PARSE, MYF(0), hint);
    DBUG_RETURN(true);
  }

  const char *msg = "unknown";
  if (!record->check_valid(&msg)) {
    my_error(ER_OUTLINE_INVALID, MYF(0), 0, "add optimizer outline", msg);
    DBUG_RETURN(true);
  }
  error = add_outline(thd, (Conf_record *)(record));
  DBUG_RETURN(error);
}

/**
  Add the index outline into the mysql.outline table,
  and copy into outline cache.

  First write into table, then update the outline cache,
  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_index_outline_proc_add::pc_execute(THD *thd) {
  bool error = false;
  Outline_record *record;
  DBUG_ENTER("Sql_cmd_ccl_proc_add::pc_execute");
  record = get_record(thd);

  /* Parse the sql and compute sql digest */
  if (!blank(record->query)) {
    bool digest_truncated = false;
    if ((error = outline_compute_digest(thd, record, &digest_truncated)))
      DBUG_RETURN(true);

    /** IF truncated sql text when generate digest,
        report warning or error. */
    if (digest_truncated) {
      if (thd->variables.outline_allowed_sql_digest_truncate) {
        push_warning_printf(
            thd, Sql_condition::SL_WARNING, ER_OUTLINE_SQL_DIGEST_TRUNCATED,
            ER_THD(thd, ER_OUTLINE_SQL_DIGEST_TRUNCATED), record->query);
      } else {
        my_error(ER_OUTLINE_SQL_DIGEST_TRUNCATED, MYF(0), record->query);
        DBUG_RETURN(true);
      }
    }
  }

  const char *msg = "unknown";
  if (!record->check_valid(&msg)) {
    my_error(ER_OUTLINE_INVALID, MYF(0), 0, "add index outline", msg);
    DBUG_RETURN(true);
  }
  error = add_outline(thd, (Conf_record *)(record));
  DBUG_RETURN(error);
}
/**
  Delete the outlinefrom cache and table mysql.outline.

  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_outline_proc_del::pc_execute(THD *thd) {
  bool error;
  Conf_record *record;
  DBUG_ENTER("Sql_cmd_outline_proc_del::pc_execute");
  record = new (thd->mem_root) Outline_record();
  record->set_id((*m_list)[0]->val_int());
  if ((error = del_outline(thd, record))) DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Clear the cache and read outlines from mysql.outline table,
  and copy into cache.

  report my_error if failed.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_outline_proc_flush::pc_execute(THD *thd) {
  Conf_error error;
  DBUG_ENTER("Sql_cmd_outline_proc_flush::pc_execute");
  /* It should already report client error if failed. */
  if ((error = reload_outlines(thd)) != Conf_error::CONF_OK) DBUG_RETURN(true);

  DBUG_RETURN(false);
}

/**
  Show the outlines in cache

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_outline_proc_show::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_outline_proc_show::pc_execute");
  DBUG_RETURN(false);
}

/**
  Preview a query outline

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_outline_proc_preview::pc_execute(THD *) {
  DBUG_ENTER("Sql_cmd_outline_proc_preview::pc_execute");
  DBUG_RETURN(false);
}
void Sql_cmd_outline_proc_show::send_result(THD *thd, bool error) {
  Protocol *protocol = thd->get_protocol();
  Outline_show_result_container results(key_memory_outline);
  DBUG_ENTER("Sql_cmd_outline_proc_show::send_result");
  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }

  System_outline::instance()->aggregate_outline(&results);

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto it = results.cbegin(); it != results.cend(); it++) {
    Outline_show_result &result = const_cast<Outline_show_result &>(it->second);
    protocol->start_row();
    protocol->store(result.id);
    protocol->store_string(result.schema.c_str(), result.schema.length(),
                           system_charset_info);
    protocol->store_string(result.digest.c_str(), result.digest.length(),
                           system_charset_info);
    protocol->store(outline_type_str[static_cast<size_t>(result.type)],
                    system_charset_info);
    protocol->store_string(result.scope.str.str, result.scope.str.length,
                           system_charset_info);
    protocol->store(result.pos);
    protocol->store_string(result.hint.c_str(), result.hint.length(),
                           system_charset_info);
    protocol->store(result.stats.hit);
    protocol->store(result.stats.overflow);
    protocol->store_string(result.digest_text.c_str(),
                           result.digest_text.length(), system_charset_info);

    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);
  DBUG_VOID_RETURN;
}

/**
  Override the default send result;
  Try to parse the query statement and invoke the outlines.

  @param[in]        thd       thread context
  @param[in]        error     pc_execute() result
*/
void Sql_cmd_outline_proc_preview::send_result(THD *thd, bool error) {
  Outline_preview_result_container container;
  Protocol *protocol = thd->get_protocol();
  Outline_record *record;
  DBUG_ENTER("Sql_cmd_outline_proc_preview::send_result");

  if (error) {
    assert(thd->is_error());
    DBUG_VOID_RETURN;
  }
  record = get_record(thd);
  if (preview_statement_outline(thd, record->schema, record->query, &container))
    DBUG_VOID_RETURN;

  if (m_proc->send_result_metadata(thd)) DBUG_VOID_RETURN;

  for (auto it = container.cbegin(); it != container.cend(); it++) {
    Outline_preview_result *result = *it;
    protocol->start_row();
    protocol->store_string(result->schema.c_str(), result->schema.length(),
                           system_charset_info);
    protocol->store_string(result->digest.c_str(), result->digest.length(),
                           system_charset_info);
    protocol->store_string(result->block_type.c_str(),
                           result->block_type.length(), system_charset_info);
    protocol->store_string(result->block_name.c_str(),
                           result->block_name.length(), system_charset_info);
    protocol->store(result->block);
    protocol->store_string(result->hint_text.c_str(),
                           result->hint_text.length(), system_charset_info);
    if (protocol->end_row()) DBUG_VOID_RETURN;
  }

  my_eof(thd);

  /* Free all the String_outline object */
  for (auto it = container.cbegin(); it != container.cend(); it++) {
    Outline_preview_result *result = *it;
    result->~Outline_preview_result();
  }
  container.clear();
  DBUG_VOID_RETURN;
}

} /* namespace im */
