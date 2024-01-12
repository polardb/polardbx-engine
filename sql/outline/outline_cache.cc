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

#include "sql/outline/outline.h"

#include "my_macros.h"
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "sql/sql_lex.h"

#include "sql/common/reload.h"
#include "sql/outline/outline_digest.h"
#include "sql/outline/outline_reload.h"

#include <stack>

namespace im {

/* System outline memory usage */
PSI_memory_key key_memory_outline;

/* System outline rwlock psi */
PSI_rwlock_key key_rwlock_outline;

/* System outline structure partition count */
ulonglong outline_partitions = 16;

/* The switch of statement outline */
bool opt_outline_enabled = true;

/* Init the singleton instance opointer when load ELF */
System_outline *System_outline::m_system_outline = nullptr;

static bool outline_inited = false;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_info outline_memory[] = {
    {&key_memory_outline, "im::outline", 0, 0, PSI_DOCUMENT_ME}};

static PSI_rwlock_info outline_rwlocks[] = {
    {&key_rwlock_outline, "RWLOCK_outlines", 0, 0, PSI_DOCUMENT_ME}};
/**
  Init all the outline psi keys
*/
static void init_outline_psi_key() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(outline_memory));
  mysql_memory_register(category, outline_memory, count);

  count = static_cast<int>(array_elements(outline_rwlocks));
  mysql_rwlock_register(category, outline_rwlocks, count);
}
#endif

/* Initialize outline system. */
void outline_init() {
  DBUG_ENTER("outline_init");

#ifdef HAVE_PSI_INTERFACE
  init_outline_psi_key();
#endif
  assert(System_outline::instance() == nullptr);

  System_outline::m_system_outline =
      allocate_outline_object<System_outline>(outline_partitions);

  register_outline_reload_entry();
  outline_inited = true;
  DBUG_VOID_RETURN;
}

/* Destroy outline system. */
void outline_destroy() {
  DBUG_ENTER("outline_destroy");
  assert(System_outline::instance() != nullptr);
  remove_outline_reload_entry();
  destroy_object<System_outline>(System_outline::instance());
  System_outline::m_system_outline = nullptr;
  DBUG_VOID_RETURN;
}

/**
  Flush outline cache if force_clean is true and fill new outlines.

  @param[in]      records         outlines
  @param[in]      force_clean     whether clear cache
*/
void refresh_outline_cache(Conf_records *records, bool force_clean) {
  DBUG_ENTER("refresh_outline_cache");
  assert(outline_inited);
  /**
    In advance clear all outlines since the record
    may be modified through update statement.
  */
  System_outline::instance()->fill_records(records, force_clean);

  DBUG_VOID_RETURN;
}

/**
  Invoke the outline index hint by db and sql digest.

  @param[in]    thd               thread context
  @param[in]    all tables        TABLE LIST
  @param[in]    schema            schema name
  @param[in]    digest            digest string
*/
static void invoke_index_outlines(THD *thd, Table_ref *all_tables,
                                  String_outline &schema,
                                  String_outline &digest) {
  System_outline::instance()
      ->find_and_fill_hints<Table_ref, Outline_category::CATEGORY_INDEX>(
          thd, all_tables, schema, digest);
}

/**
  Invoke the outline index hint by db and sql digest.

  @param[in]    thd               thread context
  @param[in]    all select        all query blocks
  @param[in]    schema            schema name
  @param[in]    digest            digest string

*/
static void invoke_optimizer_outlines(THD *thd, Query_block *all_selects,
                                      String_outline &schema,
                                      String_outline &digest) {
  DBUG_ENTER("invoke_optimizer_outlines");
  std::stack<Query_block *> t_stack;
  System_outline::instance()
      ->find_and_fill_hints<Query_block, Outline_category::CATEGORY_OPTIMIZER>(
          thd, all_selects, schema, digest);

  /* Reverse the parse progress */
  Query_block *select_lex = all_selects;
  while (select_lex) {
    if (select_lex->outline_optimizer_list) t_stack.push(select_lex);
    select_lex = select_lex->next_qb;
  }

  while ((!t_stack.empty())) {
    contextualize_optimizer_hint(thd, t_stack.top());
    t_stack.pop();
  }

  DBUG_VOID_RETURN;
}

/**
  Invoke the outline  hint by db and sql digest.


  @param[in]    thd               thread context
  @param[in]    db                current db
  @param[in]    digest storage    sql digest storage by parser
  @param[in]    strip length      number of bytes stripped for explain token
*/
void invoke_outlines(THD *thd, const char *db,
                     const sql_digest_storage *digest_storage,
                     const uint strip_length) {
  uchar hash[DIGEST_HASH_SIZE];
  char buff[DIGEST_HASH_TO_STRING_LENGTH + 1];
  const char *schema_name = nullptr;
  Table_ref *all_tables;
  Query_block *all_selects;
  DBUG_ENTER("invoke_outlines");

  /* Global variable control the behavior */
  if (!opt_outline_enabled) DBUG_VOID_RETURN;

  all_tables = thd->lex->query_tables;
  all_selects = thd->lex->all_query_blocks;

  /* Maybe didn't set db within current context */
  schema_name = db ? db : "";

  if ((all_tables == nullptr) && (all_selects == nullptr)) DBUG_VOID_RETURN;

  /* Compute the digest hash string */
  compute_digest_hash(digest_storage, hash, strip_length);
  DIGEST_HASH_TO_STRING(hash, buff);

  String_outline schema(schema_name);
  String_outline digest(buff);

  if (all_tables) invoke_index_outlines(thd, all_tables, schema, digest);

  if (all_selects) invoke_optimizer_outlines(thd, all_selects, schema, digest);

  DBUG_VOID_RETURN;
}

#define TABLE_BLOCK_TYPE_STRING "TABLE"
#define QUERY_BLOCK_TYPE_STRING "QUERY"

/**
  Generate the outline_preview_result object from index hint.

  @param[in]      thd           thread context
  @param[in]      db            schema
  @param[in]      digest        query digest
  @param[in]      block         query or table block position
  @param[in/out]  container     outline preview result container
*/
static void generate_outline_preview(
    THD *thd, const char *db, const char *digest, ulonglong block,
    Table_ref *table_list, Outline_preview_result_container *container) {
  char buff[64];
  const char *table_name;
  Index_hint *index_hint = nullptr;
  DBUG_ENTER("generate_outline_preview");

  if (table_list && table_list->index_hints) {
    List_iterator_fast<Index_hint> it(*table_list->index_hints);
    while ((index_hint = it++)) {
      table_name = table_list->alias ? table_list->alias : "";
      String hint_str(buff, sizeof(buff), system_charset_info);
      hint_str.length(0);
      index_hint->print(thd, &hint_str);
      const char *str = thd->strmake(hint_str.c_ptr_quick(), hint_str.length());
      Outline_preview_result *res = new (thd->mem_root) Outline_preview_result(
          db, digest, TABLE_BLOCK_TYPE_STRING, table_name, block, str);
      container->push_back(res);
    }
  }
  DBUG_VOID_RETURN;
}
/**
  Generate the outline_preview_result object from optimizer hint.

  @param[in]      thd           thread context
  @param[in]      db            schema
  @param[in]      digest        query digest
  @param[in]      block         query or table block position
  @param[in/out]  container     outline preview result container
*/
static void generate_outline_preview(
    THD *thd, const char *db, const char *digest, ulonglong block,
    Query_block *select_lex, Outline_preview_result_container *container) {
  char buff[64];
  const char *select_name = "";
  DBUG_ENTER("generate_outline_preview");
  if (select_lex) {
    String hint_str(buff, sizeof(buff), system_charset_info);
    hint_str.length(0);
    /* The table and index level optimizer hint still unresolved, so define
       QT_NORMALIZED_FORMAT to print */
    select_lex->print_hints(thd, &hint_str, QT_NORMALIZED_FORMAT);
    if (hint_str.length() > 0) {
      const char *str = thd->strmake(hint_str.c_ptr_quick(), hint_str.length());
      Outline_preview_result *res = new (thd->mem_root) Outline_preview_result(
          db, digest, QUERY_BLOCK_TYPE_STRING, select_name, block, str);
      container->push_back(res);
    }
  }
  DBUG_VOID_RETURN;
}

/**
  Preview the statement outline;
  It will parse the statement once, and invoke the outlines.

  @param[in]      thd         thread context
  @param[in]      db          schema name
  @param[in]      query       statement string
  @param[in/out]  container   outline preview result container

  @retval         false       success
  @retval         true        failure
*/
bool preview_statement_outline(THD *thd, LEX_CSTRING &db, LEX_CSTRING &query,
                               Outline_preview_result_container *container) {
  Table_ref *table_list;
  Query_block *select_lex;
  uchar hash[DIGEST_HASH_SIZE];
  char buff[DIGEST_HASH_TO_STRING_LENGTH + 1];
  DBUG_ENTER("preview_statement_outline");

  Thd_parser_context parser_context(thd, db);

  Parser_state ps;
  if (ps.init(thd, query.str, query.length)) DBUG_RETURN(true);

  ps.m_lip.m_digest = thd->m_digest;
  ps.m_lip.m_digest->m_digest_storage.m_charset_number = thd->charset()->number;
  ps.m_lip.multi_statements = false;

  thd->m_parser_state = &ps;

  {
    Parser_error_handler error_handler(thd);
    if (thd->sql_parser()) DBUG_RETURN(true);
  }

  /* Compute the digest hash string */
  compute_digest_hash(&thd->m_digest->m_digest_storage, hash);
  DIGEST_HASH_TO_STRING(hash, buff);

  invoke_outlines(thd, db.str, &thd->m_digest->m_digest_storage);
  /* Update system variables specified in SET_VAR hints. */
  if (thd->lex->opt_hints_global && thd->lex->opt_hints_global->sys_var_hint)
    thd->lex->opt_hints_global->sys_var_hint->update_vars(thd);

  table_list = thd->lex->query_tables;
  select_lex = thd->lex->all_query_blocks;

  size_t i = 1;
  while (table_list) {
    generate_outline_preview(thd, db.str, buff, i, table_list, container);
    table_list = table_list->next_global;
    i++;
  }
  i = 1;
  while (select_lex) {
    generate_outline_preview(thd, db.str, buff, i, select_lex, container);
    select_lex = select_lex->next_qb;
    i++;
  }

  /* Restore system variables which were changed by SET_VAR hint. */
  if (thd->lex->opt_hints_global && thd->lex->opt_hints_global->sys_var_hint)
    thd->lex->opt_hints_global->sys_var_hint->restore_vars(thd);

  DBUG_RETURN(false);
}

/**
  Enable the digest compute in parser that required by outline.

  @param[in]      parser state
*/
void enable_digest_by_outline(Parser_state *ps) {
  ps->m_input.m_compute_digest = true;
}

/**
  Reset the query blocks list.
*/
void Query_blocks_list::reset_query_blocks_list() {
  all_query_blocks = nullptr;
  all_query_blocks_last = &all_query_blocks;
}

/**
  Add the select query block into the global list.
*/
void Query_blocks_list::add_to_query_blocks(Query_block *select_lex) {
  *(select_lex->prev_qb = all_query_blocks_last) = select_lex;
  all_query_blocks_last = &select_lex->next_qb;
}

} /* namespace im */
