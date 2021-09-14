/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxySQL hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxySQL.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/trans_proc/returning.h"
#include "sql/common/component.h"
#include "sql/item.h"
#include "sql/mysqld.h"
#include "sql/package/package_common.h"
#include "sql/sql_parse.h"
#include "sql/trans_proc/common.h"
#include "sql/trans_proc/returning_parse.h"

namespace im {

static constexpr const char *FIELD_SEPARATOR = ",";

/* Backup current thd lex_returning */
Thd_lex_returning_context::Thd_lex_returning_context(THD *thd)
    : m_thd(thd),
      m_lex_returning(true, thd->mem_root),
      m_old_lex_returning(m_thd->lex_returning.release()) {
  m_thd->lex_returning.reset(&m_lex_returning);
}

/* Restore the thd lex_returning */
Thd_lex_returning_context::~Thd_lex_returning_context() {
  m_thd->lex_returning.release();
  m_thd->lex_returning.reset(m_old_lex_returning);
}

/* Singleton instance for returning */
Proc *Trans_proc_returning::instance() {
  static Proc *proc = new Trans_proc_returning(key_memory_package);

  return proc;
}

/**
  Evoke the sql_cmd object for returning() proc.
*/
Sql_cmd *Trans_proc_returning::evoke_cmd(THD *thd,
                                         mem_root_deque<Item *> *list) const {
  return new (thd->mem_root) Sql_cmd_type(thd, list, this);
}

/**
  Generate all the field items and statement from parameters.

  @param[in]    THD           Thread context

  @retval       statement
*/
LEX_CSTRING Sql_cmd_trans_proc_returning::get_field_items_and_stmt(THD *thd) {
  char buff[1024];
  String str(buff, sizeof(buff), system_charset_info);
  String *res;
  std::vector<std::string> container;
  DBUG_ENTER("Sql_cmd_trans_proc_returning::get_field_items_and_stmt");
  res = (*m_list)[0]->val_str(&str);
  char *item_string = strmake_root(thd->mem_root, res->ptr(), res->length());

  split<std::string, std::vector<std::string>, true>(
      item_string, FIELD_SEPARATOR, &container);

  for (std::string s : container) {
    char *field_name = strmake_root(thd->mem_root, s.c_str(), s.length());
    Item *item;
    if (strcmp(field_name, "*") == 0) {
      item = new (thd->mem_root) Item_asterisk(POS(), NullS, NullS);
    } else {
      item = new (thd->mem_root) Item_field(POS(), NullS, NullS, field_name);
    }
    thd->lex_returning->add_item(item);

    if (s == "*") thd->lex_returning->inc_wild();
  }

  res = (*m_list)[1]->val_str(&str);
  DBUG_RETURN(
      to_lex_cstring(strmake_root(thd->mem_root, res->ptr(), res->length())));
}

/**
  Execute the sub statement and add returning clause.

  @param[in]    THD           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_trans_proc_returning::pc_execute(THD *thd) {
  LEX_CSTRING query;
  DBUG_ENTER("Sql_cmd_trans_proc_returning::pc_execute");

  /* Override the current context */
  Thd_lex_returning_context ctx(thd);
  Sub_statement_context stmt_ctx(thd);

  /* Prepare all the field items */
  query = get_field_items_and_stmt(thd);

  thd->set_query(query.str, query.length);
  thd->set_query_id(next_query_id());

  Parser_state parser_state;
  if (parser_state.init(thd, thd->query().str, thd->query().length)) {
    DBUG_RETURN(true);
  }
  stmt_ctx.start_statement();

  dispatch_sql_command(thd, &parser_state);

  DBUG_RETURN(thd->is_error());
}

void Sql_cmd_trans_proc_returning::send_result(THD *, bool) {
  DBUG_ENTER("Sql_cmd_trans_proc_returning::send_result");
  DBUG_VOID_RETURN;
}

} /* namespace im */
