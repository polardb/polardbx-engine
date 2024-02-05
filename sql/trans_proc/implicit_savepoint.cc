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

#include "sql/trans_proc/implicit_savepoint.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/sql_parse.h"

namespace im {

Proc *Trans_proc_implicit_savepoint::instance() {
  static Trans_proc_implicit_savepoint *proc =
      new Trans_proc_implicit_savepoint(key_memory_package);
  return proc;
}

Sql_cmd *Trans_proc_implicit_savepoint::evoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root)
      Sql_cmd_trans_proc_implicit_savepoint(thd, list, this);
}

/**
  Execute 'ROLLBACK TO savepoint_name' sub statement internally.

  @param[in]    thd           Thread context

  @retval       true          Failure
  @retval       false         Success
*/
bool Sql_cmd_trans_proc_implicit_savepoint::pc_execute(THD *thd) {
  DBUG_ENTER("Sql_cmd_trans_proc_implicit_savepoint::pc_execute");

  String cmd;
  Parser_state parser_state;

  /* Save the current context */
  Sub_statement_context stmt_ctx(thd);

  /* Make statement 'ROLLBACK TO savepoint_name' */
  if (cmd.append(STRING_WITH_LEN("ROLLBACK TO ")) || cmd.append("`") ||
      cmd.append(STRING_WITH_LEN(mysql_implicit_savepoint)) || cmd.append("`"))
    DBUG_RETURN(true);

  thd->set_query(cmd.lex_cstring());
  thd->set_query_id(next_query_id());

  if (parser_state.init(thd, thd->query().str, thd->query().length))
    DBUG_RETURN(true);

  stmt_ctx.start_statement();

  dispatch_sql_command(thd, &parser_state);

  DBUG_RETURN(thd->is_error());
}

/**
  Send result to client.

  @param[in]    thd           Thread context
  @param[in]    error         Whether current proc runs into error
*/
void Sql_cmd_trans_proc_implicit_savepoint::send_result(THD *thd, bool error) {
  DBUG_ENTER("Sql_cmd_trans_proc_implicit_savepoint::send_result");

  if (error) {
    MY_UNUSED(thd);
    assert(thd->is_error());
  }

  DBUG_VOID_RETURN;
}

}  // namespace im
