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

#include "sql/trans_proc/common.h"
#include "sql/mysqld.h"
#include "sql/trans_proc/returning_parse.h"

namespace im {

LEX_CSTRING TRANS_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_trans")};

Sub_statement_context::Sub_statement_context(THD *thd)
    : m_thd(thd),
      m_old_query_id(thd->query_id),
      m_old_lex(thd->lex),
      m_arena(thd->mem_root, Query_arena::STMT_REGULAR_EXECUTION),
      arena_backup(),
      save_stmt_arena(thd->stmt_arena),
      m_old_digest_state(thd->m_digest),
      m_inited(false) {
  m_old_query = m_thd->query();
  m_thd->swap_query_arena(m_arena, &arena_backup);
  m_thd->stmt_arena = &m_arena;
  m_thd->m_digest = &m_digest_state;
  m_thd->m_digest->reset(m_thd->m_token_array, max_digest_length);
  m_thd->lex = &m_lex;

#ifdef HAVE_PSI_STATEMENT_INTERFACE
  m_old_locker = m_thd->m_statement_psi;
#endif
}

Sub_statement_context::~Sub_statement_context() {
  end_statement();

  m_thd->set_query_id(m_old_query_id);
  m_thd->lex = m_old_lex;
  m_thd->swap_query_arena(arena_backup, &m_arena);
  m_thd->stmt_arena = save_stmt_arena;
  m_thd->m_digest = m_old_digest_state;
  m_thd->set_query(m_old_query);

#ifdef HAVE_PSI_STATEMENT_INTERFACE
  m_thd->m_statement_psi = m_old_locker;
#endif
}

void Sub_statement_context::start_statement() {
#ifdef HAVE_PSI_STATEMENT_INTERFACE
  m_thd->m_statement_psi = MYSQL_START_STATEMENT(
      &m_psi_state, com_statement_info[COM_QUERY].m_key, m_thd->db().str,
      m_thd->db().length, m_thd->charset(), nullptr);
#endif

  m_inited = true;
}

void Sub_statement_context::end_statement() {
  if (m_inited) {
#ifdef HAVE_PSI_STATEMENT_INTERFACE
    MYSQL_END_STATEMENT(m_thd->m_statement_psi, m_thd->get_stmt_da());
#endif
  }
  m_inited = false;
}

} /* namespace im */
