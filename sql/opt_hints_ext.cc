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

#include "opt_hints_ext.h"
#include "sql/sql_lex.h"

void Sample_percentage_hint::print(const THD *, String *str) {
  std::stringstream ss;
  ss << "SAMPLE_PERCENTAGE"
     << "(" << this->pct << ")";
  str->append(ss.str().c_str(), ss.str().length());
}

bool check_sample_semantic(LEX *lex, const TABLE *table) {
  assert(lex);

  auto select = lex->current_query_block();
  if (select == nullptr) {
    return false;
  }

  if (!lex->is_single_level_stmt() || select->where_cond() != nullptr ||
      select->having_cond() != nullptr || select->is_ordered() ||
      /* Don't know why 'select count(*)' was turn to 'is_grouped()' */
      /* select->is_grouped() || */
      select->has_limit() || select->is_distinct() ||
      (select->active_options() & OPTION_BUFFER_RESULT) ||
      /* CREATE TABLE AS SELECT*/
      (lex->m_sql_cmd && lex->m_sql_cmd->sql_command_code() != SQLCOM_SELECT)) {
    return true;
  }
  if (select->m_table_list.elements != 1 ||
      select->get_table_list()->index_hints != nullptr ||
      select->get_table_list()->is_view_or_derived() ||
      select->get_table_list()->is_table_function()) {
    return true;
  }

  if (table != nullptr &&
      /* skip select ... lock in share mode / for update */
      !(table->reginfo.lock_type == TL_READ)
      /** Not sure for generated column for now. */
      /* || table->has_gcol() || table->has_virtual_gcol() */
      /** Only innodb */
      /* || table->s->db_type() == nullptr || */
      /* || table->s->db_type()->db_type != DB_TYPE_INNODB */
  ) {
    return true;
  }

  return false;
}

#ifdef POLARX_SAMPLE_TEST
static bool srv_turn_regular_query_to_sample = true;

/** Try treating all regular queries as sample queries with a sampling rate of
100%. */
inline void turn_regular_query_to_sample(LEX *lex, const TABLE *table) {
  if (srv_turn_regular_query_to_sample && !lex->hint_polarx_sample &&
      !check_sample_semantic(lex, table)) {
    /* Not set sample percentage 100% for avoiding false triggering */
    lex->sample_percentage = 95.0;
    lex->hint_polarx_sample = true;
  }
}
#endif
bool is_polarx_sample_query(LEX *lex, const TABLE *table [[maybe_unused]]) {
#ifdef POLARX_SAMPLE_TEST
  turn_regular_query_to_sample(lex, table);
#endif

  return lex->hint_polarx_sample;
}