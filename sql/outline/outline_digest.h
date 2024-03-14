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
#ifndef SQL_OUTLINE_OUTLINE_DIGEST_INCLUDED
#define SQL_OUTLINE_OUTLINE_DIGEST_INCLUDED

#include "sql/error_handler.h"
#include "sql/outline/outline.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

namespace im {

/**
  Save the current parser context

  We can safely get statement digest through parser.
*/
class Thd_parser_context {
 public:
  Thd_parser_context(THD *thd, const LEX_CSTRING &db)
      : m_thd(thd),
        m_arena(&m_mem_root, Query_arena::STMT_REGULAR_EXECUTION),
        m_backed_up_lex(thd->lex),
        m_saved_parser_state(thd->m_parser_state),
        m_saved_digest(thd->m_digest),
        m_save_db(thd->db()) {
    thd->reset_db(db);
    thd->m_digest = &m_digest_state;
    m_digest_state.reset(thd->m_token_array, get_max_digest_length());
    m_arena.set_query_arena(*thd);
    thd->lex = &m_lex;
    lex_start(thd);
  }

  ~Thd_parser_context() {
    lex_end(&m_lex);
    m_thd->lex = m_backed_up_lex;
    m_thd->set_query_arena(m_arena);
    m_thd->m_parser_state = m_saved_parser_state;
    m_thd->m_digest = m_saved_digest;
    m_thd->reset_db(m_save_db);
  }

 private:
  THD *m_thd;
  MEM_ROOT m_mem_root;
  Query_arena m_arena;
  LEX *m_backed_up_lex;
  LEX m_lex;
  sql_digest_state m_digest_state;
  Parser_state *m_saved_parser_state;
  sql_digest_state *m_saved_digest;
  LEX_CSTRING m_save_db;
};

/**
  Backup the parser state,  it will be used when comsume optimizer hint.
*/
class Thd_backup_parser_state {
 public:
  Thd_backup_parser_state(THD *thd)
      : m_thd(thd), m_saved_parser_state(thd->m_parser_state) {}

  ~Thd_backup_parser_state() { m_thd->m_parser_state = m_saved_parser_state; }

 private:
  THD *m_thd;
  Parser_state *m_saved_parser_state;
};

class Parser_error_handler : public Internal_error_handler {
 public:
  Parser_error_handler(THD *thd) : m_thd(thd) {
    thd->push_internal_handler(this);
  }

  bool handle_condition(THD *, uint, const char *,
                        Sql_condition::enum_severity_level *level,
                        const char *message) override {
    // Silence warnings.
    if (*level == Sql_condition::SL_WARNING) return true;

    if (is_handling) return false;

    is_handling = true;
    my_error(ER_STATEMENT_DIGEST_PARSE, MYF(0), message);
    is_handling = false;
    return true;
  }

  ~Parser_error_handler() override { m_thd->pop_internal_handler(); }

 private:
  THD *m_thd;
  /* Avoid infinite recursive call through my_error() */
  bool is_handling = false;
};

/**
  Calculate the offset of the real query in the digest storage.

  EXPLAIN query is a special case in the calculation of digest. The digest of a
  EXPLAIN query could be used in two different methods:

  1. Only read the explained query (e.g. "SELECT * FROM t1;" of "EXPLAIN SELECT
  * FORM t1;"). In the invoking of outline, to generate the correct execution
  plan, the digest of the explained query instead of the whole EXPLAIN query is
  needed to look up the outline rules.

  2. Pass the digest of the whole query to performance schema to distinguish
  EXPLAIN queries and the real queries themselves. For example, The "EXPLAIN"
  token should be kept in the performance schema summary tables to distinguish
  between SELECT query and EXPLAIN query. Otherwise, performance schema would
  keep a wrong number of SELECT query executed and misestimate the performance.

  @param[in]    digest_storage    Digest storage

  @retval       strip_length      The number of bytes that need to be stripped
  to skip the EXPLAIN token
*/
uint calculate_strip_length_for_explain(
    const sql_digest_storage *digest_storage);

/**
  Generate statement digest and digest text.

  @param[in]    thd           Thread context
  @param[in]    db            Target db context
  @param[in]    query         Statement
  @param[in]    query_lenth   Statement string length
  @param[out]   digest        Digest hash string
  @param[out]   digest_text   Digest text string
  @param[out]   truncated     Whether truncated sql text


  @retval       true          failure
  @retval       fase          success
*/
bool generate_statement_digest(THD *thd, const LEX_CSTRING &db,
                               const LEX_CSTRING &query, String *digest,
                               String *digest_text, bool *truncated);

/**
   Parse the optimizer hint for one query block.

   @param[in]       thd         Thread context
   @param[in]       hint        Optimizer hint

   @reval           hint list   Optimizer hint list
*/
PT_hint_list *parse_optimizer_hint(THD *thd, LEX_CSTRING &hint);
/**
  Contextualize the parsed hint list.

  @param[in]        thd           Thread context
  @param[in]        select_lex    Target query block
*/
void contextualize_optimizer_hint(THD *thd, Query_block *select_lex);
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
                               Outline_preview_result_container *container);

/**
  Trim input query as follows:
    Left trim ' '
    Right trim ' ' and ';'

  @param[in]        thd           Thread context
  @param[in]        str           query string
  @param[in]        len           query length
  @param[out]       query         Trimed query
*/
void alloc_query(THD *thd, const char *str, size_t len, LEX_CSTRING &query);

} /* namespace im */

#endif
