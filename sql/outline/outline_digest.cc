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

#include "sql/outline/outline_digest.h"
#include "sql/outline/outline.h"
#include "sql/outline/outline_common.h"
#include "sql/sql_digest.h"
#include "sql/sql_lex.h"
#include "sql/sql_yacc.h"  // Generated code.

#include "sql/lex_token.h"

namespace im {

#define EXPLAIN_TOKEN_SIZE 2
#define EXPLAIN_FORMAT_MINIMAL_LENGTH 6
#define IDENT_LENGTH_SIZE 2
#define LEAST_TOKEN_SIZE 2

/**
  Calculate the offset of the real query in the digest storage.

  The digesting process converts a SQL statement to normalized form by
  translating each token to a few digest bytes. This function calculates the
  number of bytes that should be stripped to skip the digest bytes corresponding
  to "EXPLAIN" or "EXPLAIN FORMAT = xxx".

  @param[in]    digest_storage    Digest storage

  @retval       strip_length      The number of bytes that need to be stripped
  to skip the EXPLAIN token
*/
uint calculate_strip_length_for_explain(
    const sql_digest_storage *digest_storage) {
  uint strip_length = 0;

  if (digest_storage && digest_storage->m_byte_count > EXPLAIN_TOKEN_SIZE) {
    const unsigned char *src = &digest_storage->m_token_array[0];
    uint tok = src[0] | (src[1] << 8);

    if (tok == DESCRIBE) {
      strip_length += 2;

      uint expected_length = EXPLAIN_FORMAT_MINIMAL_LENGTH;
      if (digest_storage->m_byte_count >=
          EXPLAIN_TOKEN_SIZE + expected_length + LEAST_TOKEN_SIZE) {
        /*
          Make sure that there is enough size for "FORMAT = xxx" and at least
          one more token.
         */
        uint tok_format = src[2] | (src[3] << 8);
        uint tok_eq = src[4] | (src[5] << 8);
        uint tok_type = src[6] | (src[7] << 8);

        if (tok_format == FORMAT_SYM && tok_eq == EQ) {
          /*
            Note that the format type may also be specified as quoted:

                EXPLAIN FORMAT='TrAdItIoNaL' SELECT 1

            The quoted type is reduced as TOK_GENERIC_VALUE in digest_storage.
            However, the input token stream is intact and still recognized
            by the parser.
           */
          if (tok_type == JSON_SYM || tok_type == TOK_GENERIC_VALUE)
            strip_length += expected_length;
          else if (tok_type == TOK_IDENT) {
            expected_length += IDENT_LENGTH_SIZE;
            if (digest_storage->m_byte_count >=
                EXPLAIN_TOKEN_SIZE + expected_length + LEAST_TOKEN_SIZE) {
              uint id_len = src[8] | (src[9] << 8);
              expected_length += id_len;
              if (digest_storage->m_byte_count >=
                  EXPLAIN_TOKEN_SIZE + expected_length + LEAST_TOKEN_SIZE) {
                strip_length += expected_length;
              } else {
                // Incomplete identifier payload
                assert(0);
              }
            } else {
              // Missing identifier length
              assert(0);
            }
          } else {
            // Unexpected EXPLAIN format type token
            assert(0);
          }
        }  // FORMAT = { json | traditional | tree }
      }
    }  // DESCRIBE
  }
  return strip_length;
}

/**
  Generate statement digest and digest text.

  @param[in]    thd           Thread context
  @param[in]    db            Target db context
  @param[in]    query         Statement
  @param[out]   digest        Digest hash string
  @param[out]   digest_text   Digest text string
  @param[out]   truncated     Whether truncated sql text


  @retval       true          failure
  @retval       fase          success
*/
bool generate_statement_digest(THD *thd, const LEX_CSTRING &db,
                               const LEX_CSTRING &query, String *digest,
                               String *digest_text, bool *truncated) {
  if (blank(query)) return true;

  Thd_parser_context parser_context(thd, db);

  Parser_state ps;
  if (ps.init(thd, query.str, query.length)) return true;

  DBUG_EXECUTE_IF("outline_simulate_oom", { return true; });

  ps.m_lip.m_digest = thd->m_digest;
  ps.m_lip.m_digest->m_digest_storage.m_charset_number = thd->charset()->number;
  ps.m_lip.multi_statements = false;

  thd->m_parser_state = &ps;

  {
    Parser_error_handler error_handler(thd);
    if (thd->sql_parser()) return true;
  }

  uchar hash[DIGEST_HASH_SIZE];
  compute_digest_hash(&thd->m_digest->m_digest_storage, hash);

  digest->reserve(DIGEST_HASH_TO_STRING_LENGTH);
  digest->length(DIGEST_HASH_TO_STRING_LENGTH);
  DIGEST_HASH_TO_STRING(hash, digest->c_ptr_quick());

  compute_digest_text(&thd->m_digest->m_digest_storage, digest_text);

  *truncated = thd->m_digest->m_digest_storage.m_full;

  return false;
}

/**
   Parse the optimizer hint for one query block.

   @param[in]       thd         Thread context
   @param[in]       hint        Optimizer hint

   @reval           hint list   Optimizer hint list
*/
PT_hint_list *parse_optimizer_hint(THD *thd, LEX_CSTRING &hint) {
  DBUG_ENTER("parse_optimizer_hint");
  if (blank(hint)) DBUG_RETURN(nullptr);

  Thd_backup_parser_state backup_parser(thd);
  Parser_state ps;
  Lexer_yystype yylval;
  yylval.optimizer_hints = nullptr;

  if (ps.init(thd, hint.str, hint.length)) DBUG_RETURN(nullptr);

  ps.m_lip.yylval = &yylval;
  ps.m_lip.m_digest = nullptr;
  ps.m_lip.multi_statements = false;
  thd->m_parser_state = &ps;

  {
    Parser_error_handler error_handler(thd);
    consume_optimizer_hints(&(ps.m_lip));
  }
  DBUG_RETURN(ps.m_lip.yylval->optimizer_hints);
}

/**
  Contextualize the parsed hint list.

  @param[in]        thd           Thread context
  @param[in]        select_lex    Target query block
*/
void contextualize_optimizer_hint(THD *thd, Query_block *select_lex) {
  PT_hint_list *pt_list = nullptr;
  List<Lex_optimizer_hint> *list = nullptr;
  Lex_optimizer_hint *optimizer_hint;
  DBUG_ENTER("contextualize_optimizer_hint");
  list = select_lex->outline_optimizer_list;

  if (!list) DBUG_VOID_RETURN;

  List_iterator_fast<Lex_optimizer_hint> it(*list);

  while ((optimizer_hint = it++)) {
    Parse_context pc(thd, select_lex);
    if ((pt_list = parse_optimizer_hint(thd, optimizer_hint->hint)))
      pt_list->contextualize(&pc);
  }

  DBUG_VOID_RETURN;
}

/**
  Trim input query as follows:
    Left trim ' '
    Right trim ' ' and ';'

  @param[in]        thd           Thread context
  @param[in]        str           query string
  @param[in]        len           query length
  @param[out]       query         Trimed query
*/
void alloc_query(THD *thd, const char *str, size_t len, LEX_CSTRING &query) {
  if (!str || len == 0) {
    query = to_lex_cstring(NULL);
    return;
  }

  while (len > 0 && my_isspace(thd->charset(), str[0])) {
    str++;
    len--;
  }
  const char *pos = str + len;  // Point at end null
  while (len > 0 && (pos[-1] == ';' || my_isspace(thd->charset(), pos[-1]))) {
    pos--;
    len--;
  }

  query = to_lex_cstring(strmake_root(thd->mem_root, str, len));
}

} /* namespace im */
