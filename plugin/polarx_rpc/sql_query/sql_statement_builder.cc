/*
 * Copyright (c) 2018, 2019, Oracle and/or its affiliates. All rights reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#include "my_sys.h"  // escape_string_for_mysql

#include "../utility/error.h"

#include "getter_any.h"
#include "query_string_builder.h"
#include "sql_statement_builder.h"

namespace polarx_rpc {

const std::string HINT_RETURNING_FIELD = "fields";
const std::string RETURNING_CLAUSE = "call dbms_trans.returning('?', '?')";

#define HINT_RETURNING "returning"
#define FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE 27
#define SECOND_PLACEHOLDER_OF_RETURNING_CLAUSE 32

namespace {

class Arg_inserter {
 public:
  explicit Arg_inserter(Query_string_builder *qb) : m_qb(qb) {}

  void operator()() {
    static const char *const k_value_null = "NULL";
    m_qb->format() % Query_formatter::No_escape<const char *>(k_value_null);
  }

  template <typename Value_type>
  void operator()(const Value_type &value) {
    m_qb->format() % value;
  }

  void operator()(const std::string &value, const uint32_t) {
    m_qb->format() % value;
  }

 private:
  Query_string_builder *m_qb;
};

}  // namespace

std::string get_returning_field(const std::string &hint, size_t pos) {
  std::string fields = "*";
  if ((hint.length() <= pos + HINT_RETURNING_FIELD.length()) ||
      (hint[pos + HINT_RETURNING_FIELD.length()] != '('))
    throw err_t(ER_POLARX_RPC_ERROR_MSG,
                "Invalid hint format, expecting /* +sql field() */");
  size_t end_pos = 0;
  if ((end_pos = hint.find(')', pos)) != std::string::npos) {
    if (end_pos - pos > 1)
      fields = hint.substr(pos + HINT_RETURNING_FIELD.length() + 1,
                           end_pos - (pos + HINT_RETURNING_FIELD.length()) - 1);
  } else
    throw err_t(ER_POLARX_RPC_ERROR_MSG,
                "Invalid hint format, expecting /* +sql field() */");
  return fields;
}

std::string trans_returning(const std::string &query,
                            const std::string &fields) {
  std::string returning;
  returning.reserve(FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE + fields.length() +
                    (SECOND_PLACEHOLDER_OF_RETURNING_CLAUSE -
                     FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE - 1) +
                    query.length() + 3);
  returning +=
      RETURNING_CLAUSE.substr(0, FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE);
  returning += fields;
  returning +=
      RETURNING_CLAUSE.substr(FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE + 1,
                              SECOND_PLACEHOLDER_OF_RETURNING_CLAUSE -
                                  FIRST_PLACEHOLDER_OF_RETURNING_CLAUSE - 1);
  returning += query;
  returning +=
      RETURNING_CLAUSE.substr(SECOND_PLACEHOLDER_OF_RETURNING_CLAUSE + 1);
  return returning;
}

void Sql_statement_builder::build(const std::string &query,
                                  const Arg_list &args,
                                  const CHARSET_INFO &charset,
                                  const std::string &hint) const {
  /// build original sql
  build(query, args, charset);

  std::string out_query;
  if (hint.find(HINT_RETURNING) != std::string::npos) {
    /// escape inner sql string
    auto sql_str(m_qb->get());
    m_qb->clear();
    m_qb->escape_string(sql_str.data(), sql_str.length());

    /// build returning
    std::string::size_type pos;
    if ((pos = hint.find(HINT_RETURNING_FIELD)) != std::string::npos)
      out_query = trans_returning(m_qb->get(), get_returning_field(hint, pos));
    else
      out_query = trans_returning(m_qb->get(), "*");
  } else
    out_query = m_qb->get();

  m_qb->clear();
  std::string hint_query;
  hint_query.reserve(hint.length() + out_query.length() + 1);
  hint_query += hint;
  hint_query += out_query;
  m_qb->put(hint_query);
}

void Sql_statement_builder::build(const std::string &query,
                                  const Arg_list &args,
                                  const CHARSET_INFO &charset) const {
  m_qb->set_charset(&charset);
  m_qb->put(query);

  Arg_inserter inserter(m_qb);
  for (int i = 0; i < args.size(); ++i) {
    Getter_any::put_scalar_value_to_functor(args.Get(i), inserter, charset);
  }
  m_qb->format_finalize();
}

}  // namespace polarx_rpc
