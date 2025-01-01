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

#ifndef SQL_TRANS_PROC_RETURNING_PARSE_INCLUDED
#define SQL_TRANS_PROC_RETURNING_PARSE_INCLUDED

#include "field_types.h"
#include "sql/query_result.h"
#include "sql/sql_list.h"

class THD;
struct LEX;
class Query_block;

namespace im {
struct Fixed_item {
 public:
  Fixed_item(enum_field_types data_type = MYSQL_TYPE_NULL,
             const char *name = "None")
      : m_data_type(data_type), m_name(name) {}

  bool is_null() const { return m_name == "None"; }

  Item *allocate();

  void reset() {
    m_data_type = MYSQL_TYPE_NULL;
    m_name = "None";
  }

 private:
  enum_field_types m_data_type;
  std::string m_name;
};

/**
  Returning clause lex object. it will be constructed within THD local,
  and copy into lex->lex_returning, mainly because returning syntax is
  simulated through dbms_trans.returning() procedure.
*/
class Lex_returning {
 public:
  explicit Lex_returning(bool is_returning_stmt, MEM_ROOT *mem_root);

  ~Lex_returning();

  /* Call it when lex_start() */
  void reset();

  bool has_items() const { return m_has_items; }

  bool is_returning_call() const { return m_is_returning_call; }

  bool is_backfill_returning() { return m_is_backfill_returning; }

  void set_backfill_returning(bool value) { m_is_backfill_returning = value; }

  bool is_full_image() const { return !m_fixed_item.is_null(); }

  void set_fixed_item(const Fixed_item &item) { m_fixed_item = item; }

  Fixed_item get_fixed_item() const { return m_fixed_item; }

  void add_item(Item *item);
  void inc_wild() { m_with_wild++; }

  uint with_wild() const { return m_with_wild; }

  mem_root_deque<Item *> *get_fields() { return m_items; }

  Lex_returning &operator=(const Lex_returning &tmp);

 private:
  /* Whether define some returning items */
  bool m_has_items;
  /* Whether it's from dbms_trans.returning() */
  bool m_is_returning_call;
  /* Whether it's from dbms_trans.backfill() */
  bool m_is_backfill_returning;
  /* Used for quety of '*' */
  uint m_with_wild;
  /* Return field items */
  mem_root_deque<Item *> *m_items;

  Fixed_item m_fixed_item;
};

/** Wrapper of the query result send */
class Update_returning_statement {
 public:
  explicit Update_returning_statement(THD *thd);

  bool is_returning() const {
    return m_lex_returning != nullptr && m_lex_returning->is_returning_call();
  }

  bool is_backfill_returning() {
    return m_lex_returning != nullptr && m_lex_returning->is_backfill_returning();
  }

  bool is_full_image() const {
    return is_returning() && m_lex_returning->is_full_image();
  }

  /**
    Itemize all the field_items from procedure parameters.

    @param[in]      thd           thread context
    @param[in]      fields        field items from proc
    @param[in]      query_block    the update/delete query_block

    @retval         false         success
    @retval         true          failure
  */
  bool itemize_fields(THD *thd, mem_root_deque<Item *> &fields,
                      Query_block *query_block);

  /**
    Expand "*" and setup all fields.

    @param[in]      thd           thread context
    @param[in]      query_block    the update/delete query_block

    @retval         false         success
    @retval         true          failure
  */
  bool setup(THD *thd, Query_block *query_block);

  /**
    Send the row data.

    @param[in]      thd           thread context

    @retval         false         success
    @retval         true          failure
  */
  bool send_data(THD *thd, bool is_before = false, ptrdiff_t diff = 0);

  /**
    Send the EOF.

    @param[in]      thd           thread context
  */
  void send_eof(THD *thd);

 private:
  /**
    Init the returning statement context.

    Require it must be come from dbms_trans.returning() call
    and give fields.
  */
  void init();

 private:
  THD *m_thd;
  Lex_returning *m_lex_returning;
  Query_result_send result;
};

/**
  Only allowed certain sql command has returning clause

  Report error if failed.

  @retval     false       success
  @retval     true        failure
*/
bool deny_returning_clause_by_command(THD *thd, LEX *lex);

} /* namespace im */
#endif
