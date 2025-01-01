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

#include "sql/trans_proc/returning_parse.h"
#include "sql/auth/auth_acls.h"
#include "sql/protocol.h"
#include "sql/sql_base.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

namespace im {

Item* Fixed_item::allocate() {
    Item *returning_item;
    switch (m_data_type) {
      case MYSQL_TYPE_LONGLONG:
        returning_item = new Item_return_int(m_name.c_str(), strlen(m_name.c_str()), MYSQL_TYPE_LONGLONG);
        break;
      default:
        returning_item = new Item_empty_string(m_name.c_str(), strlen(m_name.c_str()), &my_charset_utf8mb4_bin);
    }
    return returning_item;
}

/* Constructor */
Lex_returning::Lex_returning(bool is_returning_stmt, MEM_ROOT *mem_root)
    : m_has_items(false),
      m_is_returning_call(is_returning_stmt),
      m_with_wild(0),
      m_items(),
      m_fixed_item() {
  m_items = new (mem_root) mem_root_deque<Item *>(mem_root);
}

/* Destructor */
Lex_returning::~Lex_returning() {
  DBUG_ENTER("Lex_returning::~Lex_returning");

  DBUG_VOID_RETURN;
}

void Lex_returning::reset() {
  DBUG_ENTER("Lex_returning::reset");
  m_has_items = false;
  m_is_returning_call = false;
  m_with_wild = 0;
  m_items->clear();
  m_fixed_item.reset();
  DBUG_VOID_RETURN;
}

Lex_returning &Lex_returning::operator=(const Lex_returning &tmp) {
  m_has_items = tmp.m_has_items;
  m_is_returning_call = tmp.m_is_returning_call;
  m_items = tmp.m_items;
  m_with_wild = tmp.m_with_wild;
  m_fixed_item = tmp.m_fixed_item;
  return *this;
}

void Lex_returning::add_item(Item *item) {
  m_items->push_back(item);
  if (!m_has_items) m_has_items = true;
}
/* Constructor */
Update_returning_statement::Update_returning_statement(THD *thd)
    : m_thd(thd),
      m_lex_returning(nullptr) {
  init();
}

/**
  Init the returning statement context.

  Require it must be come from dbms_trans.returning() call
  and give fields.
*/
void Update_returning_statement::init() {
  Lex_returning *lex_ret;
  DBUG_ENTER("Update_returning_statement::init");
  lex_ret = m_thd->get_lex_returning();

  /* Must be dbms_trans.returning call and item list count > 0 */
  if (lex_ret->has_items() && lex_ret->is_returning_call()) {
    m_lex_returning = lex_ret;
  }
  DBUG_VOID_RETURN;
}

/**
  Backup the select_lex field_list and with_wild attributes.
*/
class Backup_select_lex_fields {
 public:
  Backup_select_lex_fields(Query_block *query_block,
                           mem_root_deque<Item *> *fields, uint with_wild)
      : m_query_block(query_block),
        m_backup_fields(*query_block->get_fields_list()),
        m_origin_fields(fields),
        m_backup_wild(0) {
    m_backup_wild = m_query_block->with_wild;
    *query_block->get_fields_list() = *fields;
    query_block->with_wild = with_wild;
  }

  ~Backup_select_lex_fields() {
    /* Override the original fields list after expanding "*" */
    *m_origin_fields = *m_query_block->get_fields_list();
    *m_query_block->get_fields_list() = m_backup_fields;
    m_query_block->with_wild = m_backup_wild;
  }

 private:
  Query_block *m_query_block;
  mem_root_deque<Item *> m_backup_fields;
  mem_root_deque<Item *> *m_origin_fields;
  uint m_backup_wild;
};

/**
  Itemize all the field_items from procedure parameters.

  @param[in]      thd           thread context
  @param[in]      fields        field items from proc
  @param[in]      query_block    the update/delete query_block

  @retval         false         success
  @retval         true          failure
*/
bool Update_returning_statement::itemize_fields(THD *thd,
                                                mem_root_deque<Item *> &fields,
                                                Query_block *query_block) {
  Parse_context pc(thd, query_block);
  Item *item;
  /* Itemize all the field_item */
  for (auto it = fields.begin(); it != fields.end(); it++) {
    item = *it;
    if (item->itemize(&pc, &item)) return true;
  }
  return false;
}

/**
  Expand "*" and setup all fields.

  @param[in]      thd           thread context
  @param[in]      query_block    the update/delete query block

  @retval         false         success
  @retval         true          failure
*/
bool Update_returning_statement::setup(THD *thd, Query_block *query_block) {
  mem_root_deque<Item *> *fields;
  uint with_wild;
  Fixed_item fixed_item;
  bool returning = is_returning();
  bool full_image = is_full_image();
  DBUG_ENTER("Update_returning_statement::setup");
  if (returning) {
    fields = m_lex_returning->get_fields();
    with_wild = m_lex_returning->with_wild();
    fixed_item = m_lex_returning->get_fixed_item();
    /* Itemize all the field_item */
    itemize_fields(thd, *fields, query_block);

    /* Backup select_lex context, expand * fields */
    if (with_wild > 0) {
      Backup_select_lex_fields bs(query_block, fields, with_wild);
      if (query_block->setup_wild(thd)) DBUG_RETURN(true);
    }

    /* Setup all fields */
    if (setup_fields(thd, SELECT_ACL, false, false, false, nullptr, fields,
                     Ref_item_array()))
      DBUG_RETURN(true);

    /* Prepare the result set */
    result.prepare(thd, *fields, thd->lex->unit);

    /* push returning item to the front */
    if (full_image) {
      fields->push_front(fixed_item.allocate());
    }
    if (result.send_result_set_metadata(
            thd, *fields, Protocol::SEND_NUM_ROWS | Protocol::SEND_EOF))
      DBUG_RETURN(true);
      
    if (full_image) {
      fields->pop_front();
    }
  }

  DBUG_RETURN(false);
}

/**
  Send the row data.

  @param[in]      thd           thread context

  @retval         false         success
  @retval         true          failure
*/
bool Update_returning_statement::send_data(THD *thd, bool is_before,
                                           ptrdiff_t diff) {
  if (is_returning()) {
    if (is_full_image()) {
      return result.send_returning_data(thd, *(m_lex_returning->get_fields()),
                                        is_before, diff);
    } else {
      return result.send_data(thd, *(m_lex_returning->get_fields()));
    }
  }
  return false;
}
/**
  Send the EOF.

  @param[in]      thd           thread context
*/
void Update_returning_statement::send_eof(THD *thd) {
  if (is_returning()) {
    result.send_eof(thd);
  }
}

/**
  Only allowed certain sql command has returning clause

  Report error if failed.

  @retval     false       success
  @retval     true        failure
*/
bool deny_returning_clause_by_command(THD *thd, LEX *lex) {
  /**
    If it's the returning call and the sub statement is not update or delete,
    report error here.
    Pls update here if support more command.
  */
 if (thd->get_lex_returning()->is_returning_call() && 
      thd->get_lex_returning()->is_backfill_returning() &&
      (lex->is_explain() || lex->sql_command != SQLCOM_INSERT)) {
    my_error(ER_NOT_SUPPORT_RETURNING_CLAUSE, MYF(0));
    return true;
  }
  if (thd->get_lex_returning()->is_returning_call() &&
      (lex->is_explain() || (lex->sql_command != SQLCOM_UPDATE &&
                             lex->sql_command != SQLCOM_REPLACE &&
                             lex->sql_command != SQLCOM_DELETE &&
                             lex->sql_command != SQLCOM_INSERT))) {
    my_error(ER_NOT_SUPPORT_RETURNING_CLAUSE, MYF(0));
    return true;
  }
  return false;
}

}  // namespace im
