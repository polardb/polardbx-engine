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

#include "sql/ccl/ccl_hint_parse.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

namespace im {

bool PT_hint_ccl_queue::contextualize(Parse_context *pc) {
  char buff[64];
  String str(buff, sizeof(buff), system_charset_info);
  LEX_CSTRING lex_str;
  Ccl_queue_hint *hint = nullptr;
  if (super::contextualize(pc)) return true;

  Opt_hints_global *global_hint = get_global_hints(pc);

  if (global_hint->is_specified(type())) {
    print_warn(pc->thd, ER_WARN_CONFLICTING_HINT, NULL, NULL, NULL, this);
    return false;
  }

  String *res = m_item->val_str(&str);
  lex_str = to_lex_cstring(
      strmake_root(pc->thd->mem_root, res->ptr(), res->length()));

  if (m_type == Ccl_hint_type::CCL_HINT_QUEUE_FIELD)
    hint = new (pc->thd->mem_root) Ccl_queue_field_hint(lex_str, m_item);
  else if (m_type == Ccl_hint_type::CCL_HINT_QUEUE_VALUE)
    hint = new (pc->thd->mem_root) Ccl_queue_value_hint(lex_str, m_item);
  else
    hint = nullptr;

  global_hint->set_switch(switch_on(), type(), false);
  global_hint->ccl_queue_hint = hint;
  return false;
}

/**
  Fill the where condition into ccl hint structure.

  @param[in]      pc        Parse context
  @param[in]      cond      Equal condition
  @param[in]      left      field item
  @param[in]      right     value item

*/
void fill_ccl_queue_field_cond(Parse_context *pc, Item *cond, Item *left,
                               Item *right) {
  Query_block *select = pc->select;
  /* Require the condition is in WHERE clause */
  if (!select || select->parsing_place != CTX_WHERE) return;

  /*
    Requirement:
      1) Condition must be function like '=', '!=', '>', and so on;
      2) left arg item must be field;
      3) right arg item must be int or string const item;
  */
  if (cond->type() == Item_func::FUNC_ITEM) {
    Item_func *func_cond = (Item_func *)(cond);
    Item **args = func_cond->arguments();
    if ((func_cond->argument_count() == 2) && (args[0] == left) &&
        (args[1] == right) && (left->type() == Item::FIELD_ITEM) &&
        (right->type() == Item::STRING_ITEM ||
         right->type() == Item::INT_ITEM)) {
      select->ccl_queue_field_cond_list.push_back(cond);
    }
  }
}

} /* namespace im */
