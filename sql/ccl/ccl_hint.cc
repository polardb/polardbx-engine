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

#include "sql/ccl/ccl_hint.h"
#include "my_inttypes.h"
#include "my_murmur3.h"  // my_murmur3_32
#include "sql/derror.h"
#include "sql/item.h"
#include "sql/item_func.h"
#include "sql/sql_lex.h"

namespace im {

std::string Ccl_queue_field_hint::name() {
  return std::string("CCL_QUEUE_FIELD");
}

std::string Ccl_queue_value_hint::name() {
  return std::string("CCL_QUEUE_VALUE");
}

ulonglong get_item_hash_value(Item *m_item) {
  if (m_item->type() == Item::INT_ITEM) {
    return m_item->val_int();
  } else {
    char buff[1024];
    String str(buff, sizeof(buff), system_charset_info);
    String *res = m_item->val_str(&str);
    return murmur3_32(reinterpret_cast<const uchar *>(res->ptr()),
                      res->length(), 0);
  }
}

/**
  Hash value for the ccl_queue_value(value);

  @param[in]      thd         thread context
  @param[in]      select      the top-level select context
*/
ulonglong Ccl_queue_value_hint::hash_value(THD *, const Query_block *) {
  return get_item_hash_value(m_item);
}

/**
  Hash value for the ccl_queue_field(field_name);

  @param[in]      thd         thread context
  @param[in]      select      the top-level select context
*/
ulonglong Ccl_queue_field_hint::hash_value(THD *thd,
                                           const Query_block *select) {
  Item *cond;
  List_iterator_fast<Item> it(
      const_cast<Query_block *>(select)->ccl_queue_field_cond_list);
  while ((cond = it++)) {
    Item_func *func_cond = (Item_func *)(cond);
    Item **args = func_cond->arguments();
    assert(func_cond->argument_count() == 2);
    if ((args[0]->type() == Item::FIELD_ITEM) &&
        (my_strcasecmp(system_charset_info, ((Item_field *)args[0])->field_name,
                       m_field_name.str) == 0)) {
      return get_item_hash_value(args[1]);
    }
  }
  push_warning_printf(thd, Sql_condition::SL_WARNING,
                      ER_CCL_QUEUE_FIELD_NOT_FOUND,
                      ER_THD(thd, ER_CCL_QUEUE_FIELD_NOT_FOUND), "");
  return 0;
}

/**
  Print the hint string when explain.
*/
void Ccl_queue_hint::print(const THD *thd, String *str) {
  std::stringstream ss;
  ss << name() << "(";
  str->append(ss.str().c_str(), ss.str().length());
  if (m_item) m_item->print(thd, str, QT_ORDINARY);
  str->append(STRING_WITH_LEN(") "));
}

} /* namespace im */
