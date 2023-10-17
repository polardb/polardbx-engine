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

#ifndef SQL_CCL_CCL_HINT_PARSE_INCLUDED
#define SQL_CCL_CCL_HINT_PARSE_INCLUDED

#include "sql/ccl/ccl_hint.h"
#include "sql/parse_tree_hints.h"

Opt_hints_global *get_global_hints(Parse_context *pc);

namespace im {

/**
  Hint PT definition for hint parser.
*/
class PT_hint_ccl_queue : public PT_hint {
  typedef PT_hint super;

 public:
  explicit PT_hint_ccl_queue(Ccl_hint_type type_arg, Item *item_arg)
      : PT_hint(CCL_QUEUE_HINT_ENUM, true),
        m_type(type_arg),
        m_item(item_arg) {}

  virtual bool contextualize(Parse_context *pc) override;

 private:
  Ccl_hint_type m_type;
  Item *m_item;
};

/**
  Fill the where condition into ccl hint structure.

  @param[in]      pc        Parse context
  @param[in]      cond      Equal condition
  @param[in]      left      field item
  @param[in]      right     value item

*/
extern void fill_ccl_queue_field_cond(Parse_context *pc, Item *cond, Item *left,
                                      Item *right);

} /* namespace im */

#endif
