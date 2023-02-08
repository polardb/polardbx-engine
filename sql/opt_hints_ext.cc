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

void Sample_percentage_hint::print(const THD *, String *str) {
  std::stringstream ss;
  ss << "SAMPLE_PERCENTAGE"
     << "(" << this->pct << ")";
  str->append(ss.str().c_str(), ss.str().length());
}

bool check_sample_semantic(LEX *lex) {
  DBUG_ASSERT(lex);

  auto select = lex->current_select();
  if (select == nullptr) {
    return false;
  }

  if (!lex->is_single_level_stmt() || select->where_cond() != nullptr ||
      select->having_cond() != nullptr || select->is_ordered() ||
      select->is_grouped() || select->explicit_limit) {
    return true;
  }

  if (select->table_list.elements != 1 ||
      select->table_list.first->index_hints != nullptr ||
      select->table_list.first->is_view_or_derived() ||
      select->table_list.first->is_table_function()) {
    return true;
  }

  return false;
}
