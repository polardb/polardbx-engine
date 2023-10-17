/* Copyright (c) 2018, 2020, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/ccl/ccl_interface.h"
#include "sql/ccl/ccl.h"
#include "sql/ccl/ccl_common.h"
#include "sql/sql_class.h"
#include "sql/sql_lex.h"

namespace im {

Ccl_comply_wrapper::Ccl_comply_wrapper(THD *thd) : m_thd(thd) {
  Ccl_comply_handler *handler = new Ccl_comply_handler(m_thd);

  m_thd->ccl_comply_handlers.push_back(handler);

  m_handle = handler;
}

Ccl_comply_wrapper::~Ccl_comply_wrapper() {
  assert(m_thd->ccl_comply_handlers.size() > 0);

  Ccl_comply_handler *handler = m_thd->ccl_comply_handlers.back();

  assert(handler == m_handle);

  handler->comply_end();

  m_thd->ccl_comply_handlers.pop_back();
  delete handler;
}

void Ccl_comply_handler::comply_rule(enum_sql_command sql_command,
                                     Table_ref *all_tables,
                                     const LEX_CSTRING &query) {
  if (m_complied) return;

  m_complied = m_thd->ccl_comply->comply_rule(sql_command, all_tables, query);
}

void Ccl_comply_handler::comply_queue(THD *thd, LEX *lex,
                                      Ccl_queue_hint *hint) {
  assert(thd == m_thd);

  if (m_complied) return;

  m_complied = m_thd->ccl_comply->comply_queue(thd, lex, hint);
}

void Ccl_comply_handler::comply_end() {
  if (m_complied) m_thd->ccl_comply->comply_end();

  m_complied = false;
}

void do_ccl_comply_queue_or_rule(THD *thd, LEX *lex, Table_ref *table_list) {
  if (!thd || thd->ccl_comply_handlers.size() == 0) return;

  Ccl_comply_handler *handler = thd->ccl_comply_handlers.back();

  if (!handler) return;

  if (lex->opt_hints_global && lex->opt_hints_global->ccl_queue_hint)
    handler->comply_queue(thd, lex, lex->opt_hints_global->ccl_queue_hint);
  else
    handler->comply_rule(lex->sql_command, table_list, thd->query());
}

}  // namespace im
