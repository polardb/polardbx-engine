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

#ifndef SQL_CCL_CCL_INTERFACE_INCLUDED
#define SQL_CCL_CCL_INTERFACE_INCLUDED

#include "lex_string.h"
#include "my_sqlcommand.h"

class THD;
struct LEX;
class Table_ref;
class Item;

namespace im {

class Ccl_queue_hint;

extern bool ccl_queue_hot_update;
extern bool ccl_queue_hot_delete;

/* Initialize system concurrency control */
extern void ccl_init();
/* Destroy system concurrency control */
extern void ccl_destroy();
/**
  Init the rules when mysqld reboot

  It should log error message if failed, reported client error
  will be ingored.

  @param[in]      bootstrap     Whether initialize or restart.
*/
extern void ccl_rules_init(bool bootstrap);

class Ccl_comply_handler {
 public:
  explicit Ccl_comply_handler(THD *thd) : m_thd(thd), m_complied(false) {}

 public:
  void comply_rule(enum_sql_command sql_command, Table_ref *all_tables,
                   const LEX_CSTRING &query);

  void comply_queue(THD *thd, LEX *lex, Ccl_queue_hint *hint);

  void comply_end();

 private:
  THD *m_thd;
  bool m_complied;
};

class Ccl_comply_wrapper {
 public:
  explicit Ccl_comply_wrapper(THD *thd);

  ~Ccl_comply_wrapper();

 private:
  THD *m_thd;
  Ccl_comply_handler *m_handle;
};

void do_ccl_comply_queue_or_rule(THD *thd, LEX *lex, Table_ref *table_list);

} /* namespace im */

#endif
