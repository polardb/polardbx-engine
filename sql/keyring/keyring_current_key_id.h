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

#ifndef SQL_PACKAGE_PROC_KEY_ID_CURRENT_INCLUDED
#define SQL_PACKAGE_PROC_KEY_ID_CURRENT_INCLUDED

#include "keyring_common.h"

namespace im {

/**
  Usage: call dbms_keyring.current_key_id()
*/
class Cmd_current_key_id : public Sql_cmd_admin_proc {
 public:
  explicit Cmd_current_key_id(THD *thd, mem_root_deque<Item *> *list,
                              const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  bool pc_execute(THD *) override { return false; }

  void send_result(THD *thd, bool error) override;
};

class Proc_current_key_id : public Keyring_proc_base {
 public:
  explicit Proc_current_key_id(PSI_memory_key key) : Keyring_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    static const std::string col_name("key_id");
    Column_element element = {MYSQL_TYPE_VARCHAR, col_name.c_str(),
                              col_name.length(), 64};
    m_columns.push_back(std::move(element));
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("current_key_id");
  }
};

}  // namespace im

#endif
