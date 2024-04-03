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

#ifndef SQL_PACKAGE_PROC_GENERATE_INCLUDED
#define SQL_PACKAGE_PROC_GENERATE_INCLUDED

#include "keyring_common.h"

namespace im {

/**
  Usage: call dbms_keyring.generate_key(),
  called for generating a master key.
*/
class Cmd_generate_key : public Sql_cmd_admin_proc {
 public:
  explicit Cmd_generate_key(THD *thd, mem_root_deque<Item *> *list,
                            const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  bool pc_execute(THD *thd) override;
};

class Proc_generate_key : public Keyring_proc_base {
 public:
  explicit Proc_generate_key(PSI_memory_key key) : Keyring_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override { return std::string("generate_key"); }
};

}  // namespace im

#endif
