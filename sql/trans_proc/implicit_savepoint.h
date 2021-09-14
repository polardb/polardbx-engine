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

#ifndef SQL_TRANS_PROC_IMPLICIT_SAVEPOINT_INCLUDED
#define SQL_TRANS_PROC_IMPLICIT_SAVEPOINT_INCLUDED

#include "sql/trans_proc/common.h"

/**
  Rollback to the last implicit savepoint which is made internally when running
  INSERT/DELETE/UPDATE statement if variable 'auto_saveponit' is ON.

  CALL dbms_trans.rollback_to_implicit_savepoint();

*/

namespace im {

/**
  Proc of rollback_to_implicit_savepoint
*/
class Trans_proc_implicit_savepoint : public Trans_proc_base {
 public:
  explicit Trans_proc_implicit_savepoint(PSI_memory_key key)
      : Trans_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  /* Singleton instance of this class */
  static Proc *instance();

  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("rollback_to_implicit_savepoint");
  }
};

/**
  Cmd of rollback_to_implicit_savepoint
*/
class Sql_cmd_trans_proc_implicit_savepoint : public Sql_cmd_trans_proc {
 public:
  explicit Sql_cmd_trans_proc_implicit_savepoint(THD *thd,
                                                 mem_root_deque<Item *> *list,
                                                 const Proc *proc)
      : Sql_cmd_trans_proc(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

}  // namespace im

#endif
