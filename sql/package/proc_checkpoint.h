/*****************************************************************************

Copyright (c) 2013, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file sql/package/proc_checkpoint.h
 Native procedure for retrieving checkpoint related statistics.
 *******************************************************/

#ifndef SQL_PROC_CHECKPOINT_INCLUDED
#define SQL_PROC_CHECKPOINT_INCLUDED

#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING PROC_CHECKPOINT_SCHEMA;

/**
    Proc base for dbms_checkpoint
*/
class Checkpoint_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Checkpoint_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << PROC_CHECKPOINT_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_checkpoint

  1) dbms_checkpoint didn't require any privileges;
*/
class Sql_cmd_checkpoint_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_checkpoint_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    /**
      Require not any privileges when execute
      comment_connection()
    */
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
1) dbms_checkpoint.advance_checkpoint(...)

It will advance checkpoint in thd.
 */
class Sql_cmd_checkpoint_advance : public Sql_cmd_checkpoint_proc_base {
 public:
  explicit Sql_cmd_checkpoint_advance(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_checkpoint_proc_base(thd, list, proc) {}
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Advance_checkpoint_proc : public Checkpoint_proc_base {
  using Sql_cmd_type = Sql_cmd_checkpoint_advance;

 public:
  explicit Advance_checkpoint_proc(PSI_memory_key key)
      : Checkpoint_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Advance_checkpoint_proc() override {}

  virtual const std::string str() const override {
    return std::string("advance_checkpoint");
  }
};

} /* namespace im */

#endif
