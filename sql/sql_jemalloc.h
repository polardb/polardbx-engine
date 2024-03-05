/* Copyright (c) 2000, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_SQL_JEMALLOC_INCLUDED
#define SQL_SQL_JEMALLOC_INCLUDED

#ifdef RDS_HAVE_JEMALLOC

#include "sql/package/proc.h"

class sys_var;
class THD;
class set_var;

namespace im {

extern bool opt_rds_active_memory_profiling;

extern void jemalloc_profiling_state();

extern const LEX_CSTRING JEMALLOC_PROC_SCHEMA;

extern bool check_active_memory_profiling(sys_var *, THD *thd, set_var *var);

class Sql_cmd_jemalloc_profile : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_jemalloc_profile(THD *thd, mem_root_deque<Item *> *list,
                                    const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}
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

class Jemalloc_profile_proc : public Proc, public Disable_copy_base {
  using Sql_cmd_type = Sql_cmd_jemalloc_profile;

  enum enum_column { COLUMN_STATUS = 0, COLUMN_MESSAGE, COLUMN_LAST };

 public:
  explicit Jemalloc_profile_proc(PSI_memory_key key = 0) : Proc(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("STATUS"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("MESSAGE"), 2048}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /* Singleton instance */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual ~Jemalloc_profile_proc() {}

  /* Proc name */
  virtual const std::string str() const override { return std::string("profile"); }

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << JEMALLOC_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

} /* namespace im */

#endif /* RDS_HAVE_JEMALLOC */

#endif /* SQL_SQL_JEMALLOC_INCLUDED */

