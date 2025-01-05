/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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

//
// Created by 0xCC on 2024/11/26.
//

#pragma once

#include "plugin/polarx_rpc/global_defines.h"

#include "m_string.h"
#include "sql/package/proc.h"

// proc for proxy
namespace im {

extern LEX_CSTRING PROXY_PROC_SCHEMA;

class proxy_proc_base : public Proc {
 public:
  explicit proxy_proc_base(PSI_memory_key key) : Proc(key) {}

  /* Setting timestamp native procedure schema */
  const std::string qname() const final {
    return std::string(PROXY_PROC_SCHEMA.str) + "." + str();
  }
};

class Proc_reset_db : public proxy_proc_base {
 public:
  explicit Proc_reset_db(PSI_memory_key key) : proxy_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  static Proc *instance();

#ifdef MYSQL8PLUS
  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;
#else
  Sql_cmd *invoke_cmd(THD *thd, List<Item> *list) const final;
#endif

  const std::string str() const final { return {"reset_db"}; }
};

class Proc_ping : public proxy_proc_base {
 public:
  explicit Proc_ping(PSI_memory_key key) : proxy_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  static Proc *instance();

  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;

  const std::string str() const final { return {"ping"}; }
};

class Cmd_reset_db : public Sql_cmd_trans_proc {
 public:
#ifdef MYSQL8PLUS
  Cmd_reset_db(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
#else
  Cmd_reset_db(THD *thd, List<Item> *list, const Proc *proc)
#endif
      : Sql_cmd_trans_proc(thd, list, proc) {
  }

  bool pc_execute(THD *thd) final;
};

class Cmd_ping : public Sql_cmd_trans_proc {
 public:
  Cmd_ping(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
      : Sql_cmd_trans_proc(thd, list, proc) {
  }

  bool pc_execute(THD *thd) final;
};

class Proc_get_token : public proxy_proc_base {
 public:
  explicit Proc_get_token(PSI_memory_key key) : proxy_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("token"), 128});
  }

  static Proc *instance();

#ifdef MYSQL8PLUS
  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;
#else
  Sql_cmd *invoke_cmd(THD *thd, List<Item> *list) const final;
#endif

  const std::string str() const final { return {"get_token"}; }
};

class Cmd_get_token : public Sql_cmd_admin_proc {
 public:
#ifdef MYSQL8PLUS
  Cmd_get_token(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
#else
  Cmd_get_token(THD *thd, List<Item> *list, const Proc *proc)
#endif
      : Sql_cmd_admin_proc(thd, list, proc) {
  }

  bool pc_execute(THD *) final { return false; }

  void send_result(THD *thd, bool error) final;
};

class Proc_switch_user : public proxy_proc_base {
 public:
  explicit Proc_switch_user(PSI_memory_key key) : proxy_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    // token, user, host
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
  }

  static Proc *instance();

#ifdef MYSQL8PLUS
  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;
#else
  Sql_cmd *invoke_cmd(THD *thd, List<Item> *list) const final;
#endif

  const std::string str() const final { return {"switch_user"}; }
};

class Cmd_switch_user : public Sql_cmd_trans_proc {
 public:
#ifdef MYSQL8PLUS
  Cmd_switch_user(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
#else
  Cmd_switch_user(THD *thd, List<Item> *list, const Proc *proc)
#endif
      : Sql_cmd_trans_proc(thd, list, proc) {
  }

  bool pc_execute(THD *thd) final;
};

}  // namespace im
