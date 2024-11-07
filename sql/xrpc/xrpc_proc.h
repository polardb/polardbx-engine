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
// Created by zzy on 2022/8/25.
//

#pragma once

#include "plugin/polarx_rpc/global_defines.h"

#include "m_string.h"
#include "sql/package/proc.h"

/// proc for hist show
namespace im {

extern LEX_CSTRING XRPC_PROC_SCHEMA;

class Xrpc_proc_base : public Proc {
 public:
  explicit Xrpc_proc_base(PSI_memory_key key) : Proc(key) {}

  /* Setting timestamp native procedure schema */
  const std::string qname() const final {
    return std::string(XRPC_PROC_SCHEMA.str) + "." + str();
  }
};

class Proc_perf_hist : public Xrpc_proc_base {
 public:
  explicit Proc_perf_hist(PSI_memory_key key) : Xrpc_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("name"), 128});
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("hist"), 65535});
  }

  static Proc *instance();

#ifdef MYSQL8PLUS
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;
#else
  Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const final;
#endif

  const std::string str() const final { return {"perf_hist"}; }
};

class Cmd_perf_hist : public Sql_cmd_admin_proc {
 public:
#ifdef MYSQL8PLUS
  Cmd_perf_hist(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
#else
  Cmd_perf_hist(THD *thd, List<Item> *list, const Proc *proc)
#endif
      : Sql_cmd_admin_proc(thd, list, proc) {
  }

  bool pc_execute(THD *thd) final;

  void send_result(THD *thd, bool error) final;

 private:
  std::string name_;
};

class Proc_cmd : public Xrpc_proc_base {
 public:
  explicit Proc_cmd(PSI_memory_key key) : Xrpc_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_columns.push_back(
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("result"), 65535});
  }

  static Proc *instance();

#ifdef MYSQL8PLUS
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const final;
#else
  Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const final;
#endif

  const std::string str() const final { return {"cmd"}; }
};

class Cmd_cmd : public Sql_cmd_admin_proc {
 public:
#ifdef MYSQL8PLUS
  Cmd_cmd(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
#else
  Cmd_cmd(THD *thd, List<Item> *list, const Proc *proc)
#endif
      : Sql_cmd_admin_proc(thd, list, proc) {
  }

  bool pc_execute(THD *thd) final;

  void send_result(THD *thd, bool error) final;

 private:
  std::string name_;
};

}  // namespace im
