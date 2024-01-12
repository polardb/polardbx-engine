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

}  // namespace im
