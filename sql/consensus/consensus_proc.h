/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef CONSENSUS_PROC_INCLUDED
#define CONSENSUS_PROC_INCLUDED

#include <sstream>
#include <vector>
#include "sql/package/proc.h"

/**
  Consensus procedures (dbms_consensus package)

  Configure commands:
    dbms_consensus.change_leader
    dbms_consensus.add_learner
    dbms_consensus.add_follower
    dbms_consensus.drop_learner
    dbms_consensus.upgrade_learner
    dbms_consensus.downgrade_follower
    dbms_consensus.refresh_learner_meta
    dbms_consensus.configure_follower
    dbms_consensus.configure_learner
    dbms_consensus.force_single_mode
    dbms_consensus.fix_cluster_id
    dbms_consensus.fix_matchindex

  Show commands:
    dbms_consensus.show_cluster_global
    dbms_consensus.show_cluster_local
*/

namespace im {

enum class Consensus_proc_type_enum : uint64_t {
  IP_PORT,
  NODE,
  UINT,
  BOOL,
};

class Consensus_proc_type {
 public:
  virtual ~Consensus_proc_type() = default;
  virtual bool check(Item *item) const = 0;
  [[nodiscard]] virtual enum_field_types mysql_field_type() const = 0;
  [[nodiscard]] virtual Consensus_proc_type_enum consensus_field_type_enum()
      const = 0;

  // output
  virtual uint64_t get_uint64_t(Item *) const {
    assert(0);
    return 0;
  }
  virtual std::string get_string(Item *) const {
    assert(0);
    return "";
  }
  virtual bool get_bool(Item *) const {
    assert(0);
    return false;
  }

 protected:
  Consensus_proc_type() = default;
};

class Consensus_proc_type_ip_port : public Consensus_proc_type {
 public:
  static const Consensus_proc_type *instance() {
    static Consensus_proc_type_ip_port instance;
    return &instance;
  }
  bool check(Item *item) const override;
  [[nodiscard]] enum_field_types mysql_field_type() const override {
    return MYSQL_TYPE_VARCHAR;
  }
  [[nodiscard]] Consensus_proc_type_enum consensus_field_type_enum()
      const override {
    return Consensus_proc_type_enum::IP_PORT;
  }
  std::string get_string(Item *item) const override;
};

class Consensus_proc_type_node : public Consensus_proc_type {
 public:
  static const Consensus_proc_type *instance() {
    static Consensus_proc_type_node instance;
    return &instance;
  }
  bool check(Item *item) const override;
  [[nodiscard]] enum_field_types mysql_field_type() const override {
    return MYSQL_TYPE_VARCHAR;
  }
  [[nodiscard]] Consensus_proc_type_enum consensus_field_type_enum()
      const override {
    return Consensus_proc_type_enum::NODE;
  }
  uint64_t get_uint64_t(Item *item) const override;
  std::string get_string(Item *item) const override;
};

class Consensus_proc_type_uint : public Consensus_proc_type {
 public:
  static const Consensus_proc_type *instance() {
    static Consensus_proc_type_uint instance;
    return &instance;
  }
  bool check(Item *item) const override;
  [[nodiscard]] enum_field_types mysql_field_type() const override {
    return MYSQL_TYPE_LONGLONG;
  }
  [[nodiscard]] Consensus_proc_type_enum consensus_field_type_enum()
      const override {
    return Consensus_proc_type_enum::UINT;
  }
  uint64_t get_uint64_t(Item *item) const override;
};

class Consensus_proc_type_bool : public Consensus_proc_type {
 public:
  static const Consensus_proc_type *instance() {
    static Consensus_proc_type_bool instance;
    return &instance;
  }
  bool check(Item *item) const override;
  [[nodiscard]] enum_field_types mysql_field_type() const override {
    return MYSQL_TYPE_LONGLONG;
  }
  [[nodiscard]] Consensus_proc_type_enum consensus_field_type_enum()
      const override {
    return Consensus_proc_type_enum::BOOL;
  }
  bool get_bool(Item *item) const override;
};

class Consensus_proc_type_factory {
 public:
  static const Consensus_proc_type *create(Consensus_proc_type_enum type);
};

extern LEX_CSTRING CONSENSUS_PROC_SCHEMA;

/**
  Proc base for dbms_consensus

  1) Uniform schema: dbms_consensus
*/
class Consensus_proc : public Proc, public Disable_copy_base {
 protected:
  std::vector<const Consensus_proc_type *> m_consensus_proc_params;

 public:
  explicit Consensus_proc(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  const std::string qname() const override {
    std::stringstream ss;
    ss << CONSENSUS_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }

  [[nodiscard]] const std::vector<const Consensus_proc_type *>
      &consensus_proc_params() const {
    return m_consensus_proc_params;
  }

 protected:
  template <typename T>
  void fill_params(const T &params) {
    for (const auto &param : params) {
      const Consensus_proc_type *consensus_proc_type =
          Consensus_proc_type_factory::create(param);
      m_parameters.push_back(consensus_proc_type->mysql_field_type());
      m_consensus_proc_params.emplace_back(consensus_proc_type);
    }
  }
};

/**
  Base class for proc with only one "ip:port" parameter.
*/
class Consensus_proc_node_param : public Consensus_proc {
 public:
  explicit Consensus_proc_node_param(PSI_memory_key key) : Consensus_proc(key) {
    static constexpr auto params = {Consensus_proc_type_enum::NODE};
    fill_params(params);
  }
};

/**
  Sql command base for dbms_consensus

  1) dbms_consensus require super privileges;
*/
class Sql_cmd_consensus_proc : public Sql_cmd_admin_proc {
 protected:
  const Consensus_proc *m_consensus_proc;

 public:
  Sql_cmd_consensus_proc(THD *thd, mem_root_deque<Item *> *list,
                         const Consensus_proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc), m_consensus_proc(proc) {}

  bool pc_execute(THD *) override { return false; }
  bool check_parameter() override;
  bool check_access(THD *thd) override;

 protected:
  virtual bool check_parameter_num();
};

/**
  Base class for Sql_cmd_proc with last params optional.
*/
class Sql_cmd_consensus_option_last_proc : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_option_last_proc(THD *thd, mem_root_deque<Item *> *list,
                                     const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

 protected:
  bool check_parameter_num() override;
};

/**
  Base class for Sql_cmd_proc which is not allowed to execute on logger.
*/
class Sql_cmd_consensus_no_logger_proc : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_no_logger_proc(THD *thd, mem_root_deque<Item *> *list,
                                   const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  bool check_access(THD *thd) override;
};

/**
  Base class for Sql_cmd_consensus_option_last_proc which is not allowed to
  execute on logger.
*/
class Sql_cmd_consensus_option_last_no_logger_proc
    : public Sql_cmd_consensus_option_last_proc {
 public:
  Sql_cmd_consensus_option_last_no_logger_proc(THD *thd,
                                               mem_root_deque<Item *> *list,
                                               const Consensus_proc *proc)
      : Sql_cmd_consensus_option_last_proc(thd, list, proc) {}
  bool check_access(THD *thd) override;
};

/**
  dbms_consensus.change_leader(...)
*/
class Sql_cmd_consensus_proc_change_leader : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_change_leader(THD *thd, mem_root_deque<Item *> *list,
                                       const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_change_leader final : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_change_leader;

 public:
  explicit Consensus_proc_change_leader(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}

  ~Consensus_proc_change_leader() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("change_leader");
  }
};

/**
  dbms_consensus.add_learner(...)
*/
class Sql_cmd_consensus_proc_add_learner
    : public Sql_cmd_consensus_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_add_learner(THD *thd, mem_root_deque<Item *> *list,
                                     const Consensus_proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
  bool prepare(THD *thd) override;
};

class Consensus_proc_add_learner final : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_add_learner;

 public:
  explicit Consensus_proc_add_learner(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}

  ~Consensus_proc_add_learner() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override { return std::string("add_learner"); }
};

/**
  dbms_consensus.add_follower(...)
*/
class Sql_cmd_consensus_proc_add_follower
    : public Sql_cmd_consensus_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_add_follower(THD *thd, mem_root_deque<Item *> *list,
                                      const Consensus_proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  bool pc_execute(THD *thd) override;
  bool prepare(THD *thd) override;
};

class Consensus_proc_add_follower final : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_add_follower;

 public:
  explicit Consensus_proc_add_follower(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}
  ~Consensus_proc_add_follower() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override { return std::string("add_follower"); }
};

/**
  dbms_consensus.drop_learner(...)
*/
class Sql_cmd_consensus_proc_drop_learner
    : public Sql_cmd_consensus_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_drop_learner(THD *thd, mem_root_deque<Item *> *list,
                                      const Consensus_proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  bool pc_execute(THD *thd) override;
};

class Consensus_proc_drop_learner final : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_drop_learner;

 public:
  explicit Consensus_proc_drop_learner(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}
  ~Consensus_proc_drop_learner() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override { return std::string("drop_learner"); }
};

/**
  dbms_consensus.upgrade_learner(...)
*/
class Sql_cmd_consensus_proc_upgrade_learner
    : public Sql_cmd_consensus_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_upgrade_learner(THD *thd, mem_root_deque<Item *> *list,
                                         const Consensus_proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  bool pc_execute(THD *thd) override;
};

class Consensus_proc_upgrade_learner final : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_upgrade_learner;

 public:
  explicit Consensus_proc_upgrade_learner(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}
  ~Consensus_proc_upgrade_learner() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("upgrade_learner");
  }
};

/**
  dbms_consensus.downgrade_follower(...)
*/
class Sql_cmd_consensus_proc_downgrade_follower
    : public Sql_cmd_consensus_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_downgrade_follower(THD *thd,
                                            mem_root_deque<Item *> *list,
                                            const Consensus_proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  bool pc_execute(THD *thd) override;
};

class Consensus_proc_downgrade_follower final
    : public Consensus_proc_node_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_downgrade_follower;

 public:
  explicit Consensus_proc_downgrade_follower(PSI_memory_key key)
      : Consensus_proc_node_param(key) {}
  ~Consensus_proc_downgrade_follower() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("downgrade_follower");
  }
};

/**
  dbms_consensus.refresh_learner_meta()
*/
class Sql_cmd_consensus_proc_refresh_learner_meta
    : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_refresh_learner_meta(THD *thd,
                                              mem_root_deque<Item *> *list,
                                              const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  bool pc_execute(THD *thd) override;
};

class Consensus_proc_refresh_learner_meta final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_refresh_learner_meta;

 public:
  explicit Consensus_proc_refresh_learner_meta(PSI_memory_key key)
      : Consensus_proc(key) {}
  ~Consensus_proc_refresh_learner_meta() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("refresh_learner_meta");
  }
};

/**
  dbms_consensus.configure_follower(...)
*/
class Sql_cmd_consensus_proc_configure_follower
    : public Sql_cmd_consensus_option_last_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_configure_follower(THD *thd,
                                            mem_root_deque<Item *> *list,
                                            const Consensus_proc *proc)
      : Sql_cmd_consensus_option_last_no_logger_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_configure_follower final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_configure_follower;

 public:
  explicit Consensus_proc_configure_follower(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::NODE,
        Consensus_proc_type_enum::UINT,  // weight
        Consensus_proc_type_enum::BOOL,  // force_sync
    };
    fill_params(params);
  }
  ~Consensus_proc_configure_follower() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("configure_follower");
  }
};

/**
  dbms_consensus.configure_learner(...)
*/
class Sql_cmd_consensus_proc_configure_learner
    : public Sql_cmd_consensus_option_last_no_logger_proc {
 public:
  Sql_cmd_consensus_proc_configure_learner(THD *thd,
                                           mem_root_deque<Item *> *list,
                                           const Consensus_proc *proc)
      : Sql_cmd_consensus_option_last_no_logger_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_configure_learner final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_configure_learner;

 public:
  explicit Consensus_proc_configure_learner(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::NODE, Consensus_proc_type_enum::NODE,
        Consensus_proc_type_enum::BOOL,  // use_applied
    };
    fill_params(params);
  }

  ~Consensus_proc_configure_learner() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("configure_learner");
  }
};

/**
  dbms_consensus.force_single_mode()
*/
class Sql_cmd_consensus_proc_force_single_mode : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_force_single_mode(THD *thd,
                                           mem_root_deque<Item *> *list,
                                           const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd) override;
};

class Consensus_proc_force_single_mode final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_force_single_mode;

 public:
  explicit Consensus_proc_force_single_mode(PSI_memory_key key)
      : Consensus_proc(key) {}
  ~Consensus_proc_force_single_mode() override {}
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("force_single_mode");
  }
};

/**
  dbms_consensus.fix_cluster_id(...)
*/
class Sql_cmd_consensus_proc_fix_cluster_id : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_fix_cluster_id(THD *thd, mem_root_deque<Item *> *list,
                                        const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_fix_cluster_id final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_fix_cluster_id;

 public:
  explicit Consensus_proc_fix_cluster_id(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::UINT,  // cluster_id
    };
    fill_params(params);
  }
  ~Consensus_proc_fix_cluster_id() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("fix_cluster_id");
  }
};

/**
  dbms_consensus.fix_matchindex(...)
*/
class Sql_cmd_consensus_proc_fix_matchindex : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_fix_matchindex(THD *thd, mem_root_deque<Item *> *list,
                                        const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_fix_matchindex final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_fix_matchindex;

 public:
  explicit Consensus_proc_fix_matchindex(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::NODE,
        Consensus_proc_type_enum::UINT,  // index
    };
    fill_params(params);
  }

  ~Consensus_proc_fix_matchindex() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("fix_matchindex");
  }
};

/**
  dbms_consensus.show_cluster_global()
*/
class Sql_cmd_consensus_proc_show_global : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_show_global(THD *thd, mem_root_deque<Item *> *list,
                                     const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  void send_result(THD *thd, bool error) override;
};

class Consensus_proc_show_global final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_show_global;

  enum enum_column {
    COLUMN_ID = 0,
    COLUMN_IP_PORT,
    COLUMN_MATCH_INDEX,
    COLUMN_NEXT_INDEX,
    COLUMN_ROLE,
    COLUMN_FORCE_SYNC,
    COLUMN_ELECTION_WEIGHT,
    COLUMN_LEARNER_SOURCE,
    COLUMN_APPLIED_INDEX,
    COLUMN_PIPELINING,
    COLUMN_SEND_APPLIED,
    COLUMN_LAST
  };

 public:
  explicit Consensus_proc_show_global(PSI_memory_key key)
      : Consensus_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ID"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("IP_PORT"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MATCH_INDEX"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NEXT_INDEX"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ROLE"), 16},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FORCE_SYNC"), 8},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ELECTION_WEIGHT"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("LEARNER_SOURCE"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("APPLIED_INDEX"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("PIPELINING"), 8},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SEND_APPLIED"), 8},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.push_back(elements[i]);
    }
  }
  ~Consensus_proc_show_global() override {}
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("show_cluster_global");
  }
};

/**
  dbms_consensus.show_cluster_local()
*/
class Sql_cmd_consensus_proc_show_local : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_show_local(THD *thd, mem_root_deque<Item *> *list,
                                    const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  void send_result(THD *thd, bool error) override;
};

class Consensus_proc_show_local final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_show_local;

  enum enum_column {
    COLUMN_ID = 0,
    COLUMN_TERM,
    COLUMN_CURRENT_LEADER,
    COLUMN_COMMIT_INDEX,
    COLUMN_LAST_LOG_TERM,
    COLUMN_LAST_LOG_INDEX,
    COLUMN_ROLE,
    COLUMN_VOTE_FOR,
    COLUMN_APPLIED_INDEX,
    COLUMN_SERVER_READY_FOR_RW,
    COLUMN_INSTANCE_TYPE,
    COLUMN_LAST
  };

 public:
  explicit Consensus_proc_show_local(PSI_memory_key key) : Consensus_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ID"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("CURRENT_TERM"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("CURRENT_LEADER"), 8},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("COMMIT_INDEX"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("LAST_LOG_TERM"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("LAST_LOG_INDEX"), 16},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ROLE"), 8},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("VOTE_FOR"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("APPLIED_INDEX"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SERVER_READY_FOR_RW"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("INSTANCE_TYPE"), 8},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.push_back(elements[i]);
    }
  }
  ~Consensus_proc_show_local() override {}
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("show_cluster_local");
  }
};

/**
  dbms_consensus.show_logs()
*/
class Sql_cmd_consensus_proc_show_logs : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_show_logs(THD *thd, mem_root_deque<Item *> *list,
                                   const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  bool check_access(THD *thd) override;
  void send_result(THD *thd, bool error) override;
};

class Consensus_proc_show_logs final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_show_logs;

  enum enum_column {
    COLUMN_LOG_NAME = 0,
    COLUMN_FILE_SIZE,
    COLUMN_START_LOG_INEDX,
    COLUMN_LAST
  };

 public:
  explicit Consensus_proc_show_logs(PSI_memory_key key) : Consensus_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Log_name"), 255},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("File_size"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Start_log_index"), 0},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.push_back(elements[i]);
    }
  }
  ~Consensus_proc_show_logs() override {}
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override { return std::string("show_logs"); }
};

/**
  dbms_consensus.purge_log(...)
*/
class Sql_cmd_consensus_proc_purge_log : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_purge_log(THD *thd, mem_root_deque<Item *> *list,
                                   const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_purge_log final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_purge_log;

 public:
  explicit Consensus_proc_purge_log(PSI_memory_key key) : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::UINT,  // index
    };
    fill_params(params);
  }

  ~Consensus_proc_purge_log() override = default;

  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override { return std::string("purge_log"); }
};

/**
  dbms_consensus.local_purge_log(...)
*/
class Sql_cmd_consensus_proc_local_purge_log : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_local_purge_log(THD *thd, mem_root_deque<Item *> *list,
                                         const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_local_purge_log final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_local_purge_log;

 public:
  explicit Consensus_proc_local_purge_log(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::UINT,  // index
    };
    fill_params(params);
  }

  ~Consensus_proc_local_purge_log() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("local_purge_log");
  }
};

/**
  dbms_consensus.force_purge_log(...)
*/
class Sql_cmd_consensus_proc_force_purge_log : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_force_purge_log(THD *thd, mem_root_deque<Item *> *list,
                                         const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_force_purge_log final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_force_purge_log;

 public:
  explicit Consensus_proc_force_purge_log(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::UINT,  // index
    };
    fill_params(params);
  }

  ~Consensus_proc_force_purge_log() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("force_purge_log");
  }
};

/**
  dbms_consensus.drop_prefetch_channel(...)
*/
class Sql_cmd_consensus_proc_drop_prefetch_channel
    : public Sql_cmd_consensus_proc {
 public:
  Sql_cmd_consensus_proc_drop_prefetch_channel(THD *thd,
                                               mem_root_deque<Item *> *list,
                                               const Consensus_proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}

  bool pc_execute(THD *thd) override;
};

class Consensus_proc_drop_prefetch_channel final : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_drop_prefetch_channel;

 public:
  explicit Consensus_proc_drop_prefetch_channel(PSI_memory_key key)
      : Consensus_proc(key) {
    static constexpr auto params = {
        Consensus_proc_type_enum::UINT,  // channel_id
    };
    fill_params(params);
  }
  ~Consensus_proc_drop_prefetch_channel() override = default;
  static Proc *instance();
  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;
  const std::string str() const override {
    return std::string("drop_prefetch_channel");
  }
};

} /* namespace im */

#endif
