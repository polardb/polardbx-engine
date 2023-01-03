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

extern LEX_CSTRING CONSENSUS_PROC_SCHEMA;

/**
  Proc base for dbms_consensus

  1) Uniform schema: dbms_consensus
*/
class Consensus_proc : public Proc, public Disable_copy_base {
public:
  explicit Consensus_proc(PSI_memory_key key) : Proc(key) {
    /* By default, consensus proc return only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }
  virtual const std::string qname() const {
    std::stringstream ss;
    ss << CONSENSUS_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Base class for proc with only one "ip:port" parameter.
*/
class Consensus_proc_str_param : public Consensus_proc {
  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_IPPORT,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_IPPORT:
      return MYSQL_TYPE_VARCHAR;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_str_param(PSI_memory_key key) : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_IPPORT; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
};

/**
  Sql command base for dbms_consensus

  1) dbms_consensus require super privileges;
*/
class Sql_cmd_consensus_proc : public Sql_cmd_admin_proc {
public:
  explicit Sql_cmd_consensus_proc(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *) { return false; };
  virtual bool check_parameter();
  virtual bool check_access(THD *thd);
protected:
  /* check whether a string is valid ip:port format */
  bool check_addr_format(const char *node_addr);
};

/**
  Base class for Sql_cmd_proc with last params optional.
*/
class Sql_cmd_consensus_option_last_proc : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_option_last_proc(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool check_parameter();
};

/**
  Base class for Sql_cmd_proc which is not allowed to execute on logger.
*/
class Sql_cmd_consensus_no_logger_proc : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_no_logger_proc(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool check_access(THD *thd);
};
/**
  Base class for Sql_cmd_consensus_option_last_proc which is not allowed to execute on logger.
*/
class Sql_cmd_consensus_option_last_no_logger_proc : public Sql_cmd_consensus_option_last_proc {
public:
  explicit Sql_cmd_consensus_option_last_no_logger_proc(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_option_last_proc(thd, list, proc) {}
  virtual bool check_access(THD *thd);
};

/**
  dbms_consensus.change_leader(...)
*/
class Sql_cmd_consensus_proc_change_leader : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_change_leader(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_change_leader : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_change_leader;

public:
  explicit Consensus_proc_change_leader(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_change_leader() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("change_leader"); }
};

/**
  dbms_consensus.add_learner(...)
*/
class Sql_cmd_consensus_proc_add_learner : public Sql_cmd_consensus_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_add_learner(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
  virtual bool prepare(THD *thd);
};

class Consensus_proc_add_learner : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_add_learner;

public:
  explicit Consensus_proc_add_learner(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_add_learner() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("add_learner"); }
};

/**
  dbms_consensus.add_follower(...)
*/
class Sql_cmd_consensus_proc_add_follower : public Sql_cmd_consensus_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_add_follower(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
  virtual bool prepare(THD *thd);
};

class Consensus_proc_add_follower : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_add_follower;

public:
  explicit Consensus_proc_add_follower(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_add_follower() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("add_follower"); }
};

/**
  dbms_consensus.drop_learner(...)
*/
class Sql_cmd_consensus_proc_drop_learner : public Sql_cmd_consensus_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_drop_learner(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_drop_learner : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_drop_learner;

public:
  explicit Consensus_proc_drop_learner(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_drop_learner() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("drop_learner"); }
};

/**
  dbms_consensus.upgrade_learner(...)
*/
class Sql_cmd_consensus_proc_upgrade_learner : public Sql_cmd_consensus_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_upgrade_learner(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_upgrade_learner : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_upgrade_learner;

public:
  explicit Consensus_proc_upgrade_learner(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_upgrade_learner() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("upgrade_learner"); }
};

/**
  dbms_consensus.downgrade_follower(...)
*/
class Sql_cmd_consensus_proc_downgrade_follower : public Sql_cmd_consensus_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_downgrade_follower(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_downgrade_follower : public Consensus_proc_str_param {
  using Sql_cmd_type = Sql_cmd_consensus_proc_downgrade_follower;

public:
  explicit Consensus_proc_downgrade_follower(PSI_memory_key key)
      : Consensus_proc_str_param(key) {}
  virtual ~Consensus_proc_downgrade_follower() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("downgrade_follower"); }
};

/**
  dbms_consensus.refresh_learner_meta()
*/
class Sql_cmd_consensus_proc_refresh_learner_meta : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_refresh_learner_meta(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_refresh_learner_meta : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_refresh_learner_meta;

public:
  explicit Consensus_proc_refresh_learner_meta(PSI_memory_key key)
      : Consensus_proc(key) {}
  virtual ~Consensus_proc_refresh_learner_meta() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("refresh_learner_meta"); }
};

/**
  dbms_consensus.configure_follower(...)
*/
class Sql_cmd_consensus_proc_configure_follower : public Sql_cmd_consensus_option_last_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_configure_follower(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_option_last_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_configure_follower : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_configure_follower;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_IPPORT,
    CONSENSUS_PARAM_WEIGHT,
    CONSENSUS_PARAM_FORCE_SYNC,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_IPPORT:
      return MYSQL_TYPE_VARCHAR;
    case CONSENSUS_PARAM_WEIGHT:
    case CONSENSUS_PARAM_FORCE_SYNC:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_configure_follower(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_IPPORT; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_configure_follower() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("configure_follower"); }
};

/**
  dbms_consensus.configure_learner(...)
*/
class Sql_cmd_consensus_proc_configure_learner : public Sql_cmd_consensus_option_last_no_logger_proc {
public:
  explicit Sql_cmd_consensus_proc_configure_learner(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_option_last_no_logger_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_configure_learner : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_configure_learner;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_IPPORT,
    CONSENSUS_PARAM_SOURCE,
    CONSENSUS_PARAM_USE_APPLIED,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_IPPORT:
    case CONSENSUS_PARAM_SOURCE:
      return MYSQL_TYPE_VARCHAR;
    case CONSENSUS_PARAM_USE_APPLIED:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_configure_learner(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_IPPORT; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_configure_learner() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("configure_learner"); }
};

/**
  dbms_consensus.force_single_mode()
*/
class Sql_cmd_consensus_proc_force_single_mode : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_force_single_mode(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_force_single_mode : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_force_single_mode;

public:
  explicit Consensus_proc_force_single_mode(PSI_memory_key key)
      : Consensus_proc(key) {}
  virtual ~Consensus_proc_force_single_mode() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("force_single_mode"); }
};

/**
  dbms_consensus.fix_cluster_id(...)
*/
class Sql_cmd_consensus_proc_fix_cluster_id : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_fix_cluster_id(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_fix_cluster_id : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_fix_cluster_id;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_CLUSTER_ID,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_CLUSTER_ID:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_fix_cluster_id(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_CLUSTER_ID; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_fix_cluster_id() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("fix_cluster_id"); }
};

/**
  dbms_consensus.fix_matchindex(...)
*/
class Sql_cmd_consensus_proc_fix_matchindex : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_fix_matchindex(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_fix_matchindex : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_fix_matchindex;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_IPPORT,
    CONSENSUS_PARAM_MATCHINDEX,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_IPPORT:
      return MYSQL_TYPE_VARCHAR;
    case CONSENSUS_PARAM_MATCHINDEX:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_fix_matchindex(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_IPPORT; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_fix_matchindex() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("fix_matchindex"); }
};

/**
  dbms_consensus.show_cluster_global()
*/
class Sql_cmd_consensus_proc_show_global : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_show_global(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual void send_result(THD *thd, bool error);
};

class Consensus_proc_show_global : public Consensus_proc {
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
  explicit Consensus_proc_show_global(PSI_memory_key key) : Consensus_proc(key) {
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
  virtual ~Consensus_proc_show_global() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("show_cluster_global"); }
};

/**
  dbms_consensus.show_cluster_local()
*/
class Sql_cmd_consensus_proc_show_local : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_show_local(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual void send_result(THD *thd, bool error);
};

class Consensus_proc_show_local : public Consensus_proc {
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
  virtual ~Consensus_proc_show_local() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("show_cluster_local"); }
};

/**
  dbms_consensus.show_logs()
*/
class Sql_cmd_consensus_proc_show_logs : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_show_logs(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool check_access(THD *thd);
  virtual void send_result(THD *thd, bool error);
};

class Consensus_proc_show_logs : public Consensus_proc {
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
  virtual ~Consensus_proc_show_logs() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("show_logs"); }
};

/**
  dbms_consensus.purge_log(...)
*/
class Sql_cmd_consensus_proc_purge_log : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_purge_log(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_purge_log : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_purge_log;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_INDEX,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_INDEX:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_purge_log(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_INDEX; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_purge_log() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("purge_log"); }
};

/**
  dbms_consensus.local_purge_log(...)
*/
class Sql_cmd_consensus_proc_local_purge_log : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_local_purge_log(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_local_purge_log : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_local_purge_log;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_INDEX,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_INDEX:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_local_purge_log(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_INDEX; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_local_purge_log() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("local_purge_log"); }
};

/**
  dbms_consensus.force_purge_log(...)
*/
class Sql_cmd_consensus_proc_force_purge_log : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_force_purge_log(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_force_purge_log : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_force_purge_log;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_INDEX,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_INDEX:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_force_purge_log(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_INDEX; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_force_purge_log() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("force_purge_log"); }
};

/**
  dbms_consensus.drop_prefetch_channel(...)
*/
class Sql_cmd_consensus_proc_drop_prefetch_channel : public Sql_cmd_consensus_proc {
public:
  explicit Sql_cmd_consensus_proc_drop_prefetch_channel(THD *thd, List<Item> *list, const Proc *proc)
      : Sql_cmd_consensus_proc(thd, list, proc) {}
  virtual bool pc_execute(THD *thd);
};

class Consensus_proc_drop_prefetch_channel : public Consensus_proc {
  using Sql_cmd_type = Sql_cmd_consensus_proc_drop_prefetch_channel;

  /* All the parameters */
  enum enum_parameter {
    CONSENSUS_PARAM_CHANNEL_ID,
    CONSENSUS_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
    case CONSENSUS_PARAM_CHANNEL_ID:
      return MYSQL_TYPE_LONGLONG;
    case CONSENSUS_PARAM_LAST:
      DBUG_ASSERT(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }
public:
  explicit Consensus_proc_drop_prefetch_channel(PSI_memory_key key)
      : Consensus_proc(key) {
    /* Init parameters */
    for (size_t i = CONSENSUS_PARAM_CHANNEL_ID; i < CONSENSUS_PARAM_LAST; i++) {
      m_parameters.push_back(get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  virtual ~Consensus_proc_drop_prefetch_channel() {}
  static Proc *instance();
  virtual Sql_cmd *evoke_cmd(THD *thd, List<Item> *list) const;
  virtual const std::string str() const { return std::string("drop_prefetch_channel"); }
};

} /* namespace im */

#endif
