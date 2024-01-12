//
// Created by wumu on 2022/10/19.
//

#ifndef MYSQL_CHANGESET_PROC_H
#define MYSQL_CHANGESET_PROC_H

#include "changeset_manager.h"

namespace im {
/**
 * @brief Base class for all changeset procedures
 */
class Changeset_proc : public Proc, public Disable_copy_base {
 public:
  explicit Changeset_proc(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_OK;
  }

  const std::string qname() const override {
    std::stringstream ss;
    ss << POLARX_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
 * @brief Base class for changeset procedures execution
 *
 */
class Sql_cmd_changeset_proc : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_changeset_proc(THD *thd, mem_root_deque<Item *> *list,
                                  const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  bool pc_execute(THD *) override { return false; }

  bool check_parameter() override;

  bool check_access(THD *thd) override;

 protected:
};

/**
 * @brief procedure changeset_start
 *
 */
class Sql_cmd_changeset_proc_start : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_start(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_start : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_start;

 public:
  explicit Changeset_proc_start(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[1] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TABLE"), 64},
    };
    for (auto &element : elements) {
      m_columns.push_back(element);
    }
  }

  ~Changeset_proc_start() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("changeset_start");
  }
};

/**
 * @brief procedure changeset_fetch
 *
 */
class Sql_cmd_changeset_proc_fetch : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_fetch(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_fetch : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_fetch;

 public:
  explicit Changeset_proc_fetch(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[2] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("OP"), 24},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("PK"), 64},
    };
    for (auto element : elements) {
      m_columns.push_back(element);
    }
  }

  ~Changeset_proc_fetch() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("changeset_fetch");
  }

  bool my_send_result_metadata(THD *thd, Proc::Columns columns,
                               std::list<Field *> fields) const;
};

/**
 * @brief procedure changeset_finish
 *
 */
class Sql_cmd_changeset_proc_finish : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_finish(THD *thd, mem_root_deque<Item *> *list,
                                         const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_finish : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_finish;

 public:
  explicit Changeset_proc_finish(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[2] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("SEQ"), 24},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ROW"), 64},
    };
    for (auto &element : elements) {
      m_columns.push_back(element);
    }
  }

  ~Changeset_proc_finish() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("changeset_finish");
  }
};

/**
 * @brief procedure changeset_stop
 *
 */
class Sql_cmd_changeset_proc_stop : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_stop(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_stop : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_stop;

 public:
  explicit Changeset_proc_stop(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[2] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("SEQ"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ROW"), 64},
    };
    for (auto &element : elements) {
      m_columns.push_back(element);
    }
  }

  ~Changeset_proc_stop() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("changeset_stop");
  }
};

/**
 * @brief procedure changeset_stats
 *
 */
class Sql_cmd_changeset_proc_stats : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_stats(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_stats : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_stats;

 public:
  explicit Changeset_proc_stats(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    // Columns: TABLE,  CHANGES
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCHEMA"), 64});
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TABLE"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NUM_INSERTS"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NUM_UPDATES"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NUM_DELETES"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NUM_FILES"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MEMORY_SIZE"), 64});
  }

  ~Changeset_proc_stats() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("changeset_stats");
  }
};

/**
 * @brief procedure changeset_times
 *
 */
class Sql_cmd_changeset_proc_times : public Sql_cmd_changeset_proc {
 public:
  explicit Sql_cmd_changeset_proc_times(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_changeset_proc(thd, list, proc) {}

  void send_result(THD *thd, bool error) override;
};

class Changeset_proc_times : public Changeset_proc {
  using Sql_cmd_type = Sql_cmd_changeset_proc_times;

 public:
  explicit Changeset_proc_times(PSI_memory_key key) : Changeset_proc(key) {
    m_result_type = Result_type::RESULT_SET;

    // Columns: TABLE,  CHANGES
    m_columns.push_back({MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TABLE"), 64});
    m_columns.push_back(
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("NUM_INSERTS"), 64});
  }

  ~Changeset_proc_times() override = default;

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("changeset_times");
  }
};
}  // namespace im

#endif  // MYSQL_CHANGESET_PROC_H
