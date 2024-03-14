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
#ifndef SQL_CCL_PROC_CCL_INCLUDED
#define SQL_CCL_PROC_CCL_INCLUDED

#include "sql/package/proc.h"

/**
  Concurrency control procedures (dbms_ccl package)

  1) add_ccl_rule(type, schema, table, concurrency_count, keywords)

  2) del_ccl_rule(rule_id)

  3) flush_ccl_rule()

  4) show_ccl_rule()

*/

namespace im {

extern const LEX_CSTRING CCL_PROC_SCHEMA;

/**
  Proc base for dbms_ccl

  1) Uniform schema: dbms_ccl
*/
class Ccl_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Ccl_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << CCL_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_ccl

  1) dbms_ccl didn't require any privileges;
*/
class Sql_cmd_ccl_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_ccl_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                 const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    /**
      Require not any privileges when execute
      add_ccl_rule()
    */
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  1) dbms_ccl.add_ccl_rule(...);

  It will add new rule into mysql.concurrency_control table,
  and the ccl cache that take effect immediately.
*/
class Sql_cmd_ccl_proc_add : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_add(THD *thd, mem_root_deque<Item *> *list,
                                const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Ccl_proc_add : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_add;

  /* All the parameters */
  enum enum_parameter {
    CCL_PARAM_TYPE = 0,
    CCL_PARAM_SCHEMA,
    CCL_PARAM_TABLE,
    CCL_PARAM_CCC,
    CCL_PARAM_KEYWORDS,
    CCL_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case CCL_PARAM_TYPE:
      case CCL_PARAM_SCHEMA:
      case CCL_PARAM_TABLE:
      case CCL_PARAM_KEYWORDS:
        return MYSQL_TYPE_VARCHAR;
      case CCL_PARAM_CCC:
        return MYSQL_TYPE_LONGLONG;
      case CCL_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Ccl_proc_add(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = CCL_PARAM_TYPE; i < CCL_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }

  /* Singleton instance for add_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for add_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_add() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("add_ccl_rule");
  }
};

/**
  2) dbms_ccl.flush_ccl_rule();

  It will flush all ccl cache and read the rules from mysql.concurrency_control
  and add into ccl cache.
*/
class Sql_cmd_ccl_proc_flush : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_flush(THD *thd, mem_root_deque<Item *> *list,
                                  const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Ccl_proc_flush : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_flush;

 public:
  explicit Ccl_proc_flush(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }
  /* Singleton instance for add_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for add_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_flush() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("flush_ccl_rule");
  }
};

/**
  3) dbms_ccl.del_ccl_rule();

  It will delete rule from mysql.concurrency_table and cache.
*/
class Sql_cmd_ccl_proc_del : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_del(THD *thd, mem_root_deque<Item *> *list,
                                const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Ccl_proc_del : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_del;

  /* All the parameters */
  enum enum_parameter { CCL_PARAM_ID = 0, CCL_PARAM_LAST };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case CCL_PARAM_ID:
        return MYSQL_TYPE_LONGLONG;
      case CCL_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Ccl_proc_del(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = CCL_PARAM_ID; i < CCL_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }
  /* Singleton instance for del_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for del_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_del() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("del_ccl_rule");
  }
};

/**
  4) dbms_ccl.show_ccl_rule();

  It will show rules in cache.
*/
class Sql_cmd_ccl_proc_show : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_show(THD *thd, mem_root_deque<Item *> *list,
                                 const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

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

class Ccl_proc_show : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_show;

  enum enum_column {
    COLUMN_ID = 0,
    COLUMN_TYPE,
    COLUMN_SCHEMA,
    COLUMN_TABLE,
    COLUMN_STATE,
    COLUMN_ORDERED,
    COLUMN_CCC,
    COLUMN_MATCHED,
    COLUMN_RUNNING,
    COLUMN_WAITTING,
    COLUMN_KEYWORDS,
    COLUMN_LAST
  };

 public:
  explicit Ccl_proc_show(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ID"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TYPE"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCHEMA"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TABLE"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("STATE"), 16},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ORDER"), 16},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("CONCURRENCY_COUNT"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MATCHED"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("RUNNING"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("WAITTING"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("KEYWORDS"), 256},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /* Singleton instance for show_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for show_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_show() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("show_ccl_rule");
  }
};

/**
  5) dbms_ccl.flush_ccl_queue();

  It will flush ccl queue buckets and reinit it.
*/
class Sql_cmd_ccl_proc_flush_queue : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_flush_queue(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Inherit the default send_result */
};

class Ccl_proc_flush_queue : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_flush_queue;

 public:
  explicit Ccl_proc_flush_queue(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;
  }
  /* Singleton instance for add_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for add_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_flush_queue() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("flush_ccl_queue");
  }
};

/**
  6) dbms_ccl.show_ccl_queue();

  It will show queues in cache.
*/
class Sql_cmd_ccl_proc_show_queue : public Sql_cmd_ccl_proc_base {
 public:
  explicit Sql_cmd_ccl_proc_show_queue(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_ccl_proc_base(thd, list, proc) {}

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

class Ccl_proc_show_queue : public Ccl_proc_base {
  using Sql_cmd_type = Sql_cmd_ccl_proc_show_queue;

  enum enum_column {
    COLUMN_ID = 0,
    COLUMN_TYPE,
    COLUMN_CCC,
    COLUMN_MATCHED,
    COLUMN_RUNNING,
    COLUMN_WAITTING,
    COLUMN_LAST
  };

 public:
  explicit Ccl_proc_show_queue(PSI_memory_key key) : Ccl_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ID"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TYPE"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("CONCURRENCY_COUNT"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MATCHED"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("RUNNING"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("WAITTING"), 0}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /* Singleton instance for show_ccl_rule */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for show_ccl_rule() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Ccl_proc_show_queue() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("show_ccl_queue");
  }
};

} /* namespace im */

#endif
