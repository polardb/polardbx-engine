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

#ifndef SQL_RECYCLE_BIN_RECYCLE_PROC_INCLUDED
#define SQL_RECYCLE_BIN_RECYCLE_PROC_INCLUDED

#include "sql/package/proc.h"
#include "sql/recycle_bin/recycle.h"

namespace im {

namespace recycle_bin {

/* Uniform schema name for recycle bin */
extern LEX_CSTRING RECYCLE_BIN_PROC_SCHEMA;

/**
  Proc base for dbms_recycle

  1) Uniform schema: dbms_recycle
*/
class Recycle_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Recycle_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << RECYCLE_BIN_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_recycle

  1) dbms_recycle didn't require any privileges;
*/
class Sql_cmd_recycle_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_recycle_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    /**
      Require not any privileges
    */
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  1) dbms_recycle.show_tables();

*/
class Sql_cmd_recycle_proc_show : public Sql_cmd_recycle_proc_base {
 public:
  explicit Sql_cmd_recycle_proc_show(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_recycle_proc_base(thd, list, proc) {}

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

class Recycle_proc_show : public Recycle_proc_base {
  using Sql_cmd_type = Sql_cmd_recycle_proc_show;

  enum enum_column {
    COLUMN_SCHEMA = 0,
    COLUMN_TABLE,
    COLUMN_ORIGIN_SCHEMA,
    COLUMN_ORIGIN_TABLE,
    COLUMN_RECYCLED_TIME,
    COLUMN_PURGE_TIME,
    COLUMN_LAST
  };

 public:
  explicit Recycle_proc_show(PSI_memory_key key) : Recycle_proc_base(key) {
    /* Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("SCHEMA"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("TABLE"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ORIGIN_SCHEMA"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ORIGIN_TABLE"), 64},
        {MYSQL_TYPE_TIMESTAMP, C_STRING_WITH_LEN("RECYCLED_TIME"), 0},
        {MYSQL_TYPE_TIMESTAMP, C_STRING_WITH_LEN("PURGE_TIME"), 0}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /* Singleton instance for show_tables */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for show_tables() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Recycle_proc_show() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("show_tables");
  }
};

/**
  2) dbms_recycle.purge_table(table_name);

*/
class Sql_cmd_recycle_proc_purge : public Sql_cmd_recycle_proc_base {
 public:
  explicit Sql_cmd_recycle_proc_purge(THD *thd, mem_root_deque<Item *> *list,
                                      const Proc *proc)
      : Sql_cmd_recycle_proc_base(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

  /* Require DROP_ACL privilege on purge table */
  virtual bool check_access(THD *thd) override;
};

class Recycle_proc_purge : public Recycle_proc_base {
  using Sql_cmd_type = Sql_cmd_recycle_proc_purge;

  /* All the parameters */
  enum enum_parameter { RECYCLE_PARAM_TABLE = 0, RECYCLE_PARAM_LAST };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case RECYCLE_PARAM_TABLE:
        return MYSQL_TYPE_VARCHAR;
      case RECYCLE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Recycle_proc_purge(PSI_memory_key key) : Recycle_proc_base(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = RECYCLE_PARAM_TABLE; i < RECYCLE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }

  /* Singleton instance for purge_table */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for purge_table() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Recycle_proc_purge() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("purge_table");
  }
};

/**
  3) dbms_recycle.restore_table(table_name, new_db_name, new_table_name);

*/
class Sql_cmd_recycle_proc_restore : public Sql_cmd_recycle_proc_base {
 public:
  explicit Sql_cmd_recycle_proc_restore(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_recycle_proc_base(thd, list, proc) {
    /**
      Require not any privileges
    */
    set_priv_type(Priv_type::PRIV_SUPER_ACL);
  }

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

  /**
    Check the parameters and privileges.

    Require ALTER_ACL and DROP_ACL privilege on purge table,
    and require CREATE_ACL and INSERT_ACL on dest table.
  */
  virtual bool check_access(THD *thd) override;
};

class Recycle_proc_restore : public Recycle_proc_base {
  using Sql_cmd_type = Sql_cmd_recycle_proc_restore;

  /* All the parameters */
  enum enum_parameter {
    RECYCLE_PARAM_TABLE = 0,
    RECYCLE_PARAM_NEW_DATABASE = 1,
    RECYCLE_PARAM_NEW_TABLE = 2,
    RECYCLE_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case RECYCLE_PARAM_TABLE:
      case RECYCLE_PARAM_NEW_DATABASE:
      case RECYCLE_PARAM_NEW_TABLE:
        return MYSQL_TYPE_VARCHAR;
      case RECYCLE_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Recycle_proc_restore(PSI_memory_key key)
      : Recycle_proc_base(key), m_parameters_2(key) {
    /* Only OK or ERROR protocol packet */
    m_result_type = Result_type::RESULT_OK;

    /* Init parameters */
    for (size_t i = RECYCLE_PARAM_TABLE; i < RECYCLE_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    for (size_t i = RECYCLE_PARAM_TABLE; i < RECYCLE_PARAM_NEW_DATABASE; i++) {
      m_parameters_2.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* Add the parameter formats to list */
    m_parameters_list.push_back(&m_parameters);
    m_parameters_list.push_back(&m_parameters_2);
  }

  /* Singleton instance for restore_table */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for restore_table() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Recycle_proc_restore() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("restore_table");
  }

 protected:
  Parameters m_parameters_2;
};

} /* namespace recycle_bin */

} /* namespace im */
#endif
