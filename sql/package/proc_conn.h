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

/** @file sql/package/proc_jdbc_conn.h
 Native procedure for retrieving jdbc related statistics.

 Created 2024-04-10 by Ting Yuan
 *******************************************************/

#ifndef SQL_PROC_CONN_INCLUDED
#define SQL_PROC_CONN_INCLUDED

#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING PROC_CONN_SCHEMA;

/**
    Proc base for dbms_conn
*/
class Conn_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Conn_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << PROC_CONN_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_conn

  1) dbms_conn didn't require any privileges;
*/
class Sql_cmd_conn_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_conn_proc_base(THD *thd, mem_root_deque<Item *> *list,
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
1) dbms_conn.comment_connection(...)

It will add comment of jdbc connection in thd.
 */
class Sql_cmd_conn_comment : public Sql_cmd_conn_proc_base {
 public:
  explicit Sql_cmd_conn_comment(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_conn_proc_base(thd, list, proc) {}
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;
};

class Conn_proc_comment : public Conn_proc_base {
  using Sql_cmd_type = Sql_cmd_conn_comment;

 public:
  explicit Conn_proc_comment(PSI_memory_key key) : Conn_proc_base(key) {
    m_result_type = Result_type::RESULT_OK;
    m_parameters.assign_at(0, MYSQL_TYPE_VARCHAR);
  }


  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;
                        
  ~Conn_proc_comment() override {}

  virtual const std::string str() const override {
    return std::string("comment_connection");
  }
};

/**
2) dbms_conn.show_connection(...)

It will show the comment of jdbc connection in thd.
*/
class Sql_cmd_conn_show : public Sql_cmd_conn_proc_base {
 public:
  explicit Sql_cmd_conn_show(THD *thd, mem_root_deque<Item *> *list,
                             const Proc *proc)
      : Sql_cmd_conn_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

class Conn_proc_show : public Conn_proc_base {
  using Sql_cmd_type = Sql_cmd_conn_show;

 public:
  explicit Conn_proc_show(PSI_memory_key key) : Conn_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;
    Column_element elements = {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("JDBC_COMMENT"), 65536};
    m_columns.assign_at(0, elements);
  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Conn_proc_show() override {}
  virtual const std::string str() const override {
    return std::string("show_connection");
  }

};


/**
3) dbms_conn.show_client_error_code(...)

It will show the client error code of jdbc connection in thd.
*/
class Sql_cmd_conn_show_client_error_code : public Sql_cmd_conn_proc_base {
 public:
  explicit Sql_cmd_conn_show_client_error_code(THD *thd, mem_root_deque<Item *> *list,
                             const Proc *proc)
      : Sql_cmd_conn_proc_base(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

  static constexpr int errmsg_section_index = 3;
};

class Conn_proc_show_client_error_code : public Conn_proc_base {
  using Sql_cmd_type = Sql_cmd_conn_show_client_error_code;
  static constexpr int COLUMN_LAST = 2;

 public:
  explicit Conn_proc_show_client_error_code(PSI_memory_key key) : Conn_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ERROR_CODE_NUM"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("ERROR_CODE_MESSAGE"), 256}
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }

  }

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  ~Conn_proc_show_client_error_code() override {}
  virtual const std::string str() const override {
    return std::string("show_client_error_code");
  }

};



} /* namespace im */

#endif
