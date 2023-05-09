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

#ifndef SQL_PACKAGE_PROC_INCLUDED
#define SQL_PACKAGE_PROC_INCLUDED

#include "field_types.h"
#include "prealloced_array.h"
#include "sql/package/package_common.h"
#include "sql/package/package_parse.h"
#include "sql/sql_cmd.h"

class THD;
class sp_name;
class PT_item_list;

/**
  Interface of native procedure.

  Any native procedure should implement these two interface:

    1) Proc
        It's abstract class declaration.

        - Parser structure
          All subclass of proc will has the same parse tree root,
            PT_package_proc(sp_name, item_list)

        - Sql command
          All subclass of proc need to implement itself command class.

        - Procedure name
          All subclass should define itself name.

    2) Sql_cmd_proc
        It's abstract class declaration.

        - Execute logic (pc_execute())
          All subclass need to implement it.

        - SQL command type
          All subclass has the same command type (SQLCOM_PROC).

        - Default behaviour (pls override these if individualization)
            1. send_result
            2. check_access
            3. check_parameter
            4. prepare
*/
namespace im {

/**
   Native procedure interface
*/
class Proc : public PSI_memory_base {

  static constexpr unsigned int PROC_PREALLOC_SIZE = 10;
 public:
  /**
     All the native procedures have uniform parse tree root.
     It includes:
        - sp_name
        - pt_expr_list
      from sql_yacc.yy

        - Proc
      from searching native proc map
  */
  typedef PT_package_proc PT_proc_type;

  /* Container of proc parameters */
  typedef Prealloced_array<enum_field_types, PROC_PREALLOC_SIZE>
      Parameters;

  /* Column element */
  typedef struct st_column_element {
    enum enum_field_types type;
    const char *name;
    std::size_t name_len;
    std::size_t size;

  } Column_element;

  /* Container of proc columns */
  typedef Prealloced_array<Column_element, PROC_PREALLOC_SIZE> Columns;

  /**
  */
  enum class Result_type {
    RESULT_NONE,  // Initiail state
    RESULT_OK,    // Only OK or ERROR protocal
    RESULT_SET    // Send result set
  };

 public:
  explicit Proc(PSI_memory_key key)
      : PSI_memory_base(key),
        m_result_type(Result_type::RESULT_NONE),
        m_parameters(key),
        m_columns(key) {}

  virtual ~Proc() {}

  /**
    Generate the parse tree root.

    @param[in]    THD           Thread context
    @param[in]    pt_expr_list  Parameters
    @param[in]    proc          Native predefined proc

    @retval       Parse_tree_root
  */
  Parse_tree_root *PT_evoke(THD *thd, PT_item_list *pt_expr_list,
                            const Proc *proc) const;

  /**
    Interface of generating proc execution logic.

    @param[in]    THD           Thread context
    @param[in]    pt_expr_list  Parameters

    @retval       Sql cmd
  */
  virtual Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const = 0;

  Result_type get_result_type() const { return m_result_type; }

  const Parameters *get_parameters() const { return &m_parameters; }

  const Columns &get_columns() const { return m_columns; }

  /**
    Send the result meta data by columns definition.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  bool send_result_metadata(THD *thd) const;

  /**
    Interface of proc name.

    @retval       string        Proc name
  */
  virtual const std::string str() const = 0;

  virtual const std::string qname() const {
    std::stringstream ss;
    ss << PACKAGE_SCHEMA << "." << str();
    return ss.str();
  }

  /* Disable copy and assign function */
  Proc(const Proc &) = delete;
  Proc(const Proc &&) = delete;
  Proc &operator=(const Proc &) = delete;

 protected:
  /* The type of result packet */
  Result_type m_result_type;
  /* The list of proc parameters */
  Parameters m_parameters;
  /* The list of proc columns */
  Columns m_columns;
};



/**
  Interface of proc execution.

  Should implement pc_execute() function at least!
*/
class Sql_cmd_proc : public Sql_cmd {
 public:
  explicit Sql_cmd_proc(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
      : m_thd(thd), m_list(list), m_proc(proc) {}

  /**
    Interface of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool execute(THD *thd) override;
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) = 0;

  /**
    All the proc has the uniform sql command code : SQLCOM_PROC
  */
  virtual enum_sql_command sql_command_code() const override { return SQLCOM_PROC; }

  /**
    Send the ok or error packet defaultly,
    Override it if any result set.
  */
  virtual void send_result(THD *thd, bool error);

  /**
    Check access, require SUPER_ACL defaultly.
    Override it if any other requirement.
  */
  virtual bool check_access(THD *thd);

  /**
    Check the parameters
    Override it if any other requirement.
  */
  virtual bool check_parameter();

  /**
    Prepare the proc before execution.
  */
  virtual bool prepare(THD *thd) override;

 protected:
  THD *m_thd;
  mem_root_deque<Item *> *m_list;
  const Proc *m_proc;
};

} /* namespace im */

#endif
