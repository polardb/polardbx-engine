/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxySQL hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxySQL.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#ifndef SQL_TRANS_PROC_RETURNING_INCLUDED
#define SQL_TRANS_PROC_RETURNING_INCLUDED

#include "sql/trans_proc/common.h"
#include "sql/trans_proc/returning_parse.h"

/**
  Return the resultset when dml operation.

  1) returning(item_list, statement);

*/
namespace im {

class Sql_cmd_trans_proc_returning : public Sql_cmd_trans_proc {
 public:
  explicit Sql_cmd_trans_proc_returning(THD *thd, mem_root_deque<Item *> *list,
                                        const Proc *proc)
      : Sql_cmd_trans_proc(thd, list, proc) {}
  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /**
    Generate all the field items and statement from parameters.

    @param[in]    THD           Thread context

    @retval       statement
  */
  LEX_CSTRING get_field_items_and_stmt(THD *thd);

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;
};

class Trans_proc_returning : public Trans_proc_base {
  using Sql_cmd_type = Sql_cmd_trans_proc_returning;

  /* All the parameters */
  enum enum_parameter {
    RETURNING_PARAM_ITEMS = 0,
    RETURNING_PARAM_STMT,
    RETURNING_PARAM_LAST
  };

  /* Corresponding field type */
  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case RETURNING_PARAM_ITEMS:
      case RETURNING_PARAM_STMT:
        return MYSQL_TYPE_VARCHAR;
      case RETURNING_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

 public:
  explicit Trans_proc_returning(PSI_memory_key key) : Trans_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    /* Init parameters */
    for (size_t i = RETURNING_PARAM_ITEMS; i < RETURNING_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
  }

  /* Singleton instance for returning */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for returning() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  ~Trans_proc_returning() override {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("returning");
  }
};

/**
  Returning clause context, it's a backup/restore class wrapper.
  Used by dbms_trans.returning() call.
*/
class Thd_lex_returning_context {
 public:
  explicit Thd_lex_returning_context(THD *thd);

  ~Thd_lex_returning_context();

 private:
  THD *m_thd;
  /* It's returning statement */
  Lex_returning m_lex_returning;
  Lex_returning *m_old_lex_returning;
};

} /* namespace im */

#endif
