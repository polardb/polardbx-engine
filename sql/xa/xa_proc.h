/* Copyright (c) 2018, 2023, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_XA_PROC_XA_INCLUDED
#define SQL_XA_PROC_XA_INCLUDED

#include "sql/package/proc.h"

/**
  XA procedures (dbms_xa package)

  1) find_by_xid(gtrid, bqual, formatID)
*/

namespace im {
extern const LEX_CSTRING XA_PROC_SCHEMA;

/**
  Proc base for dbms_xa

  1) Uniform schema: dbms_xa
*/
class Xa_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit Xa_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << XA_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_xa

  1) dbms_xa didn't require any privileges;
*/
class Sql_cmd_xa_proc_base : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_xa_proc_base(THD *thd, mem_root_deque<Item *> *list, const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
  1) dbms_xa.find_by_xid(gtrid, bqual, formatID)

  Find transactions status in the finalized state by XID.
*/
class Sql_cmd_xa_proc_find_by_xid : public Sql_cmd_xa_proc_base {
 public:
  explicit Sql_cmd_xa_proc_find_by_xid(THD *thd, mem_root_deque<Item *> *list,
                                       const Proc *proc)
      : Sql_cmd_xa_proc_base(thd, list, proc) {}

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

class Xa_proc_find_by_xid : public Xa_proc_base {
  using Sql_cmd_type = Sql_cmd_xa_proc_find_by_xid;

  enum enum_parameter {
    XA_PARAM_GTRID = 0,
    XA_PARAM_BQUAL,
    XA_PARAM_FORMATID,
    XA_PARAM_LAST
  };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case XA_PARAM_GTRID:
      case XA_PARAM_BQUAL:
        return MYSQL_TYPE_VARCHAR;
      case XA_PARAM_FORMATID:
        return MYSQL_TYPE_LONGLONG;
      case XA_PARAM_LAST:
        assert(0);
    }
    return MYSQL_TYPE_LONGLONG;
  }

  enum enum_column { COLUMN_GCN = 0, COLUMN_STATE, COLUMN_LAST };

 public:
  explicit Xa_proc_find_by_xid(PSI_memory_key key) : Xa_proc_base(key) {
    /* 1. Init parameters */
    for (size_t i = XA_PARAM_GTRID; i < XA_PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }

    /* 2. Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("GCN"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("State"), 16},
    };

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /* Singleton instance for find_by_xid */
  static Proc *instance();

  /**
    Evoke the sql_cmd object for find_by_xid() proc.
  */
  virtual Sql_cmd *evoke_cmd(THD *thd,
                             mem_root_deque<Item *> *list) const override;

  virtual ~Xa_proc_find_by_xid() {}

  /* Proc name */
  virtual const std::string str() const override {
    return std::string("find_by_xid");
  }
};

}

#endif
