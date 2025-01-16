/*****************************************************************************

Copyright (c) 2013, 2023, Alibaba and/or its affiliates. All Rights Reserved.

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
/** @file sql/pli/license_proc.h

  License Procedure.

  Created 2025-01-15 by Zefeng.Liu
 *******************************************************/

#ifndef SQL_PLI_LICENSE_PROC_H_INCLUDED
#define SQL_PLI_LICENSE_PROC_H_INCLUDED
#include "pli/pli_common.h"
#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING LICENSE_PROC_SCHEMA;

/**
  Proc base for dbms_license

  1) Uniform schema: dbms_license
*/

class License_proc_base : public Proc, public Disable_copy_base {
 public:
  explicit License_proc_base(PSI_memory_key key) : Proc(key) {}

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << LICENSE_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Sql command base for dbms_license

  1) dbms_license didn't require any privileges;
  2) dbms_license didn't auto commit trans (is trans_proc).
*/
class Sql_cmd_license_proc_base : public Sql_cmd_trans_proc {
 public:
  explicit Sql_cmd_license_proc_base(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_trans_proc(thd, list, proc) {
    set_priv_type(Priv_type::PRIV_NONE_ACL);
  }
};

/**
 * 1) dbms_license.show_info
 *
 * Show current license information.
 */
class Sql_cmd_license_show_info : public Sql_cmd_license_proc_base {
 public:
  explicit Sql_cmd_license_show_info(THD *thd, mem_root_deque<Item *> *list,
                                     const Proc *proc)
      : Sql_cmd_license_proc_base(thd, list, proc), m_show_info() {}
  /** Implementation of Proc execution body. */
  virtual bool pc_execute(THD *thd) override;

  /** Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

 private:
  xlicense::License_show_info m_show_info;
};

class License_proc_show_info : public License_proc_base {
  using Sql_cmd_type = Sql_cmd_license_show_info;

  /** Now only suppot below infomations. */
  enum enum_column {
    COLUMN_FILE_PATH = 0,
    COLUMN_FILE_STATUS = 1,
    COLUMN_APPLY_TIME = 2,
    COLUMN_EXPIRE_TIME = 3,
    COLUMN_LICENSE_TYPE = 4,
    COLUMN_CONNS_CONSTRAINS = 5,
    COLUMN_BP_CONSTRAINS = 6,
    COLUMN_LAST = 7
  };

 public:
  explicit License_proc_show_info(PSI_memory_key key) : License_proc_base(key) {
    /** Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;
    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FILE_PATH"), 512},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FILE_STATUS"), 64},
        {MYSQL_TYPE_DATETIME, C_STRING_WITH_LEN("APPLY_TIME"), 64},
        {MYSQL_TYPE_DATETIME, C_STRING_WITH_LEN("EXPIRE_TIME"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("LICENSE_TYPE"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MAX CONNS"), 32},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MAX BP SIZE"), 32},
    };
    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  /** Singleton instance */
  static Proc *instance();

  /** Invoke the sql_cmd object for show_info proc.  */
  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  /** Proc name */
  virtual const std::string str() const override {
    return std::string("show_info");
  }

  virtual ~License_proc_show_info() {}
};

/**
 * 2) dbms_license.check_license(...)
 *
 * Check License only but not refresh.
 */
class Sql_cmd_license_check_license : public Sql_cmd_license_proc_base {
 public:
  explicit Sql_cmd_license_check_license(THD *thd, mem_root_deque<Item *> *list,
                                         const Proc *proc)
      : Sql_cmd_license_proc_base(thd, list, proc), m_show_info() {}
  /** Implementation of Proc execution body. */
  virtual bool pc_execute(THD *thd) override;

  /** Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

 private:
  xlicense::License_show_info m_show_info;
};

class License_proc_check_license : public License_proc_base {
  using Sql_cmd_type = Sql_cmd_license_check_license;

  /** Now only support one parameter. */
  enum enum_parameter { PARAM_FILE_PATH = 0, PARAM_LAST };

  /** Now only support below output information */
  enum enum_column {
    COLUMN_FILE_PATH = 0,
    COLUMN_FILE_STATUS = 1,
    COLUMN_APPLY_TIME = 2,
    COLUMN_EXPIRE_TIME = 3,
    COLUMN_LICENSE_TYPE = 4,
    COLUMN_CONNS_CONSTRAINS = 5,
    COLUMN_BP_CONSTRAINS = 6,
    COLUMN_LAST = 7
  };

  enum_field_types get_field_type(enum_parameter param) {
    switch (param) {
      case PARAM_FILE_PATH:
        return MYSQL_TYPE_VARCHAR;
      default:
        assert(0);
    }
    return MYSQL_TYPE_VARCHAR;
  }

 public:
  explicit License_proc_check_license(PSI_memory_key key)
      : License_proc_base(key) {
    /** 1. Init parameters. */
    for (size_t i = 0; i < PARAM_LAST; i++) {
      m_parameters.assign_at(
          i, get_field_type(static_cast<enum enum_parameter>(i)));
    }
    /** 2. Result set protocol packet */
    m_result_type = Result_type::RESULT_SET;
    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FILE_PATH"), 512},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("FILE_STATUS"), 64},
        {MYSQL_TYPE_DATETIME, C_STRING_WITH_LEN("APPLY_TIME"), 64},
        {MYSQL_TYPE_DATETIME, C_STRING_WITH_LEN("EXPIRE_TIME"), 64},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("LICENSE_TYPE"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MAX CONNS"), 32},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("MAX BP SIZE"), 32},
    };
    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }
  /** Singleton instance */
  static Proc *instance();

  /** Invoke the sql_cmd object for show_info proc.  */
  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  /** Proc name */
  virtual const std::string str() const override {
    return std::string("check_license");
  }

  virtual ~License_proc_check_license() {}
};
}  // namespace im

#endif