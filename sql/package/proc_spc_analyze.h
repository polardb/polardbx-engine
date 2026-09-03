/* Copyright (c) 2026, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_PACKAGE_PROC_SPC_ANALYZE_INCLUDED
#define SQL_PACKAGE_PROC_SPC_ANALYZE_INCLUDED

#include <string>
#include "sql/package/proc.h"
#include "sql/trans_proc/common.h"

/**
  Analyze SPC table statistics.

  CALL dbms_spc.analyze_table(table_name);

  Returns result set with SPC statistics as a single formatted string.
*/

namespace im {

/* Schema: dbms_spc */
extern LEX_CSTRING SPC_PROC_SCHEMA;

/**
  Proc of dbms_spc.analyze_table

  Note: We inherit from Proc directly (not Trans_proc_base) because
  Trans_proc_base::qname() hardcodes TRANS_PROC_SCHEMA ("dbms_trans"),
  but we need "dbms_spc".
*/
class Proc_spc_analyze_table : public Proc {
  /* Result set columns */
  enum enum_column { COLUMN_STATUS = 0, COLUMN_LAST };

  static constexpr const char *column_status = "Status";

 public:
  explicit Proc_spc_analyze_table(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_SET;

    /* Parameter definition - one VARCHAR parameter for table_name */
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);

    /* Column definition - single VARCHAR column for status output */
    Column_element element;

    element = {MYSQL_TYPE_VARCHAR, column_status, strlen(column_status), 65535};
    m_columns.assign_at(COLUMN_STATUS, element);
  }

  /* Singleton instance of this class */
  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("analyze_table");
  }

  /* Override qname() to return correct schema "dbms_spc" instead of
   * "dbms_trans" */
  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << SPC_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Cmd of dbms_spc.analyze_table

  Note: We use Sql_cmd_admin_proc because this is an administrative
  operation that should require SUPER privilege.
*/
class Sql_cmd_proc_spc_analyze_table : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_proc_spc_analyze_table(THD *thd,
                                          mem_root_deque<Item *> *list,
                                          const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  /**
    Implementation of Proc execution body.

    @param[in]    THD           Thread context

    @retval       true          Failure
    @retval       false         Success
  */
  virtual bool pc_execute(THD *thd) override;

  /* Override default send_result */
  virtual void send_result(THD *thd, bool error) override;

  /* Storage for formatted status string (allocated on thd->mem_root) */
  char *m_status{nullptr};
  size_t m_status_len{0};
};

}  // namespace im

#endif
