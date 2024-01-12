/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_TSO_PROC_INCLUDED
#define SQL_TSO_PROC_INCLUDED

#include "m_string.h"
#include "sql/package/proc.h"

namespace im {

/**
  Timestamp service native procedure schema: dbms_tso
*/
extern const LEX_CSTRING TSO_PROC_SCHEMA;

/**
  Base class of the timestamp service native procedure interfaces.
*/
class Tso_proc_base : public Proc {
 public:
  explicit Tso_proc_base(PSI_memory_key key) : Proc(key) {}

  /* Setting timestamp native procedure schema */
  const std::string qname() const override {
    std::stringstream ss;
    ss << TSO_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Proc of get_timestamp()

  This procedure gets the next batch of timestamp value from existing timestamp
  sequence, the usage is

    dbms_tso.get_timestamp(db_name, table_name, batch_size)
*/
class Proc_get_timestamp : public Tso_proc_base {
 public:
  explicit Proc_get_timestamp(PSI_memory_key key) : Tso_proc_base(key) {
    m_result_type = Result_type::RESULT_SET;

    /*
      This procedure accepts two paramaters
      - db name, the name of db where base table resides
      - table name, the name of base table which is created when creating
        timestamp sequence
      - batch size, number of timestamp value requested in a batch
    */
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters.push_back(MYSQL_TYPE_LONGLONG);

    Column_element element = {MYSQL_TYPE_LONGLONG,
                              C_STRING_WITH_LEN("Timestamp"), 128};

    m_columns.push_back(std::move(element));
  }

  static Proc *instance();

  Sql_cmd *evoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("get_timestamp");
  }
};

/**
  Cmd of get_timestamp()
*/
class Cmd_get_timestamp : public Sql_cmd_admin_proc {
 public:
  explicit Cmd_get_timestamp(THD *thd, mem_root_deque<Item *> *list,
                             const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {
    m_timestamp = 0;
  }

  bool pc_execute(THD *thd) override;

  void send_result(THD *thd, bool error) override;

 private:
  /* The timestamp value to send to client */
  uint64_t m_timestamp;
};

}  // namespace im

#endif
