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

#ifndef SQL_LIZARD_UNDO_PROC_INCLUDED
#define SQL_LIZARD_UNDO_PROC_INCLUDED

#include <string>

#include "lex_string.h"
#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING PROC_UNDO_SCHEMA;

/* dbms_undo.trunc_status() */
class Sql_cmd_trunc_status : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_trunc_status(THD *thd, mem_root_deque<Item *> *list,
                               const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;

  virtual void send_result(THD *thd, bool error) override;

 private:
  size_t utc_to_str(ulonglong timestamp, String *s);
};

class Proc_trunc_status : public Proc {
 public:
  typedef Proc_trunc_status Sql_cmd_type;

  enum enum_column {
    COLUMN_UNDO_NAME = 0,
    COLUMN_FILE_PAGES = 1,
    COLUMN_RSEG_PAGES = 2,
    COLUMN_HISTORY_LENGTH = 3,
    COLUMN_HISTORY_PAGES = 4,
    COLUMN_SECONDARY_LENGTH = 5,
    COLUMN_SECONDARY_PAGES = 6,
    COLUMN_OLDEST_HISTORY_SEC = 7,
    COLUMN_OLDEST_SECONDARY_SEC = 8,
    COLUMN_OLDEST_HISTORY_SCN = 9,
    COLUMN_OLDEST_SECONDARY_SCN = 10,
    COLUMN_OLDEST_HISTORY_GCN = 11,
    COLUMN_OLDEST_SECONDARY_GCN = 12,
    COLUMN_LAST = 13
  };

 public:
  explicit Proc_trunc_status(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("UNDO_NAME"), 255},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("FILE_PAGES"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("RSEG_PAGES"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("HISTORY_LENGTH"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("HISTORY_PAGES"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("SECONDARY_LENGTH"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("SECONDARY_PAGES"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_HISTORY_SEC"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_SECONDARY_SEC"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_HISTORY_SCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_SECONDARY_SCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_HISTORY_GCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("OLDEST_SECONDARY_GCN"), 0}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  ~Proc_trunc_status() override {}

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("trunc_status");
  }

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << PROC_UNDO_SCHEMA.str << "." << str();
    return ss.str();
  }
};



/* dbms_undo.purge_status() */
class Sql_cmd_purge_status : public Sql_cmd_admin_proc {
 public:
  explicit Sql_cmd_purge_status(THD *thd, mem_root_deque<Item *> *list,
                               const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  virtual bool pc_execute(THD *thd) override;

  virtual void send_result(THD *thd, bool error) override;

 private:
  size_t utc_to_str(ulonglong timestamp, String *s);
};

class Proc_purge_status : public Proc {
 public:
  typedef Proc_purge_status Sql_cmd_type;

  enum enum_column {
    COLUMN_HISTORY_LENGTH = 0,
    COLUMN_CURRENT_SCN = 1,
    COLUMN_CURRENT_GCN = 2,
    COLUMN_PURGED_SCN = 3,
    COLUMN_PURGED_GCN = 4,
    COLUMN_ERASED_SCN = 5,
    COLUMN_ERASE_GCN = 6,
    COLUMN_LAST = 7
  };

 public:
  explicit Proc_purge_status(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("HISTORY_LENGTH"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("CURRENT_SCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("CURRENT_GCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("PURGED_SCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("PURGED_GCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ERASED_SCN"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("ERASED_GCN"), 0}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  ~Proc_purge_status() override {}

  static Proc *instance();

  virtual Sql_cmd *invoke_cmd(THD *thd,
                              mem_root_deque<Item *> *list) const override;

  virtual const std::string str() const override {
    return std::string("purge_status");
  }

  virtual const std::string qname() const override {
    std::stringstream ss;
    ss << PROC_UNDO_SCHEMA.str << "." << str();
    return ss.str();
  }
};

} /* namespace im */

#endif
