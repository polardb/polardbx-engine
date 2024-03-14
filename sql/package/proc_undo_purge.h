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

#ifndef SQL_PROC_UNDO_PURGE_INCLUDED
#define SQL_PROC_UNDO_PURGE_INCLUDED

#include <string>

#include "lex_string.h"
#include "sql/package/proc.h"

namespace im {

extern const LEX_CSTRING PROC_UNDO_SCHEMA;

/* Result structure for purge_status */
typedef struct Undo_purge_show_result {
  /* current info of undo */
  ulong used_size;
  ulong file_size;
  ulong retained_time;
  /* current undo retention config */
  ulong reserved_size;
  ulong retention_size_limit;
  ulong retention_time;
  /* last blocked reason of purge sys */
  String blocked_cause;
  ulong blocked_utc;

 public:
  Undo_purge_show_result() {
    used_size = 0;
    file_size = 0;
    retained_time = 0;
    reserved_size = 0;
    retention_size_limit = 0;
    retention_time = 0;
    blocked_utc = 0;
  }
} Undo_purge_show_result;

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
    COLUMN_USED_SIZE = 0,
    COLUMN_TABLESPACE_SIZE = 1,
    COLUMN_RETAINED_TIME = 2,
    COLUMN_RETENTION_TIME = 3,
    COLUMN_RESERVED_SIZE = 4,
    COLUMN_SUPREMUM_SIZE = 5,
    COLUMN_BLOCKED_REASON = 6,
    COLUMN_BLOCKED_UTC = 7,
    COLUMN_LAST = 8
  };

 public:
  explicit Proc_purge_status(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_SET;

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("UNDO_USED_SIZE(MB)"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("UNDO_TABLESPACE_SIZE(MB)"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("RETAINED_TIME(sec)"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("INNODB_UNDO_RETENTION(sec)"),
         0},
        {MYSQL_TYPE_LONGLONG,
         C_STRING_WITH_LEN("INNODB_UNDO_SPACE_RESERVED_SIZE(MB)"), 0},
        {MYSQL_TYPE_LONGLONG,
         C_STRING_WITH_LEN("INNODB_UNDO_SPACE_SUPREMUM_SIZE(MB)"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("LAST_BLOCKED_REASON"), 255},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("LAST_BLOCKED_UTC"), 255}};

    for (size_t i = 0; i < COLUMN_LAST; i++) {
      m_columns.assign_at(i, elements[i]);
    }
  }

  ~Proc_purge_status() override {}

  static Proc *instance();

  virtual Sql_cmd *evoke_cmd(THD *thd,
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
