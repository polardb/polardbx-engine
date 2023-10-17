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

#ifndef SQL_CCL_CCL_TABLE_COMMON_INCLUDED
#define SQL_CCL_CCL_TABLE_COMMON_INCLUDED

#include <vector>
#include "m_ctype.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sqlcommand.h"
#include "prealloced_array.h"

#include "sql/common/table_common.h"

namespace im {

enum mysql_ccl_table_field {
  MYSQL_CCL_FIELD_ID = 0,
  MYSQL_CCL_FIELD_TYPE,
  MYSQL_CCL_FIELD_SCHEMA_NAME,
  MYSQL_CCL_FIELD_TABLE_NAME,
  MYSQL_CCL_FIELD_CONCURRENCY_COUNT,
  MYSQL_CCL_FIELD_KEYWORDS,
  MYSQL_CCL_FIELD_STATE,
  MYSQL_CCL_FIELD_ORDERED,
  MYSQL_CCL_FIELD_COUNT
};

enum class Ccl_error_level { CCL_WARNING, CCL_CRITICAL };

extern void ccl_log_error(Conf_error err);

/* These rule type, state, order enum should start from 1. */
enum class Ccl_rule_type {
  RULE_SELECT = 0, /* Should be start 1. */
  RULE_UPDATE,
  RULE_INSERT,
  RULE_DELETE,
  RULE_QUEUE,
  RULE_UNKNOWN
};

/* Rule state */
enum class Ccl_rule_state { RULE_INACTIVE = 0, RULE_ACTIVE };

/* Rule Keywords that match mode. */
enum class Ccl_rule_ordered { RULE_DISORDER = 0, RULE_ORDER };

extern const char *ccl_rule_type_str[];
extern const char ccl_rule_state_str[];
extern const char ccl_rule_ordered_str[];

inline Ccl_rule_type sql_command_to_ccl_type(enum_sql_command sql_command) {
  if (sql_command == SQLCOM_SELECT)
    return Ccl_rule_type::RULE_SELECT;
  else if (sql_command == SQLCOM_UPDATE)
    return Ccl_rule_type::RULE_UPDATE;
  else if (sql_command == SQLCOM_INSERT)
    return Ccl_rule_type::RULE_INSERT;
  else if (sql_command == SQLCOM_DELETE)
    return Ccl_rule_type::RULE_DELETE;
  else
    return Ccl_rule_type::RULE_UNKNOWN;
}

inline Ccl_rule_type to_ccl_type(longlong val) {
  assert(val >= 1);
  return static_cast<Ccl_rule_type>(val - 1);
}

extern Ccl_rule_type to_ccl_type(const char *str);

inline Ccl_rule_state to_ccl_state(longlong val) {
  assert(val >= 1);
  return static_cast<Ccl_rule_state>(val - 1);
}
inline Ccl_rule_ordered to_ccl_ordered(longlong val) {
  assert(val >= 1);
  return static_cast<Ccl_rule_ordered>(val - 1);
}

/* CCL table concurrency_control record format */
struct Ccl_record : public Conf_record {
 public:
  ulonglong id;
  Ccl_rule_type type;
  const char *schema_name;
  const char *table_name;
  ulonglong concurrency_count;
  const char *keywords;
  Ccl_rule_state state;
  Ccl_rule_ordered ordered;

 public:
  Ccl_record() { reset(); }

 public:
  void reset() {
    id = 0;
    type = Ccl_rule_type::RULE_SELECT;
    schema_name = NULL;
    table_name = NULL;
    concurrency_count = 0;
    keywords = NULL;
    state = Ccl_rule_state::RULE_ACTIVE;
    ordered = Ccl_rule_ordered::RULE_DISORDER;
  }

  bool blank_schema() const { return blank(schema_name); }
  bool blank_table() const { return blank(table_name); }
  bool blank_keywords() const { return blank(keywords); }

  /**
    Valid rule requirement:
     0) type is valid

     1) concurrency_count > 0;

     2) schema_name and table_name are both null or notnull
         at the same time.
  */
  virtual bool check_valid(const char **) const override {
    if ((type == Ccl_rule_type::RULE_UNKNOWN) ||
        (blank(schema_name) != blank(table_name)))
      return false;

    return true;
  }
  virtual bool check_active() const override {
    return state == Ccl_rule_state::RULE_ACTIVE;
  }
  virtual ulonglong get_id() const override { return id; }

  virtual void set_id(ulonglong value) override { id = value; }
};

#define PREALLOC_RECORD_COUNT 20

using Ccl_records = Prealloced_array<Ccl_record *, PREALLOC_RECORD_COUNT>;

struct Ccl_record_group {
  std::vector<Ccl_record *> intact_rules;
  std::vector<Ccl_record *> keyword_rules;
  std::vector<Ccl_record *> command_rules;
  Ccl_rule_type m_type;

 public:
  size_t classify_rule(Ccl_rule_type type, Conf_records *records);
  size_t size() {
    return intact_rules.size() + keyword_rules.size() + command_rules.size();
  }
};

} /* namespace im */

#endif
