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

#ifndef SQL_OUTLINE_OUTLINE_TABLE_COMMON_INCLUDED
#define SQL_OUTLINE_OUTLINE_TABLE_COMMON_INCLUDED

#include <vector>
#include "m_ctype.h"
#include "my_dbug.h"
#include "my_inttypes.h"
#include "my_sqlcommand.h"
#include "prealloced_array.h"
#include "sql/sql_lex.h"

#include "sql/common/table_common.h"
#include "sql/outline/outline_common.h"

class THD;

namespace im {

enum mysql_outline_table_field {
  MYSQL_OUTLINE_FIELD_ID = 0,
  MYSQL_OUTLINE_FIELD_SCHEMA,
  MYSQL_OUTLINE_FIELD_DIGEST,
  MYSQL_OUTLINE_FIELD_DIGEST_TEXT,
  MYSQL_OUTLINE_FIELD_TYPE,
  MYSQL_OUTLINE_FIELD_SCOPE,
  MYSQL_OUTLINE_FIELD_STATE,
  MYSQL_OUTLINE_FIELD_POSITION,
  MYSQL_OUTLINE_FIELD_HINT,
  MYSQL_OUTLINE_FIELD_COUNT
};

/* Statement outline type  */
enum class Outline_type {
  INDEX_IGNORE = 0,
  INDEX_USE,
  INDEX_FORCE,
  OPTIMIZER,
  UNKNOWN
};

/* Convert string to outline type */
extern Outline_type to_outline_type(const char *str);

extern const char *outline_type_str[];

/* Convert the val_int() to outline type */
inline Outline_type to_outline_type(longlong val) {
  assert(val >= 1);
  return static_cast<Outline_type>(val - 1);
}

/* Statement outline scope structure */
struct Outline_scope {
  LEX_CSTRING str;
  uchar mask;
};

/* Conver string to outline scope */
extern Outline_scope to_outline_scope(const char *str);

extern const Outline_scope outline_scopes[];
extern const Outline_scope OUTLINE_NULL_SCOPE;

/* Outline state */
enum class Outline_state { OUTLINE_INACTIVE = 0, OUTLINE_ACTIVE };

/* Convert the val_int() to outline state */
inline Outline_state to_outline_state(longlong val) {
  assert(val >= 1);
  return static_cast<Outline_state>(val - 1);
}

struct Outline_record : public Conf_record {
  ulonglong id;
  LEX_CSTRING schema;
  LEX_CSTRING digest;
  LEX_CSTRING digest_text;
  Outline_type type;
  Outline_scope scope;
  Outline_state state;
  ulonglong pos;
  LEX_CSTRING hint;
  LEX_CSTRING query;

 public:
  Outline_record() { reset(); }

  void reset() {
    id = 0;
    schema = {nullptr, 0};
    digest = {nullptr, 0};
    digest_text = {nullptr, 0};
    type = Outline_type::UNKNOWN;
    scope = OUTLINE_NULL_SCOPE;
    state = Outline_state::OUTLINE_ACTIVE;
    pos = 0;
    hint = {nullptr, 0};
    query = {nullptr, 0};
  }
  virtual bool check_valid(const char **msg) const override;
  /**
    Try to parse the optimizer hint and check the valid.

    @param[in]      thd       thread context

    @retval         true      valid
    @retval         false     not valid
  */
  bool optimizer_hint_valid(THD *thd);

  virtual ulonglong get_id() const override { return id; }
  virtual void set_id(ulonglong value) override { id = value; }
  virtual bool check_active() const override {
    return state == Outline_state::OUTLINE_ACTIVE;
  }
};

} /* namespace im */

#endif
