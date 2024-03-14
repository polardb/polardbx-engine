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

#ifndef SQL_CCL_CCL_HINT_INCLUDED
#define SQL_CCL_CCL_HINT_INCLUDED

#include "lex_string.h"
#include "my_inttypes.h"
#include "sql_string.h"

class THD;
class Item;
class Query_block;

namespace im {

/**
  All the ccl hint type.
*/
enum class Ccl_hint_type { CCL_HINT_QUEUE_FIELD, CCL_HINT_QUEUE_VALUE };

/**
  The base class for the ccl queue hint.
*/
class Ccl_queue_hint {
 public:
  explicit Ccl_queue_hint(Item *item) : m_item(item) {}

  virtual ~Ccl_queue_hint() {}

  virtual std::string name() = 0;

  virtual ulonglong hash_value(THD *thd, const Query_block *select) = 0;

  virtual void print(const THD *thd, String *str);

 protected:
  Item *m_item;
};

/**
  Ccl queue field hint declaration.
*/
class Ccl_queue_field_hint : public Ccl_queue_hint {
 public:
  explicit Ccl_queue_field_hint(LEX_CSTRING lex_str, Item *item)
      : Ccl_queue_hint(item), m_field_name(lex_str) {}

  ~Ccl_queue_field_hint() override {}

  virtual std::string name() override;
  /**
    Hash value for the ccl_queue_field(field_name);

    @param[in]      thd         thread context
    @param[in]      select      the top-level select context
  */
  virtual ulonglong hash_value(THD *thd, const Query_block *select) override;

 private:
  LEX_CSTRING m_field_name;
};

/**
  Ccl queue field hint declaration.
*/
class Ccl_queue_value_hint : public Ccl_queue_hint {
 public:
  explicit Ccl_queue_value_hint(LEX_CSTRING lex_str, Item *item)
      : Ccl_queue_hint(item), m_value_str(lex_str) {}

  ~Ccl_queue_value_hint() override {}

  virtual std::string name() override;

  /**
    Hash value for the ccl_queue_value(value);

    @param[in]      thd         thread context
    @param[in]      select      the top-level select context
  */
  virtual ulonglong hash_value(THD *thd, const Query_block *select) override;

 private:
  LEX_CSTRING m_value_str;
};

ulonglong get_item_hash_value(Item *m_item);

} /* namespace im */
#endif
