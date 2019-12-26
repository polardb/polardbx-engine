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

#ifndef SQL_INVENTORY_INVENTORY_HINT_PARSE_INCLUDED
#define SQL_INVENTORY_INVENTORY_HINT_PARSE_INCLUDED

#include "sql/parse_tree_hints.h"

class Item;

Opt_hints_global *get_global_hints(Parse_context *pc);

namespace im {

class PT_hint_ic : public PT_hint {
  typedef PT_hint super;
public:
  explicit PT_hint_ic() : PT_hint(IC_HINT_ENUM, true) {}

  virtual bool contextualize(Parse_context *pc) override;
};

class PT_hint_ic_commit : public PT_hint_ic {
  typedef PT_hint_ic super;

public:
  explicit PT_hint_ic_commit() : PT_hint_ic() {}

  virtual bool contextualize(Parse_context *pc) override;
};

class PT_hint_ic_rollback : public PT_hint_ic {
  typedef PT_hint_ic super;

public:
  explicit PT_hint_ic_rollback() : PT_hint_ic() {}

  virtual bool contextualize(Parse_context *pc) override;
};

class PT_hint_ic_target : public PT_hint_ic {
  typedef PT_hint_ic super;

public:
  explicit PT_hint_ic_target(ulonglong value) : PT_hint_ic(), m_rows(value) {}

  virtual bool contextualize(Parse_context *pc) override;

 private:
  ulonglong m_rows;
};

} /* namespace im */

#endif
