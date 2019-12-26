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

#ifndef SQL_INVENTORY_INVENTORY_HINT_INCLUDED
#define SQL_INVENTORY_INVENTORY_HINT_INCLUDED

#include "my_inttypes.h"

#define IC_HINT_COMMIT (1U << 0)
#define IC_HINT_ROLLBACK (1U << 1)
#define IC_HINT_TARGET (1U << 2)

class THD;
class String;

namespace im {

class Inventory_hint {
public:
  explicit Inventory_hint() : m_flags(0), m_target_rows(0) {}

  ~Inventory_hint() {}

  void specify(ulonglong hint) { m_flags |= hint; }

  bool is_specified(ulonglong hint) { return hint & m_flags; }

  ulonglong get_target_rows() { return m_target_rows; }

  void set_target_rows(ulonglong value) { m_target_rows = value; }

  virtual void print(const THD *thd, String *str);

private:
  ulonglong m_flags;
  ulonglong m_target_rows;
};
/**
  Precheck the inventory hint whether it's reasonable.
*/
extern bool disable_inventory_hint(THD *thd, Inventory_hint *hint);

extern void process_inventory_target_hint(THD *thd, Inventory_hint *hint);

extern void process_inventory_transactional_hint(THD *thd,
                                                 Inventory_hint *hint);

} /* namespace im */

#endif

