/* Copyright (c) 2014, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software Foundation,
   51 Franklin Street, Suite 500, Boston, MA 02110-1335 USA */

#ifndef DD_TABLES__FOREIGN_KEYS_INCLUDED
#define DD_TABLES__FOREIGN_KEYS_INCLUDED

#include "my_global.h"

#include "dd/object_id.h"                    // dd::Object_id
#include "dd/impl/types/object_table_impl.h" // dd::Object_table_impl

namespace dd {
  class Object_key;
namespace tables {

///////////////////////////////////////////////////////////////////////////

class Foreign_keys : public Object_table_impl
{
public:
  static const Foreign_keys &instance();

  static const std::string &table_name()
  {
    static std::string s_table_name("foreign_keys");
    return s_table_name;
  }

public:
  enum enum_fields
  {
    FIELD_ID,
    FIELD_SCHEMA_ID,
    FIELD_TABLE_ID,
    FIELD_NAME,
    FIELD_UNIQUE_CONSTRAINT_ID,
    FIELD_MATCH_OPTION,
    FIELD_UPDATE_RULE,
    FIELD_DELETE_RULE,
    FIELD_REFERENCED_CATALOG,
    FIELD_REFERENCED_SCHEMA,
    FIELD_REFERENCED_TABLE
  };

public:
  Foreign_keys();

  virtual const std::string &name() const
  { return Foreign_keys::table_name(); }

public:
  static Object_key *create_key_by_table_id(Object_id table_id);
};

///////////////////////////////////////////////////////////////////////////

}
}

#endif // DD_TABLES__FOREIGN_KEYS_INCLUDED
