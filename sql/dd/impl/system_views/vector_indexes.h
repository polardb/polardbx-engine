/* Copyright (c) 2024, Alibaba and/or its affiliates.

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

#ifndef DD_SYSTEM_VIEWS__VECTOR_INDEXES_INCLUDED
#define DD_SYSTEM_VIEWS__VECTOR_INDEXES_INCLUDED

#include "sql/dd/impl/system_views/system_view_definition_impl.h"
#include "sql/dd/impl/system_views/system_view_impl.h"
#include "sql/dd/string_type.h"

namespace dd {
namespace system_views {

/*
  The class representing INFORMATION_SCHEMA.VECTOR_INDEXES system view
  definition.

  Vector indexes are not stored as dd::Index objects (HA_VECTOR keys are
  skipped in dd_table.cc). Their metadata lives in DD table options: the base
  table option '__hlindexes__' marks the index name, and the auxiliary table
  ('vidx_<se_private_id>_0') options carry '__vector_m__'/'__vector_distance__'/
  '__vector_column__'. This view reconstructs vector index metadata purely from
  those DD options via GET_DD_PROPERTY_KEY_VALUE(), without any in-memory cache.
*/
class Vector_indexes
    : public System_view_impl<System_view_select_definition_impl> {
 public:
  enum enum_fields {
    FIELD_TABLE_CATALOG,
    FIELD_TABLE_SCHEMA,
    FIELD_TABLE_NAME,
    FIELD_INDEX_NAME,
    FIELD_HLINDEX_TABLE_NAME,
    FIELD_COLUMN_NAME,
    FIELD_ALGORITHM,
    FIELD_METRIC_TYPE,
    FIELD_DIMENSION,
    FIELD_M,
    FIELD_EF_CONSTRUCTION,
    FIELD_QUANTIZE_TYPE
  };

  Vector_indexes();

  static const Vector_indexes &instance();

  static const String_type &view_name() {
    static String_type s_view_name("VECTOR_INDEXES");
    return s_view_name;
  }

  const String_type &name() const override {
    return Vector_indexes::view_name();
  }
};

}  // namespace system_views
}  // namespace dd

#endif  // DD_SYSTEM_VIEWS__VECTOR_INDEXES_INCLUDED
