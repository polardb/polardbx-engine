/* Copyright (c) 2017 Oracle and/or its affiliates. All rights reserved.

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

#include "dd/impl/types/function_impl.h"

#include <sstream>

#include "dd/string_type.h"                      // dd::String_type
#include "dd/impl/transaction_impl.h"            // Open_dictionary_tables_ctx
#include "dd/impl/raw/raw_record.h"              // Raw_record
#include "dd/impl/tables/routines.h"             // Routines
#include "dd/impl/transaction_impl.h"            // Open_dictionary_tables_ctx
#include "dd/types/dictionary_object_table.h"
#include "dd/types/parameter.h"                  // Parameter
#include "dd/types/weak_object.h"
#include "my_sys.h"
#include "mysqld_error.h"

using dd::tables::Routines;

namespace dd {

///////////////////////////////////////////////////////////////////////////
// Function implementation.
///////////////////////////////////////////////////////////////////////////

const Object_type &Function::TYPE()
{
  static Function_type s_instance;
  return s_instance;
}

///////////////////////////////////////////////////////////////////////////

Function_impl::Function_impl()
 :m_result_data_type(enum_column_types::LONG),
  m_result_data_type_null(false),
  m_result_is_zerofill(false),
  m_result_is_unsigned(false),
  m_result_numeric_precision_null(true),
  m_result_numeric_scale_null(true),
  m_result_datetime_precision_null(true),
  m_result_numeric_precision(0),
  m_result_numeric_scale(0),
  m_result_datetime_precision(0),
  m_result_char_length(0),
  m_result_collation_id(INVALID_OBJECT_ID)
{
  set_type(RT_FUNCTION);
}

///////////////////////////////////////////////////////////////////////////

bool Function_impl::validate() const
{
  if (result_collation_id() == INVALID_OBJECT_ID)
  {
    my_error(ER_INVALID_DD_OBJECT,
             MYF(0),
             Routine_impl::OBJECT_TABLE().name().c_str(),
             "Result_collation_id ID is not set");
    return true;
  }

  return false;
}

/////////////////////////////////////////////////////////////////////////

bool Function_impl::restore_attributes(const Raw_record &r)
{
  if (Routine_impl::restore_attributes(r))
    return true;

  m_result_data_type=
    (enum_column_types) r.read_int(Routines::FIELD_RESULT_DATA_TYPE);
  m_result_data_type_utf8= r.read_str(Routines::FIELD_RESULT_DATA_TYPE_UTF8);
  m_result_data_type_null= r.is_null(Routines::FIELD_RESULT_DATA_TYPE);

  // Read booleans
  m_result_is_zerofill= r.read_bool(Routines::FIELD_RESULT_IS_ZEROFILL);
  m_result_is_unsigned= r.read_bool(Routines::FIELD_RESULT_IS_UNSIGNED);

  // Read numerics
  m_result_numeric_precision=
    r.read_uint(Routines::FIELD_RESULT_NUMERIC_PRECISION);
  m_result_numeric_precision_null=
    r.is_null(Routines::FIELD_RESULT_NUMERIC_PRECISION);
  m_result_numeric_scale=
    r.read_uint(Routines::FIELD_RESULT_NUMERIC_SCALE);
  m_result_numeric_scale_null=
    r.is_null(Routines::FIELD_RESULT_NUMERIC_SCALE);
  m_result_datetime_precision=
    r.read_uint(Routines::FIELD_RESULT_DATETIME_PRECISION);
  m_result_datetime_precision_null=
    r.is_null(Routines::FIELD_RESULT_DATETIME_PRECISION);
  m_result_char_length= r.read_uint(Routines::FIELD_RESULT_CHAR_LENGTH);

  m_result_collation_id= r.read_ref_id(Routines::FIELD_RESULT_COLLATION_ID);

  return false;
}

///////////////////////////////////////////////////////////////////////////

bool Function_impl::store_attributes(Raw_record *r)
{
  // Store function attributes.
  return Routine_impl::store_attributes(r) ||
         r->store(Routines::FIELD_RESULT_DATA_TYPE,
                  static_cast<int>(m_result_data_type),
                  m_result_data_type_null) ||
         r->store(Routines::FIELD_RESULT_DATA_TYPE_UTF8,
                  m_result_data_type_utf8,
                  m_result_data_type_null) ||
         r->store(Routines::FIELD_RESULT_IS_ZEROFILL, m_result_is_zerofill) ||
         r->store(Routines::FIELD_RESULT_IS_UNSIGNED, m_result_is_unsigned) ||
         r->store(Routines::FIELD_RESULT_CHAR_LENGTH,
                  (ulonglong) m_result_char_length) ||
         r->store(Routines::FIELD_RESULT_NUMERIC_PRECISION,
                  m_result_numeric_precision,
                  m_result_numeric_precision_null) ||
         r->store(Routines::FIELD_RESULT_NUMERIC_SCALE,
                  m_result_numeric_scale,
                  m_result_numeric_scale_null) ||
         r->store(Routines::FIELD_RESULT_DATETIME_PRECISION,
                  m_result_datetime_precision,
                  m_result_datetime_precision_null) ||
         r->store(Routines::FIELD_RESULT_COLLATION_ID, m_result_collation_id);
}

///////////////////////////////////////////////////////////////////////////

bool Function_impl::update_routine_name_key(name_key_type *key,
                                            Object_id schema_id,
                                            const String_type &name) const
{
  return Function::update_name_key(key, schema_id, name);
}

///////////////////////////////////////////////////////////////////////////

bool Function::update_name_key(name_key_type *key,
                               Object_id schema_id,
                               const String_type &name)
{
  return Routines::update_object_key(key,
                                     schema_id,
                                     Routine::RT_FUNCTION,
                                     name);
}

///////////////////////////////////////////////////////////////////////////

/* purecov: begin deadcode */
void Function_impl::debug_print(String_type &outb) const
{
  dd::Stringstream_type ss;

  String_type s;
  Routine_impl::debug_print(s);

  ss
  << "FUNCTION OBJECT: { "
  << s
  << "m_result_data_type: " << static_cast<int>(m_result_data_type) << "; "
  << "m_result_data_type_utf8: " << m_result_data_type_utf8 << "; "
  << "m_result_data_type_null: " << m_result_data_type_null << "; "
  << "m_result_is_zerofill: " << m_result_is_zerofill << "; "
  << "m_result_is_unsigned: " << m_result_is_unsigned << "; "
  << "m_result_numeric_precision: " << m_result_numeric_precision << "; "
  << "m_result_numeric_precision_null: "
      << m_result_numeric_precision_null << "; "
  << "m_result_numeric_scale: " << m_result_numeric_scale << "; "
  << "m_result_numeric_scale_null: " << m_result_numeric_scale_null << "; "
  << "m_result_datetime_precision: " << m_result_datetime_precision << "; "
  << "m_result_datetime_precision_null: "
      << m_result_datetime_precision_null << "; "
  << "m_result_char_length: " << m_result_char_length << "; "
  << "m_result_collation_id: " << m_result_collation_id << "; "
  << "} ";

  outb= ss.str();
}
/* purecov: end */

///////////////////////////////////////////////////////////////////////////
// Routine_type implementation.
///////////////////////////////////////////////////////////////////////////

void Function_type::register_tables(Open_dictionary_tables_ctx *otx) const
{
  otx->add_table<Routines>();

  otx->register_tables<Parameter>();
}

///////////////////////////////////////////////////////////////////////////

Function_impl::Function_impl(const Function_impl &src)
  :Weak_object(src), Routine_impl(src),
   m_result_data_type(src.m_result_data_type),
   m_result_data_type_utf8(src.m_result_data_type_utf8),
   m_result_data_type_null(src.m_result_data_type_null),
   m_result_is_zerofill(src.m_result_is_zerofill),
   m_result_is_unsigned(src.m_result_is_unsigned),
   m_result_numeric_precision_null(src.m_result_numeric_precision_null),
   m_result_numeric_scale_null(src.m_result_numeric_scale_null),
   m_result_datetime_precision_null(src.m_result_datetime_precision_null),
   m_result_numeric_precision(src.m_result_numeric_precision),
   m_result_numeric_scale(src.m_result_numeric_scale),
   m_result_datetime_precision(src.m_result_datetime_precision),
   m_result_char_length(src.m_result_char_length),
   m_result_collation_id(src.m_result_collation_id)
{ }

///////////////////////////////////////////////////////////////////////////

}
