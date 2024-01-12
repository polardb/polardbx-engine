/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "registrator.h"

namespace gs {
namespace udf {

/**
   Register udf

   @param[in]     def     UDF

   @retval        true    Failure
   @retval        false   Success
*/
bool Registrator::udf_register(gs::udf::Udf_definition def) {
  switch (def.m_type) {
    case UDFTYPE_FUNCTION:
      return m_udf_registry.udf_register(def.m_name, def.m_result, def.m_func,
                                         def.m_func_init, def.m_func_deinit);
    case UDFTYPE_AGGREGATE:
      return m_udf_registry_aggregate.udf_register(
          def.m_name, def.m_result, def.m_func, def.m_func_init,
          def.m_func_deinit, def.m_func_add, def.m_func_clear);
  }

  return true;
}

/**
  Unregister udf

  @param[in]     name     UDF name
  @param[out]    present  Whether exists

  @retval        true     Failure
  @retval        false    Success
*/
bool Registrator::udf_unregister(const Udf_name name, int *was_present) {
  std::string func_name = name.first;
  Item_udftype type = name.second;

  switch (type) {
    case UDFTYPE_FUNCTION:
      return m_udf_registry.udf_unregister(func_name.c_str(), was_present);
    case UDFTYPE_AGGREGATE:
      return m_udf_registry_aggregate.udf_unregister(func_name.c_str(),
                                                     was_present);
  }
  return true;
}

}  // namespace udf
}  // namespace gs
