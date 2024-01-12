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

#include "registry.h"
#include "registrator.h"
#include "udf.h"

#include "my_dbug.h"

namespace gs {
namespace udf {

/**
  Register the udf collection.

  @params   defs      Collection

  @retval             Number of udf register success
*/
int Registry::insert(Defs defs) {
  bool result;
  int udf_counter = 0;
  Registrator registrator;

  for (Udf_definition def : defs) {
    result = registrator.udf_register(def);
    if (!result) {
      m_names.insert(std::make_pair(def.m_name, def.m_type));
      udf_counter++;
    }
  }

  return udf_counter;
}

/**
  Unregister the udf collection.
*/
void Registry::drop() {
  int present = 0;
  Registrator registrator;

  for (auto it = m_names.cbegin(); it != m_names.cend(); it++) {
    registrator.udf_unregister(*it, &present);
    assert(present);
  }

  m_names.clear();
}

}  // namespace udf
}  // namespace gs
