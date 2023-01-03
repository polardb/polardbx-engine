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


#include <stddef.h>
#include <sys/types.h>
#include "mysql/plugin.h"
#include "mysql/status_var.h"

#include "my_inttypes.h"
#include "my_macros.h"

#include "plugin/x/src/variables/galaxy_status.h"

namespace gx {

#define GALAXY_SHOW_FUNC(NAME) galaxy_show_##NAME

#define GALAXY_SHOW_PROPERTY(NAME, PROPERTY_TYPE, SHOW_TYPE)             \
  static int GALAXY_SHOW_FUNC(NAME)(MYSQL_THD, SHOW_VAR * var, char *) { \
    var->type = SHOW_TYPE;                                               \
    var->value = const_cast<char *>(                                     \
        Galaxy_status_variables::get_property(PROPERTY_TYPE).c_str());   \
    return 0;                                                            \
  }

GALAXY_SHOW_PROPERTY(port, Galaxy_property_type::TCP_PORT, SHOW_CHAR)
GALAXY_SHOW_PROPERTY(bind_address, Galaxy_property_type::TCP_BIND_ADDRESS,
                     SHOW_CHAR)

Galaxy_properties Galaxy_status_variables::m_properties;

struct SHOW_VAR Galaxy_status_variables::m_status_variables[] = {
    {"Galaxyx_port", (char *)&GALAXY_SHOW_FUNC(port), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"Galaxyx_address", (char *)&GALAXY_SHOW_FUNC(bind_address), SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {nullptr, nullptr, SHOW_LONG, SHOW_SCOPE_GLOBAL},

};

std::string Galaxy_status_variables::get_property(
    const Galaxy_property_type type) {
  auto it = m_properties.find(type);
  if (it != m_properties.end())
    return it->second;
  else
    return GALAXY_PROPERTY_UNDEFINED;
}

bool Galaxy_status_variables::set_property(Galaxy_property_type type,
                                           std::string value) {
  return m_properties.insert(std::make_pair(type, value)).second;
}

}  // namespace gx
