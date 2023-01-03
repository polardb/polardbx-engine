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

#include "plugin/x/client/galaxy_protocol_string.h"

#include <cstring>

namespace gx {

/** The const string of MYSQLX protocol */
const char *PROTOCOL_MYSQLX = "MYSQLX";

/** The const string of GALAXYX protocol */
const char *PROTOCOL_GALAXYX = "GALAXYX";

/**
  Convert the protocol string into protocol type.

  @param      value       string
  @retval     type        protocol type
*/
Protocol_type convert_to_protocol_type(const char *value) {
  if (value == nullptr || value[0] == '\0') return Protocol_type::MYSQLX;

  if (strcmp(value, PROTOCOL_MYSQLX) == 0)
    return Protocol_type::MYSQLX;
  else if (strcmp(value, PROTOCOL_GALAXYX) == 0)
    return Protocol_type::GALAXYX;
  else
    return Protocol_type::MYSQLX;
}
}  // namespace gx
