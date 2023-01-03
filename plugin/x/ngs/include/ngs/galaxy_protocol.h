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


#ifndef PLUGIN_X_NGS_INCLUDE_NGS_GALAXY_PROTOCOL_H
#define PLUGIN_X_NGS_INCLUDE_NGS_GALAXY_PROTOCOL_H

namespace gx {

/**
  Protocol type
  --------------------------------------
  1) MYSQLX
    Packet Format:
      4   message length
      1   message type

      payload

  2) GALAXYX
    Packet Format:
      8   session id
      1   version
      4   message length
      1   message type

      payload
*/
enum class Protocol_type { MYSQLX, GALAXYX };

}  // namespace gx

#endif
