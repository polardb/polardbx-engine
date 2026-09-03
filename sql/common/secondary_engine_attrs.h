/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

#ifndef SQL_COMMON_SECONDARY_ENGINE_ATTRS_H_
#define SQL_COMMON_SECONDARY_ENGINE_ATTRS_H_

#include "sql/dd/string_type.h"

struct THD;
struct TABLE;
namespace komodo {
/**
  Parse the secondary engine attribute JSON to check if table is marked
  readonly.

  Looks for { "polarx.readonly": true } in the JSON.

  @param json  The secondary engine attribute JSON string
  @param readonly_out  Pointer to bool to store the result
  @return true if parsing was successful, false otherwise
*/
bool parse_polarx_readonly_from_json(const dd::String_type &json,
                                     bool *readonly_out);

bool check_table_readonly_for_dml(THD *thd, TABLE *t);

}  // namespace komodo
#endif  // SQL_COMMON_SECONDARY_ENGINE_ATTRS_H_