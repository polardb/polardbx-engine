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

#include "secondary_engine_attrs.h"

#include <string.h>

#include "sql/dd/cache/dictionary_client.h"
#include "sql/dd/string_type.h"
#include "sql/sql_class.h"
#include "sql/table.h"
namespace komodo {
bool parse_polarx_readonly_from_json(const dd::String_type &json,
                                     bool *readonly_out) {
  if (readonly_out == nullptr) return false;
  *readonly_out = false;
  if (json.empty()) return false;

  // Add basic validity checks to prevent undefined behavior
  const size_t max_json_length = 1024 * 1024;  // 1MB limit
  if (json.length() > max_json_length) return false;

  // Very small, robust scan to find: "polarx.readonly": true
  // Avoid bringing a full JSON parser into hot path.
  const char *s = json.c_str();
  const char *needle = "\"polarx.readonly\"";

  // Additional safety check: ensure needle is not longer than the JSON string
  if (strlen(needle) > json.length()) return false;

  const char *pkey = strstr(s, needle);
  if (pkey == nullptr) return false;
  // Find colon following the key
  const char *pcolon = strchr(pkey + strlen(needle), ':');
  if (pcolon == nullptr) return false;
  // Skip spaces
  const char *pval = pcolon + 1;
  while (*pval == ' ' || *pval == '\t') ++pval;
  // Check for true/false
  if (strncmp(pval, "true", 4) == 0) {
    *readonly_out = true;
    return true;
  }
  if (strncmp(pval, "false", 5) == 0) {
    *readonly_out = false;
    return true;
  }
  return false;
}

/**
 * Check if table is readonly for DML operations via secondary engine
 * This is komodo-specific functionality to distinguish from MySQL community
 */
bool check_table_readonly_for_dml(THD *thd, TABLE *t) {
  TABLE_SHARE *table_meta = t->s;
#ifndef NDEBUG
  // Mark that we've checked DML for debug assertion in handler layer
  if (!thd->security_context()->is_skip_grants_user()) {
    const bool has_exclusive_mdl = thd->mdl_context.owns_equal_or_stronger_lock(
        MDL_key::TABLE, table_meta->db.str, table_meta->table_name.str,
        MDL_EXCLUSIVE);
    const bool has_shared_no_read_write_mdl =
        thd->mdl_context.owns_equal_or_stronger_lock(
            MDL_key::TABLE, table_meta->db.str, table_meta->table_name.str,
            MDL_SHARED_NO_READ_WRITE);
    const bool is_dml_operation =
        !has_exclusive_mdl && !has_shared_no_read_write_mdl;
    if (is_dml_operation) {
      thd->set_dml_readonly_checked(true);
    }
  }
#endif
  if (table_meta->cached_secondary_engine_readonly) {
    // readonly check: check if table is marked readonly via secondary engine
    //  Only check DML operations (INSERT, UPDATE, DELETE, REPLACE, LOAD DATA,
    //  etc.) DDL operations (CREATE TABLE, ALTER TABLE, DROP TABLE) typically
    //  use MDL_EXCLUSIVE or MDL_SHARED_NO_READ_WRITE locks, while DML
    //  operations use MDL_SHARED_WRITE locks. SELECT queries have lock_type <
    //  TL_WRITE_ALLOW_WRITE, so they won't enter this branch either.
    //
    //  Distinguish DML from DDL by checking MDL lock type:
    //  - DML operations: hold MDL_SHARED_WRITE lock (but not stronger locks)
    //  - DDL operations: hold MDL_EXCLUSIVE or MDL_SHARED_NO_READ_WRITE locks
    const bool has_exclusive_mdl = thd->mdl_context.owns_equal_or_stronger_lock(
        MDL_key::TABLE, table_meta->db.str, table_meta->table_name.str,
        MDL_EXCLUSIVE);
    const bool has_shared_no_read_write_mdl =
        thd->mdl_context.owns_equal_or_stronger_lock(
            MDL_key::TABLE, table_meta->db.str, table_meta->table_name.str,
            MDL_SHARED_NO_READ_WRITE);

    // Only check DML operations (exclude DDL which has stronger MDL locks)
    // Note: CREATE TABLE ... SELECT may hold MDL_SHARED_WRITE for the target
    // table, but the target table is newly created and shouldn't be readonly
    // anyway.
    const bool is_dml_operation =
        !has_exclusive_mdl && !has_shared_no_read_write_mdl;
    if (!thd->security_context()->is_skip_grants_user() && is_dml_operation) {
      my_error(ER_TABLE_READONLY_ERROR, MYF(0), "DML");
      return true;
    }
  }
  return false;
}

}  // namespace komodo