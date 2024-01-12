/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/recycle_bin/recycle_auth.h"
#include "sql/auth/auth_acls.h"
#include "sql/current_thd.h"
#include "sql/recycle_bin/recycle.h"
#include "sql/sql_class.h"

namespace im {
namespace recycle_bin {

ACL_internal_access_result Recycle_internal_schema_access::check(
    ulong want_access, ulong *, bool) const {
  const ulong forbidden_for_normal_user =
      INSERT_ACL | UPDATE_ACL | DELETE_ACL | CREATE_ACL | DROP_ACL |
      REFERENCES_ACL | INDEX_ACL | ALTER_ACL | LOCK_TABLES_ACL | EXECUTE_ACL |
      CREATE_PROC_ACL | ALTER_PROC_ACL | EVENT_ACL | TRIGGER_ACL;

  THD *thd = current_thd;

  if (!thd) return ACL_INTERNAL_ACCESS_CHECK_GRANT;
  /* If SUPER_ACL user or sepcial flag, check the ACL continually. */
  if (thd->security_context()->check_access(SUPER_ACL) ||
      thd->recycle_state->is_priv_relax())
    return ACL_INTERNAL_ACCESS_CHECK_GRANT;

  if (want_access & forbidden_for_normal_user)
    return ACL_INTERNAL_ACCESS_DENIED;

  return ACL_INTERNAL_ACCESS_CHECK_GRANT;
}

const ACL_internal_table_access *Recycle_internal_schema_access::lookup(
    const char *) const {
  /* Didn't set ACL on per table for recycle database. */
  return NULL;
}

Recycle_internal_schema_access *Recycle_internal_schema_access::instance() {
  static Recycle_internal_schema_access schema_access;
  return &schema_access;
}

} /* namespace recycle_bin */
} /* namespace im */
