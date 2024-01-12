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

#include "lex_string.h"
#include "m_ctype.h"
#include "my_dbug.h"
#include "mysql/thread_type.h"
#include "sql/auth/sql_security_ctx.h"
#include "sql/bootstrap.h"                   // bootstrap::run_bootstrap_thread
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_minor_upgrade.h"         // get_extra_mvu_version
#include "sql/dd/dd_schema.h"                // dd::schema_exists
#include "sql/dd/impl/utils.h"               // execute_query
#include "sql/dd/upgrade/server.h"
#include "sql/mysqld.h"
#include "sql/sql_class.h"
#include "sql/thd_raii.h"

#include "sql/table.h"

#include "sql/dd/recycle_bin/dd_recycle.h"
#include "sql/recycle_bin/recycle_table.h"

namespace im {
namespace recycle_bin {

/**
  Create recycle bin schema.

  @param[in]    thd       thread context

  @retval       true      failure
  @retval       false     success
*/
static bool create_schema(THD *thd) {
  bool exists = false;

  if (dd::schema_exists(thd, RECYCLE_BIN_SCHEMA.str, &exists)) return true;

  bool ret = false;
  if (!exists)
    ret = dd::execute_query(thd, dd::String_type("CREATE SCHEMA ") +
                                     dd::String_type(RECYCLE_BIN_SCHEMA.str) +
                                     dd::String_type(" CHARACTER SET utf8mb4"));
  return ret;
}
/**
  Prepare the recycle bin schema.

  @para[in]     thd       thread context

  @retval       true      failure
  @retval       false     success
*/
static bool initialize_recycle_bin(THD *thd) {
  bool error;
  thd->variables.transaction_read_only = false;
  thd->tx_read_only = false;

  Disable_autocommit_guard autocommit_guard(thd);
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  /**
     By version 'EXTRA_MYSQLD_VERSION_UPGRADED' to judge whether
     to create recycle_bin schema.

     Still create it if force_upgrade.

     MVU version will be modified after upgrading system tables
     that depend on it.
  */
  if (thd->system_thread == SYSTEM_THREAD_DD_RESTART &&
      (dd::Minor_upgrade_ctx::instance()->get_extra_mvu_version() ==
       dd::Minor_upgrade_ctx::instance()->get_target_extra_mvu_version()) &&
      (opt_upgrade_mode != UPGRADE_FORCE))
    return false;

  error = create_schema(thd);

  error = dd::end_transaction(thd, error);

  return error;
}
/**
  Init thread to prepare recycle bin context

  @param[in]    init_type     mysqld boot type

  @retval       false         success
  @retval       true          failure
*/
bool init(dd::enum_dd_init_type init_type) {
  if (init_type == dd::enum_dd_init_type::DD_INITIALIZE)
    return ::bootstrap::run_bootstrap_thread(
        nullptr, nullptr, &initialize_recycle_bin, SYSTEM_THREAD_DD_INITIALIZE);
  else if (init_type == dd::enum_dd_init_type::DD_RESTART_OR_UPGRADE)
    return ::bootstrap::run_bootstrap_thread(
        nullptr, nullptr, &initialize_recycle_bin, SYSTEM_THREAD_DD_RESTART);
  else {
    assert(false);
    return true;
  }
}

} /* namespace recycle_bin */

} /* namespace im */
