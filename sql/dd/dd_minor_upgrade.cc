/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#include "my_inttypes.h"
#include "sql/dd/impl/tables/dd_properties.h"
#include "sql/dd/string_type.h"
#include "sql/sql_class.h"

#include "sql/dd/dd_minor_upgrade.h"

namespace dd {
/**
  Version for server I_S tables and System Views.
  PLS increase it if update server I_S tables or System Views.


  Historical I_S version number published:

  1. Published in Issue 46658
  ---------------------------
  Object statistics:
    INFORMATION_SCHEMA.TABLE_STATISTICS
    INFORMATION_SCHEMA.INDEX_STATISTICS

  2. Published in Issue 55125
  ---------------------------
  InnoDB IO statistics:
    INFORMATION_SCHEMA.IO_STATISTICS
*/
static const uint EXTRA_IS_DD_VERSION = 2;
/**
  Version for PFS tables.
  PLS increase it if update PFS table structure.


  Historical I_S version number published:

  1. Published in Issue 51139
  ---------------------------
  Top SQL statistics:

  PERFORMANCE_SCHEMA.EVENTS_STATEMENTS_SUMMARY_BY_DIGEST_SUPPLEMENT
*/
static const uint EXTRA_PS_DD_VERSION = 1;

static const String_type EXTRA_IS_DD_VERSION_STRING("EXTRA_IS_VERSION");

static const String_type EXTRA_PS_DD_VERSION_STRING("EXTRA_PS_VERSION");

static const String_type EXTRA_MYSQLD_VERSION_UPGRADED_STRING(
    "EXTRA_MYSQLD_VERSION_UPGRADED");

/**
  Version for mysql.* system tables that are visible to user.
  PLS increase it if update system table structure.

  Historical mysqld_version_upgrade  version number published:

  1. Published by Concurrency control system
  ------------------------------------------
  mysql.concurrency_control

  2. Published by outline system
  ------------------------------------------
  mysql.outline

  3. Published by Recycle bin
  ------------------------------------------
  __recycle_bin__
*/
static const uint EXTRA_MYSQLD_VERSION_UPGRADED = 3;

static const unsigned int UNKNOWN_EXTRA_VERSION = -1;

// Get the singleton instance
Minor_upgrade_ctx *Minor_upgrade_ctx::instance() {
  static Minor_upgrade_ctx ctx;
  return &ctx;
}

// Get compiled extra I_S version
uint Minor_upgrade_ctx::get_target_extra_I_S_version() {
  return EXTRA_IS_DD_VERSION;
}

// Get compiled extra P_S version
uint Minor_upgrade_ctx::get_target_extra_P_S_version() {
  return EXTRA_PS_DD_VERSION;
}

// Get compiled extra mysqld_version_upgraded version
uint Minor_upgrade_ctx::get_target_extra_mvu_version() {
  return EXTRA_MYSQLD_VERSION_UPGRADED;
}

// Get persisted extra I_S version
uint Minor_upgrade_ctx::get_actual_extra_I_S_version(THD *thd) {
  bool exists = false;
  uint version = 0;
  bool error MY_ATTRIBUTE((unused)) = tables::DD_properties::instance().get(
      thd, EXTRA_IS_DD_VERSION_STRING.c_str(), &version, &exists);
  if (error || !exists)
    return UNKNOWN_EXTRA_VERSION;
  else
    return version;
}

// Get persisted extra P_S version
uint Minor_upgrade_ctx::get_actual_extra_P_S_version(THD *thd) {
  bool exists = false;
  uint version = 0;
  bool error MY_ATTRIBUTE((unused)) = tables::DD_properties::instance().get(
      thd, EXTRA_PS_DD_VERSION_STRING.c_str(), &version, &exists);

  if (error || !exists)
    return UNKNOWN_EXTRA_VERSION;
  else
    return version;
}

// Persist extra I_S version into dd_properites
uint Minor_upgrade_ctx::set_extra_I_S_version(THD *thd, uint version) {
  return tables::DD_properties::instance().set(
      thd, EXTRA_IS_DD_VERSION_STRING.c_str(), version);
}

// Persist extra P_S version into dd_properites
uint Minor_upgrade_ctx::set_extra_P_S_version(THD *thd, uint version) {
  return tables::DD_properties::instance().set(
      thd, EXTRA_PS_DD_VERSION_STRING.c_str(), version);
}

// Set extra mysqld_version_upgraded variable
void Minor_upgrade_ctx::set_extra_mvu_version(uint version) {
  m_extra_mvu_version = version;
}
// Get extra mysqld_version_upgraded variable
uint Minor_upgrade_ctx::get_extra_mvu_version() { return m_extra_mvu_version; }

// Query the version and set variable
void Minor_upgrade_ctx::retrieve_and_set_extra_mvu_version(THD *thd) {
  bool exists = false;
  uint version = 0;
  bool error MY_ATTRIBUTE((unused)) = tables::DD_properties::instance().get(
      thd, EXTRA_MYSQLD_VERSION_UPGRADED_STRING.c_str(), &version, &exists);
  if (error || !exists)
    set_extra_mvu_version(UNKNOWN_EXTRA_VERSION);
  else
    set_extra_mvu_version(version);
}

// Save the version and set variables
bool Minor_upgrade_ctx::save_and_set_extra_mvu_version(THD *thd, uint version) {
  bool error;
  error = tables::DD_properties::instance().set(
      thd, EXTRA_MYSQLD_VERSION_UPGRADED_STRING.c_str(), version);

  if (!error) set_extra_mvu_version(version);

  return error;
}

}  // namespace dd
