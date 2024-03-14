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

#ifndef SQL_DD_DD_MINOR_UPGRADE_H_INCLUDED
#define SQL_DD_DD_MINOR_UPGRADE_H_INCLUDED

/**
  Minor Upgrade is designed to upgrade I_S tables or System views by RDS.

  This strategy adds extra_I_S_version and extra_P_S_version into
  DD_properties table, we should increase these two version if modify I_S
  or P_S table struture.

  DD restart will construct I_S and P_S tables if versions are
  not consistent.

  This strategy didn't affect official upgrade.
*/
#include <sys/types.h>

class THD;

namespace dd {

/**
  Version for plugin I_S tables.
  PLS increase it if update plugin I_S table structure.

  Attention:
  Need to update I_S plugin version calculation to take effect.
*/
static const uint EXTRA_IS_PLUGIN_VERSION = 0;

/* The singleton instance to deal with minor upgrade */
class Minor_upgrade_ctx {
 public:
  Minor_upgrade_ctx() {}

  virtual ~Minor_upgrade_ctx() {}

  // Get the singleton instance
  static Minor_upgrade_ctx *instance();

  // Get compiled extra I_S version
  static uint get_target_extra_I_S_version();

  // Get compiled extra P_S version
  static uint get_target_extra_P_S_version();

  // Get persisted extra I_S version
  virtual uint get_actual_extra_I_S_version(THD *thd);

  // Get persisted extra P_S version
  virtual uint get_actual_extra_P_S_version(THD *thd);

  // Persist extra I_S version into dd_properites
  uint set_extra_I_S_version(THD *thd, uint version);

  // Persist extra P_S version into dd_properites
  uint set_extra_P_S_version(THD *thd, uint version);

  // Get compiled extra mysqld_version_upgraded version
  static uint get_target_extra_mvu_version();

  // Set extra mysqld_version_upgraded variable
  void set_extra_mvu_version(uint version);

  // Get extra mysqld_version_upgraded variable
  uint get_extra_mvu_version();

  // Query the version and set variable
  void retrieve_and_set_extra_mvu_version(THD *thd);

  // Save the version and set variables
  bool save_and_set_extra_mvu_version(THD *thd, uint version);

 private:
  uint m_extra_mvu_version;
};

}  // namespace dd

#endif
