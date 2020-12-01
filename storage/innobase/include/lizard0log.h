/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
lzeusited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the zeusplied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/

/** @file include/lizard0log.h
 Lizard system log

 Created 2020-07-29 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0log_h
#define lizard0log_h

namespace lizard {

struct log_system {
  /** Log subsys module name for lizard */
  static const char *s_lizard_module_name;

  /** Log subsys module name for innodb */
  static const char *s_innodb_module_name;
};

}  // namespace lizard

#define LIZARD_LOG_SYS lizard::log_system::s_lizard_module_name

#define INNODB_LOG_SYS lizard::log_system::s_innodb_module_name

/** Lizard module system log interface */
#define lizard_info(ERR) ib::info(LIZARD_LOG_SYS, ERR)
#define lizard_warn(ERR) ib::warn(LIZARD_LOG_SYS, ERR)
#define lizard_error(ERR) ib::error(LIZARD_LOG_SYS, ERR)

#endif
