/* Copyright (c) 2016, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef KEYRING_RDS_LOGGER_H
#define KEYRING_RDS_LOGGER_H

#include <stdarg.h>

#include <mysql/components/services/log_builtins.h>
#include <mysql/plugin.h>
#include <mysqld_error.h>

namespace keyring_rds {

/**
  Log API.
*/
class Logger {
 public:
  static void log(longlong level, longlong errcode, ...) {
    va_list vl;
    va_start(vl, errcode);
    LogPluginErrV(level, errcode, vl);
    va_end(vl);
  }

  static void log(longlong level, const char *fmt, ...)
      __attribute__((format(gnu_printf, 2, 3))) {
    char ebuff[MYSQL_ERRMSG_SIZE];

    va_list args;
    va_start(args, fmt);
    (void)vsnprintf(ebuff, sizeof(ebuff), fmt, args);
    va_end(args);

    LogPluginErr(level, ER_KEYRING_LOGGER_ERROR_MSG, ebuff);
  }
};

}  // namespace keyring_rds

#endif  // KEYRING_RDS_LOGGER_H
