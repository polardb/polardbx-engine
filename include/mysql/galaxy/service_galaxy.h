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

#ifndef MYSQL_GALAXY_SERVICE_GALAXY_H
#define MYSQL_GALAXY_SERVICE_GALAXY_H

#ifndef MYSQL_ABI_CHECK
#include <stdint.h>
#endif /* MYSQL_ABI_CHECK */

#ifdef __cplusplus
class THD;
#define MYSQL_THD THD *
#else
#define MYSQL_THD void *
#endif

/**
  bloomfilter function that was implemented within galaxy plugin.

  @param opaque_thd      Thread handle. If NULL, current_thd will be used.

  @retval 1              Exec failed, error has been reported.
  @retval 0              Exec successful.
*/
typedef void (*mysql_bloomfilter_t)(MYSQL_THD opaque_thd);

extern "C" struct mysql_galaxy_service_st {
  mysql_bloomfilter_t mysql_bloomfilter;
} * mysql_galaxy_service;

/**
  Interface
*/

#ifdef MYSQL_DYNAMIC_PLUGIN

#define mysql_launch_bloomfilter(_THD) \
    mysql_galaxy_service->mysql_bloomfilter(_THD)

#else

void mysql_launch_bloomfilter(MYSQL_THD opaque_thd);

#endif /* MYSQL_DYNAMIC_PLUGIN */

#endif /*MYSQL_GALAXY_SERVICE_GALAXY_H */
