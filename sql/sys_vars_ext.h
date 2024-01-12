#ifndef SYS_VARS_EXT_INCLUDED
#define SYS_VARS_EXT_INCLUDED
/* Copyright (c) 2002, 2018, Alibaba and/or its affiliates. All rights reserved.

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

/**
  @file
  "private" interface to sys_var - server configuration variables.

  This header is included by the file that contains declarations
  of extra sys_var variables (sys_vars_ext.cc).
*/

#include "my_config.h"
#include "my_inttypes.h"
#include "my_sharedlib.h"
#include "mysql_com.h"

#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>

extern "C" MYSQL_PLUGIN_IMPORT char innodb_version[SERVER_VERSION_LENGTH];

static constexpr uint DEFAULT_RPC_PORT = 33660;
extern int32 opt_rpc_port;
extern bool opt_enable_polarx_rpc;

static constexpr ulonglong DEFAULT_IMPORT_TABLESPACE_ITERATOR_INTERVAL = 0;
extern ulonglong opt_import_tablespace_iterator_interval_ms;

extern void customize_server_version();

extern void print_build_info();
#endif /* SYS_VARS_EXT_INCLUDED */
