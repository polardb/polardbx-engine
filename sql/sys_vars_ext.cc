/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxySQL hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql_statistics_common.h"
#include "sys_vars.h"
#include "sys_vars_ext.h"
#include "plugin/performance_point/pps.h"
/**
  Performance_point is statically compiled plugin,
  so include it directly
*/
#include "plugin/performance_point/pps_server.h"

static Sys_var_bool Sys_opt_tablestat("opt_tablestat",
                                      "When this option is enabled,"
                                      "it will accumulate the table statistics",
                                      GLOBAL_VAR(opt_tablestat),
                                      CMD_LINE(OPT_ARG), DEFAULT(true),
                                      NO_MUTEX_GUARD, NOT_IN_BINLOG,
                                      ON_CHECK(nullptr), ON_UPDATE(nullptr));

static Sys_var_bool Sys_opt_indexstat("opt_indexstat",
                                      "When this option is enabled,"
                                      "it will accumulate the index statistics",
                                      GLOBAL_VAR(opt_indexstat),
                                      CMD_LINE(OPT_ARG), DEFAULT(true),
                                      NO_MUTEX_GUARD, NOT_IN_BINLOG,
                                      ON_CHECK(nullptr), ON_UPDATE(nullptr));

static Sys_var_bool Sys_opt_performance_point_enabled(
    "performance_point_enabled",
    "whether open the performance point system plugin",
    READ_ONLY GLOBAL_VAR(opt_performance_point_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr), ON_UPDATE(nullptr));

static Sys_var_ulong Sys_performance_point_iostat_volume_size(
    "performance_point_iostat_volume_size",
    "The max iostat records that keeped in memory",
    READ_ONLY GLOBAL_VAR(performance_point_iostat_volume_size),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, 100000), DEFAULT(10000),
    BLOCK_SIZE(1));

static Sys_var_ulong Sys_performance_point_iostat_interval(
    "performance_point_iostat_interval",
    "The time interval every iostat aggregation (second time)",
    GLOBAL_VAR(performance_point_iostat_interval), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 60), DEFAULT(2), BLOCK_SIZE(1));

static Sys_var_bool Sys_opt_performance_point_lock_rwlock_enabled(
    "performance_point_lock_rwlock_enabled",
    "Enable Performance Point statement level rwlock aggregation(enabled by "
    "default)",
    GLOBAL_VAR(opt_performance_point_lock_rwlock_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_opt_performance_point_dbug_enabled(
    "performance_point_dbug_enabled", "Enable Performance Point debug mode",
    GLOBAL_VAR(opt_performance_point_dbug_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_charptr Sys_client_endpoint_ip(
  "client_endpoint_ip",
  "The endpoint ip that client use to connect.",
  SESSION_VAR(client_endpoint_ip),
  CMD_LINE(REQUIRED_ARG),
  IN_SYSTEM_CHARSET, DEFAULT(0),
  NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulonglong Sys_innodb_snapshot_seq(
    "innodb_snapshot_seq", "Innodb snapshot sequence.",
    HINT_UPDATEABLE SESSION_ONLY(innodb_snapshot_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1024, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL), BLOCK_SIZE(1));

static Sys_var_ulonglong Sys_innodb_commit_seq(
    "innodb_commit_seq", "Innodb commit sequence",
    HINT_UPDATEABLE SESSION_ONLY(innodb_commit_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1024, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL), BLOCK_SIZE(1));

