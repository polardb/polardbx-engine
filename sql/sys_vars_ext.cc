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
#include "sql/auth/sql_guard.h"
#include "sql/auth/sql_internal_account.h"

#include "my_config.h"
#include "mysqld.h"

#include <assert.h>
#include <limits.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <sys/stat.h>
#include <zlib.h>
#include <atomic>
/**
  Performance_point is statically compiled plugin,
  so include it directly
*/
#include "plugin/performance_point/pps_server.h"
#include "sql/ha_sequence.h"

#include "sql/xa/lizard_xa_trx.h"

/* Global scope variables */
char innodb_version[SERVER_VERSION_LENGTH];

/* Local scope variables */
static uint rds_version = 0;
static char *rds_release_date_ptr = NULL;


/**
  Output the latest commit id for the MYSQLD binary.

  @returns void.
*/
void print_commit_id() {
  printf("commit id: %s\n", RDS_COMMIT_ID);
}

/**
  Customize mysqld server version

  MYSQL_VERSION_PATCH that's among version string can be reset
  by "rds_version" variable dynamically.

  @returns void.
*/
void customize_server_version() {
  char tmp_version[SERVER_VERSION_LENGTH];
  uint version_patch;
  size_t size;
  char *end;

  memset(tmp_version, '\0', SERVER_VERSION_LENGTH);
  version_patch =
      rds_version > MYSQL_VERSION_PATCH ? rds_version : MYSQL_VERSION_PATCH;

  size = snprintf(tmp_version, SERVER_VERSION_LENGTH, "%d.%d.%d%s",
                  MYSQL_VERSION_MAJOR, MYSQL_VERSION_MINOR, version_patch,
                  MYSQL_VERSION_EXTRA);

  strxmov(innodb_version, tmp_version, NullS);

  end = strstr(server_version, "-");
  if (end && (size < SERVER_VERSION_LENGTH)) {
    snprintf(tmp_version + size, (SERVER_VERSION_LENGTH - size), "%s", end);
  }
  strxmov(server_version, tmp_version, NullS);
}

static bool fix_server_version(sys_var *, THD *, enum_var_type) {
  customize_server_version();
  return false;
}

/**
  RDS DEFINED variables
*/
static Sys_var_uint Sys_rds_version(
    "rds_version",
    "The mysql patch version",
    GLOBAL_VAR(rds_version), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 999), DEFAULT(MYSQL_VERSION_PATCH),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(fix_server_version));

static Sys_var_charptr Sys_rds_release_date(
    "rds_release_date", "RDS RPM package release date",
    READ_ONLY GLOBAL_VAR(rds_release_date_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(RDS_RELEASE_DATE));

/* Internal Account variables. */
using namespace im;

static PolyLock_mutex Plock_internal_account_string(
    &LOCK_internal_account_string);

static bool update_kill_user(sys_var *, THD *, enum_var_type) {
  Internal_account_ctx::instance()->build_array(IA_type::KILL_USER);
  return (false);
}

static Sys_var_charptr Sys_rds_kill_user_list(
    "rds_kill_user_list", "user can kill non-super user, string split by ','",
    GLOBAL_VAR(ia_config.user_str[IA_type::KILL_USER]),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(0),
    &Plock_internal_account_string, NOT_IN_BINLOG, ON_CHECK(NULL),
    ON_UPDATE(update_kill_user));

static Sys_var_ulong Sys_rds_kill_connections(
    "rds_kill_connections", "Max conenction count for rds kill user",
    GLOBAL_VAR(ia_config.connections[IA_type::KILL_USER]),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, ULONG_MAX), DEFAULT(0),
    BLOCK_SIZE(1));

static bool update_maintain_user(sys_var *, THD *, enum_var_type) {
  Internal_account_ctx::instance()->build_array(IA_type::MAINTENACE_USER);
  return (false);
}
static Sys_var_charptr Sys_rds_maintain_user_list(
    "maintain_user_list", "maintenace account string split by ','",
    GLOBAL_VAR(ia_config.user_str[IA_type::MAINTENACE_USER]),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(0),
    &Plock_internal_account_string, NOT_IN_BINLOG, ON_CHECK(NULL),
    ON_UPDATE(update_maintain_user));

static Sys_var_ulong Sys_rds_maintain_max_connections(
    "maintain_max_connections", "The max connection count for maintain user",
    GLOBAL_VAR(ia_config.connections[IA_type::MAINTENACE_USER]),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, ULONG_MAX), DEFAULT(0),
    BLOCK_SIZE(1));

static Sys_var_bool Sys_opt_enable_rds_priv_strategy(
    "opt_enable_rds_priv_strategy",
    "When this option is enabled,"
    "it will protect reserved account and privileges",
    GLOBAL_VAR(opt_enable_rds_priv_strategy), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

/* RDS DEFINED */

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

static bool set_owned_vision_gcn_on_update(sys_var *, THD *thd, enum_var_type) {
  if (thd->variables.innodb_snapshot_gcn == MYSQL_GCN_NULL) {
    thd->owned_vision_gcn.reset();
  } else {
    thd->owned_vision_gcn.set(
        MYSQL_CSR_ASSIGNED, thd->variables.innodb_snapshot_gcn, MYSQL_SCN_NULL);
  }
  return false;
}

static Sys_var_ulonglong Sys_innodb_snapshot_seq(
    "innodb_snapshot_seq", "Innodb snapshot sequence.",
    HINT_UPDATEABLE SESSION_ONLY(innodb_snapshot_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(MYSQL_GCN_MIN, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(set_owned_vision_gcn_on_update));

static bool set_owned_commit_gcn_on_update(sys_var *, THD *thd, enum_var_type) {
  thd->owned_commit_gcn.set(thd->variables.innodb_commit_gcn,
                            MYSQL_CSR_ASSIGNED);
  return false;
}

static Sys_var_ulonglong Sys_innodb_commit_seq(
    "innodb_commit_seq", "Innodb commit sequence",
    HINT_UPDATEABLE SESSION_ONLY(innodb_commit_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(MYSQL_GCN_MIN, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(set_owned_commit_gcn_on_update));

static Sys_var_bool Sys_only_report_warning_when_skip(
    "only_report_warning_when_skip_sequence",
    "Whether reporting warning when the value skipped to is not valid "
    "instead of raising error",
    GLOBAL_VAR(opt_only_report_warning_when_skip_sequence), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_innodb_current_snapshot_gcn(
    "innodb_current_snapshot_seq",
    "Get snapshot_seq from innodb,"
    "the value is current max snapshot sequence and plus one",
    HINT_UPDATEABLE SESSION_ONLY(innodb_current_snapshot_gcn), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

/**
  If enable the hb_freezer, pretend to send heartbeat before updating, so it
  won't be blocked because of timeout.
*/
bool freeze_db_if_no_cn_heartbeat_enable_on_update(sys_var *, THD *,
                                                   enum_var_type) {
  const bool is_enable = lizard::xa::no_heartbeat_freeze;

  /** 1. Pretend to send heartbeat. */
  if (is_enable) {
    lizard::xa::hb_freezer_heartbeat();
  }

  lizard::xa::opt_no_heartbeat_freeze = lizard::xa::no_heartbeat_freeze;
  return false;
}
static Sys_var_bool Sys_freeze_db_if_no_cn_heartbeat_enable(
    "innodb_freeze_db_if_no_cn_heartbeat_enable",
    "If set to true, will freeze purge sys and updating "
    "if there is no heartbeat.",
    GLOBAL_VAR(lizard::xa::no_heartbeat_freeze), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(freeze_db_if_no_cn_heartbeat_enable_on_update));

static Sys_var_ulonglong Sys_freeze_db_if_no_cn_heartbeat_timeout_sec(
    "innodb_freeze_db_if_no_cn_heartbeat_timeout_sec",
    "If the heartbeat has not been received after the "
    "timeout, freezing the purge sys and updating.",
    GLOBAL_VAR(lizard::xa::opt_no_heartbeat_freeze_timeout),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(1, 24 * 60 * 60), DEFAULT(10),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern bool opt_gcn_write_event;
static Sys_var_bool Sys_gcn_write_event(
    "gcn_write_event",
    "Writting a gcn event which content is gcn number for every transaction.",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_gcn_write_event),
    CMD_LINE(OPT_ARG), DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));
