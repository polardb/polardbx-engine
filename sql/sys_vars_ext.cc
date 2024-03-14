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

#include "sys_vars_ext.h"
#include "my_config.h"
#include "sql/auth/sql_guard.h"
#include "sql/auth/sql_internal_account.h"
#include "sql/ccl/ccl.h"
#include "sql/ccl/ccl_bucket.h"
#include "sql/ccl/ccl_interface.h"
#include "sql/log_table.h"
#include "sql/outline/outline_interface.h"
#include "sql/recycle_bin/recycle_scheduler.h"
#include "sql/recycle_bin/recycle_table.h"
#include "sql/sql_common_ext.h"
#include "sql/sys_vars.h"
#include "sql_statistics_common.h"
#include "sys_vars.h"

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

#include "sql/replica_read_manager.h"
#include "sql/xa/lizard_xa_trx.h"
#include "sql/sql_implicit_element.h"

#ifdef RDS_HAVE_JEMALLOC
#include "sql/sql_jemalloc.h"
#endif


/* Global scope variables */
char innodb_version[SERVER_VERSION_LENGTH];

/* Local scope variables */
static uint rds_version = 0;
static char *polardbx_release_date_ptr = NULL;
static char *polardbx_engine_version_ptr = NULL;

int32 opt_rpc_port = DEFAULT_RPC_PORT;
bool opt_enable_polarx_rpc = true;

ulonglong opt_import_tablespace_iterator_interval_ms =
    DEFAULT_IMPORT_TABLESPACE_ITERATOR_INTERVAL;

/**
  Output the latest build info for the MYSQLD binary.

  @returns void.
*/
void print_build_info() {
  printf("Engine Malloc Library: %s\n", MALLOC_LIBRARY);
  printf("Engine Version: %s\n", POLARDBX_ENGINE_VERSION);
  printf("Engine Release Date: %s\n", POLARDBX_RELEASE_DATE);
  printf("Engine Version Extra: %s\n", POLARDBX_VERSION_EXTRA);
  printf("Engine Build Type: %s\n", BUILD_TYPE);
  printf("Engine Build Branch: %s\n", BUILD_BRANCH);
  printf("Engine Build Commit: %s\n", BUILD_COMMIT);
  printf("Engine Build Time: %s %s\n", BUILD_DATE, BUILD_TIME);
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

  memset(tmp_version, '\0', SERVER_VERSION_LENGTH);
  version_patch =
      rds_version > MYSQL_VERSION_PATCH ? rds_version : MYSQL_VERSION_PATCH;

  snprintf(tmp_version, SERVER_VERSION_LENGTH, "%d.%d.%d%s",
           MYSQL_VERSION_MAJOR, MYSQL_VERSION_MINOR, version_patch,
           MYSQL_VERSION_EXTRA);

  strxmov(innodb_version, tmp_version, NullS);
}

static bool fix_server_version(sys_var *, THD *, enum_var_type) {
  customize_server_version();
  return false;
}

/* lock_instance mode > LOCK_INSTANCE_TABLE_CREATION require read only */
static bool check_lock_instance_mode(sys_var *, THD *, set_var *var) {
  if (var->save_result.ulonglong_value > LOCK_INSTANCE_TABLE_CREATION) {
    if (!opt_readonly && !opt_super_readonly) {
      my_error(ER_LOCK_INSTANCE_REQUIRE_READ_ONLY, MYF(0),
               lock_instance_mode_names[var->save_result.ulonglong_value]);
      return true;
    }
  }

  return false;
}

/**
  RDS DEFINED variables
*/
static Sys_var_uint Sys_rds_version("rds_version", "The mysql patch version",
                                    GLOBAL_VAR(rds_version), CMD_LINE(OPT_ARG),
                                    VALID_RANGE(1, 999),
                                    DEFAULT(MYSQL_VERSION_PATCH), BLOCK_SIZE(1),
                                    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
                                    ON_UPDATE(fix_server_version));

static Sys_var_charptr Sys_polardbx_release_date(
    "polardbx_release_date", "PolarDB-X RPM package release date",
    READ_ONLY GLOBAL_VAR(polardbx_release_date_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(POLARDBX_RELEASE_DATE));

static Sys_var_charptr Sys_polardbx_release_version(
    "polardbx_engine_version", "PolarDB-X engine version",
    READ_ONLY GLOBAL_VAR(polardbx_engine_version_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(POLARDBX_ENGINE_VERSION));

static Sys_var_deprecated_alias Sys_rds_release_date(
    "rds_release_date", Sys_polardbx_release_date);

static PolyLock_mutex Plock_internal_account_string(
    &im::LOCK_internal_account_string);

static bool update_kill_user(sys_var *, THD *, enum_var_type) {
  im::Internal_account_ctx::instance()->build_array(im::IA_type::KILL_USER);
  return (false);
}

static bool update_inner_user(sys_var *, THD *, enum_var_type) {
  im::Internal_account_ctx::instance()->build_array(im::IA_type::INNER_USER);
  return (false);
}

static Sys_var_charptr Sys_rds_kill_user_list(
    "rds_kill_user_list", "user can kill non-super user, string split by ','",
    GLOBAL_VAR(im::ia_config.user_str[im::IA_type::KILL_USER]), CMD_LINE(REQUIRED_ARG),
    IN_FS_CHARSET, DEFAULT(0), &Plock_internal_account_string, NOT_IN_BINLOG,
    ON_CHECK(NULL), ON_UPDATE(update_kill_user));

static Sys_var_ulong Sys_rds_kill_connections(
    "rds_kill_connections", "Max conenction count for rds kill user",
    GLOBAL_VAR(im::ia_config.connections[im::IA_type::KILL_USER]),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, ULONG_MAX), DEFAULT(0),
    BLOCK_SIZE(1));

static bool update_maintain_user(sys_var *, THD *, enum_var_type) {
  im::Internal_account_ctx::instance()->build_array(im::IA_type::MAINTENACE_USER);
  return (false);
}
static Sys_var_charptr Sys_rds_maintain_user_list(
    "maintain_user_list", "maintenace account string split by ','",
    GLOBAL_VAR(im::ia_config.user_str[im::IA_type::MAINTENACE_USER]),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(0),
    &Plock_internal_account_string, NOT_IN_BINLOG, ON_CHECK(NULL),
    ON_UPDATE(update_maintain_user));

static Sys_var_ulong Sys_rds_maintain_max_connections(
    "maintain_max_connections", "The max connection count for maintain user",
    GLOBAL_VAR(im::ia_config.connections[im::IA_type::MAINTENACE_USER]),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(0, ULONG_MAX), DEFAULT(0),
    BLOCK_SIZE(1));

static Sys_var_bool Sys_opt_enable_rds_priv_strategy(
    "opt_enable_rds_priv_strategy",
    "When this option is enabled,"
    "it will protect reserved account and privileges",
    GLOBAL_VAR(im::opt_enable_rds_priv_strategy), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_enum Sys_lock_instance_mode(
    "lock_instance_mode",
    "Instance lock mode: "
    "LOCK_NON is nothing to do;"
    "LOCK_TABLE_CREATION deny all SQLCOM_CREATE_TABLE;"
    "LOCK_WRITE_GROWTH equal read only but allow shrink space;"
    "LOCK_WRITE equal read only;"
    "LOCK_READ block all query;",
    GLOBAL_VAR(lock_instance_mode), CMD_LINE(REQUIRED_ARG),
    lock_instance_mode_names, DEFAULT(LOCK_INSTANCE_NON), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(check_lock_instance_mode), ON_UPDATE(NULL));

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
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(nullptr));

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
    "client_endpoint_ip", "The endpoint ip that client use to connect.",
    SESSION_VAR(client_endpoint_ip), CMD_LINE(REQUIRED_ARG), IN_SYSTEM_CHARSET,
    DEFAULT(0), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

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
    HINT_UPDATEABLE SESSION_ONLY(innodb_current_snapshot_gcn),
    CMD_LINE(OPT_ARG), DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));

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
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_gcn_write_event), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulong Sys_ccl_wait_timeout(
    "ccl_wait_timeout", "Timeout in seconds to wait when concurrency control.",
    GLOBAL_VAR(im::ccl_wait_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, LONG_TIMEOUT), DEFAULT(CCL_LONG_WAIT), BLOCK_SIZE(1));

static Sys_var_ulong Sys_ccl_max_waiting(
    "ccl_max_waiting_count", "max waiting count in one ccl rule or bucket",
    GLOBAL_VAR(im::ccl_max_waiting_count), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, INT_MAX64), DEFAULT(CCL_DEFAULT_WAITING_COUNT),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static bool update_ccl_queue(sys_var *, THD *, enum_var_type) {
  im::System_ccl::instance()->get_queue_buckets()->init_queue_buckets(
      im::ccl_queue_bucket_count, im::ccl_queue_bucket_size,
      im::Ccl_error_level::CCL_WARNING);
  return false;
}

static Sys_var_ulong Sys_ccl_queue_size(
    "ccl_queue_bucket_size", "The max concurrency allowed when use ccl queue",
    GLOBAL_VAR(im::ccl_queue_bucket_size), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, CCL_QUEUE_BUCKET_SIZE_MAX),
    DEFAULT(CCL_QUEUE_BUCKET_SIZE_DEFAULT), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(update_ccl_queue));

static Sys_var_ulong Sys_ccl_queue_bucket(
    "ccl_queue_bucket_count", "How many groups when use ccl queue",
    GLOBAL_VAR(im::ccl_queue_bucket_count), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, CCL_QUEUE_BUCKET_COUNT_MAX),
    DEFAULT(CCL_QUEUE_BUCKET_COUNT_DEFAULT), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(update_ccl_queue));

static Sys_var_bool Sys_recycle_bin(
    "recycle_bin", "Whether recycle the table which is going to be dropped",
    SESSION_VAR(recycle_bin), CMD_LINE(OPT_ARG), DEFAULT(false), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulong Sys_recycle_scheduler_interval(
    "recycle_scheduler_interval", "Interval in seconds for recycle scheduler.",
    GLOBAL_VAR(im::recycle_bin::recycle_scheduler_interval),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(1, 60), DEFAULT(30), BLOCK_SIZE(1));

static bool recycle_scheduler_retention_update(sys_var *, THD *,
                                               enum_var_type) {
  im::recycle_bin::Recycle_scheduler::instance()->wakeup();
  return false;
}

static Sys_var_ulong Sys_recycle_bin_retention(
    "recycle_bin_retention",
    "Seconds before really purging the recycled table.",
    GLOBAL_VAR(im::recycle_bin::recycle_bin_retention), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 365 * 24 * 60 * 60), DEFAULT(7 * 24 * 60 * 60),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(recycle_scheduler_retention_update));

static bool recycle_scheduler_update(sys_var *, THD *, enum_var_type) {
  bool res = false;
  bool value = im::recycle_bin::opt_recycle_scheduler;
  mysql_mutex_unlock(&LOCK_global_system_variables);

  if (value) {
    res = im::recycle_bin::Recycle_scheduler::instance()->start();
  } else {
    res = im::recycle_bin::Recycle_scheduler::instance()->stop();
  }
  mysql_mutex_lock(&LOCK_global_system_variables);
  if (res) {
    im::recycle_bin::opt_recycle_scheduler = false;
    my_error(ER_EVENT_SET_VAR_ERROR, MYF(0), 0);
  }
  return res;
}

static Sys_var_bool Sys_recycle_scheduler(
    "recycle_scheduler", "Enable the recycle scheduler.",
    GLOBAL_VAR(im::recycle_bin::opt_recycle_scheduler), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(recycle_scheduler_update));

static Sys_var_bool Sys_recycle_scheduler_purge_table_print(
    "recycle_scheduler_purge_table_print",
    "Print the recycle scheduler process.",
    GLOBAL_VAR(im::recycle_bin::recycle_scheduler_purge_table_print),
    CMD_LINE(OPT_ARG), DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));
static Sys_var_ulong Sys_outline_partitions(
    "outline_partitions", "How many parititon of system outline structure.",
    READ_ONLY GLOBAL_VAR(im::outline_partitions), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 256), DEFAULT(16), BLOCK_SIZE(1));

static Sys_var_bool Sys_opt_outline_enabled(
    "opt_outline_enabled",
    "When this option is enabled,"
    "it will invoke statement outline when execute sql",
    GLOBAL_VAR(im::opt_outline_enabled), CMD_LINE(OPT_ARG), DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_outline_allowed_sql_digest_truncate(
    "outline_allowed_sql_digest_truncate",
    "Whether allowed the incomplete of sql digest when add outline",
    SESSION_VAR(outline_allowed_sql_digest_truncate), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));
static Sys_var_bool Sys_auto_savepoint("auto_savepoint",
                                       "Whether to make implicit savepoint for "
                                       "each INSERT/DELETE/UPDATE statement",
                                       SESSION_VAR(auto_savepoint), NO_CMD_LINE,
                                       DEFAULT(false), NO_MUTEX_GUARD,
                                       NOT_IN_BINLOG, ON_CHECK(0),
                                       ON_UPDATE(0));

extern uint64_t opt_replica_read_timeout;
static const uint64_t DEFAULT_REPLICA_READ_TIMEOUT = 3000;  // ms

static Sys_var_ulonglong Sys_replica_read_timeout(
    "replica_read_timeout",
    "Maximum wait period (milliseconds) when performing replica consistent "
    "reads",
    GLOBAL_VAR(opt_replica_read_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 3600000), DEFAULT(DEFAULT_REPLICA_READ_TIMEOUT),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(NULL),
    ON_UPDATE(NULL));

static bool check_read_lsn(sys_var *, THD *, set_var *var) {
  ulonglong read_lsn = var->save_result.ulonglong_value;
  return !replica_read_manager.wait_for_lsn(read_lsn);
}

static Sys_var_ulonglong Sys_read_lsn(
    "read_lsn",
    "Minimun log applied index required for replica consistent reads",
    SESSION_VAR(opt_read_lsn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, LONG_LONG_MAX), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(check_read_lsn), ON_UPDATE(NULL));

extern bool opt_consensus_index_buf_enabled;
static Sys_var_bool Sys_mts_consensus_index_buf_enabled(
    "consensus_index_buf_enabled",
    "Whether to enable Relay_log_info::consensus_index_buf",
    GLOBAL_VAR(opt_consensus_index_buf_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern bool opt_disable_wait_commitindex;
static Sys_var_bool Sys_disable_wait_commitindex(
    "disable_wait_commitindex",
    "Whether to wait commitdex when applying binlog in follower, "
    "it may detory the cluster data if some crash happen",
    GLOBAL_VAR(opt_disable_wait_commitindex), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern bool opt_force_index_pct_cached;
static Sys_var_bool Sys_force_index_percentage_cached(
    "opt_force_index_pct_cached",
    "force cached table index in memory when estimate query cost",
    GLOBAL_VAR(opt_force_index_pct_cached), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0));

static Sys_var_ulong Sys_kill_idle_transaction(
    "kill_idle_transaction_timeout",
    "If non-zero, number of seconds to wait before killing idle "
    "connections that have open transactions",
    GLOBAL_VAR(kill_idle_transaction_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, LONG_TIMEOUT), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_rotate_log_table(
    "rotate_log_table",
    "Whether rotate the data file when flush slow_log or general_log",
    SESSION_ONLY(rotate_log_table), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_charptr Sys_rotate_log_table_last_name(
    "rotate_log_table_last_name", "Last rotated log table file name",
    READ_ONLY GLOBAL_VAR(im::rotate_log_table_last_name_ptr),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(im::rotate_log_table_last_name),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(NULL), ON_UPDATE(NULL));

static Sys_var_charptr Sys_rds_inner_schema_list(
    "inner_schema_list", "Inner schema string split by ','",
    READ_ONLY GLOBAL_VAR(im::inner_schema_list_str), CMD_LINE(REQUIRED_ARG),
    IN_FS_CHARSET, DEFAULT(0), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(NULL),
    ON_UPDATE(0));

static Sys_var_charptr Sys_rds_inner_user_list(
    "inner_user_list", "Inner account string split by ','",
    GLOBAL_VAR(im::ia_config.user_str[im::IA_type::INNER_USER]), CMD_LINE(REQUIRED_ARG),
    IN_FS_CHARSET, DEFAULT(0), &Plock_internal_account_string, NOT_IN_BINLOG,
    ON_CHECK(NULL), ON_UPDATE(update_inner_user));


#ifdef RDS_HAVE_JEMALLOC

static Sys_var_bool Sys_rds_active_memory_profiling(
    "rds_active_memory_profiling",
    "Active jemalloc memory profing, jemalloc must "
    "be enabled at server start up.",
    GLOBAL_VAR(im::opt_rds_active_memory_profiling), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(im::check_active_memory_profiling), ON_UPDATE(0));

#endif
/* RDS DEFINED */

/* PolarDB-X RPC */

static Sys_var_int32 Sys_rpc_port("rpc_port", "RPC port for PolarDB-X",
                                  READ_ONLY GLOBAL_VAR(opt_rpc_port),
                                  CMD_LINE(OPT_ARG), VALID_RANGE(0, 65535),
                                  DEFAULT(DEFAULT_RPC_PORT), BLOCK_SIZE(1),
                                  NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
                                  ON_UPDATE(0));

static Sys_var_bool Sys_enable_polarx_rpc(
    "enable_polarx_rpc", "Use new open PolarDB-X RPC",
    READ_ONLY GLOBAL_VAR(opt_enable_polarx_rpc), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_deprecated_alias Sys_new_rpc("new_rpc", Sys_enable_polarx_rpc);

static Sys_var_ulonglong Sys_import_tablespace_iterator_interval_ms(
    "import_tablespace_iterator_interval_ms",
    "The interval sleep between each import tablespace iterator in "
    "microseconds",
    GLOBAL_VAR(opt_import_tablespace_iterator_interval_ms), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 1000000), DEFAULT(10), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_deprecated_alias Sys_import_tablespace_iterator_interval(
    "import_tablespace_iterator_interval",
    Sys_import_tablespace_iterator_interval_ms);

static Sys_var_bool Sys_enable_physical_backfill(
    "enable_physical_backfill",
    "Whether support x-proto physical backfill, readonly option",
    READ_ONLY GLOBAL_VAR(opt_physical_backfill), NO_CMD_LINE, DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_deprecated_alias Sys_physical_backfill_opt(
    "physical_backfill_opt", Sys_enable_physical_backfill);

static Sys_var_bool Sys_enable_udf_bloomfilter_xxhash(
    "enable_udf_bloomfilter_xxhash", "Whether support xxhash for bloomfilter",
    READ_ONLY GLOBAL_VAR(opt_support_bloomfilter_xxhash), NO_CMD_LINE,
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_deprecated_alias Sys_udf_bloomfilter_xxhash(
    "udf_bloomfilter_xxhash", Sys_enable_udf_bloomfilter_xxhash);

static Sys_var_bool Sys_enable_implicit_row_id(
    "enable_implicit_primary_key",
    "MySQL add the primary key for every tables "
    "that haven't any unique indexes automaticly",
    GLOBAL_VAR(opt_enable_implicit_row_id), CMD_LINE(OPT_ARG), DEFAULT(0));

static Sys_var_deprecated_alias Sys_implicit_row_id(
    "implicit_primary_key", Sys_enable_implicit_row_id);

static Sys_var_bool Sys_enable_show_ipk_info(
    "enable_show_ipk_info", "Show everything about implicit key info",
    SESSION_VAR(opt_enable_show_ipk_info), CMD_LINE(OPT_ARG), DEFAULT(0), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_deprecated_alias Sys_show_ipk_info(
    "show_ipk_info", Sys_enable_show_ipk_info);
