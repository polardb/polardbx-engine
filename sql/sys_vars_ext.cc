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


/**
  @file
  Definitions of extra RDS server's session or global variables.

  How to add new variables:

  1. copy one of the existing variables, and edit the declaration.
  2. if you need special behavior on assignment or additional checks
     use ON_CHECK and ON_UPDATE callbacks.
  3. *Don't* add new Sys_var classes or uncle Occam will come
     with his razor to haunt you at nights

  Note - all storage engine variables (for example myisam_whatever)
  should go into the corresponding storage engine sources
  (for example in storage/myisam/ha_myisam.cc) !
*/

#include "my_config.h"
#include "sql/ccl/ccl.h"
#include "sql/ccl/ccl_bucket.h"
#include "sql/ccl/ccl_interface.h"
#include "sql/outline/outline_interface.h"
#include "sql/recycle_bin/recycle_scheduler.h"
#include "sql/recycle_bin/recycle_table.h"
#include "sql/sys_vars.h"
#include "sql/ha_sequence.h"
#include "sql/log_table.h"
#include "sql/sys_vars_ext.h"
#include "sql/replica_read_manager.h"

static char *polardbx_engine_version_ptr = NULL;
int32 rpc_port = DEFAULT_RPC_PORT;
bool new_rpc = false;

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

static Sys_var_charptr Sys_client_endpoint_ip(
  "client_endpoint_ip",
  "The endpoint ip that client use to connect.",
  SESSION_VAR(client_endpoint_ip),
  CMD_LINE(REQUIRED_ARG),
  IN_SYSTEM_CHARSET, DEFAULT(0),
  NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

using namespace im;
static Sys_var_ulong Sys_outline_partitions(
    "outline_partitions", "How many partitions of system outline structure.",
    READ_ONLY GLOBAL_VAR(outline_partitions), CMD_LINE(REQUIRED_ARG),
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
/**
  @file
  Definitions of extra RDS server's session or global variables.

  How to add new variables:

  1. copy one of the existing variables, and edit the declaration.
  2. if you need special behavior on assignment or additional checks
     use ON_CHECK and ON_UPDATE callbacks.
  3. *Don't* add new Sys_var classes or uncle Occam will come
     with his razor to haunt you at nights

  Note - all storage engine variables (for example myisam_whatever)
  should go into the corresponding storage engine sources
  (for example in storage/myisam/ha_myisam.cc) !
*/

#include "sql/sql_statistics_common.h"
#include "sql/sys_vars.h"
#include "sql/sys_vars_ext.h"

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

/* Global scope variables */
char innodb_version[SERVER_VERSION_LENGTH];

/* Local scope variables */
static uint rds_version = 0;

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
static Sys_var_uint Sys_rds_version("rds_version", "The mysql patch version",
                                    GLOBAL_VAR(rds_version), CMD_LINE(OPT_ARG),
                                    VALID_RANGE(1, 999),
                                    DEFAULT(30), BLOCK_SIZE(1),
                                    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
                                    ON_UPDATE(fix_server_version));

static Sys_var_bool Sys_opt_tablestat("opt_tablestat",
                                      "When this option is enabled,"
                                      "it will accumulate the table statistics",
                                      GLOBAL_VAR(opt_tablestat),
                                      CMD_LINE(OPT_ARG), DEFAULT(true),
                                      NO_MUTEX_GUARD, NOT_IN_BINLOG,
                                      ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_opt_indexstat("opt_indexstat",
                                      "When this option is enabled,"
                                      "it will accumulate the index statistics",
                                      GLOBAL_VAR(opt_indexstat),
                                      CMD_LINE(OPT_ARG), DEFAULT(true),
                                      NO_MUTEX_GUARD, NOT_IN_BINLOG,
                                      ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_opt_performance_point_enabled(
    "opt_performance_point_enabled",
    "whether open the performance point system plugin",
    READ_ONLY GLOBAL_VAR(opt_performance_point_enabled), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_bool Sys_auto_savepoint("auto_savepoint",
                                       "Whether to make implicit savepoint for "
                                       "each INSERT/DELETE/UPDATE statement",
                                       SESSION_VAR(auto_savepoint), NO_CMD_LINE,
                                       DEFAULT(false), NO_MUTEX_GUARD,
                                       NOT_IN_BINLOG, ON_CHECK(0),
                                       ON_UPDATE(0));

static Sys_var_ulonglong Sys_innodb_snapshot_seq(
    "innodb_snapshot_seq", "Innodb snapshot sequence.",
    HINT_UPDATEABLE SESSION_ONLY(innodb_snapshot_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(MYSQL_GCN_MIN, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL), BLOCK_SIZE(1));

static Sys_var_ulonglong Sys_innodb_commit_seq(
    "innodb_commit_seq", "Innodb commit sequence",
    HINT_UPDATEABLE SESSION_ONLY(innodb_commit_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(MYSQL_GCN_MIN, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL), BLOCK_SIZE(1));

static Sys_var_ulonglong Sys_innodb_prepare_seq(
    "innodb_prepare_seq", "Innodb xa prepare sequence",
    HINT_UPDATEABLE SESSION_ONLY(innodb_prepare_gcn), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1024, MYSQL_GCN_NULL), DEFAULT(MYSQL_GCN_NULL), BLOCK_SIZE(1));

static Sys_var_bool Sys_only_report_warning_when_skip(
    "only_report_warning_when_skip_sequence",
    "Whether reporting warning when the value skipped to is not valid "
    "instead of raising error",
    GLOBAL_VAR(opt_only_report_warning_when_skip_sequence), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern bool opt_gcn_write_event;
static Sys_var_bool Sys_gcn_write_event(
    "gcn_write_event",
    "Writting a gcn event which content is gcn number for every transaction.",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_gcn_write_event),
    CMD_LINE(OPT_ARG), DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));

static Sys_var_charptr Sys_polardbx_engine_version(
    "polardbx_engine_version", "Version of the PolarDB-X Engine",
    READ_ONLY GLOBAL_VAR(polardbx_engine_version_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(GALAXYENGINE_VERSION));

static Sys_var_bool Sys_rotate_log_table(
    "rotate_log_table",
    "Whether rotate the data file when flush slow_log or general_log",
    SESSION_ONLY(rotate_log_table), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_charptr Sys_rotate_log_table_last_name(
    "rotate_log_table_last_name", "Last rotated log table file name",
    READ_ONLY GLOBAL_VAR(im::rotate_log_table_last_name_ptr),
    CMD_LINE(REQUIRED_ARG), IN_FS_CHARSET, DEFAULT(rotate_log_table_last_name),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(NULL), ON_UPDATE(NULL));

static Sys_var_bool Sys_innodb_current_snapshot_gcn(
    "innodb_current_snapshot_seq",
    "Get snapshot_seq from innodb," 
    "the value is current max snapshot sequence and plus one",
    HINT_UPDATEABLE SESSION_ONLY(innodb_current_snapshot_gcn), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

extern ulong opt_recovery_apply_binlog;
static const char *recovery_apply_binlog_type_names[] = {"OFF", "ON", "SAME_AS_GTID", 0};
static Sys_var_enum Sys_recovery_apply_binlog(
    "recovery_apply_binlog",
    "Applying binlog to generate the lost data at server startup. 0: OFF, 1: ON, 2:SAME_AS_GTID",
    READ_ONLY NON_PERSIST GLOBAL_VAR(opt_recovery_apply_binlog), 
    CMD_LINE(OPT_ARG), recovery_apply_binlog_type_names, 
    DEFAULT(2), NO_MUTEX_GUARD, 
    NOT_IN_BINLOG, ON_CHECK(NULL), ON_UPDATE(NULL));

extern uint opt_recovery_apply_binlog_skip_counter;
static Sys_var_uint Sys_recovery_apply_binlog_skip_counter(
    "recovery_apply_binlog_skip_counter", "recovery_apply_binlog_skip_counter",
    GLOBAL_VAR(opt_recovery_apply_binlog_skip_counter), 
    CMD_LINE(OPT_ARG), VALID_RANGE(0, UINT_MAX),
    DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));

extern uint opt_print_gtid_info_during_recovery;
static Sys_var_uint Sys_print_gtid_info_during_recovery(
    "print_gtid_info_during_recovery",
    "0 - dont print; 1 - print basic info; 2 - print detailed info",
    NON_PERSIST GLOBAL_VAR(opt_print_gtid_info_during_recovery),
    CMD_LINE(OPT_ARG), VALID_RANGE(0, UINT_MAX),
    DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(0), ON_UPDATE(0));

static Sys_var_int32 Sys_rpc_port("rpc_port", "RPC port for PolarDB-X",
                                 READ_ONLY GLOBAL_VAR(rpc_port),
                                 CMD_LINE(OPT_ARG), VALID_RANGE(0, 65535),
                                 DEFAULT(DEFAULT_RPC_PORT), BLOCK_SIZE(1),
                                 NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
                                 ON_UPDATE(0));

static Sys_var_bool Sys_new_rpc("new_rpc", "Use new open PolarDB-X RPC",
                                READ_ONLY GLOBAL_VAR(new_rpc),
                                CMD_LINE(OPT_ARG), DEFAULT(false),
                                NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
                                ON_UPDATE(0));

static Sys_var_ulonglong Sys_replica_read_timeout(
      "replica_read_timeout",
      "Maximum wait period (milliseconds) when performing replica consistent reads",
      GLOBAL_VAR(opt_replica_read_timeout),
      CMD_LINE(REQUIRED_ARG),
      VALID_RANGE(1, 3600000),
      DEFAULT(DEFAULT_REPLICA_READ_TIMEOUT),
      BLOCK_SIZE(1),
      NO_MUTEX_GUARD, NOT_IN_BINLOG,
      ON_CHECK(NULL),
      ON_UPDATE(NULL));

static bool check_read_lsn(sys_var *self, THD *thd, set_var *var)
{
	ulonglong read_lsn = var->save_result.ulonglong_value;
	return !replica_read_manager.wait_for_lsn(read_lsn);
}

static Sys_var_ulonglong Sys_read_lsn(
      "read_lsn",
      "Minimun log applied index required for replica consistent reads",
      SESSION_VAR(read_lsn),
      CMD_LINE(REQUIRED_ARG),
      VALID_RANGE(0, LONG_LONG_MAX),
      DEFAULT(0),
      BLOCK_SIZE(1),
      NO_MUTEX_GUARD, NOT_IN_BINLOG,
      ON_CHECK(check_read_lsn),
      ON_UPDATE(NULL));

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
       GLOBAL_VAR(opt_disable_wait_commitindex), CMD_LINE(OPT_ARG),
       DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

/* RDS DEFINED */
