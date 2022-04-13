/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyEngine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyEngine.
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

static char *galaxyengine_version_ptr = NULL;

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

static Sys_var_charptr Sys_galaxyengine_version(
    "galaxyengine_version", "Version of the GalaxyEngine",
    READ_ONLY GLOBAL_VAR(galaxyengine_version_ptr), NO_CMD_LINE, IN_SYSTEM_CHARSET,
    DEFAULT(GALAXYENGINE_VERSION));

/* RDS DEFINED */
