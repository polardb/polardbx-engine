/* Copyright (c) 2009, 2018, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/sys_vars.h"
#include "my_config.h"
#include "sql/ccl/ccl.h"
#include "sql/ccl/ccl_bucket.h"
#include "sql/ccl/ccl_interface.h"

static Sys_var_ulong Sys_ccl_wait_timeout(
    "ccl_wait_timeout", "Timeout in seconds to wait when concurrency control.",
    GLOBAL_VAR(im::ccl_wait_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, LONG_TIMEOUT), DEFAULT(CCL_LONG_WAIT), BLOCK_SIZE(1));

static Sys_var_ulong Sys_ccl_max_waiting(
    "ccl_max_waiting_count", "max waiting count in one ccl rule or bucket",
    GLOBAL_VAR(im::ccl_max_waiting_count), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, INT_MAX64),
    DEFAULT(CCL_DEFAULT_WAITING_COUNT), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

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

/* RDS DEFINED */
