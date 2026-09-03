/*****************************************************************************
Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/** @file sql/sys_vars_deprecated.cc

 System variables was deprecated.

 Created 2025-05-26 by komodo
 *******************************************************/

namespace komodo {

/** Large event and transaction limit was deprecated. */
bool opt_consensus_check_large_event;
static Sys_var_bool Sys_consensus_check_large_event(
    "consensus_check_large_event", "check consensus large event or not",
    GLOBAL_VAR(opt_consensus_check_large_event), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

ulonglong opt_consensus_large_event_size_limit;
static Sys_var_ulonglong Sys_consensus_large_event_size_limit(
    "consensus_large_event_size_limit", "Consensus large event size limit",
    GLOBAL_VAR(opt_consensus_large_event_size_limit), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1 * 1024 * 1024, ULLONG_MAX), DEFAULT(1024 * 1024 * 1024),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

static Sys_var_deprecated_alias Sys_consensus_large_event_limit(
    "consensus_large_event_limit", Sys_consensus_large_event_size_limit);

bool opt_consensus_large_trx;
static Sys_var_bool Sys_consensus_large_trx(
    "consensus_large_trx", "support consensus large trx or not",
    GLOBAL_VAR(opt_consensus_large_trx), CMD_LINE(OPT_ARG), DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

ulonglong opt_consensus_large_event_count_limit;
static Sys_var_ulonglong Sys_consensus_large_event_count_limit(
    "consensus_large_event_count_limit",
    "Consensus large event count limit in one trx",
    READ_ONLY GLOBAL_VAR(opt_consensus_large_event_count_limit),
    CMD_LINE(OPT_ARG), VALID_RANGE(1, ULLONG_MAX), DEFAULT(2), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

bool opt_consensus_disable_fifo_cache;
static Sys_var_bool Sys_consensus_disable_fifo_cache(
    "consensus_disable_fifo_cache",
    "disable consensus fifo cache (run with weak consensus mode)",
    READ_ONLY GLOBAL_VAR(opt_consensus_disable_fifo_cache), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

bool opt_consensus_checksum;
static Sys_var_bool Sys_consensus_checksum(
    "consensus_checksum", "Checksum when consensus receive log. discard now",
    GLOBAL_VAR(opt_consensus_checksum), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

bool opt_consensus_force_recovery;
static Sys_var_bool Sys_consensus_force_recovery(
    "consensus_force_recovery", "for innodb_force_recovery, discard now",
    READ_ONLY GLOBAL_VAR(opt_consensus_force_recovery), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(update_deprecated));

}  // namespace komodo

static ulonglong opt_consensus_flush_delay_after_replicate = 0;
Sys_var_ulonglong Sys_consensus_flush_delay_after_replicate(
    "consensus_flush_delay_after_replicate",
    "The delay in milliseconds after each bgc replicate log "
    "before flush into file when consensus_replicate_with_cache_log = ON",
    GLOBAL_VAR(opt_consensus_flush_delay_after_replicate), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, LONG_TIMEOUT), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG);

static bool opt_polarx_binlog_writeset_mode = true;
Sys_var_bool Sys_opt_polarx_binlog_writeset_mode(
    "polarx_binlog_writeset_mode", "optimize binlog writeset mode.",
    GLOBAL_VAR(opt_polarx_binlog_writeset_mode), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(nullptr),
    ON_UPDATE(nullptr));

static ulonglong txn_undo_page_reuse_max_percent_empty = 20;
Sys_var_ulonglong Sys_txn_undo_page_reuse_max_percent(
    "innodb_txn_undo_page_reuse_max_percent",
    "The max percent of txn undo page that can be reused",
    GLOBAL_VAR(txn_undo_page_reuse_max_percent_empty), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));
