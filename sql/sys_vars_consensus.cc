/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

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

#include "sys_vars_consensus.h"
#include "bl_consensus_log.h"          // ConsensusLogManager and alisql::Paxos
#include "sql/appliedindex_checker.h"  // AppliedIndexChecker
#include "sql/events.h"
#include "sql/log.h"
#include "sql/sys_vars.h"

#include "sql/replica_read_manager.h"

bool opt_old_show_timestamp = 0;
bool opt_enable_consensus_leader = 0;
ulonglong opt_truncate_consensus_log = 0;
bool opt_append_consensus_log = 0;
ulonglong opt_read_consensus_log = 0;
bool opt_reset_consensus_prefetch_cache = 0;
bool opt_consensus_checksum = 0;
bool opt_consensus_disable_election = 0;
bool opt_consensus_dynamic_easyindex = 1;
bool opt_consensus_easy_pool_size = 0;
ulonglong opt_cluster_id;
bool opt_cluster_learner_node;
bool opt_cluster_log_type_instance;
char *opt_cluster_info;
char *opt_cluster_purged_gtid;
ulonglong opt_cluster_current_term;
ulonglong opt_cluster_force_recover_index;
bool opt_cluster_force_change_meta;
bool opt_cluster_dump_meta;
ulonglong opt_consensus_log_cache_size;
bool opt_consensus_disable_fifo_cache;
bool opt_consensus_prefetch_fast_fetch;
ulonglong opt_consensus_prefetch_cache_size;
ulonglong opt_consensus_prefetch_window_size;
ulonglong opt_consensus_prefetch_wakeup_ratio;
ulonglong opt_consensus_max_log_size;
ulonglong opt_consensus_large_trx_split_size;
ulonglong opt_consensus_new_follower_threshold = 10000;
bool opt_consensus_large_trx;
bool opt_consensus_check_large_event;
ulonglong opt_consensus_large_event_size_limit;
ulonglong opt_consensus_large_event_count_limit;
ulonglong opt_consensus_large_event_split_size;
uint opt_consensus_send_timeout;
uint opt_consensus_learner_timeout;
bool opt_consensus_learner_pipelining = 0;
uint opt_consensus_configure_change_timeout = 60 * 1000;
uint opt_consensus_election_timeout;
uint opt_consensus_io_thread_cnt;
uint opt_consensus_worker_thread_cnt;
uint opt_consensus_heartbeat_thread_cnt;
ulong opt_consensus_max_packet_size;
char *opt_consensus_msg_compress_option;
ulong opt_consensus_pipelining_timeout = 1;
ulong opt_consensus_large_batch_ratio = 50;
ulonglong opt_consensus_max_delay_index;
ulonglong opt_consensus_min_delay_index;
bool opt_consensus_optimistic_heartbeat;
ulong opt_consensus_sync_follower_meta_interval = 1;
ulong opt_consensus_log_level;
uint64 opt_consensus_start_index;
bool opt_mts_recover_use_index;
bool opt_cluster_force_single_mode;
bool opt_weak_consensus_mode;
bool opt_consensus_replicate_with_cache_log;
bool opt_consensus_old_compact_mode;
bool opt_consensus_leader_stop_apply;
ulong opt_consensus_leader_stop_apply_time;
ulonglong opt_consensus_force_sync_epoch_diff = 0;
bool opt_consensus_force_recovery;
bool opt_enable_appliedindex_checker;
ulonglong opt_appliedindex_force_delay;
char *opt_consensus_flow_control = NULL;
ulonglong opt_consensus_check_commit_index_interval = 0;
bool opt_commit_pos_watcher = false;
ulonglong opt_commit_pos_watcher_interval = 0;
bool opt_consensus_force_promote = 0;
bool opt_consensus_auto_reset_match_index = 1;
bool opt_consensus_learner_heartbeat;
bool opt_consensus_auto_leader_transfer;
ulonglong opt_consensus_auto_leader_transfer_check_seconds;
bool opt_consensuslog_revise;
bool opt_recover_snapshot = false;
ulong thread_stack_warning = 65536;
ulong opt_configured_event_scheduler = Events::EVENTS_OFF;

static bool fix_consensus_checksum(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setChecksumMode(opt_consensus_checksum);
  return false;
}

static Sys_var_bool Sys_consensus_checksum(
    "consensus_checksum",
    "Checksum when consensus receive log. Disabled by default.",
    GLOBAL_VAR(opt_consensus_checksum), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_checksum));

static bool fix_consensus_disable_election(sys_var *, THD *, enum_var_type) {
  if (!consensus_ptr->getConsensusAsync() && opt_consensus_disable_election)
    xp::warn(ER_XP_0) << "Disable election while cluster is not in weak mode.";
  consensus_ptr->debugDisableElection = opt_consensus_disable_election;
  consensus_ptr->debugDisableStepDown = opt_consensus_disable_election;
  return false;
}

static Sys_var_bool Sys_consensus_disable_election(
    "consensus_disable_election",
    "Disable consensus election and stepdown. Disabled by default.",
    GLOBAL_VAR(opt_consensus_disable_election), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_disable_election));

static bool fix_consensus_dynamic_easyindex(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setEnableDynamicEasyIndex(opt_consensus_dynamic_easyindex);
  return false;
}

static Sys_var_bool Sys_consensus_dynamic_easyindex(
    "consensus_dynamic_easyindex",
    "Enable dynamic easy addr cidx. Enabled by default.",
    GLOBAL_VAR(opt_consensus_dynamic_easyindex), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_dynamic_easyindex));

static bool handle_weak_consensus_mode(sys_var *, THD *, enum_var_type) {
  replica_exec_mode_options = opt_weak_consensus_mode ? 2 : 0;
  consensus_ptr->setConsensusAsync(opt_weak_consensus_mode);
  return false;
}

static Sys_var_bool Sys_weak_consensus_mode(
    "weak_consensus_mode", "set server to weak consensus mode",
    GLOBAL_VAR(opt_weak_consensus_mode), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(handle_weak_consensus_mode));

static bool handle_consensus_replicate_with_cache_log(sys_var *, THD *,
                                                      enum_var_type) {
  consensus_ptr->setReplicateWithCacheLog(
      opt_consensus_replicate_with_cache_log);
  return false;
}

static Sys_var_bool Sys_consensus_replicate_with_cache_log(
    "consensus_replicate_with_cache_log",
    "set server to replicate with cache log",
    GLOBAL_VAR(opt_consensus_replicate_with_cache_log), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(handle_consensus_replicate_with_cache_log));

static bool handle_consensus_old_compact_mode(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setCompactOldMode(opt_consensus_old_compact_mode);
  return false;
}

static Sys_var_bool Sys_consensus_old_compact_mode(
    "consensus_old_compact_mode",
    "set server to old compact mode to use to be compactible with version "
    "before 1.2.0.2",
    GLOBAL_VAR(opt_consensus_old_compact_mode), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(handle_consensus_old_compact_mode));

static Sys_var_bool Sys_consensus_leader_stop_apply(
    "consensus_leader_stop_apply", "leader stop apply",
    GLOBAL_VAR(opt_consensus_leader_stop_apply), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static Sys_var_ulong Sys_consensus_leader_stop_apply_time(
    "consensus_leader_stop_apply_time", "leader stop apply time",
    GLOBAL_VAR(opt_consensus_leader_stop_apply_time), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, UINT_MAX), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));

static bool fix_consensus_force_sync_epoch_diff(sys_var *, THD *,
                                                enum_var_type) {
  consensus_ptr->setForceSyncEpochDiff(opt_consensus_force_sync_epoch_diff);
  return false;
}

static Sys_var_ulonglong Sys_consensus_force_sync_epoch_diff(
    "consensus_force_sync_epoch_diff", "consensus forceSync epoch diff",
    GLOBAL_VAR(opt_consensus_force_sync_epoch_diff), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 10000000), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_force_sync_epoch_diff));

static Sys_var_bool Sys_consensus_force_recovery(
    "consensus_force_recovery", "for innodb_force_recovery",
    READ_ONLY GLOBAL_VAR(opt_consensus_force_recovery), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(0));
static Sys_var_bool Sys_force_revise("force_revise", "force revise or not",
                                     SESSION_VAR(opt_force_revise),
                                     CMD_LINE(OPT_ARG), DEFAULT(false),
                                     NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_ulonglong Sys_cluster_id("cluster_id", "cluster id",
                                        READ_ONLY GLOBAL_VAR(opt_cluster_id),
                                        CMD_LINE(REQUIRED_ARG),
                                        VALID_RANGE(0, ULLONG_MAX), DEFAULT(0),
                                        BLOCK_SIZE(1));

static bool fix_consensus_log_cache_size(sys_var *, THD *, enum_var_type) {
  consensus_log_manager.get_fifo_cache_manager()->set_max_log_cache_size(
      opt_consensus_log_cache_size);
  return false;
}

static Sys_var_ulonglong Sys_consensus_log_cache_size(
    "consensus_log_cache_size", "Max cached logs size",
    GLOBAL_VAR(opt_consensus_log_cache_size), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, ULLONG_MAX), DEFAULT(64 * 1024 * 1024), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_log_cache_size));

static Sys_var_bool Sys_consensus_disable_fifo_cache(
    "consensus_disable_fifo_cache",
    "disable consensus fifo cache (run with weak consensus mode)",
    READ_ONLY GLOBAL_VAR(opt_consensus_disable_fifo_cache), CMD_LINE(OPT_ARG),
    DEFAULT(false));

static Sys_var_bool Sys_consensuslog_revise(
    "consensuslog_revise", "revise consensuslog end_pos before flush to disk",
    GLOBAL_VAR(opt_consensuslog_revise), CMD_LINE(OPT_ARG), DEFAULT(true));

static Sys_var_bool Sys_consensus_prefetch_fast_fetch(
    "consensus_prefetch_fast_fetch", "prefetch speed optimize",
    GLOBAL_VAR(opt_consensus_prefetch_fast_fetch), CMD_LINE(OPT_ARG),
    DEFAULT(false));

static bool fix_consensus_prefetch_cache_size(sys_var *, THD *, enum_var_type) {
  consensus_log_manager.get_prefetch_manager()->set_max_prefetch_cache_size(
      opt_consensus_prefetch_cache_size);
  return false;
}

/*
 * opt_consensus_prefetch_cache_size >= 3 * opt_consensus_max_log_size
 * make sure at least 3 log entries can be stored in cache
 */
static bool check_consensus_prefetch_cache_size(sys_var *, THD *,
                                                set_var *var) {
  ulonglong val = var->save_result.ulonglong_value;
  if (val < opt_consensus_max_log_size * 3) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0), "consensus_prefetch_cache_size",
             val, "need >= consensus_max_log_size * 3");
    return true;
  }
  return false;
}

static Sys_var_ulonglong Sys_consensus_prefetch_cache_size(
    "consensus_prefetch_cache_size", "Max cached logs size",
    GLOBAL_VAR(opt_consensus_prefetch_cache_size), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, ULLONG_MAX), DEFAULT(64 * 1024 * 1024), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_consensus_prefetch_cache_size),
    ON_UPDATE(fix_consensus_prefetch_cache_size));

static bool fix_consensus_prefetch_window_size(sys_var *, THD *,
                                               enum_var_type) {
  consensus_log_manager.get_prefetch_manager()->set_prefetch_window_size(
      opt_consensus_prefetch_window_size);
  return false;
}

static Sys_var_ulonglong Sys_consensus_prefetch_window_size(
    "consensus_prefetch_window_size", "prefetch window size",
    GLOBAL_VAR(opt_consensus_prefetch_window_size), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, ULLONG_MAX), DEFAULT(10), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_prefetch_window_size));

static bool fix_consensus_prefetch_wakeup_ratio(sys_var *, THD *,
                                                enum_var_type) {
  consensus_log_manager.get_prefetch_manager()->set_prefetch_wakeup_ratio(
      opt_consensus_prefetch_wakeup_ratio);
  return false;
}

static Sys_var_ulonglong Sys_consensus_prefetch_wakeup_ratio(
    "consensus_prefetch_wakeup_ratio", "prefetch wakeup ratio ",
    GLOBAL_VAR(opt_consensus_prefetch_wakeup_ratio), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, ULLONG_MAX), DEFAULT(2), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_prefetch_wakeup_ratio));

/*
 * opt_consensus_prefetch_cache_size >= 3 * opt_consensus_max_log_size
 * make sure at least 3 log entries can be stored in cache
 */
static bool check_consensus_max_log_size(sys_var *, THD *, set_var *var) {
  ulonglong val = var->save_result.ulonglong_value;
  if ((val * 3) > opt_consensus_prefetch_cache_size) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0), "consensus_max_log_size", val,
             "need <= consensus_prefetch_cache_size / 3");
    return true;
  }

  if (val < opt_consensus_large_trx_split_size) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0), "consensus_max_log_size", val,
             "need >= consensus_large_trx_split_size");
    return true;
  }

  if (val < opt_consensus_large_event_split_size) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0), "consensus_max_log_size", val,
             "need >= consensus_large_event_split_size");
    return true;
  }
  return false;
}

static Sys_var_ulonglong Sys_consensus_max_log_size(
    "consensus_max_log_size", "Max one log size. (default: 20M)",
    GLOBAL_VAR(opt_consensus_max_log_size), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 1024 * 1024 * 1024), DEFAULT(20 * 1024 * 1024),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_consensus_max_log_size), ON_UPDATE(NULL));

static bool check_consensus_large_trx_split_size(sys_var *, THD *,
                                                 set_var *var) {
  ulonglong val = var->save_result.ulonglong_value;
  if (val > opt_consensus_max_log_size) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0), "consensus_large_trx_split_size",
             val, "need <= consensus_max_log_size");
    return true;
  }
  return false;
}

static Sys_var_ulonglong Sys_consensus_large_trx_split_size(
    "consensus_large_trx_split_size",
    "Max size to split large trx into multi consensus logs. (default 2M)",
    GLOBAL_VAR(opt_consensus_large_trx_split_size), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1, 62 * 1024 * 1024), DEFAULT(2 * 1024 * 1024), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_consensus_large_trx_split_size), ON_UPDATE(NULL));

static bool fix_consensus_new_follower_threshold(sys_var *, THD *,
                                                 enum_var_type) {
  consensus_ptr->setMaxDelayIndex4NewMember(
      opt_consensus_new_follower_threshold);
  return false;
}

static Sys_var_ulonglong Sys_consensus_new_follower_threshold(
    "consensus_new_follower_threshold",
    "Max delay index to allow a learner becomes a follower",
    GLOBAL_VAR(opt_consensus_new_follower_threshold), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, ULLONG_MAX), DEFAULT(10000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_new_follower_threshold));

static Sys_var_bool Sys_consensus_large_trx(
    "consensus_large_trx", "support consensus large trx or not",
    GLOBAL_VAR(opt_consensus_large_trx), CMD_LINE(OPT_ARG), DEFAULT(true),
    NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_bool Sys_consensus_check_large_event(
    "consensus_check_large_event", "check consensus large event or not",
    GLOBAL_VAR(opt_consensus_check_large_event), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_ulonglong Sys_consensus_large_event_size_limit(
    "consensus_large_event_size_limit", "Consensus large event size limit",
    GLOBAL_VAR(opt_consensus_large_event_size_limit), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1 * 1024 * 1024, ULLONG_MAX), DEFAULT(1024 * 1024 * 1024),
    BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_ulonglong Sys_consensus_large_event_count_limit(
    "consensus_large_event_count_limit",
    "Consensus large event count limit in one trx",
    READ_ONLY GLOBAL_VAR(opt_consensus_large_event_count_limit),
    CMD_LINE(OPT_ARG), VALID_RANGE(1, ULLONG_MAX), DEFAULT(2), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_deprecated_alias Sys_consensus_large_event_limit(
    "consensus_large_event_limit", Sys_consensus_large_event_size_limit);

static bool check_consensus_large_event_split_size(sys_var *, THD *,
                                                   set_var *var) {
  ulonglong val = var->save_result.ulonglong_value;
  if (val > opt_consensus_max_log_size) {
    my_error(ER_CONSENSUS_CONFIG_BAD, MYF(0),
             "consensus_large_event_split_size", val,
             "need <= consensus_max_log_size");
    return true;
  }
  return false;
}

static Sys_var_ulonglong Sys_consensus_large_event_split_size(
    "consensus_large_event_split_size",
    "split size for large event, dangerous to change this variable",
    READ_ONLY GLOBAL_VAR(opt_consensus_large_event_split_size),
    CMD_LINE(OPT_ARG), VALID_RANGE(1, 20 * 1024 * 1024),
    DEFAULT(2 * 1024 * 1024), BLOCK_SIZE(1), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_consensus_large_event_split_size), ON_UPDATE(NULL));

static bool fix_consensus_send_timeout(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setSendPacketTimeout(opt_consensus_send_timeout);
  return false;
}

static Sys_var_uint Sys_consensus_send_timeout(
    "consensus_send_timeout", "Consensus send packet timeout",
    GLOBAL_VAR(opt_consensus_send_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 200000), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_send_timeout));

static bool fix_consensus_learner_timeout(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setLearnerConnTimeout(opt_consensus_learner_timeout);
  return false;
}

static Sys_var_uint Sys_consensus_learner_timeout(
    "consensus_learner_timeout", "Consensus learner connection timeout",
    GLOBAL_VAR(opt_consensus_learner_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 200000), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_learner_timeout));

static bool fix_consensus_learner_pipelining(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setEnableLearnerPipelining(opt_consensus_learner_pipelining);
  return false;
}

static Sys_var_bool Sys_consensus_learner_pipelining(
    "consensus_learner_pipelining", "enable pipelining send msg to learner",
    GLOBAL_VAR(opt_consensus_learner_pipelining), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_learner_pipelining));

static bool fix_consensus_configure_change_timeout(sys_var *, THD *,
                                                   enum_var_type) {
  consensus_ptr->setConfigureChangeTimeout(
      opt_consensus_configure_change_timeout);
  return false;
}

static Sys_var_uint Sys_consensus_configure_change_timeout(
    "consensus_configure_change_timeout",
    "Consensus configure change timeout (ms). Default 1 min",
    GLOBAL_VAR(opt_consensus_configure_change_timeout), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(0, 200000), DEFAULT(60 * 1000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_configure_change_timeout));

static Sys_var_uint Sys_consensus_election_timeout(
    "consensus_election_timeout", "Consensus election timeout",
    READ_ONLY GLOBAL_VAR(opt_consensus_election_timeout),
    CMD_LINE(REQUIRED_ARG), VALID_RANGE(2000, 200000), DEFAULT(5000),
    BLOCK_SIZE(1));

static Sys_var_uint Sys_consensus_io_thread_count(
    "consensus_io_thread_cnt", "Number of consensus io thread",
    READ_ONLY GLOBAL_VAR(opt_consensus_io_thread_cnt), CMD_LINE(OPT_ARG),
    VALID_RANGE(2, 10), DEFAULT(3), BLOCK_SIZE(1));

static Sys_var_uint Sys_consensus_worker_thread_count(
    "consensus_worker_thread_cnt", "Number of consensus worker thread",
    READ_ONLY GLOBAL_VAR(opt_consensus_worker_thread_cnt), CMD_LINE(OPT_ARG),
    VALID_RANGE(2, 10), DEFAULT(3), BLOCK_SIZE(1));

static Sys_var_uint Sys_consensus_heartbeat_thread_count(
    "consensus_heartbeat_thread_cnt", "Number of consensus heartbeat thread",
    READ_ONLY GLOBAL_VAR(opt_consensus_heartbeat_thread_cnt), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 2), DEFAULT(0), BLOCK_SIZE(1));

static bool fix_consensus_max_packet_size(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setMaxPacketSize(opt_consensus_max_packet_size);
  return false;
}

static Sys_var_ulong Sys_consensus_max_packet_size(
    "consensus_max_packet_size",
    "Max package size the consensus server send at once",
    GLOBAL_VAR(opt_consensus_max_packet_size), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 1024 * 1024L * 1024L), DEFAULT(128 * 1024), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_max_packet_size));

static bool fix_consensus_msg_compress_option(sys_var *, THD *, enum_var_type) {
  if (NULL == opt_consensus_msg_compress_option) return false;
  // format: ip1:port1 type threshold checksum; ip2:port2 type threshold
  // checksum...
  std::size_t current, previous = 0;
  std::string fcstr(opt_consensus_msg_compress_option);
  std::vector<std::string> splits;
  current = fcstr.find(';');
  while (current != std::string::npos) {
    splits.push_back(fcstr.substr(previous, current - previous));
    previous = current + 1;
    current = fcstr.find(';', previous);
  }
  splits.push_back(fcstr.substr(previous, current - previous));
  consensus_ptr->resetMsgCompressOption();
  for (auto &kv : splits) {
    if (kv.empty()) continue;
    char addr[256];
    uint32 type, threshold, checksum;
    if (std::sscanf(kv.c_str(), "%s %u %u %u", addr, &type, &threshold,
                    &checksum) == 4) {
      std::string address(addr);
      if (address == "*:*") address = "";
      if (consensus_ptr->setMsgCompressOption(type, threshold, (bool)checksum,
                                              address) == 0)
        xp::warn(ER_XP_0) << "Set consensus server "
                          << (address == "" ? "all" : address.c_str())
                          << " msg compress option succeed: type(" << type
                          << "), threshold(" << threshold << "), checksum("
                          << checksum << ")";
      else
        xp::warn(ER_XP_0) << "Set consensus server "
                          << (address == "" ? "all" : address.c_str())
                          << "msg compress option failed, wrong address!";
    }
  }
  return false;
}

static bool check_consensus_msg_compress_option(sys_var *, THD *,
                                                set_var *var) {
  if (NULL == var->save_result.string_value.str) return false;
  // format: ip1:port1 type threshold checksum; ip2:port2 type threshold
  // checksum...
  std::size_t current, previous = 0;
  std::string fcstr(var->save_result.string_value.str);
  std::vector<std::string> splits;
  current = fcstr.find(';');
  while (current != std::string::npos) {
    splits.push_back(fcstr.substr(previous, current - previous));
    previous = current + 1;
    current = fcstr.find(';', previous);
  }
  splits.push_back(fcstr.substr(previous, current - previous));
  for (auto &kv : splits) {
    if (kv.empty()) continue;
    char addr[256];
    uint32 type, threshold, checksum;
    if (std::sscanf(kv.c_str(), "%s %u %u %u", addr, &type, &threshold,
                    &checksum) != 4) {
      xp::warn(ER_XP_0)
          << "Set consensus server msg compress option failed, wrong syntax!";
      return true;
    }
  }
  return false;
}

static Sys_var_charptr Sys_consensus_msg_compress_option(
    "consensus_msg_compress_option",
    "consensus msg compress option to one or multiple addresses"
    "ip:port type(0: no compression, 1: lz4 compression, 2: zstd compression) threshold checksum, \
        if ip:port is *:* it means set option to entire cluster.",
    GLOBAL_VAR(opt_consensus_msg_compress_option), CMD_LINE(OPT_ARG),
    IN_FS_CHARSET, DEFAULT(0), NO_MUTEX_GUARD, NOT_IN_BINLOG,
    ON_CHECK(check_consensus_msg_compress_option),
    ON_UPDATE(fix_consensus_msg_compress_option));

static bool fix_consensus_pipelining_timeout(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setPipeliningTimeout(opt_consensus_pipelining_timeout);
  return false;
}

static Sys_var_ulong Sys_consensus_pipelining_timeout(
    "consensus_pipelining_timeout",
    "the timeout the consensus server cache the log (milliseconds)",
    GLOBAL_VAR(opt_consensus_pipelining_timeout), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, 1024 * 1024L * 1024L), DEFAULT(1), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_pipelining_timeout));

static bool fix_consensus_large_batch_ratio(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setLargeBatchRatio(opt_consensus_large_batch_ratio);
  return false;
}

static Sys_var_ulong Sys_consensus_large_batch_ratio(
    "consensus_large_batch_ratio",
    "Large batch ratio of consensus server send at once",
    GLOBAL_VAR(opt_consensus_large_batch_ratio), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 1024), DEFAULT(50), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_large_batch_ratio));

static bool fix_consensus_max_delay_index(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setMaxDelayIndex(opt_consensus_max_delay_index);
  return false;
}

static Sys_var_ulonglong Sys_consensus_max_delay_index(
    "consensus_max_delay_index", "Max index delay for pipeline log delivery",
    GLOBAL_VAR(opt_consensus_max_delay_index), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, INT_MAX64), DEFAULT(50000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_max_delay_index));

static bool fix_consensus_min_delay_index(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setMinDelayIndex(opt_consensus_min_delay_index);
  return false;
}

static Sys_var_ulonglong Sys_consensus_min_delay_index(
    "consensus_min_delay_index", "Min index delay for pipeline log delivery",
    GLOBAL_VAR(opt_consensus_min_delay_index), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, INT_MAX64), DEFAULT(5000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_min_delay_index));

static bool fix_consensus_optimistic_heartbeat(sys_var *, THD *,
                                               enum_var_type) {
  consensus_ptr->setOptimisticHeartbeat(opt_consensus_optimistic_heartbeat);
  return false;
}

static Sys_var_bool Sys_consensus_optimistic_heartbeat(
    "consensus_optimistic_heartbeat",
    "whether to use optimistic heartbeat in consensus layer",
    GLOBAL_VAR(opt_consensus_optimistic_heartbeat), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_optimistic_heartbeat));

static bool fix_consensus_sync_follower_meta_interval(sys_var *, THD *,
                                                      enum_var_type) {
  consensus_ptr->setSyncFollowerMetaInterval(
      opt_consensus_sync_follower_meta_interval);
  return false;
}

static Sys_var_ulong Sys_consensus_sync_follower_meta_interval(
    "consensus_sync_follower_meta_interva",
    "Interval of leader sync follower's meta for learner source",
    GLOBAL_VAR(opt_consensus_sync_follower_meta_interval), CMD_LINE(OPT_ARG),
    VALID_RANGE(1, 1024 * 1024L * 1024L), DEFAULT(1), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_sync_follower_meta_interval));

static bool fix_enable_appliedindex_checker(sys_var *, THD *, enum_var_type) {
  appliedindex_checker.reset();
  return false;
}

static Sys_var_bool Sys_enable_appliedindex_checker(
    "enable_appliedindex_checker",
    "enable applied index checker during ordered_commit",
    GLOBAL_VAR(opt_enable_appliedindex_checker), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_enable_appliedindex_checker));

static Sys_var_ulonglong Sys_appliedindex_force_delay(
    "appliedindex_force_delay", "force set a smaller appliedindex",
    GLOBAL_VAR(opt_appliedindex_force_delay), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, UINT64_MAX), DEFAULT(0), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG);

static bool fix_consensus_flow_control(sys_var *, THD *, enum_var_type) {
  if (NULL == opt_consensus_flow_control) return false;
  // format: ip1:port1 fc1;ip2:port2 fc2...
  std::size_t current, previous = 0;
  std::string fcstr(opt_consensus_flow_control);
  std::vector<std::string> splits;
  current = fcstr.find(';');
  while (current != std::string::npos) {
    splits.push_back(fcstr.substr(previous, current - previous));
    previous = current + 1;
    current = fcstr.find(';', previous);
  }
  splits.push_back(fcstr.substr(previous, current - previous));
  if (splits.size() > 0) consensus_ptr->reset_flow_control();
  for (auto &kv : splits) {
    char addr[300];
    uint64 serverid;
    int64 fc;
    if (std::sscanf(kv.c_str(), "%s %ld", addr, &fc) == 2) {
      serverid = consensus_ptr->getServerIdFromAddr(addr);
      xp::warn(ER_XP_0) << "Add consensus server " << serverid
                        << " flow control " << fc;
      consensus_ptr->set_flow_control(serverid, fc);
    }
  }
  return false;
}

static Sys_var_charptr Sys_consensus_flow_control(
    "consensus_flow_control",
    "consensus flow control "
    "(<-1: no log send, -1: only send log during heartbeat, 0: no flow "
    "control, >0: [TODO] flow control).",
    GLOBAL_VAR(opt_consensus_flow_control), CMD_LINE(OPT_ARG), IN_FS_CHARSET,
    DEFAULT(0), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_flow_control));

static bool fix_consensus_log_level(sys_var *, THD *, enum_var_type) {
  // opt_consensus_log_level + 3 equal to easy log level
  consensus_ptr->setAlertLogLevel(
      alisql::Paxos::AlertLogLevel(opt_consensus_log_level + 3));
  return false;
}

const char *internal_tmp_consensus_log_level_names[] = {
    "LOG_ERROR", "LOG_WARN", "LOG_INFO", "LOG_DEBUG", "LOG_TRACE", 0};
static Sys_var_enum Sys_consensus_log_level(
    "consensus_log_level", "consensus log level",
    GLOBAL_VAR(opt_consensus_log_level), CMD_LINE(OPT_ARG),
    internal_tmp_consensus_log_level_names, DEFAULT(0), NO_MUTEX_GUARD,
    NOT_IN_BINLOG, ON_CHECK(0), ON_UPDATE(fix_consensus_log_level));

static Sys_var_ulonglong Sys_consensus_check_commit_index_interval(
    "consensus_check_commit_index_interval",
    "check interval for slave calling checkCommitIndex",
    GLOBAL_VAR(opt_consensus_check_commit_index_interval), CMD_LINE(OPT_ARG),
    VALID_RANGE(0, INT_MAX64), DEFAULT(1000), BLOCK_SIZE(1), NO_MUTEX_GUARD,
    NOT_IN_BINLOG);

static bool handler_reset_consensus_prefetch_cache(sys_var *, THD *,
                                                   enum_var_type) {
  DBUG_ENTER("handle_reset_consensus_prefetch_cache");
  consensus_log_manager.get_prefetch_manager()->reset_prefetch_cache();
  opt_reset_consensus_prefetch_cache = 0;
  DBUG_RETURN(false);
}

Sys_var_bool Sys_reset_consensus_prefetch_cache(
    "reset_consensus_prefetch_cache", "reset consensus prefetch_cache",
    GLOBAL_VAR(opt_reset_consensus_prefetch_cache), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG, NULL,
    ON_UPDATE(handler_reset_consensus_prefetch_cache));

static Sys_var_bool Sys_commit_pos_watcher(
    "commit_pos_watcher",
    "background thread checking and updating binlog commit position",
    READ_ONLY GLOBAL_VAR(opt_commit_pos_watcher), CMD_LINE(OPT_ARG),
    DEFAULT(true));

static Sys_var_ulonglong Sys_commit_pos_watcher_interval(
    "commit_pos_watcher_interval",
    "interval(us) for background thread commit_pos_watcher",
    GLOBAL_VAR(opt_commit_pos_watcher_interval), CMD_LINE(OPT_ARG),
    VALID_RANGE(100000, ULLONG_MAX), DEFAULT(1000000), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG);

static Sys_var_ulong Sys_thread_stack_warning(
    "thread_stack_warning", "The warning stack size for each thread",
    GLOBAL_VAR(thread_stack_warning), CMD_LINE(REQUIRED_ARG),
    VALID_RANGE(1 * 1024, ULONG_MAX), DEFAULT(DEFAULT_THREAD_STACK),
    BLOCK_SIZE(1024));

static bool update_session_track_index(sys_var *, THD *thd, enum_var_type) {
  DBUG_ENTER("update_session_track_index");
  DBUG_RETURN(
      thd->session_tracker.get_tracker(SESSION_INDEX_TRACKER)->update(thd));
}

static Sys_var_bool Sys_session_track_index(
    "session_track_index", "Track current index for non-select request.",
    SESSION_VAR(session_track_index), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(update_session_track_index));

static bool handle_consensus_force_promote(sys_var *, THD *, enum_var_type) {
  DBUG_ENTER("handle_reset_consensus_prefetch_cache");
  if (opt_consensus_force_promote) consensus_ptr->forcePromote();
  opt_consensus_force_promote = 0;
  DBUG_RETURN(false);
}

static Sys_var_bool Sys_consensus_force_promote(
    "consensus_force_promote", "Try to force promote a follower to leader",
    GLOBAL_VAR(opt_consensus_force_promote), CMD_LINE(OPT_ARG), DEFAULT(false),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, NULL,
    ON_UPDATE(handle_consensus_force_promote));

static bool fix_consensus_auto_reset_match_index(sys_var *, THD *,
                                                 enum_var_type) {
  consensus_ptr->setEnableAutoResetMatchIndex(
      opt_consensus_auto_reset_match_index);
  return false;
}

static Sys_var_bool Sys_consensus_auto_reset_match_index(
    "consensus_auto_reset_match_index",
    "enable auto reset match index when consensus follower has fewer logs",
    GLOBAL_VAR(opt_consensus_auto_reset_match_index), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_auto_reset_match_index));

static bool fix_consensus_learner_heartbeat(sys_var *, THD *, enum_var_type) {
  consensus_ptr->setEnableLearnerHeartbeat(opt_consensus_learner_heartbeat);
  return false;
}

static Sys_var_bool Sys_consensus_learner_heartbeat(
    "consensus_learner_heartbeat", "enable send heartbeat to learner",
    GLOBAL_VAR(opt_consensus_learner_heartbeat), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_learner_heartbeat));

static bool fix_consensus_auto_leader_transfer(sys_var *, THD *,
                                               enum_var_type) {
  consensus_ptr->setEnableAutoLeaderTransfer(
      opt_consensus_auto_leader_transfer);
  return false;
}

static Sys_var_bool Sys_consensus_auto_leader_transfer(
    "consensus_auto_leader_transfer",
    "whether to enable auto leader transfer in consensus layer",
    GLOBAL_VAR(opt_consensus_auto_leader_transfer), CMD_LINE(OPT_ARG),
    DEFAULT(true), NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_auto_leader_transfer));

static bool fix_consensus_auto_leader_transfer_check_seconds(sys_var *, THD *,
                                                             enum_var_type) {
  consensus_ptr->setAutoLeaderTransferCheckSeconds(
      opt_consensus_auto_leader_transfer_check_seconds);
  return false;
}

static Sys_var_ulonglong Sys_consensus_auto_leader_transfer_check_seconds(
    "consensus_auto_leader_transfer_check_seconds",
    "the interval between a leader check its health for a transfer",
    GLOBAL_VAR(opt_consensus_auto_leader_transfer_check_seconds),
    CMD_LINE(OPT_ARG), VALID_RANGE(10, 300), DEFAULT(60), BLOCK_SIZE(1),
    NO_MUTEX_GUARD, NOT_IN_BINLOG, ON_CHECK(0),
    ON_UPDATE(fix_consensus_auto_leader_transfer_check_seconds));

static Sys_var_bool Sys_consensus_safe_for_reset_master(
    "consensus_safe_for_reset_master",
    "insert Consensus_empty event into binblog after reset master",
    SESSION_VAR(opt_consensus_safe_for_reset_master), CMD_LINE(OPT_ARG),
    DEFAULT(false), NO_MUTEX_GUARD, NOT_IN_BINLOG);
