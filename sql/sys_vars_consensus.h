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
#ifndef SYS_VARS_CONSENSUS_INCLUDE
#define SYS_VARS_CONSENSUS_INCLUDE
#include "include/my_inttypes.h"

extern bool opt_old_show_timestamp;
extern ulonglong opt_cluster_id;
extern bool opt_cluster_log_type_instance;
extern ulonglong opt_consensus_log_cache_size;
extern bool opt_consensus_disable_fifo_cache;
extern bool opt_consensus_prefetch_fast_fetch;
extern ulonglong opt_consensus_prefetch_cache_size;
extern ulonglong opt_consensus_prefetch_window_size;
extern ulonglong opt_consensus_prefetch_wakeup_ratio;
extern ulonglong opt_consensus_max_log_size;
extern ulonglong opt_consensus_large_trx_split_size;
extern ulonglong opt_consensus_new_follower_threshold;
extern bool opt_consensus_large_trx;
extern bool opt_consensus_check_large_event;
extern ulonglong opt_consensus_large_event_size_limit;
extern ulonglong opt_consensus_large_event_count_limit;
extern ulonglong opt_consensus_large_event_split_size;
extern uint opt_consensus_send_timeout;
extern uint opt_consensus_learner_timeout;
extern bool opt_consensus_learner_pipelining;
extern uint opt_consensus_configure_change_timeout;
extern uint opt_consensus_election_timeout;
extern uint opt_consensus_io_thread_cnt;
extern uint opt_consensus_worker_thread_cnt;
extern uint opt_consensus_heartbeat_thread_cnt;
extern ulong opt_consensus_max_packet_size;
extern char *opt_consensus_msg_compress_option;
extern ulong opt_consensus_pipelining_timeout;
extern ulong opt_consensus_large_batch_ratio;
extern ulonglong opt_consensus_max_delay_index;
extern ulonglong opt_consensus_min_delay_index;
extern bool opt_consensus_optimistic_heartbeat;
extern ulong opt_consensus_sync_follower_meta_interval;
extern bool opt_mts_recover_use_index;
extern ulong opt_consensus_log_level;
extern bool opt_weak_consensus_mode;
extern bool opt_consensus_replicate_with_cache_log;
extern bool opt_consensus_old_compact_mode;
extern bool opt_consensus_leader_stop_apply;
extern ulong opt_consensus_leader_stop_apply_time;
extern ulonglong opt_consensus_force_sync_epoch_diff;
extern bool opt_consensus_force_recovery;
extern bool opt_enable_appliedindex_checker;
extern ulonglong opt_appliedindex_force_delay;
extern char *opt_consensus_flow_control;
extern ulonglong opt_consensus_check_commit_index_interval;
extern bool opt_enable_consensus_leader;
extern ulonglong opt_truncate_consensus_log;
extern bool opt_append_consensus_log;
extern bool opt_reset_consensus_prefetch_cache;
extern ulonglong opt_read_consensus_log;
extern bool opt_consensus_checksum;
extern bool opt_consensus_disable_election;
extern bool opt_consensus_dynamic_easyindex;
extern bool opt_consensus_easy_pool_size;
extern char *opt_cluster_purged_gtid;
extern bool opt_cluster_force_change_meta;
extern bool opt_cluster_learner_node;
extern char *opt_cluster_info;
extern uint64 opt_consensus_start_index;
extern bool opt_cluster_dump_meta;
extern ulonglong opt_cluster_current_term;
extern ulonglong opt_cluster_force_recover_index;
extern bool opt_cluster_force_single_mode;
extern bool opt_commit_pos_watcher;
extern ulonglong opt_commit_pos_watcher_interval;
extern bool opt_consensus_force_promote;
extern bool opt_consensus_auto_reset_match_index;
extern bool opt_consensus_learner_heartbeat;
extern bool opt_consensus_auto_leader_transfer;
extern ulonglong opt_consensus_auto_leader_transfer_check_seconds;
extern bool opt_consensuslog_revise;
extern bool opt_recover_snapshot;
extern ulong thread_stack_warning;
extern ulong opt_configured_event_scheduler;

#endif
