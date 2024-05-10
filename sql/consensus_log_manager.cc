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

#include "consensus_log_manager.h"
#include "bl_consensus_log.h"
#include "consensus_recovery_manager.h"
#include "events.h"
#include "log.h"
#include "log_event.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/thread_pool_priv.h"
#include "rpl_gtid.h"
#include "rpl_msr.h"
#include "rpl_replica.h"
#include "sql/appliedindex_checker.h"
#include "sql/consensus_admin.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql_parse.h"
#include "sys_vars_consensus.h"

#define LOG_PREFIX "ML"

ConsensusLogManager consensus_log_manager;

uint64 show_fifo_cache_size(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size =
      consensus_log_manager.get_fifo_cache_manager()->get_fifo_cache_size();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_first_index_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = consensus_log_manager.get_fifo_cache_manager()
                    ->get_first_index_of_fifo_cache();
  *value = static_cast<long long>(size);
  return 0;
}

uint64 show_log_count_in_fifo_cache(THD *, SHOW_VAR *var, char *buff) {
  var->type = SHOW_LONGLONG;
  var->value = buff;
  long *value = reinterpret_cast<long *>(buff);
  uint64 size = consensus_log_manager.get_fifo_cache_manager()
                    ->get_fifo_cache_log_count();
  *value = static_cast<long long>(size);
  return 0;
}

int show_appliedindex_checker_queue(THD *, SHOW_VAR *var, char *) {
  var->type = SHOW_CHAR;
  var->value =
      const_cast<char *>(appliedindex_checker.get_group_queue_status());
  return 0;
}

void stateChangeCb(alisql::Paxos::StateType state, uint64_t term,
                   uint64_t commitIndex) {
  ConsensusStateChange state_change = {state, term, commitIndex};
  consensus_log_manager.add_state_change_request(state_change);
}

void wait_commit_index_in_recovery() {
  auto mgr = consensus_log_manager.get_recovery_manager();
  uint64 max_consensus_index = 0;

  xp::system(ER_XP_RECOVERY)
      << "wait_commit_index_in_recovery begin"
      << ", current commitIndex " << consensus_ptr->getCommitIndex();

  while (consensus_ptr->getCommitIndex() <
         (max_consensus_index = mgr->get_max_consensus_index_from_pending_recovering_trxs())) {
    my_sleep(500);
  }

  xp::system(ER_XP_RECOVERY)
      << "Found max consensus index from pending_recovering_trxs"
      << ", current commitIndex " << consensus_ptr->getCommitIndex()
      << ", max_consensus_index " << max_consensus_index;

  /*
    If the node was leader when shutdown, it should wait for binlog truncation
    completed to get the corrent binlog end position before applying the lost
    binlog events on recovery.
  */
  auto recover_status =
      consensus_log_manager.get_consensus_info()->get_recover_status();
  auto start_apply_index =
      consensus_log_manager.get_consensus_info()->get_start_apply_index();

  if (recover_status == BINLOG_WORKING && start_apply_index == 0) {
    while (consensus_ptr->getCommitIndex() <
           mgr->get_last_leader_term_index()) {
      my_sleep(500);
    }
  }

  xp::system(ER_XP_RECOVERY)
      << "wait_commit_index_in_recovery end"
      << ", recover_status " << recover_status
      << ", start_apply_index " << start_apply_index
      << ", current commitIndex " << consensus_ptr->getCommitIndex()
      << ", last_leader_term_index " << mgr->get_last_leader_term_index();
}

ConsensusLogManager::ConsensusLogManager()
    : inited(false),
      current_index(0),
      cache_log(new MYSQL_BIN_LOG::Binlog_ofile()),
      binlog(NULL),
      rli_info(NULL) {}

ConsensusLogManager::~ConsensusLogManager() {}

int ConsensusLogManager::init(uint64 max_fifo_cache_size_arg,
                              uint64 max_prefetch_cache_size_arg,
                              uint64 fake_current_index_arg) {
  if (NULL == cache_log) return 1;
  cache_log->open();

  if (open_cached_file(cache_log->get_io_cache(), mysql_tmpdir, LOG_PREFIX,
                       binlog_cache_size, MYF(MY_WME)))
    return 1;

  cache_log->get_io_cache()->end_of_file = CACHE_BUFFER_SIZE;

  // initialize the empty log content
  Consensus_empty_log_event eev;
  eev.common_footer->checksum_alg =
      static_cast<enum_binlog_checksum_alg>(binlog_checksum_options);
  eev.write(cache_log.get());
  my_off_t buf_size = my_b_tell(cache_log->get_io_cache());
  uchar *buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root,
                                     (size_t)buf_size, MYF(MY_WME));
  reinit_io_cache(cache_log->get_io_cache(), READ_CACHE, 0, 0, 0);
  my_b_read(cache_log->get_io_cache(), buffer, (size_t)buf_size);
  reinit_io_cache(cache_log->get_io_cache(), WRITE_CACHE, 0, 0, 1);
  empty_log_event_content = std::string((char *)buffer, buf_size);
  my_free(buffer);

  set_consensus_system_status(BINLOG_WORKING);
  current_index = fake_current_index_arg;
  cache_index = 0;
  sync_index = 0;
  apply_index = 1;
  real_apply_index = 1;
  apply_index_end_pos = 0;
  apply_index_current_pos = 0;
  apply_term = 1;
  apply_catchup = 0;
  current_term = 1;
  stop_term = UINT64_MAX;
  already_set_start_index = false;
  already_set_start_term = false;
  apply_ev_seq = 1;
  in_large_trx_applying = false;
  enable_rotate = false;
  in_large_trx_appending = false;
  in_large_event_appending = false;
  atomic_logging_flag = FLAG_GU1;
  ev_tt_ = 0;

  key_LOCK_consensuslog_status = key_rwlock_ConsensusLog_status_lock;
  key_LOCK_consensuslog_sequence_stage1 =
      key_CONSENSUSLOG_LOCK_ConsensusLog_sequence_stage1_lock;
  key_LOCK_consensuslog_term = key_CONSENSUSLOG_LOCK_ConsensusLog_term_lock;
  key_LOCK_consensuslog_apply_thread =
      key_CONSENSUSLOG_LOCK_ConsensusLog_apply_thread_lock;
  key_LOCK_consensus_state_change =
      key_CONSENSUSLOG_LOCK_Consensus_stage_change;
  key_COND_consensuslog_catchup = key_COND_ConsensusLog_catchup;
  key_COND_consensus_state_change = key_COND_Consensus_state_change;
  mysql_rwlock_init(key_LOCK_consensuslog_status, &LOCK_consensuslog_status);
  mysql_mutex_init(key_LOCK_consensuslog_sequence_stage1,
                   &LOCK_consensuslog_sequence_stage1, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_consensuslog_term, &LOCK_consensuslog_term,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_consensuslog_apply_thread,
                   &LOCK_consensuslog_apply_thread, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_consensus_state_change,
                   &LOCK_consensus_state_change, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_commit_pos, &LOCK_consensus_commit_pos,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_consensuslog_catchup, &COND_consensuslog_catchup);
  mysql_cond_init(key_COND_consensus_state_change,
                  &COND_consensus_state_change);
  recovery_manager = new Consensus_recovery_manager();
  recovery_manager->init();

  fifo_cache_manager = new ConsensusFifoCacheManager();
  fifo_cache_manager->init(max_fifo_cache_size_arg);

  prefetch_manager = new ConsensusPreFetchManager();
  prefetch_manager->init(max_prefetch_cache_size_arg);

  log_file_index = new ConsensusLogIndex();
  log_file_index->init();

  Rpl_info_factory::init_consensus_repo_metadata();
  consensus_info = Rpl_info_factory::create_consensus_info();
  if (!consensus_info) return 1;

  consensus_state_change_is_running = true;
  if (mysql_thread_create(key_thread_consensus_stage_change,
                          &consensus_state_change_thread_handle, NULL,
                          run_consensus_stage_change,
                          (void *)&consensus_state_change_is_running)) {
    xp::error(ER_XP_0) << "Fail to create thread run_consensus_stage_change.";
    abort();
  }
  inited = true;
  return 0;
}

int ConsensusLogManager::init_consensus_info() {
  // init sys info
  Consensus_info *consensus_info = get_consensus_info();
  if (!opt_consensus_force_recovery) {
    if (consensus_info->consensus_init_info()) {
      xp::error(ER_XP_0) << "Fail to init consensus_info.";
      return -1;
    }
    if (consensus_info->get_cluster_info() == "" &&
        consensus_info->get_cluster_learner_info() == "") {
      consensus_info->set_cluster_id(opt_cluster_id);
      // reuse opt_cluster, if normal stands for cluster info, else stands for
      // learner info
      if (!opt_cluster_info) {
        xp::error(ER_XP_0) << "PolarDB-X Engine cluster_info must be set when "
                              "the server is running "
                           << "with --initialize(-insecure) ";
        return -1;
      }
      if (!opt_cluster_learner_node) {
        consensus_info->set_cluster_info(std::string(opt_cluster_info));
        consensus_info->set_cluster_learner_info("");
      } else {
        consensus_info->set_cluster_learner_info(std::string(opt_cluster_info));
        consensus_info->set_cluster_info("");
      }
    }
    if (!opt_initialize && !opt_cluster_force_change_meta) {
      if (consensus_info->get_last_leader_term() != 0) {
        if (!opt_cluster_log_type_instance &&
            mysql_bin_log.init_last_index_of_term(
                consensus_info->get_last_leader_term())) {
          xp::error(ER_XP_0) << "init_last_index_of_term for recovery failed";
          return -1;
        }
      }
    }
    set_consensus_system_status(RELAY_LOG_WORKING);

    // set right current term for apply thread
    set_current_term(get_consensus_info()->get_current_term());
  }

  return 0;
}

int ConsensusLogManager::dump_cluster_info_to_file(std::string meta_file_name,
                                                   std::string output_str) {
  File meta_file = -1;
  if (!my_access(meta_file_name.c_str(), F_OK) &&
      mysql_file_delete(0, meta_file_name.c_str(), MYF(0))) {
    xp::error(ER_XP_0)
        << "Dump meta file failed, access file or delete file error";
    return -1;
  }
  if ((meta_file = mysql_file_open(0, meta_file_name.c_str(),
                                   O_RDWR | O_CREAT | O_BINARY, MYF(MY_WME))) <
          0 ||
      mysql_file_sync(meta_file, MYF(MY_WME))) {
    xp::error(ER_XP_0) << "Dump meta file failed, create file error";
    return -1;
  }

  if (my_write(meta_file, (const uchar *)output_str.c_str(),
               output_str.length(), MYF(MY_WME)) != output_str.length() ||
      mysql_file_sync(meta_file, MYF(MY_WME))) {
    xp::error(ER_XP_0) << "Dump meta file failed, write meta error";
    return -1;
  }

  if (meta_file >= 0) mysql_file_close(meta_file, MYF(0));

  return 0;
}

int ConsensusLogManager::init_service() {
  if (!opt_initialize) {
    Consensus_info *consensus_info = get_consensus_info();
    if (opt_cluster_dump_meta) {
      std::string meta_file_name = "consensus.meta";
      std::ostringstream oss_apply_index;
      uint64 apply_index = get_relay_log_info()->get_consensus_apply_index();
      oss_apply_index << apply_index;
      std::string apply_index_str = oss_apply_index.str();
      std::string cluster_info = get_consensus_info()->get_cluster_info();
      std::string learner_info =
          get_consensus_info()->get_cluster_learner_info();

      std::ostringstream oss_cluster_id;
      uint64 cluster_id = get_consensus_info()->get_cluster_id();
      oss_cluster_id << cluster_id;
      std::string cluster_id_str = oss_cluster_id.str();

      std::string output_str =
          "Consensus_apply_index: " + apply_index_str + "\n" +
          "Conseneus_cluster_info: " + cluster_info + "\n" +
          "Consensus_learner_info: " + learner_info + "\n" +
          "Cluster_id: " + cluster_id_str;
      if (dump_cluster_info_to_file(meta_file_name, output_str) < 0) return -1;
      xp::warn(ER_XP_0) << "Dump meta file successfully.";
      return 1;
    }

    if (opt_cluster_force_change_meta) {
      consensus_info->set_cluster_id(opt_cluster_id);
      if (opt_cluster_current_term)
        consensus_info->set_current_term(opt_cluster_current_term);

      if (opt_cluster_force_recover_index) {
        // backup from leader can also recover like a follower
        if (consensus_info->get_recover_status() == BINLOG_WORKING) {
          consensus_info->set_recover_status(RELAY_LOG_WORKING);
          consensus_info->set_last_leader_term(0);
          consensus_info->set_start_apply_index(
              opt_cluster_force_recover_index);
        }
      }
      // reuse opt_cluster, if normal stands for cluster info, else stands for
      // learner info
      if (!opt_cluster_info) {
        xp::error(ER_XP_0) << "PolarDB-X Engine cluster_info must be set when "
                              "the server is running "
                           << "with --initialize(-insecure) ";
        return -1;
      }
      if (!opt_cluster_learner_node) {
        consensus_info->set_cluster_learner_info("");
        consensus_info->set_cluster_info(std::string(opt_cluster_info));
      } else {
        consensus_info->set_cluster_learner_info(std::string(opt_cluster_info));
        consensus_info->set_cluster_info("");
      }
      // if change meta, flush sys info, force quit
      consensus_info->flush_info(true, true);
      xp::warn(ER_XP_0) << "Force change meta to system table successfully.";
      return 1;
    } else {
      opt_cluster_id = get_consensus_info()->get_cluster_id();
    }

    // learner's cluster_info is empty or not contain @
    bool is_learner =
        consensus_info->get_cluster_info().empty() ||
        consensus_info->get_cluster_info().find('@') == std::string::npos;
    // get ip-port vector config
    std::string empty_str;
    std::vector<std::string> cluster_str_config;
    std::string cluster_info_str =
        is_learner ? consensus_info->get_cluster_learner_info()
                   : consensus_info->get_cluster_info();

    uint64 mock_start_index = std::max(get_log_file_index()->get_first_index(),
                                       opt_consensus_start_index);
    consensus_log->init(mock_start_index, &consensus_log_manager);
    alisql_server = std::make_shared<alisql::AliSQLServer>(0);
    consensus_ptr =
        new alisql::Paxos(opt_consensus_election_timeout, consensus_log);
    consensus_ptr->setStateChangeCb(stateChangeCb);
    consensus_ptr->setMaxPacketSize(opt_consensus_max_packet_size);
    consensus_ptr->setPipeliningTimeout(opt_consensus_pipelining_timeout);
    consensus_ptr->setLargeBatchRatio(opt_consensus_large_batch_ratio);
    consensus_ptr->setMaxDelayIndex(opt_consensus_max_delay_index);
    consensus_ptr->setMinDelayIndex(opt_consensus_min_delay_index);
    consensus_ptr->setSyncFollowerMetaInterval(
        opt_consensus_sync_follower_meta_interval);
    consensus_ptr->setConsensusAsync(opt_weak_consensus_mode);
    consensus_ptr->setReplicateWithCacheLog(
        opt_consensus_replicate_with_cache_log);
    consensus_ptr->setCompactOldMode(opt_consensus_old_compact_mode);
    // todo keep IS log level sync with MySQL log
    consensus_ptr->setAlertLogLevel(
        alisql::Paxos::AlertLogLevel(opt_consensus_log_level + 3));
    consensus_ptr->setForceSyncEpochDiff(opt_consensus_force_sync_epoch_diff);
    consensus_ptr->setChecksumMode(opt_consensus_checksum);
    consensus_ptr->setChecksumCb(checksum_crc32);
    consensus_ptr->setConfigureChangeTimeout(
        opt_consensus_configure_change_timeout);
    consensus_ptr->setMaxDelayIndex4NewMember(
        opt_consensus_new_follower_threshold);
    consensus_ptr->setEnableDynamicEasyIndex(opt_consensus_dynamic_easyindex);
    consensus_ptr->setEnableLearnerPipelining(opt_consensus_learner_pipelining);
    consensus_ptr->setEnableLearnerHeartbeat(opt_consensus_learner_heartbeat);
    consensus_ptr->setEnableAutoResetMatchIndex(
        opt_consensus_auto_reset_match_index);
    consensus_ptr->setEnableAutoLeaderTransfer(
        opt_consensus_auto_leader_transfer);
    consensus_ptr->setAutoLeaderTransferCheckSeconds(
        opt_consensus_auto_leader_transfer_check_seconds);
    consensus_ptr->setThreadHook([]() { my_thread_init(); }, my_thread_end);
    if (!opt_consensus_force_recovery) {
      if (!is_learner) {
        // startup as normal node
        consensus_ptr->init(
            cluster_str_config, 0, ::server_id, NULL, opt_consensus_io_thread_cnt,
            opt_consensus_worker_thread_cnt, alisql_server,
            opt_consensus_easy_pool_size, opt_consensus_heartbeat_thread_cnt);

        if (opt_cluster_log_type_instance) {
          consensus_ptr->setAsLogType(true);
        }
      } else {
        // startup as learner node, config string arg pass empty
        consensus_ptr->initAsLearner(
            empty_str, ::server_id, NULL, opt_consensus_io_thread_cnt,
            opt_consensus_worker_thread_cnt, alisql_server,
            opt_consensus_easy_pool_size, opt_consensus_heartbeat_thread_cnt);
      }
      consensus_ptr->initAutoPurgeLog(false, false, NULL);  // disable autoPurge

      if (opt_cluster_force_single_mode)  // use nuclear weapon
        consensus_ptr->forceSingleLeader();

      consensus_ptr->waitCommitIndexUpdate(0);

      if (opt_commit_pos_watcher) {
        if (start_consensus_commit_pos_watcher()) return -1;
      }

      wait_commit_index_in_recovery();

      if (!opt_cluster_log_type_instance) {
        if (consensus_log_manager.get_recovery_manager()
                ->recover_remaining_pending_recovering_trxs()) {
          return -1;
        }
        get_recovery_manager()->clear();

        // start_consensus_apply_threads();
      } else {
        assert(get_recovery_manager()->is_pending_recovering_trx_empty());
      }
    }       // end of opt_consensus_force_recovery
  } else {  // opt_initialize
    Consensus_info *consensus_info = get_consensus_info();
    consensus_info->set_cluster_id(opt_cluster_id);
    // reuse opt_cluster, if normal stands for cluster info, else stands for
    // learner info
    if (!opt_cluster_info) {
      xp::error(ER_XP_0) << "PolarDB-X Engine cluster_info must be set when "
                            "the server is running "
                         << "with --initialize(-insecure) ";
      return -1;
    }
    if (!opt_cluster_learner_node) {
      consensus_info->set_cluster_learner_info("");
      consensus_info->set_cluster_info(std::string(opt_cluster_info));
    } else {
      consensus_info->set_cluster_learner_info(std::string(opt_cluster_info));
      consensus_info->set_cluster_info("");
    }
  }

  return 0;
}

int ConsensusLogManager::cleanup() {
  if (inited) {
    /* set apply_catchup to true to skip wait_replay_log_finished */
    apply_catchup = true;
    mysql_mutex_lock(&LOCK_consensus_state_change);
    consensus_state_change_is_running = false;
    mysql_mutex_unlock(&LOCK_consensus_state_change);
    mysql_cond_broadcast(&COND_consensus_state_change);
    mysql_cond_broadcast(&COND_server_started);
    my_thread_join(&consensus_state_change_thread_handle, NULL);
    recovery_manager->cleanup();
    fifo_cache_manager->cleanup();
    prefetch_manager->cleanup();
    log_file_index->cleanup();

    close_cached_file(cache_log->get_io_cache());

    consensus_info->end_info();
    delete fifo_cache_manager;
    delete consensus_info;
    delete prefetch_manager;
    delete log_file_index;
    delete recovery_manager;

    mysql_rwlock_destroy(&LOCK_consensuslog_status);
    mysql_mutex_destroy(&LOCK_consensuslog_sequence_stage1);
    mysql_mutex_destroy(&LOCK_consensuslog_term);
    mysql_mutex_destroy(&LOCK_consensuslog_apply_thread);
    mysql_mutex_destroy(&LOCK_consensus_state_change);
    mysql_mutex_destroy(&LOCK_consensus_commit_pos);
    mysql_cond_destroy(&COND_consensuslog_catchup);
    mysql_cond_destroy(&COND_consensus_state_change);
  }
  return 0;
}

std::string ConsensusLogManager::get_empty_log() {
  std::string ret_ev;
  if (opt_consensuslog_revise) {
    // generate a new one with current time
    size_t buf_size = empty_log_event_content.length();
    uchar *buffer =
        (uchar *)my_malloc(key_memory_thd_main_mem_root, buf_size, MYF(MY_WME));
    memcpy(buffer, empty_log_event_content.data(), buf_size);
    int4store(buffer, my_micro_time() / 1000000);
    if (binlog_checksum_options != binary_log::BINLOG_CHECKSUM_ALG_OFF) {
      ha_checksum crc = checksum_crc32(0L, NULL, 0);
      crc = checksum_crc32(crc, buffer, buf_size - BINLOG_CHECKSUM_LEN);
      int4store(buffer + buf_size - BINLOG_CHECKSUM_LEN, crc);
    }
    ret_ev = std::string((char *)buffer, buf_size);
    my_free(buffer);
  } else {
    ret_ev = empty_log_event_content;
  }
  return ret_ev;
}

int ConsensusLogManager::set_start_apply_index_if_need(uint64 consensus_index) {
  if (opt_cluster_log_type_instance) return 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  if (!already_set_start_index && status == BINLOG_WORKING) {
    consensus_info->set_start_apply_index(consensus_index);
    if (consensus_info->flush_info(true, true)) {
      return 1;
    }
    already_set_start_index = true;
  }
  return 0;
}

int ConsensusLogManager::set_start_apply_term_if_need(uint64 consensus_term) {
  if (opt_cluster_log_type_instance) return 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  if (!already_set_start_term && status == BINLOG_WORKING) {
    consensus_info->set_last_leader_term(consensus_term);
    if (consensus_info->flush_info(true, true)) {
      return 1;
    }
    already_set_start_term = true;
  }
  return 0;
}

int ConsensusLogManager::write_log_entry(ConsensusLogEntry &log,
                                         uint64 *consensus_index,
                                         bool with_check) {
  int error = 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );

  enable_rotate = !(log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if (status == Consensus_Log_System_Status::BINLOG_WORKING) {
    bool do_rotate = false;
    if ((error = binlog->append_consensus_log(log, consensus_index, &do_rotate,
                                              rli_info, with_check))) {
      goto end;
    }
    if (*consensus_index == 0) goto end;
    assert(*consensus_index != 0);

    // do not rotate if binlog working because of 2 stage recovery
  } else {
    bool do_rotate = false;
    Master_info *mi = rli_info->mi;
    mysql_mutex_lock(&mi->data_lock);
    if (rli_info->relay_log.append_consensus_log(
            log, consensus_index, &do_rotate, rli_info, with_check)) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }
    if (*consensus_index == 0) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    }
    assert(*consensus_index != 0);
    if (do_rotate && recovery_manager->is_pending_recovering_trx_empty() &&
        enable_rotate) {
      if ((error = rotate_relay_log(rli_info->mi))) {
        mysql_mutex_unlock(&mi->data_lock);
        goto end;
      }
    }
    mysql_mutex_unlock(&mi->data_lock);
  }
end:
  consensus_guard.unlock();
  if (error)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::write_log_entry error, consensus index: "
        << *consensus_index;
  if (*consensus_index == 0)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::write_log_entry error, consensus index: "
        << *consensus_index << ", because of a failed term check.";
  return error;
}

int ConsensusLogManager::write_log_entries(std::vector<ConsensusLogEntry> &logs,
                                           uint64 *max_index) {
  int error = 0;
  // only follower will call this function
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  bool do_rotate = false;
  MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                           ? binlog
                           : &rli_info->relay_log;
  enable_rotate =
      !(logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if ((error = log->append_multi_consensus_logs(logs, max_index, &do_rotate,
                                                rli_info))) {
    goto end;
  }
  if (do_rotate && recovery_manager->is_pending_recovering_trx_empty() &&
      enable_rotate) {
    if (status == BINLOG_WORKING) {
      if ((error = binlog->rotate_consensus_log())) {
        goto end;
      }
    } else {
      Master_info *mi = rli_info->mi;
      mysql_mutex_lock(&mi->data_lock);
      if ((error = rotate_relay_log(rli_info->mi))) {
        mysql_mutex_unlock(&mi->data_lock);
        goto end;
      }
      mysql_mutex_unlock(&mi->data_lock);
    }
  }
end:
  if (error)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::write_log_entries error, batch size: "
        << logs.size() << " ,  max consensus index: " << *max_index;
  return error;
}

int ConsensusLogManager::get_log_directly(uint64 consensus_index,
                                          uint64 *consensus_term,
                                          std::string &log_content, bool *outer,
                                          uint *flag, uint64 *checksum,
                                          bool need_content) {
  int error = 0;
  if (consensus_index == 0) {
    *consensus_term = 1;
    *outer = false;
    *flag = 0;
    return error;
  }
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_get_log_entry(consensus_index, consensus_term, log_content,
                                   outer, flag, need_content))
    error = 1;
  if (error)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::get_log_directly error,  consensus index: "
        << consensus_index;
  else
    *checksum =
        opt_consensus_checksum
            ? checksum_crc32(0, get_uchar_str(log_content), log_content.size())
            : 0;
  return error;
}

int ConsensusLogManager::prefetch_log_directly(THD *thd, uint64 channel_id,
                                               uint64 consensus_index) {
  int error = 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_prefetch_log_entries(thd, channel_id, consensus_index))
    error = 1;
  if (error)
    xp::error(ER_XP_0) << "ConsensusLogManager::prefetch_log_directly error,  "
                          "consensus index: "
                       << consensus_index;
  return error;
}

int ConsensusLogManager::get_log_entry(uint64 channel_id,
                                       uint64 consensus_index,
                                       uint64 *consensus_term,
                                       std::string &log_content, bool *outer,
                                       uint *flag, uint64 *checksum,
                                       bool fast_fail) {
  int error = 0;
  if (!opt_consensus_disable_fifo_cache && consensus_index > cache_index)
    return 1;
  if (consensus_index == 0) {
    *consensus_term = 0;
    log_content = "";
    return 0;
  }

  error = fifo_cache_manager->get_log_from_cache(
      consensus_index, consensus_term, log_content, outer, flag, checksum);
  DBUG_EXECUTE_IF("get_log_from_fifo_fail_when_blob_end",
                  error = ALREADY_SWAP_OUT;);
  if (error == ALREADY_SWAP_OUT) {
    uint64_t last_sync_index = sync_index;
    if (consensus_index > last_sync_index) {
      // don't prefetch log if it is not written to disk
      xp::info(ER_XP_0)
          << "ConsensusLogManager::get_log_entry " << consensus_index
          << " fail, log has not been flushed to disk, sync index is "
          << last_sync_index;
      return 1;
    }
    ConsensusPreFetchChannel *channel =
        prefetch_manager->get_prefetch_channel(channel_id);
    if ((error = channel->get_log_from_prefetch_cache(
             consensus_index, consensus_term, log_content, outer, flag,
             checksum))) {
      if (!fast_fail) {
        error = get_log_directly(consensus_index, consensus_term, log_content,
                                 outer, flag, checksum,
                                 channel_id == 0 ? false : true);
      } else {
        error = 1;
      }
      channel->set_prefetch_request(consensus_index);
    }
  } else if (error == OUT_OF_RANGE) {
    xp::error(ER_XP_0) << "ConsensusLogManager::get_log_entry fail, out of "
                          "fifo range. channel_id "
                       << channel_id
                       << " consensus index : " << consensus_index;
  }
  if (error == 1)
    xp::info(ER_XP_0) << "ConsensusLogManager::get_log_entry fail, channel_id "
                      << channel_id << " consensus index: " << consensus_index
                      << " ,start prefetch.";
  return error;
}

uint64_t ConsensusLogManager::get_left_log_size(uint64 start_log_index,
                                                uint64 max_packet_size) {
  uint64 total_size = 0;
  total_size += fifo_cache_manager->get_log_size_from_cache(
      start_log_index, cache_index, max_packet_size);
  // do not consider prefetch cache here
  return total_size;
}

int ConsensusLogManager::get_log_position(uint64 consensus_index,
                                          bool need_lock, char *log_name,
                                          uint64 *pos) {
  int error = 0;
  if (need_lock) rdlock_consensus_status();
  MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_get_log_position(consensus_index, log_name, pos)) {
    error = 1;
  }
  if (need_lock) unlock_consensus_status();
  if (error)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::get_log_position error, consensus index: "
        << consensus_index;
  return error;
}

uint64 ConsensusLogManager::get_next_trx_index(uint64 consensus_index) {
  uint64 retIndex = consensus_index;
  if (consensus_index != 0) {
    auto consensus_guard = create_lock_guard(
      [&] { rdlock_consensus_status(); },
      [&] { unlock_consensus_status(); }
    );
    MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                             ? binlog
                             : &(rli_info->relay_log);

    retIndex = log->get_trx_end_index(consensus_index);
    if (retIndex == 0) {
      xp::error(ER_XP_0) << "fail to find next trx index.";
      abort();
    }
  }
  xp::system(ER_XP_0) << "get_next_trx_index"
                    << ", input index: " << consensus_index
                    << ", next transaction index is " << retIndex + 1;
  return retIndex + 1;
}

uint32 ConsensusLogManager::serialize_cache(uchar **buffer) {
  my_off_t buf_size = my_b_tell(cache_log->get_io_cache());
  *buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root, (size_t)buf_size,
                               MYF(MY_WME));
  if (reinit_io_cache(cache_log->get_io_cache(), READ_CACHE, 0, 0, 0)) return 0;
  my_b_read(cache_log->get_io_cache(), *buffer, (size_t)buf_size);
  if (reinit_io_cache(cache_log->get_io_cache(), WRITE_CACHE, 0, 0, 1))
    return 0;
  return buf_size;
}

int ConsensusLogManager::truncate_log(uint64 consensus_index) {
  int error = 0;

  xp::system(ER_XP_0)
      << "ConsensusLogManager::truncate_log before"
      << ", error: " << error
      << ", consensus index: " << consensus_index
      << ", status: " << status
      << ", relay_log name: " << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->get_log_info()->log_file_name : "")
      << ", relaylog_reader_position: " << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->relaylog_reader_position() : 0)
      << ", relay_log->position: " << (rli_info ? rli_info->relay_log.get_binlog_file()->position() : 0)
      << ", binlog name: " << binlog->get_binlog_file()->get_binlog_name()
      << ", binlog->position: " << binlog->get_binlog_file()->position();

  prefetch_manager->stop_prefetch_threads();
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  MYSQL_BIN_LOG *log =
      (status == BINLOG_WORKING ? binlog : &(rli_info->relay_log));
  auto log_guard = create_lock_guard(
    [&] { mysql_mutex_lock(log->get_log_lock()); },
    [&] { mysql_mutex_unlock(log->get_log_lock()); }
  );
  log->lock_index();

  // truncate log file
  if (log->consensus_truncate_log(consensus_index)) {
    error = 1;
    xp::error(ER_XP_0) << "Consensus Truncate log failed " << consensus_index;
    abort();
  }

  log->unlock_index();
  set_sync_index(consensus_index - 1);
  set_current_index(consensus_index);
  set_in_large_trx_appending(false);
  set_in_large_event_appending(false);
  reinit_io_cache(cache_log->get_io_cache(), WRITE_CACHE, 0, 0, 1);

  if (status == RELAY_LOG_WORKING) {
    if (rli_info->applier_reader) {
      const int cmp_result = strcmp(log->get_binlog_file()->get_binlog_name(),
                                    rli_info->applier_reader->get_log_info()->log_file_name);
      if (cmp_result < 0
          || (cmp_result == 0
              && log->get_binlog_file()->position() < rli_info->applier_reader->relaylog_reader_position())) {
        xp::error(ER_XP_COMMIT) << "relay log(" << log->get_binlog_file()->get_binlog_name()
            << ") new position " << log->get_binlog_file()->position()
            << " is small then current relaylog_reader(" << rli_info->applier_reader->get_log_info()->log_file_name
            << ") position " << rli_info->applier_reader->relaylog_reader_position()
            << ", need abort and restart";
        abort();
      }
    }

    if (recovery_manager->get_last_leader_term_index() >= consensus_index)
      recovery_manager->set_last_leader_term_index(consensus_index - 1);
    rli_info->notify_relay_log_truncated();
  }

  // truncate commit map
  recovery_manager->truncate_pending_recovering_trxs(consensus_index);

  // truncate cache
  fifo_cache_manager->trunc_log_from_cache(consensus_index);
  prefetch_manager->trunc_log_from_prefetch_cache(consensus_index);
  log_guard.unlock();

  consensus_guard.unlock();
  prefetch_manager->start_prefetch_threads();

  xp::system(ER_XP_0)
      << "ConsensusLogManager::truncate_log after"
      << ", error: " << error
      << ", consensus index: " << consensus_index
      << ", status: " << status
      << ", relay_log name: " << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->get_log_info()->log_file_name : "")
      << ", relaylog_reader_position: " << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->relaylog_reader_position() : 0)
      << ", relay_log->position: " << (rli_info ? rli_info->relay_log.get_binlog_file()->position() : 0)
      << ", binlog name: " << binlog->get_binlog_file()->get_binlog_name()
      << ", binlog->position: " << binlog->get_binlog_file()->position();

  return error;
}

int ConsensusLogManager::purge_log(uint64 consensus_index) {
  int error = 0;
  std::string file_name;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  uint64 purge_index = 0;

  if (status == BINLOG_WORKING) {
    // server still work as leader, so we should at least retain 1 binlog file
    // apply pos must at the last binlog file
    purge_index = consensus_index;
  } else {
    uint64 start_apply_index = consensus_info->get_start_apply_index();
    if (start_apply_index == 0) {
      // apply thread already start, use slave applied index as purge index
      purge_index = rli_info->get_consensus_apply_index();
    } else {
      // apply thread not start
      purge_index = start_apply_index;
    }
    purge_index = opt_cluster_log_type_instance
                      ? consensus_index
                      : std::min(purge_index, consensus_index);
  }
  // for call binlog->purge_logs
  MYSQL_BIN_LOG *log =
      status == BINLOG_WORKING ? binlog : &(rli_info->relay_log);
  if (status == RELAY_LOG_WORKING) mysql_mutex_lock(&rli_info->data_lock);
  log->lock_index();
  if (log->find_log_by_consensus_index(purge_index, file_name) ||
      log->purge_logs(file_name.c_str(), false /**include*/,
                      false /*need index lock*/, true /*update threads*/, NULL,
                      true)) {
    error = 1;
  }
  if (status == BINLOG_WORKING) {
    log->unlock_index();
  } else {
    /*
     * Need to update the log pos because purge logs has been called
     * after fetching initially the log pos at the begining of the method.
     */
    if (!opt_cluster_log_type_instance && rli_info->applier_reader) {
      LOG_INFO *log_info = rli_info->applier_reader->get_log_info();
      assert(log_info != NULL);
      if ((error =
               log->find_log_pos(log_info, rli_info->get_event_relay_log_name(),
                                 false /*need_lock_index=false*/))) {
        char buff[22];
        xp::error(ER_XP_0) << "next log error: " << error << "  offset: "
                           << llstr(log_info->index_file_offset, buff)
                           << "  log: " << rli_info->get_group_relay_log_name();
        error = 1;
      }
    }
    log->unlock_index();
    mysql_mutex_unlock(&rli_info->data_lock);
  }
  if (error)
    xp::error(ER_XP_0)
        << "ConsensusLogManager::purge_log error, consensus index: "
        << consensus_index;
  return error;
}

uint64 ConsensusLogManager::get_exist_log_length() {
  // first index in log index may not be exact value
  // so we should check whether sync_index is larger
  uint64 start_index = log_file_index->get_first_index();
  uint64 end_index = sync_index;
  if (end_index >= start_index)
    return end_index - start_index;
  else
    return 0;
}

int ConsensusLogManager::start_consensus_commit_pos_watcher() {
  assert(inited);
  // start a background thread to report current log position of commitIndex
  consensus_commit_pos_watcher_is_running = true;
  if (mysql_thread_create(key_thread_consensus_commit_pos_watcher,
                          &consensus_commit_pos_watcher_thread_handle, NULL,
                          run_consensus_commit_pos_watcher,
                          (void *)&consensus_commit_pos_watcher_is_running)) {
    xp::error(ER_XP_0)
        << "Fail to create thread run_consensus_commit_pos_watcher.";
    return 1;
  }
  return 0;
}

void ConsensusLogManager::stop_consensus_commit_pos_watcher() {
  if (consensus_commit_pos_watcher_is_running) {
    consensus_commit_pos_watcher_is_running = false;
    my_thread_join(&consensus_commit_pos_watcher_thread_handle, NULL);
  }
}

void ConsensusLogManager::update_commit_pos(const std::string &log_name,
                                            uint64_t pos, uint64_t index) {
  assert(pos != 0);
  assert(index >= commit_pos.index);
  mysql_mutex_lock(&LOCK_consensus_commit_pos);
  commit_pos.fname = log_name;
  commit_pos.pos = pos;
  commit_pos.index = index;
  mysql_mutex_unlock(&LOCK_consensus_commit_pos);

  if (opt_consensus_log_level >= 2)
    xp::info(ER_XP_0) << "Report binlog commit position. fname: "
                      << commit_pos.fname.c_str() << ", pos: " << commit_pos.pos
                      << ", index: " << commit_pos.index;
}

void ConsensusLogManager::get_commit_pos(char *const fname_ptr,
                                         uint64_t *pos_ptr,
                                         uint64_t *index_ptr) {
  if (fname_ptr == NULL || pos_ptr == NULL || index_ptr == NULL) return;
  mysql_mutex_lock(&LOCK_consensus_commit_pos);
  // parse xxx/binlog.0001 to binlog.0001
  const char *tmp = strrchr(commit_pos.fname.c_str(), '/');
  strncpy(fname_ptr, tmp == NULL ? commit_pos.fname.c_str() : tmp + 1,
          FN_REFLEN);
  *pos_ptr = commit_pos.pos;
  *index_ptr = commit_pos.index;
  mysql_mutex_unlock(&LOCK_consensus_commit_pos);
}

void ConsensusLogManager::wait_replay_log_finished() {
  mysql_mutex_lock(&LOCK_consensuslog_apply_thread);
  if (apply_catchup == true) {
    mysql_mutex_unlock(&LOCK_consensuslog_apply_thread);
  } else {
    struct timespec abstime;
    set_timespec(&abstime, 2);
    while (!apply_catchup) {
      mysql_cond_timedwait(&COND_consensuslog_catchup,
                           &LOCK_consensuslog_apply_thread, &abstime);
    }
    mysql_mutex_unlock(&LOCK_consensuslog_apply_thread);
  }

  set_apply_catchup(false);
}

void ConsensusLogManager::wait_apply_threads_start() {
  while (rli_info && !rli_info->slave_running) {
    my_sleep(200);
  }
}

void ConsensusLogManager::wait_apply_threads_stop() {
  while (rli_info && rli_info->slave_running) {
    my_sleep(200);
  }
}

int ConsensusLogManager::wait_leader_degraded(uint64 term, uint64 index) {
  xp::system(ER_XP_0)
      << "ConsensusLogManager::wait_leader_degraded, consensus term: " << term
      << ", consensus index: " << index;
  int error = 0;

  DBUG_EXECUTE_IF("simulate_leader_degrade_slow", {
    my_sleep(10000000);
    xp::info(ER_XP_0) << "wait_leader_degraded sleep 10 seconds";
  });

  // switch event scheduler off
  if (opt_configured_event_scheduler == Events::EVENTS_ON) {
    Events::opt_event_scheduler = Events::EVENTS_OFF;
    Events::stop();
  }

  // prefetch stop, and release LOCK_consensuslog_status
  prefetch_manager->disable_all_prefetch_channels();

  assert(this->status == Consensus_Log_System_Status::BINLOG_WORKING);
  // set offline mode
  opt_enable_consensus_leader = false;
  // kill all the thd
  killall_threads();
  // wait all commit trx finished
  binlog->wait_xid_disappear();

  auto consensus_guard = create_lock_guard(
    [&] { mysql_rwlock_wrlock(&LOCK_consensuslog_status); },
    [&] { mysql_rwlock_unlock(&LOCK_consensuslog_status); }
  );

  // close binlog system
  binlog->close(LOG_CLOSE_INDEX | LOG_CLOSE_TO_BE_OPENED, true, true);
  // open relay log system
  mysql_mutex_lock(&rli_info->mi->data_lock);
  mysql_mutex_lock(&rli_info->data_lock);
  rli_info->rli_init_info();
  mysql_mutex_unlock(&rli_info->data_lock);
  mysql_mutex_unlock(&rli_info->mi->data_lock);

  // record new term
  current_term = term;
  set_event_timestamp(0);
  set_consensus_system_status(RELAY_LOG_WORKING);

  stop_term = UINT64_MAX;
  // log type instance do not to recover start index
  if (!opt_cluster_log_type_instance) {
    consensus_info->set_start_apply_index(index);
  }
  if (consensus_info->flush_info(true, true)) {
    xp::error(ER_XP_0) << "Failed in flush_info() called from "
                          "ConsensusLog::wait_leader_degraded.";
    error = 1;
    goto end;
  }
  if (!opt_cluster_log_type_instance) {
    start_consensus_apply_threads();
  }

  consensus_guard.unlock();

  if (!opt_cluster_log_type_instance) {
    wait_apply_threads_start();
  }
end:
  consensus_guard.unlock();
  xp::system(ER_XP_0)
      << "ConsensusLogManager::wait_leader_degraded finish, error " << error;
  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int ConsensusLogManager::wait_follower_upgraded(uint64 term, uint64 index) {
  xp::system(ER_XP_0)
      << "ConsensusLogManager::wait_follower_upgraded, consensus term: " << term
      << ", consensus index: " << index;
  int error = 0;
  mysql_mutex_t *log_lock = nullptr;

  assert(this->status == Consensus_Log_System_Status::RELAY_LOG_WORKING);

  // record new term
  // notice, the order of stop term and current term is important for apply
  // thread, because both stop term and current term is atomic variables. so do
  // not care reorder problem
  stop_term = term;

  // wait replay thread to commit index
  if (!opt_cluster_log_type_instance) {
    DBUG_EXECUTE_IF("simulate_apply_too_slow", my_sleep(5000000););
    wait_replay_log_finished();
    wait_apply_threads_stop();
  }

  // wait recover trx all finished
  while (!(recovery_manager->is_pending_recovering_trx_empty())) {
    my_sleep(1000);
  }
  // prefetch stop, and release LOCK_consensuslog_status
  prefetch_manager->disable_all_prefetch_channels();

  // kill all binlog dump thd
  killall_dump_threads();

  auto consensus_guard = create_lock_guard(
    [&] { mysql_rwlock_wrlock(&LOCK_consensuslog_status); },
    [&] { mysql_rwlock_unlock(&LOCK_consensuslog_status); }
  );

  if (rli_info == nullptr) {
    goto end;
  }

  mysql_mutex_lock(&rli_info->data_lock);

  rli_info->end_info();
  mysql_mutex_unlock(&rli_info->data_lock);

  // open binlog index and file
  log_lock = binlog->get_log_lock();
  if (binlog->open_index_file(opt_binlog_index_name, opt_bin_logname, true)) {
    xp::error(ER_XP_0) << "Failed in open_index_file() called from "
                          "ConsensusLog::wait_follower_upgraded.";
    error = 1;
    goto end;
  }

  mysql_mutex_lock(log_lock);
  if (binlog->open_exist_binlog(opt_bin_logname, 0, max_binlog_size, true,
                                true /*need_lock_index=true*/,
                                true /*need_sid_lock=true*/, NULL)) {
    mysql_mutex_unlock(log_lock);
    xp::error(ER_XP_0) << "Failed in open_log() called from "
                          "ConsensusLog::wait_follower_upgraded.";
    error = 2;
    goto end;
  }
  mysql_mutex_unlock(log_lock);
  // set offline mode
  opt_enable_consensus_leader = true;
  appliedindex_checker.reset();
  set_event_timestamp(0);
  set_consensus_system_status(BINLOG_WORKING);

  // reset apply start point displayed in information_schema
  apply_index = 1;
  real_apply_index = 1;
  already_set_start_index = false;
  already_set_start_term = false;

  current_term = term;

  // log type instance do not to recover start index
  if (!opt_cluster_log_type_instance) {
    /*
      TODO: the global set gtid_state->binlog_previous_gtids is not maintenaned
      correctly on followers, and as a result the content of previous gtid event
      in binlog file is incorrect at binlog rotation.

      So, when follower upgrading to leader, that using the previous gtid event
      in binlog to adjust the value in memory is unreliable.

      Actually in xdb cluster, binlog previous gtid set are always equivalent to
      executed gtid set, we disable adjusting temporarily.
    */
    // if (index > 0 && binlog->reset_previous_gtids_logged(index)) {
    //   xp::error(ER_XP_0) << "Failed to reset previous gtids logged.";
    // }
    consensus_info->set_last_leader_term(term);
  }
  consensus_info->set_recover_status(
      Consensus_Log_System_Status::BINLOG_WORKING);

  if (consensus_info->flush_info(true, true)) {
    xp::error(ER_XP_0) << "Failed in flush_info() called from "
                          "ConsensusLog::wait_follower_upgraded.";
    error = 3;
    goto end;
  }
  consensus_guard.unlock();

  // switch event scheduler on
  if (opt_configured_event_scheduler == Events::EVENTS_ON) {
    int err_no = 0;
    Events::opt_event_scheduler = Events::EVENTS_ON;
    if (Events::start(&err_no)) {
      Events::opt_event_scheduler = Events::EVENTS_OFF;
      xp::error(ER_XP_0) << "Fail to set event_scheduler=on during "
                            "wait_follower_upgraded, error "
                         << err_no;
    }
  }
end:
  consensus_guard.unlock();
  xp::system(ER_XP_0)
      << "ConsensusLogManager::wait_follower_upgraded finish, error " << error;

  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int ConsensusLogManager::wait_follower_change_term(uint64 term) {
  xp::system(ER_XP_0)
      << "ConsensusLogManager::wait_follower_change_term, consensus term: "
      << term;
  current_term = term;
  return 0;
}

uint64 ConsensusLogManager::get_cache_index() { return cache_index; }

void ConsensusLogManager::set_cache_index(uint64 cache_index_arg) {
  cache_index = cache_index_arg;
}

uint64 ConsensusLogManager::get_sync_index(bool serious) {
  if (!serious) {
    return sync_index;
  } else {
    /* currently, never cover this path, deadlock risk */
    auto consensus_guard = create_lock_guard(
      [&] { rdlock_consensus_status(); },
      [&] { unlock_consensus_status(); }
    );
    MYSQL_BIN_LOG *log = status == Consensus_Log_System_Status::BINLOG_WORKING
                             ? binlog
                             : &(rli_info->relay_log);
    log->wait_xid_disappear();
    return sync_index;
  }
}

uint64 ConsensusLogManager::get_final_sync_index() {
  mysql_mutex_lock(get_sequence_stage1_lock());
  uint64_t final_sync_index = current_index ? current_index - 1 : 0;
  mysql_mutex_unlock(get_sequence_stage1_lock());
  return final_sync_index;
}

void ConsensusLogManager::set_sync_index(uint64 sync_index_arg) {
  sync_index = sync_index_arg;
}

void ConsensusLogManager::set_sync_index_if_greater(uint64 sync_index_arg) {
  for (;;) {
    uint64 old = sync_index.load();
    if (old >= sync_index_arg ||
        (old < sync_index_arg &&
         sync_index.compare_exchange_weak(old, sync_index_arg)))
      break;
  }
}

void ConsensusLogManager::add_state_change_request(
    ConsensusStateChange &state_change) {
  mysql_mutex_lock(&LOCK_consensus_state_change);
  consensus_state_change_queue.push_back(state_change);
  mysql_mutex_unlock(&LOCK_consensus_state_change);
  mysql_cond_broadcast(&COND_consensus_state_change);
}

void ConsensusLogManager::lock_consensus_state_change() {
  mysql_mutex_lock(&LOCK_consensus_state_change);
}

void ConsensusLogManager::unlock_consensus_state_change() {
  mysql_mutex_unlock(&LOCK_consensus_state_change);
}

void ConsensusLogManager::wait_state_change_cond() {
  mysql_cond_wait(&COND_consensus_state_change, &LOCK_consensus_state_change);
}

ConsensusStateChange ConsensusLogManager::get_stage_change_from_queue() {
  ConsensusStateChange state_change;

  if (consensus_state_change_queue.size() > 0) {
    state_change = consensus_state_change_queue.front();
    consensus_state_change_queue.pop_front();
  }
  return state_change;
}

void *run_consensus_stage_change(void *arg) {
  if (my_thread_init()) return NULL;

  int error = 0;
  bool *is_running = (bool *)arg;

  /*
    Waiting until mysqld_server_started == true to ensure that all server
    components have been successfully initialized.
  */
  mysql_mutex_lock(&LOCK_server_started);
  while (!mysqld_server_started && (*is_running))
    mysql_cond_wait(&COND_server_started, &LOCK_server_started);
  mysql_mutex_unlock(&LOCK_server_started);
  while (*is_running) {
    consensus_log_manager.lock_consensus_state_change();
    if (consensus_log_manager.is_state_change_queue_empty() && (*is_running))
      consensus_log_manager.wait_state_change_cond();
    if (consensus_log_manager.is_state_change_queue_empty()) {
      consensus_log_manager.unlock_consensus_state_change();
      continue;
    }
    ConsensusStateChange state_change =
        consensus_log_manager.get_stage_change_from_queue();
    if (state_change.state != alisql::Paxos::LEADER) {
      if (consensus_log_manager.get_status() == BINLOG_WORKING) {
        error = consensus_log_manager.wait_leader_degraded(state_change.term,
                                                           state_change.index);
      } else if (state_change.state != alisql::Paxos::CANDIDATE) {
        error =
            consensus_log_manager.wait_follower_change_term(state_change.term);
      }
    } else {
      // must be candidate ->leader
      error = consensus_log_manager.wait_follower_upgraded(state_change.term,
                                                           state_change.index);
    }

    consensus_log_manager.unlock_consensus_state_change();

    if (error) {
      xp::error(ER_XP_0) << "Consensus state change failed";
      abort();
    }
  }

  my_thread_end();
  return NULL;
}

void *run_consensus_commit_pos_watcher(void *arg) {
  xp::system(ER_XP_0) << "start consensus_commit_pos_watcher thread";
  binlog_commit_pos_watcher((bool *)arg);
  xp::system(ER_XP_0) << "stop consensus_commit_pos_watcher thread";
  return NULL;
}

bool ConsensusLogManager::is_state_machine_ready() {
  return status == Consensus_Log_System_Status::BINLOG_WORKING &&
         consensus_ptr->getTerm() == get_current_term();
}

bool ConsensusLogManager::option_invalid(bool log_bin) {
  if (!log_bin) {
    xp::error(ER_XP_0) << "PolarDB-X Engine log_bin must be set to ON";
    return true;
  }

  return false;
}

IO_CACHE *ConsensusLogManager::get_cache() { return cache_log->get_io_cache(); }