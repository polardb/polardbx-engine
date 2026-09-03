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
#include "komodo0consensus0xlog.h"
#include "consensus_recovery_manager.h"
#include "events.h"
#include "log.h"
#include "log_event.h"
#include "mysql/psi/mysql_file.h"
#include "mysql/thread_pool_priv.h"
#include "rpl_gtid.h"
#include "rpl_msr.h"
#include "rpl_replica.h"
#include "sql/applied_index_checker.h"
#include "sql/consensus_admin.h"
#include "sql/rpl_info_factory.h"
#include "sql/rpl_mi.h"
#include "sql/rpl_rli_pdb.h"
#include "sql_parse.h"
#include "sys_vars_consensus.h"
#include "sql/rpl_channel_service_interface.h"
#include "my_inttypes.h"
#include "sql/xa/komodo0xa.h"
#include "komodo0xlog0engine.h"

#include "sql/consensus/komodo0fifo0cache.h"

#define LOG_PREFIX "ML"

namespace komodo {

/** TODO: to be refactor because of m_is_logger. */
/** Constructor  Consensus_log_manager. */
Consensus_log_manager::Consensus_log_manager()
    : inited(false),
      current_index(0),
      cache_log(new MYSQL_BIN_LOG::Binlog_ofile()),
      binlog(NULL),
      rli_info(NULL) {}

Consensus_log_manager::~Consensus_log_manager() {}

int Consensus_log_manager::init(uint64 fake_current_index_arg) {
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
  apply_index = 0;
  real_apply_index = 0;
  apply_index_end_pos = 0;
  apply_index_current_pos = 0;
  apply_ev_finish_count = 0;
  apply_term = 1;
  apply_catchup = 0;
  current_term = 1;
  stop_term = UINT64_MAX;
  already_set_start_index = false;
  already_set_start_term = false;
  apply_ev_seq = 0;
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
  key_COND_consensuslog_catchup = key_COND_ConsensusLog_catchup;
  mysql_rwlock_init(key_LOCK_consensuslog_status, &LOCK_consensuslog_status);
  mysql_mutex_init(key_LOCK_consensuslog_sequence_stage1,
                   &LOCK_consensuslog_sequence_stage1, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_consensuslog_term, &LOCK_consensuslog_term,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_LOCK_consensuslog_apply_thread,
                   &LOCK_consensuslog_apply_thread, MY_MUTEX_INIT_FAST);
  mysql_mutex_init(key_CONSENSUSLOG_LOCK_commit_pos, &LOCK_consensus_commit_pos,
                   MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_consensuslog_catchup, &COND_consensuslog_catchup);

  log_file_index = new Consensus_logindex_manager();
  log_file_index->init();

  inited = true;
  return 0;
}
int Consensus_log_manager::create_rpl_info() {

  //used for create rpl_info
  ReplicaInitializer replica_initializer(
    false, /*opt_skip_replica_start*/ true, rpl_channel_filters,
    &opt_replica_skip_errors);

  if (!opt_cluster_force_change_meta
      && consensus_info->get_last_leader_term() != 0
      && !m_is_logger) {
    auto last_index = mysql_bin_log.get_last_index_of_term(consensus_info->get_last_leader_term());
    if (last_index == 0) {
      xp::error(ER_XP_0) << "get_last_index_of_term for recovery failed";
      return -1;
    } else {
      recovery_manager->set_last_leader_term_index(last_index);
    }
  }

  // init sys info
  set_consensus_system_status(RELAY_LOG_WORKING);
  // set right current term for apply thread
  set_current_term(get_consensus_info()->get_current_term());
  return 0;
}

int Consensus_log_manager::check_force_change_meta(const bool force_learner) {
  /** Change consensus info if specify opt_cluster_force_change_meta. */
  if (opt_cluster_force_change_meta) {
    if (consensus_info->reinit_cluster_info(force_learner))
      unireg_abort(MYSQLD_ABORT_EXIT);

    if (opt_cluster_current_term)
      consensus_info->set_current_term(opt_cluster_current_term);

    if (opt_cluster_force_recover_index &&
        consensus_info->get_recover_status() == BINLOG_WORKING) {
      // backup from leader can also recover like a follower
      consensus_info->set_recover_status(RELAY_LOG_WORKING);
      consensus_info->set_last_leader_term(0);
      consensus_info->set_start_apply_index(opt_cluster_force_recover_index);
    }

    if (opt_cluster_follower_force_recover_index &&
        consensus_info->get_recover_status() == RELAY_LOG_WORKING)
      consensus_info->set_start_apply_index(
          opt_cluster_follower_force_recover_index);

    consensus_info->flush_info(true, true);

    if (opt_consensus_mts_force_apply_index) {
      rli_info->set_consensus_apply_index(opt_consensus_mts_force_apply_index);
      rli_info->flush_info(true);
    }

    // this flag is used in situation that recovery from a backup
    // without binlog copied.
    // Binlog (also work as relaylog in PolarDB-X DN) is needed in mts
    // recovery. As MySQL use RelayLog for mts reovery.
    if (opt_consensus_reset_mts_info) {
      // Reset per-worker MTS recovery metadata when recovering from a backup
      // that has no binlog. Each Slave_worker is created temporarily to flush
      // zeroed apply positions into the worker_info table, then deleted.
      // Must delete the worker explicitly; create_worker() allocates via new
      // and the ASan-visible leak would cause mysqld to exit with code 1.
      for (uint id = 0; id < rli_info->recovery_parallel_workers; id++) {
        Slave_worker *worker = Rpl_info_factory::create_worker(
            opt_rli_repository_id, id, rli_info, false);
        worker->reset_recovery_info();
        delete worker;
      }
    }

    // if change meta, flush sys info, force quit
    xp::system(ER_XP_0) << "Force change meta to system table successfully.";
    unireg_abort(MYSQLD_SUCCESS_EXIT);
  } else {
    opt_cluster_id = get_consensus_info()->get_cluster_id();
  }
  return 0;
}

int Consensus_log_manager::dump_cluster_info_to_file(std::string meta_file_name,
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

int Consensus_log_manager::cleanup() {
  if (inited) {
    /* set apply_catchup to true to skip wait_replay_log_finished */
    apply_catchup = true;
    if (log_file_index) log_file_index->cleanup();

    close_cached_file(cache_log->get_io_cache());

    delete log_file_index;

    mysql_rwlock_destroy(&LOCK_consensuslog_status);
    mysql_mutex_destroy(&LOCK_consensuslog_sequence_stage1);
    mysql_mutex_destroy(&LOCK_consensuslog_term);
    mysql_mutex_destroy(&LOCK_consensuslog_apply_thread);
    mysql_mutex_destroy(&LOCK_consensus_commit_pos);
    mysql_cond_destroy(&COND_consensuslog_catchup);
    inited = 0;
  }
  return 0;
}

std::string Consensus_log_manager::get_empty_log() {
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

int Consensus_log_manager::set_start_apply_index_if_need(uint64 consensus_index) {
  if (m_is_logger) return 0;
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

int Consensus_log_manager::set_start_apply_term_if_need(uint64 consensus_term) {
  if (m_is_logger) return 0;
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

int Consensus_log_manager::write_log_entry(ConsensusLogEntry &log,
                                         uint64 *consensus_index,
                                         bool with_check) {
  int error = 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );

  enable_rotate = !(log.flag & Consensus_log_event_flag::FLAG_LARGE_TRX);
  if (status == Consensus_Log_State::BINLOG_WORKING) {
    bool do_rotate = false;
    if ((error = binlog->append_consensus_log(log, consensus_index, &do_rotate,
                                              rli_info, with_check))) {
      goto end;
    }
    if (*consensus_index == 0) goto end;
    assert(*consensus_index != 0);

    if (opt_enable_appliedindex_checker && log.outer) {
      intervalType inv{*consensus_index, *consensus_index, 1};
      m_applied_index_checker->prepare(inv);
    }
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
      } else if (opt_cluster_log_type_instance) {
        rli_info->relay_log.auto_purge();
      }
    }
    mysql_mutex_unlock(&mi->data_lock);
  }
end:
  consensus_guard.unlock();
  if (error)
    xp::error(ER_XP_0)
        << "Consensus_log_manager::write_log_entry error, consensus index: "
        << *consensus_index;
  if (*consensus_index == 0)
    xp::error(ER_XP_0)
        << "Consensus_log_manager::write_log_entry error, consensus index: "
        << *consensus_index << ", because of a failed term check.";
  return error;
}

int Consensus_log_manager::write_log_entries(std::vector<ConsensusLogEntry> &logs,
                                           uint64 *max_index) {
  int error = 0;
  // only follower will call this function
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  bool do_rotate = false;
  MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                           ? binlog
                           : &rli_info->relay_log;
  //disable rotate when still binlog, because not LOCK_LOG held
  enable_rotate =
      (!(logs.back().flag & Consensus_log_event_flag::FLAG_LARGE_TRX) && status != BINLOG_WORKING);
  if ((error = log->append_multi_consensus_logs(logs, max_index, &do_rotate,
                                                rli_info))) {
    goto end;
  }
  if (do_rotate && recovery_manager->is_pending_recovering_trx_empty() &&
      enable_rotate) {
    Master_info *mi = rli_info->mi;
    mysql_mutex_lock(&mi->data_lock);
    if ((error = rotate_relay_log(rli_info->mi))) {
      mysql_mutex_unlock(&mi->data_lock);
      goto end;
    } else if (opt_cluster_log_type_instance) {
      rli_info->relay_log.auto_purge();
    }
    mysql_mutex_unlock(&mi->data_lock);
  }
end:
  if (error)
    xp::error(ER_XP_0)
        << "Consensus_log_manager::write_log_entries error, batch size: "
        << logs.size() << " ,  max consensus index: " << *max_index;
  return error;
}

int Consensus_log_manager::get_log_directly(uint64 consensus_index,
                                            uint64 *consensus_term,
                                            std::string &log_content,
                                            bool *outer, uint *flag,
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
  MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_get_log_entry(consensus_index, consensus_term, log_content,
                                   outer, flag, need_content)) {
    xp::error(ER_XP_0)
        << "Consensus_log_manager::get_log_directly error,  consensus index: "
        << consensus_index;
    error = 1;
  }
  return error;
}

int Consensus_log_manager::prefetch_log_directly(THD *thd, uint64 channel_id,
                                               uint64 consensus_index) {
  int error = 0;
  auto consensus_guard = create_lock_guard(
    [&] { rdlock_consensus_status(); },
    [&] { unlock_consensus_status(); }
  );
  MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_prefetch_log_entries(thd, channel_id, consensus_index))
    error = 1;
  if (error)
    xp::warn(ER_XP_0) << "Consensus_log_manager::prefetch_log_directly error,  "
                          "consensus index: "
                       << consensus_index;
  return error;
}

int Consensus_log_manager::get_log_entry(uint64 channel_id,
                                       uint64 consensus_index,
                                       uint64 *consensus_term,
                                       std::string &log_content, bool *outer,
                                       uint *flag,
                                       bool fast_fail) {
  int error = 0;
  if (consensus_index > cache_index) return 1;

  if (consensus_index == 0) {
    *consensus_term = 0;
    log_content = "";
    return 0;
  }

  error = cache_manager->get(consensus_index, consensus_term, log_content,
                             outer, flag);

  DBUG_EXECUTE_IF("get_log_from_fifo_fail_when_blob_end",
                  error = CLC_ALREADY_SWAP_OUT;);
  if (error == CLC_ALREADY_SWAP_OUT) {
    uint64_t last_sync_index = sync_index;
    if (consensus_index > last_sync_index) {
      // don't prefetch log if it is not written to disk
      xp::info(ER_XP_0)
          << "Consensus_log_manager::get_log_entry " << consensus_index
          << " fail, log has not been flushed to disk, sync index is "
          << last_sync_index;
      return 1;
    }
    Consensus_prefetch_channel *channel =
        prefetch_manager->get_prefetch_channel(channel_id);
    if ((error = channel->get_log_from_prefetch_cache(
             consensus_index, consensus_term, log_content, outer, flag))) {
      if (!fast_fail) {
        error = get_log_directly(consensus_index, consensus_term, log_content,
                                 outer, flag, channel_id == 0 ? false : true);
      } else {
        error = 1;
      }
      channel->set_prefetch_request(consensus_index);
    }
  }
  if (error == 1)
    xp::info(ER_XP_0) << "Consensus_log_manager::get_log_entry fail, channel_id "
                      << channel_id << " consensus index: " << consensus_index
                      << " ,start prefetch.";
  return error;
}

uint64_t Consensus_log_manager::get_left_log_size(uint64 start_log_index,
                                                uint64 max_packet_size) {
  uint64 total_size = 0;
  total_size = cache_manager->get_log_size(start_log_index, cache_index,
                                           max_packet_size);
  // do not consider prefetch cache here
  return total_size;
}

int Consensus_log_manager::get_log_position(uint64 consensus_index,
                                          bool need_lock, char *log_name,
                                          uint64 *pos) {
  int error = 0;
  if (need_lock) rdlock_consensus_status();
  MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                           ? binlog
                           : &(rli_info->relay_log);

  if (log->consensus_get_log_position(consensus_index, log_name, pos)) {
    error = 1;
  }
  if (need_lock) unlock_consensus_status();
  if (error)
    xp::error(ER_XP_0)
        << "Consensus_log_manager::get_log_position error, consensus index: "
        << consensus_index;
  return error;
}

uint64 Consensus_log_manager::get_next_trx_index(uint64 consensus_index, bool enable_retry/*  = true */) {
  uint64 retIndex = consensus_index;
  int curr_retry = 0;
  if (consensus_index != 0) {
    const int max_retry_count = 2 * opt_consensus_max_wait_seconds_for_next_trx_index;
    while (curr_retry <= max_retry_count) {
      auto consensus_guard = create_lock_guard(
        [&] { rdlock_consensus_status(); },
        [&] { unlock_consensus_status(); }
      );
      MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                              ? binlog
                              : &(rli_info->relay_log);

      if (log->get_trx_end_index(consensus_index, retIndex) == 0) {
        break;
      } else if (retIndex > 0 && enable_retry) {
        consensus_guard.unlock();
        curr_retry++;
        xp::error(ER_XP_0) << "fail to find next trx index, retry after 500ms, current try " << curr_retry;
        my_sleep(500 * 1000);/* 500ms */
      } else {
        xp::error(ER_XP_0) << "fail to find next trx index from " << consensus_index;
        _exit(2);  // Using _exit(), since exit() is not async signal safe
      }
    }
  }
  xp::system(ER_XP_0) << "get_next_trx_index"
                    << ", curr_retry: " << curr_retry
                    << ", input index: " << consensus_index
                    << ", next transaction index is " << retIndex + 1;
  return retIndex + 1;
}

uint32 Consensus_log_manager::serialize_cache(uchar **buffer) {
  my_off_t buf_size = my_b_tell(cache_log->get_io_cache());
  *buffer = (uchar *)my_malloc(key_memory_thd_main_mem_root, (size_t)buf_size,
                               MYF(MY_WME));
  if (reinit_io_cache(cache_log->get_io_cache(), READ_CACHE, 0, 0, 0)) return 0;
  my_b_read(cache_log->get_io_cache(), *buffer, (size_t)buf_size);
  if (reinit_io_cache(cache_log->get_io_cache(), WRITE_CACHE, 0, 0, 1))
    return 0;
  return buf_size;
}

int Consensus_log_manager::truncate_log(uint64 consensus_index) {
  int error = 0;

  xp::system(ER_XP_0)
      << "Consensus_log_manager::truncate_log before"
      << ", error: " << error
      << ", consensus index: " << consensus_index
      << ", status: " << status
      << ", applier_reader relaylog name: " 
      << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->get_log_info()->log_file_name : "")
      << ", applier_reader relaylog position: " 
      << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->relaylog_reader_position() : 0)
      << ", relay_log name: " << (rli_info ? rli_info->relay_log.get_binlog_file()->get_binlog_name() : 0)
      << ", relay_log position: " << (rli_info ? rli_info->relay_log.get_binlog_file()->position() : 0)
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
    _exit(2);  // Using _exit(), since exit() is not async signal safe
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
        _exit(2);  // Using _exit(), since exit() is not async signal safe
      }
    }

    if (recovery_manager->get_last_leader_term_index() >= consensus_index)
      recovery_manager->set_last_leader_term_index(consensus_index - 1);
    rli_info->notify_relay_log_truncated();
  }

  // truncate commit map
  recovery_manager->truncate_pending_recovering_trxs(consensus_index);

  // truncate cache
  set_cache_index(consensus_index - 1);
  cache_manager->evict_newest_until(consensus_index);
  prefetch_manager->trunc_log_from_prefetch_cache(consensus_index);
  log_guard.unlock();

  consensus_guard.unlock();
  prefetch_manager->start_prefetch_threads();

  xp::system(ER_XP_0)
      << "Consensus_log_manager::truncate_log after"
      << ", error: " << error
      << ", consensus index: " << consensus_index
      << ", status: " << status
      << ", applier_reader relaylog name: " 
      << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->get_log_info()->log_file_name : "")
      << ", applier_reader relaylog position: " 
      << ((rli_info && rli_info->applier_reader) ? rli_info->applier_reader->relaylog_reader_position() : 0)
      << ", relay_log name: " << (rli_info ? rli_info->relay_log.get_binlog_file()->get_binlog_name() : 0)
      << ", relay_log position: " << (rli_info ? rli_info->relay_log.get_binlog_file()->position() : 0)
      << ", binlog name: " << binlog->get_binlog_file()->get_binlog_name()
      << ", binlog->position: " << binlog->get_binlog_file()->position();

  return error;
}

int Consensus_log_manager::purge_log(uint64 consensus_index) {
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
    purge_index = m_is_logger
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
    if (!m_is_logger && rli_info->applier_reader) {
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
        << "Consensus_log_manager::purge_log error, consensus index: "
        << consensus_index;
  return error;
}

uint64 Consensus_log_manager::get_exist_log_length() {
  // first index in log index may not be exact value
  // so we should check whether sync_index is larger
  uint64 start_index = log_file_index->get_first_index();
  uint64 end_index = sync_index;
  if (end_index >= start_index)
    return end_index - start_index;
  else
    return 0;
}

int Consensus_log_manager::start_consensus_commit_pos_watcher() {
  if (!opt_commit_pos_watcher) return 0;

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

void Consensus_log_manager::stop_consensus_commit_pos_watcher() {
  if (consensus_commit_pos_watcher_is_running) {
    consensus_commit_pos_watcher_is_running = false;
    my_thread_join(&consensus_commit_pos_watcher_thread_handle, NULL);
  }
}

void Consensus_log_manager::update_commit_pos(const std::string &log_name,
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

void Consensus_log_manager::get_commit_pos(char *const fname_ptr,
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

void Consensus_log_manager::wait_replay_log_finished() {
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

void Consensus_log_manager::wait_apply_threads_start() {
  while (rli_info && !rli_info->slave_running) {
    my_sleep(200);
  }
}

void Consensus_log_manager::wait_apply_threads_stop() {
  while (rli_info && rli_info->slave_running) {
    my_sleep(200);
  }
}

int Consensus_log_manager::wait_leader_degraded(uint64 term, uint64 index) {
  xp::system(ER_XP_0)
      << "Consensus_log_manager::wait_leader_degraded, consensus term: " << term
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

  assert(this->status == Consensus_Log_State::BINLOG_WORKING);
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
  channel_add_executed_gtids_to_xpaxos_received_gtids(rli_info);
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
  if (!m_is_logger) {
    consensus_info->set_start_apply_index(index);
  }
  if (consensus_info->flush_info(true, true)) {
    xp::error(ER_XP_0) << "Failed in flush_info() called from "
                          "ConsensusLog::wait_leader_degraded.";
    error = 1;
    goto end;
  }
  if (!m_is_logger) {
    stop_slave_threads();
    start_consensus_apply_threads();
  }

  consensus_guard.unlock();

  if (!m_is_logger) {
    wait_apply_threads_start();
  }
end:
  consensus_guard.unlock();
  xp::system(ER_XP_0)
      << "Consensus_log_manager::wait_leader_degraded finish, error " << error;
  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int Consensus_log_manager::wait_follower_upgraded(uint64 term, uint64 index) {
  xp::system(ER_XP_0)
      << "Consensus_log_manager::wait_follower_upgraded, consensus term: " << term
      << ", consensus index: " << index;
  int error = 0;
  mysql_mutex_t *log_lock = nullptr;

  assert(this->status == Consensus_Log_State::RELAY_LOG_WORKING);

  // record new term
  // notice, the order of stop term and current term is important for apply
  // thread, because both stop term and current term is atomic variables. so do
  // not care reorder problem
  stop_term = term;

  // wait replay thread to commit index
  if (!m_is_logger) {
    DBUG_EXECUTE_IF("simulate_apply_too_slow", my_sleep(5000000););
    wait_replay_log_finished();
    wait_apply_threads_stop();
  }

  // wait recover trx all finished
  while (!recovery_manager->is_pending_recovering_trx_empty()) {
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
  if (binlog->open_index_file(log_bin_index, opt_bin_logname, true)) {
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
  m_applied_index_checker->reset();
  set_event_timestamp(0);
  set_consensus_system_status(BINLOG_WORKING);
  /** Switch finish, add into switch history. */
  xlog_engine_get_protocol()->HaHistoryAdd(true);

  // reset apply start point displayed in information_schema
  apply_index = 0;
  real_apply_index = 0;
  already_set_start_index = false;
  already_set_start_term = false;

  current_term = term;

  // log type instance do not to recover start index
  if (!m_is_logger) {
    consensus_info->set_last_leader_term(term);
    update_applied_index(xlog_get_paxos_commit_index());
  }
  consensus_info->set_recover_status(
      Consensus_Log_State::BINLOG_WORKING);

  if (consensus_info->flush_info(true, true)) {
    xp::error(ER_XP_0) << "Failed in flush_info() called from "
                          "ConsensusLog::wait_follower_upgraded.";
    error = 3;
    goto end;
  }

  if (!m_is_logger) {
    start_slave_threads();
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
      << "Consensus_log_manager::wait_follower_upgraded finish, error " << error;

  // recover prefetch
  prefetch_manager->enable_all_prefetch_channels();
  return error;
}

int Consensus_log_manager::wait_follower_change_term(uint64 term) {
  xp::system(ER_XP_0)
      << "Consensus_log_manager::wait_follower_change_term, consensus term: "
      << term;
  current_term = term;
  return 0;
}

uint64 Consensus_log_manager::get_cache_index() { return cache_index; }

void Consensus_log_manager::set_cache_index(uint64 cache_index_arg) {
  cache_index = cache_index_arg;
}

uint64 Consensus_log_manager::get_sync_index(bool serious) {
  if (!serious) {
    return sync_index;
  } else {
    /* currently, never cover this path, deadlock risk */
    auto consensus_guard = create_lock_guard(
      [&] { rdlock_consensus_status(); },
      [&] { unlock_consensus_status(); }
    );
    MYSQL_BIN_LOG *log = status == Consensus_Log_State::BINLOG_WORKING
                             ? binlog
                             : &(rli_info->relay_log);
    log->wait_xid_disappear();
    return sync_index;
  }
}

uint64 Consensus_log_manager::get_final_sync_index() {
  mysql_mutex_lock(xlog_log_manager_get_sequence_stage1_lock());
  const uint64_t final_sync_index = current_index ? current_index - 1 : 0;
  mysql_mutex_unlock(xlog_log_manager_get_sequence_stage1_lock());
  return final_sync_index;
}

uint64 Consensus_log_manager::get_final_sync_index_no_lock() {
  const uint64_t final_sync_index = (current_index.load() - 1);
  return final_sync_index;
}

void Consensus_log_manager::set_sync_index(uint64 sync_index_arg) {
  sync_index = sync_index_arg;
}

void Consensus_log_manager::set_sync_index_if_greater(uint64 sync_index_arg) {
  for (;;) {
    uint64 old = sync_index.load();
    if (old >= sync_index_arg ||
        (old < sync_index_arg &&
         sync_index.compare_exchange_weak(old, sync_index_arg)))
      break;
  }
}

void *run_consensus_commit_pos_watcher(void *arg) {
  xp::system(ER_XP_0) << "start consensus_commit_pos_watcher thread";
  binlog_commit_pos_watcher((bool *)arg);
  xp::system(ER_XP_0) << "stop consensus_commit_pos_watcher thread";
  return NULL;
}

bool Consensus_log_manager::is_state_machine_ready() {
  return status == Consensus_Log_State::BINLOG_WORKING &&
         xlog_get_paxos_term() == get_current_term();
}

void Consensus_log_manager::force_update_applied_index(const uint64_t index) {
  if (opt_enable_appliedindex_checker) {
    m_applied_index_checker->commit(index);
  } else {
    update_applied_index(index);
  }
}

IO_CACHE *Consensus_log_manager::get_cache() { return cache_log->get_io_cache(); }

int Consensus_log_manager::check_empty_binlog() {
  std::vector<std::string> binlog_file_list;
  binlog->get_consensus_log_file_list(binlog_file_list);
  if (binlog_file_list.empty()) {
    /*
      Configures what object is used by the current log to store processed
      gtid(s). This is necessary in the MYSQL_BIN_LOG::MYSQL_BIN_LOG to
      correctly compute the set of previous gtids.
    */
    assert(!binlog->is_relay_log);
    MUTEX_LOCK(guard, binlog->get_log_lock());
    if (binlog->open_binlog(opt_bin_logname, nullptr, max_binlog_size,
                                  false, true /*need_lock_index=true*/,
                                  true /*need_sid_lock=true*/, nullptr)) {
      return 1;
    }
  }
  return 0;
}

int Consensus_log_manager::dump_consensus_info() {
  std::string meta_file_name = "consensus.meta";
  std::ostringstream oss;
  oss << "Consensus_apply_index: " << rli_info->get_consensus_apply_index() << "\n" 
      << "Consensus_cluster_info: " << consensus_info->get_cluster_info() << "\n"
      << "Consensus_learner_info: " << consensus_info->get_cluster_learner_info() << "\n"
      << "Cluster_id: " << consensus_info->get_cluster_id() << "\n"
      << "Current_term: " << consensus_info->get_current_term() << "\n"
      << "Recover_status: " << consensus_info->get_recover_status() << "\n"
      << "Last_leader_term: " << consensus_info->get_last_leader_term() << "\n"
      << "Start_apply_index: " << consensus_info->get_start_apply_index() << "\n"
      << "Binlog_sync_index: " << get_sync_index() << "\n"
      << "Pid: " << getpid() << "\n";
  const int dump_result =  dump_cluster_info_to_file(meta_file_name, oss.str());
  xp::system(ER_XP_0) << "Dump meta file(" << meta_file_name 
                      << ") " << (dump_result < 0 ? "failed" : "success")
                      << " - " << oss.str();
  return 0;
}

}
