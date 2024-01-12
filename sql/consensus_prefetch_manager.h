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
#ifndef MYSQL_CONSENSUS_PREFETCH_MANAGER_H
#define MYSQL_CONSENSUS_PREFETCH_MANAGER_H

#include <atomic>
#include <iterator>
#include <map>
#include <queue>
#include <thread>
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysqld.h"

#include "consensus_fifo_cache_manager.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_thread_key key_thread_prefetch;
extern PSI_rwlock_key key_rwlock_ConsensusLog_prefetch_channels_hash;
#endif

class ConsensusPreFetchChannel {
 public:
  ConsensusPreFetchChannel() : inited(false) {}
  ~ConsensusPreFetchChannel() {}

  int init(uint64 channel_id, uint64 max_cache_size,
           uint64 prefetch_window_size, uint64 prefetch_wakeup_ratio);
  int cleanup();

  int add_log_to_prefetch_cache(uint64 term, uint64 index, size_t buf_size,
                                uchar *buffer, bool outer, uint flag,
                                uint64 checksum);
  int get_log_from_prefetch_cache(uint64 index, uint64 *term,
                                  std::string &log_content, bool *outer,
                                  uint *flag, uint64 *checksum);
  bool log_exist(uint64 index);
  int reset_prefetch_cache();

  int truncate_prefetch_cache(uint64 index);

  int set_prefetch_request(uint64 index);
  uint64 get_prefetch_request();
  int clear_prefetch_request();

  void set_max_prefetch_cache_size(uint64 max_prefetch_cache_size_arg) {
    max_prefetch_cache_size = max_prefetch_cache_size_arg;
  }

  uint64 get_channel_id() { return channel_id; }
  uint64 get_first_index_in_cache() { return first_index_in_cache; }
  uint64 get_last_index_in_cache() {
    mysql_mutex_lock(&LOCK_prefetch_channel);
    uint64 last_index = first_index_in_cache + prefetch_cache.size() - 1;
    mysql_mutex_unlock(&LOCK_prefetch_channel);
    return last_index;
  }
  uint64 get_prefetch_cache_size() { return prefetch_cache_size; }
  PSI_thread_key get_PSI_thread_key() { return prefetch_thread_key; }
  uint64 get_current_request() {
    uint64 index = 0;
    mysql_mutex_lock(&LOCK_prefetch_request);
    index = current_prefetch_request;
    mysql_mutex_unlock(&LOCK_prefetch_request);
    return index;
  }

  int enable_prefetch_channel();

  int disable_prefetch_channel();

  int start_prefetch_thread();

  int stop_prefetch_thread();

  void set_prefetching(bool prefetching_arg) { prefetching = prefetching_arg; }

  void set_window_size(uint64 window_size_arg) {
    window_size = window_size_arg;
  }

  uint64 get_window_size() { return window_size; }

  void set_wakeup_ratio(uint64 wakeup_ratio_arg) {
    wakeup_ratio = wakeup_ratio_arg;
  }

  void set_stop_prefetch_request(bool flag) { stop_prefetch_request = flag; }

  bool get_stop_preftch_request() { return stop_prefetch_request; }

  void inc_ref_count() { ref_count++; }

  void dec_ref_count() { ref_count--; }

  void add_log_to_large_trx_table(uint64 term, uint64 index, bool outer,
                                  uint flag);
  void clear_large_trx_table();

  friend void *run_prefetch(void *arg);

 private:
  PSI_thread_key prefetch_thread_key;
  PSI_memory_key key_memory_prefetch_mem_root;
  PSI_mutex_key key_LOCK_prefetch_channel;
  PSI_mutex_key key_LOCK_prefetch_request;
  PSI_cond_key key_COND_prefetch_channel_cond;
  PSI_cond_key key_COND_prefetch_request_cond;

  PSI_mutex_key key_LOCK_prefetch_request_queue;

  mysql_mutex_t LOCK_prefetch_channel;  // used to protect prefetch
  mysql_mutex_t LOCK_prefetch_request;
  mysql_cond_t COND_prefetch_channel;  // used to control prefetch thread
  mysql_cond_t COND_prefetch_request;  // used to control prefetch thread
  std::deque<ConsensusLogEntry> prefetch_cache;
  std::atomic<uint64> current_prefetch_request;  // current request log index
  bool stop_prefetch_flag;  // used to stop prefetch when truncate log
  bool from_beginning;
  std::atomic<uint64> max_prefetch_cache_size;
  std::atomic<uint64> first_index_in_cache;
  std::atomic<uint64> prefetch_cache_size;
  my_thread_handle prefetch_thread_handle;
  bool is_running;  // used to coordinate prefetch thread
  bool inited;
  std::atomic<bool> prefetching;
  std::atomic<bool> stop_prefetch_request;
  std::atomic<uint64> window_size;
  std::atomic<uint64> wakeup_ratio;
  std::atomic<int>
      ref_count;  // used to determin whether delete channel is safe
  uint64 channel_id;

  std::map<uint64, ConsensusLogEntry> large_trx_table;
};

class ConsensusPreFetchManager {
 public:
  ConsensusPreFetchManager() : inited(false) {}
  ~ConsensusPreFetchManager() {}

  int init(uint64 max_prefetch_cache_size_arg);
  int cleanup();

  int trunc_log_from_prefetch_cache(uint64 index);
  ConsensusPreFetchChannel *get_prefetch_channel(uint64 channel_id);
  int drop_prefetch_channel(uint64 channel_id);

  int reset_prefetch_cache();

  int set_max_prefetch_cache_size(uint64 max_prefetch_cache_size_arg);

  int set_prefetch_window_size(uint64 prefetch_window_size_arg);

  int set_prefetch_wakeup_ratio(uint64 prefetch_wakeup_ratio_arg);

  int enable_all_prefetch_channels();

  int disable_all_prefetch_channels();

  int start_prefetch_threads();

  int stop_prefetch_threads();

  const std::map<uint64, ConsensusPreFetchChannel *> *get_channels_hash()
      const {
    return &prefetch_channels_hash;
  }

  int lock_prefetch_channels_hash(bool is_readlock) {
    if (is_readlock)
      return mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
    else
      return mysql_rwlock_wrlock(&LOCK_prefetch_channels_hash);
  }

  int unlock_prefetch_channels_hash() {
    return mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  }

  int stop_prefetch_channel_request(uint64 channel_id, bool flag);

 private:
  bool inited;
  PSI_rwlock_key key_LOCK_prefetch_channels_hash;
  mysql_rwlock_t LOCK_prefetch_channels_hash;  // used to protect log cache
  std::map<uint64, ConsensusPreFetchChannel *> prefetch_channels_hash;
  std::atomic<uint64> max_prefetch_cache_size;
  std::atomic<uint64> prefetch_window_size;
  std::atomic<uint64> prefetch_wakeup_ratio;
};

void *run_prefetch(void *arg);

#endif  // MYSQL_CONSENSUS_PREFETCH_MANAGER_H
