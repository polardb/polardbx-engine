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
#include "consensus_prefetch_manager.h"
#include "binlog.h"
#include "consensus_log_manager.h"
#include "log.h"
#include "mysql/thread_pool_priv.h"
#include "sql/consensus/consensus_err.h"
#include "sql/sql_thd_internal_api.h"
#include "storage/innobase/include/ut0dbg.h"

#ifdef HAVE_PSI_INTERFACE
PSI_thread_key key_thread_prefetch;
PSI_rwlock_key key_rwlock_ConsensusLog_prefetch_channels_hash;
#endif

int ConsensusPreFetchChannel::init(uint64 id, uint64 max_cache_size,
                                   uint64 prefetch_window_size,
                                   uint64 prefetch_wakeup_ratio) {
  if (!inited) {
    channel_id = id;
    max_prefetch_cache_size = max_cache_size;
    mysql_mutex_init(key_LOCK_prefetch_channel, &LOCK_prefetch_channel,
                     MY_MUTEX_INIT_FAST);
    mysql_mutex_init(key_LOCK_prefetch_request, &LOCK_prefetch_request,
                     MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_COND_prefetch_channel_cond, &COND_prefetch_channel);
    mysql_cond_init(key_COND_prefetch_request_cond, &COND_prefetch_request);
    first_index_in_cache = 0;
    is_running = true;
    stop_prefetch_flag = false;
    current_prefetch_request = 0;
    prefetch_cache_size = 0;
    prefetching = false;
    stop_prefetch_request = false;
    from_beginning = false;
    window_size = prefetch_window_size;
    wakeup_ratio = prefetch_wakeup_ratio;
    ref_count = 0;
    if (mysql_thread_create(key_thread_prefetch, &prefetch_thread_handle, NULL,
                            run_prefetch, (void *)this)) {
      xp::error(ER_XP_PREFETCH) << "Fail to create thread run_prefetch.";
      abort();
    }
    inited = true;
  }
  return 0;
}

int ConsensusPreFetchChannel::cleanup() {
  if (inited) {
    mysql_mutex_lock(&LOCK_prefetch_request);
    is_running = false;
    current_prefetch_request = 0;
    mysql_mutex_unlock(&LOCK_prefetch_request);
    mysql_mutex_lock(&LOCK_prefetch_channel);
    stop_prefetch_flag = true;
    from_beginning = true;
    mysql_mutex_unlock(&LOCK_prefetch_channel);
    mysql_cond_broadcast(&COND_prefetch_channel);
    mysql_cond_broadcast(&COND_prefetch_request);
    my_thread_join(&prefetch_thread_handle, NULL);
    // wait all get request end
    while (ref_count != 0) {
      my_sleep(1000);
    }
    mysql_mutex_destroy(&LOCK_prefetch_channel);
    mysql_mutex_destroy(&LOCK_prefetch_request);
    mysql_cond_destroy(&COND_prefetch_channel);
    mysql_cond_destroy(&COND_prefetch_request);
    prefetching = false;
    inited = false;
  }
  return 0;
}

int ConsensusPreFetchChannel::add_log_to_prefetch_cache(
    uint64 term, uint64 index, size_t buf_size, uchar *buffer, bool outer,
    uint flag, uint64 checksum) {
  mysql_mutex_lock(&LOCK_prefetch_channel);

  if (stop_prefetch_flag || from_beginning) {
    mysql_mutex_unlock(&LOCK_prefetch_channel);
    xp::info(ER_XP_PREFETCH)
        << "channel_id " << channel_id
        << " prefetch stop, stop_prefetch_flag is " << stop_prefetch_flag
        << ", from_beginning is " << from_beginning;
    return INTERRUPT;
  }

  if ((index >= first_index_in_cache) &&
      (index < first_index_in_cache + prefetch_cache.size())) {
    // already exist in prefetch cache
    mysql_mutex_unlock(&LOCK_prefetch_channel);
    return SUCCESS;
  } else if (first_index_in_cache != 0 &&
             (index < first_index_in_cache ||
              index > first_index_in_cache + prefetch_cache.size())) {
    xp::info(ER_XP_PREFETCH)
        << "Consensus prefetch add cache not fit the "
           "range , channel_id "
        << channel_id << " the first index in cache is  "
        << first_index_in_cache.load() << ", the last index is "
        << first_index_in_cache + prefetch_cache.size()
        << ", the required index is " << index;
    for (auto iter = prefetch_cache.begin(); iter != prefetch_cache.end();
         ++iter) {
      if (iter->buf_size > 0) my_free(iter->buffer);
    }
    prefetch_cache.clear();
    first_index_in_cache = 0;
    prefetch_cache_size = 0;
  }

  while (prefetch_cache_size + buf_size > max_prefetch_cache_size &&
         prefetch_cache.size() > 0) {
    if (index > current_prefetch_request.load()) {
      // xp::info(ER_XP_PREFETCH)  << "prefetch cache wait full"
      //                 << ", channel_id " << channel_id
      //                 << ", input_index " << index
      //                 << ", start_index " << first_index_in_cache
      //                 << ", current_prefetch_request " <<
      //                 current_prefetch_request.load()
      //                 << " " << get_backtrace_str();
      if ((!stop_prefetch_flag) && (!from_beginning))
        mysql_cond_wait(&COND_prefetch_channel, &LOCK_prefetch_channel);
      mysql_mutex_unlock(&LOCK_prefetch_channel);
      return FULL;
    } else {
      /* decrease the window */
      /*
      sql_print_information("Consensus prefetch is full but still in window, "
          "channel_id %llu the first index in cache is  %llu, the last index is
      %llu, " "the required index is %llu, the last request index is %llu, "
          "Just shrunk the window temporarily.\n",
          channel_id, first_index_in_cache.load(), first_index_in_cache +
      prefetch_cache.size() -1, index, current_prefetch_request.load());
      */
      ConsensusLogEntry &old_log = prefetch_cache.front();
      if (old_log.buf_size > 0) my_free(old_log.buffer);
      prefetch_cache_size -= old_log.buf_size;
      prefetch_cache.pop_front();
      first_index_in_cache++;
    }
  }

  if (prefetch_cache.size() == 0) {
    first_index_in_cache = index;
  } else {
    ConsensusLogEntry &old_log = prefetch_cache.back();
    if (old_log.index + 1 != index) {
      mysql_mutex_unlock(&LOCK_prefetch_channel);
      xp::error(ER_XP_PREFETCH)
          << "prefetch cache add invalid index, need strictly increasing"
          << ", channel_id " << channel_id << ", input_index " << index
          << ", start_index " << first_index_in_cache << ", end_index "
          << old_log.index << ", cache count " << prefetch_cache.size()
          << ", prefetch_cache_size " << prefetch_cache_size << " "
          << get_backtrace_str();
      ut_a(old_log.index + 1 == index);
      return INTERRUPT;
    }
  }

  uchar *new_buffer = (uchar *)my_memdup(key_memory_prefetch_mem_root,
                                         (char *)buffer, buf_size, MYF(MY_WME));
  ConsensusLogEntry new_log = {term,  index, buf_size, new_buffer,
                               outer, flag,  checksum};
  prefetch_cache.push_back(new_log);
  prefetch_cache_size += buf_size;

  mysql_mutex_unlock(&LOCK_prefetch_channel);

  // xp::info(ER_XP_FIFO) << "add_log_to_prefetch_cache"
  //   << ", term " << term
  //   << ", index " << index
  //   << ", flag " << flag
  //   << ", checksum " << checksum
  //   << ", first_index_in_cache " << first_index_in_cache
  //   << ", current_log_count " << prefetch_cache.size()
  //   << ", prefetch_cache_size " << prefetch_cache_size
  //   << ", " << get_backtrace_str();

  return SUCCESS;
}

void ConsensusPreFetchChannel::add_log_to_large_trx_table(uint64 term,
                                                          uint64 index,
                                                          bool outer,
                                                          uint flag) {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  assert(flag & (Consensus_log_event_flag::FLAG_LARGE_TRX |
                 Consensus_log_event_flag::FLAG_LARGE_TRX_END));
  ConsensusLogEntry new_log = {term, index, 0, NULL, outer, flag, 0};
  large_trx_table.insert(std::make_pair(index, new_log));
  mysql_mutex_unlock(&LOCK_prefetch_channel);
}

void ConsensusPreFetchChannel::clear_large_trx_table() {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  large_trx_table.clear();
  mysql_mutex_unlock(&LOCK_prefetch_channel);
}

int ConsensusPreFetchChannel::get_log_from_prefetch_cache(
    uint64 index, uint64 *term, std::string &log_content, bool *outer,
    uint *flag, uint64 *checksum) {
  int error = SUCCESS;
  mysql_mutex_lock(&LOCK_prefetch_channel);

  if (channel_id == 0) {
    /* quick path for channel 0 large trx */
    auto it = large_trx_table.find(index);
    if (it != large_trx_table.end()) {
      *term = it->second.term;
      *outer = it->second.outer;
      *flag = it->second.flag;
      ut_a(index == it->second.index);
      log_content.assign("");
      xp::info(ER_XP_PREFETCH)
          << "Consensus prefetch cache: get large trx consensus log(" << index
          << ") from large_trx_table.";
      mysql_mutex_unlock(&LOCK_prefetch_channel);
      dec_ref_count();
      return SUCCESS;
    }
  }

  if (max_prefetch_cache_size == 0 ||
      first_index_in_cache == 0 /* if fifo cache is empty */) {
    error = EMPTY;
  } else if (index < first_index_in_cache) {
    xp::info(ER_XP_PREFETCH)
        << "Consensus prefetch cache already swap out , channel_id "
        << channel_id << " the first index in cache is  "
        << first_index_in_cache.load() << ", the required index is " << index;
    for (auto iter = prefetch_cache.begin(); iter != prefetch_cache.end();
         ++iter) {
      if (iter->buf_size > 0) my_free(iter->buffer);
    }
    prefetch_cache.clear();
    first_index_in_cache = 0;
    prefetch_cache_size = 0;
    error = ALREADY_SWAP_OUT;
  } else if (index >= first_index_in_cache + prefetch_cache.size()) {
    xp::info(ER_XP_PREFETCH)
        << "Consensus prefetch cache out of range , channel_id " << channel_id
        << " the max index in cache is  "
        << first_index_in_cache + prefetch_cache.size() - 1
        << ", the required index is " << index;
    while (prefetch_cache.size() > window_size) {
      ConsensusLogEntry &old_log = prefetch_cache.front();
      if (old_log.buf_size > 0) my_free(old_log.buffer);
      prefetch_cache_size -= old_log.buf_size;
      prefetch_cache.pop_front();
      first_index_in_cache++;
    }
    error = OUT_OF_RANGE;
  } else {
    ConsensusLogEntry &log_entry =
        prefetch_cache.at(index - first_index_in_cache);
    *term = log_entry.term;
    *outer = log_entry.outer;
    *flag = log_entry.flag;
    *checksum = log_entry.checksum;
    ut_a(log_entry.index == index);
    log_content.assign((char *)(log_entry.buffer), log_entry.buf_size);

    // truncate before , retain window_size or 1/2 max_cacache_size
    // because consensus layer will always fetch index and index-1
    // at least retain two entries
    while (first_index_in_cache + window_size < index ||
           (first_index_in_cache + 1 < index &&
            prefetch_cache_size * 2 > max_prefetch_cache_size)) {
      ConsensusLogEntry &old_log = prefetch_cache.front();
      if (old_log.buf_size > 0) my_free(old_log.buffer);
      prefetch_cache_size -= old_log.buf_size;
      prefetch_cache.pop_front();
      first_index_in_cache++;
    }
  }

  // wakeup prefetch thread
  if (error == ALREADY_SWAP_OUT)
    from_beginning = true;
  else
    from_beginning = false;

  mysql_mutex_unlock(&LOCK_prefetch_channel);

  if (prefetch_cache_size * wakeup_ratio <= max_prefetch_cache_size ||
      error == OUT_OF_RANGE || error == ALREADY_SWAP_OUT)
    mysql_cond_broadcast(&COND_prefetch_channel);
  dec_ref_count();  // decrease ref count
  return error;
}

bool ConsensusPreFetchChannel::log_exist(uint64 index) {
  bool exist = false;
  mysql_mutex_lock(&LOCK_prefetch_channel);
  if (index >= first_index_in_cache &&
      index < first_index_in_cache + prefetch_cache.size()) {
    exist = true;
  }
  mysql_mutex_unlock(&LOCK_prefetch_channel);
  return exist;
}

int ConsensusPreFetchChannel::reset_prefetch_cache() {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  large_trx_table.clear();
  for (auto iter = prefetch_cache.begin(); iter != prefetch_cache.end();
       iter++) {
    if (iter->buf_size > 0) my_free(iter->buffer);
    prefetch_cache_size -= iter->buf_size;
  }
  prefetch_cache.clear();
  first_index_in_cache = 0;
  assert(prefetch_cache_size == 0);
  prefetch_cache_size = 0;
  from_beginning = true;
  mysql_mutex_unlock(&LOCK_prefetch_channel);
  mysql_cond_broadcast(&COND_prefetch_channel);
  return 0;
}

int ConsensusPreFetchChannel::enable_prefetch_channel() {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  stop_prefetch_flag = false;
  mysql_mutex_unlock(&LOCK_prefetch_channel);
  return 0;
}

int ConsensusPreFetchChannel::disable_prefetch_channel() {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  stop_prefetch_flag = true;
  mysql_mutex_unlock(&LOCK_prefetch_channel);
  mysql_cond_broadcast(&COND_prefetch_channel);
  return 0;
}

int ConsensusPreFetchChannel::start_prefetch_thread() {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  stop_prefetch_flag = false;
  mysql_mutex_unlock(&LOCK_prefetch_channel);

  mysql_mutex_lock(&LOCK_prefetch_request);
  is_running = true;
  mysql_mutex_unlock(&LOCK_prefetch_request);
  if (mysql_thread_create(key_thread_prefetch, &prefetch_thread_handle, NULL,
                          run_prefetch, (void *)this)) {
    xp::error(ER_XP_PREFETCH) << "Fail to create thread run_prefetch.";
    abort();
  }
  return 0;
}

int ConsensusPreFetchChannel::stop_prefetch_thread() {
  mysql_mutex_lock(&LOCK_prefetch_request);
  is_running = false;
  mysql_mutex_unlock(&LOCK_prefetch_request);
  mysql_cond_broadcast(&COND_prefetch_request);

  mysql_mutex_lock(&LOCK_prefetch_channel);
  stop_prefetch_flag = true;
  mysql_mutex_unlock(&LOCK_prefetch_channel);
  mysql_cond_broadcast(&COND_prefetch_channel);

  my_thread_join(&prefetch_thread_handle, NULL);
  return 0;
}

int ConsensusPreFetchChannel::truncate_prefetch_cache(uint64 index) {
  mysql_mutex_lock(&LOCK_prefetch_channel);
  /* truncate large_trx_table */
  if (channel_id == 0) {
    auto it = large_trx_table.lower_bound(index);
    large_trx_table.erase(it, large_trx_table.end());
  }
  xp::info(ER_XP_PREFETCH) << "Truncate prefetch before"
                           << ", channel " << channel_id << ", first index = ["
                           << first_index_in_cache.load() << "], end index = ["
                           << (prefetch_cache.empty()
                                   ? -1
                                   : prefetch_cache.back().index)
                           << "], cache count = [" << prefetch_cache.size()
                           << "]";
  if (max_prefetch_cache_size == 0 || prefetch_cache.size() == 0) {
    mysql_mutex_unlock(&LOCK_prefetch_channel);
    return 0;
  }

  if (index <= first_index_in_cache) {
    for (auto iter = prefetch_cache.begin(); iter != prefetch_cache.end();
         ++iter) {
      if (iter->buf_size > 0) my_free(iter->buffer);
    }
    prefetch_cache.clear();
    first_index_in_cache = 0;
    prefetch_cache_size = 0;
  } else if ((index > first_index_in_cache) &&
             (index < first_index_in_cache + prefetch_cache.size())) {
    while (index < first_index_in_cache + prefetch_cache.size()) {
      ConsensusLogEntry &old_log = prefetch_cache.back();
      if (old_log.buf_size > 0) my_free(old_log.buffer);
      prefetch_cache_size -= old_log.buf_size;
      prefetch_cache.pop_back();
    }
  }

  xp::info(ER_XP_PREFETCH) << "Truncate prefetch after"
                           << ", channel " << channel_id << ", first index = ["
                           << first_index_in_cache.load() << "], end index = ["
                           << (prefetch_cache.empty()
                                   ? -1
                                   : prefetch_cache.back().index)
                           << "], cache count = [" << prefetch_cache.size()
                           << "]";

  mysql_mutex_unlock(&LOCK_prefetch_channel);
  return 0;
}

int ConsensusPreFetchChannel::set_prefetch_request(uint64 index) {
  if (stop_prefetch_request || index == 0) return 0;
  mysql_mutex_lock(&LOCK_prefetch_request);
  current_prefetch_request = index;
  mysql_mutex_unlock(&LOCK_prefetch_request);
  mysql_cond_broadcast(&COND_prefetch_request);
  return 0;
}

uint64 ConsensusPreFetchChannel::get_prefetch_request() {
  // if queue is empty, this function will return 0
  uint64 index = 0;
  mysql_mutex_lock(&LOCK_prefetch_request);
  index = current_prefetch_request;
  if (index == 0) {
    // no current request
    mysql_cond_wait(&COND_prefetch_request, &LOCK_prefetch_request);
  }
  mysql_mutex_unlock(&LOCK_prefetch_request);
  return index;
}

int ConsensusPreFetchChannel::clear_prefetch_request() {
  mysql_mutex_lock(&LOCK_prefetch_request);
  current_prefetch_request = 0;
  mysql_mutex_unlock(&LOCK_prefetch_request);
  return 0;
}

int ConsensusPreFetchManager::init(uint64 max_prefetch_cache_size_arg) {
  if (!inited) {
    key_LOCK_prefetch_channels_hash =
        key_rwlock_ConsensusLog_prefetch_channels_hash;
    mysql_rwlock_init(key_LOCK_prefetch_channels_hash,
                      &LOCK_prefetch_channels_hash);
    max_prefetch_cache_size = max_prefetch_cache_size_arg;
    prefetch_window_size = 10;
    prefetch_wakeup_ratio = 2;
    inited = true;
  }
  return 0;
}

int ConsensusPreFetchManager::cleanup() {
  if (inited) {
    mysql_rwlock_destroy(&LOCK_prefetch_channels_hash);
    for (auto iter = prefetch_channels_hash.begin();
         iter != prefetch_channels_hash.end(); ++iter) {
      iter->second->cleanup();
      delete iter->second;
    }
    inited = false;
  }
  return 0;
}

int ConsensusPreFetchManager::trunc_log_from_prefetch_cache(uint64 index) {
  mysql_rwlock_wrlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->truncate_prefetch_cache(index);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

ConsensusPreFetchChannel *ConsensusPreFetchManager::get_prefetch_channel(
    uint64 channel_id) {
  ConsensusPreFetchChannel *channel = NULL;
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  auto it = prefetch_channels_hash.find(channel_id);
  if (it != prefetch_channels_hash.end()) {
    channel = it->second;
    channel->inc_ref_count();
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  if (channel == NULL) {
    // try create new channel
    mysql_rwlock_wrlock(&LOCK_prefetch_channels_hash);
    auto iit = prefetch_channels_hash.find(channel_id);
    if (iit == prefetch_channels_hash.end()) {
      channel = new ConsensusPreFetchChannel();
      channel->init(channel_id, max_prefetch_cache_size, prefetch_window_size,
                    prefetch_wakeup_ratio);
      prefetch_channels_hash.insert(
          std::map<uint64, ConsensusPreFetchChannel *>::value_type(channel_id,
                                                                   channel));
    } else {
      channel = iit->second;
    }
    channel->inc_ref_count();
    mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  }
  return channel;
}

int ConsensusPreFetchManager::reset_prefetch_cache() {
  int error = 0;
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    error = iter->second->reset_prefetch_cache();
    if (error) break;
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return error;
}

int ConsensusPreFetchManager::drop_prefetch_channel(uint64 channel_id) {
  int error = 0;
  ConsensusPreFetchChannel *channel = nullptr;
  mysql_rwlock_wrlock(&LOCK_prefetch_channels_hash);
  auto iter = prefetch_channels_hash.find(channel_id);
  if (iter == prefetch_channels_hash.end()) {
    error = 1;
  } else {
    channel = iter->second;
    prefetch_channels_hash.erase(channel_id);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  if (!error) {
    channel->cleanup();
    delete channel;
  }
  return error;
}

int ConsensusPreFetchManager::set_max_prefetch_cache_size(
    uint64 max_prefetch_cache_size_arg) {
  max_prefetch_cache_size = max_prefetch_cache_size_arg;
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->set_max_prefetch_cache_size(max_prefetch_cache_size_arg);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::set_prefetch_window_size(
    uint64 prefetch_window_size_arg) {
  prefetch_window_size = prefetch_window_size_arg;
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->set_window_size(prefetch_window_size_arg);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::set_prefetch_wakeup_ratio(
    uint64 prefetch_wakeup_ratio_arg) {
  prefetch_wakeup_ratio = prefetch_wakeup_ratio_arg;
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->set_wakeup_ratio(prefetch_wakeup_ratio_arg);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::stop_prefetch_channel_request(uint64 channel_id,
                                                            bool flag) {
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    if (iter->first == channel_id)
      iter->second->set_stop_prefetch_request(flag);
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::enable_all_prefetch_channels() {
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->enable_prefetch_channel();
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::disable_all_prefetch_channels() {
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->disable_prefetch_channel();
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::start_prefetch_threads() {
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->start_prefetch_thread();
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

int ConsensusPreFetchManager::stop_prefetch_threads() {
  mysql_rwlock_rdlock(&LOCK_prefetch_channels_hash);
  for (auto iter = prefetch_channels_hash.begin();
       iter != prefetch_channels_hash.end(); ++iter) {
    iter->second->stop_prefetch_thread();
  }
  mysql_rwlock_unlock(&LOCK_prefetch_channels_hash);
  return 0;
}

void *run_prefetch(void *arg) {
  if (my_thread_init()) return NULL;
  ConsensusPreFetchChannel *channel = (ConsensusPreFetchChannel *)arg;
  THD *thd = create_thd(false, true, true, channel->get_PSI_thread_key(), 0);
  while (channel->is_running) {
    uint64 index = channel->get_prefetch_request();
    if (index == 0) {
      // mysql_cond_wait is invoked in get_prefetch_request
      continue;
    }

    // get log
    if (!channel->log_exist(index)) {
      xp::info(ER_XP_PREFETCH)
          << "Consensus prefetch channel " << channel->get_channel_id()
          << " try to fetch index : " << index;
      if (consensus_log_manager.prefetch_log_directly(
              thd, channel->get_channel_id(), index)) {
        channel->clear_prefetch_request();
      }
    } else
      channel->clear_prefetch_request();
  }
  destroy_thd(thd);
  my_thread_end();
  return NULL;
}
