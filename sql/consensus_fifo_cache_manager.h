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
#ifndef MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H
#define MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H

#include <atomic>
#include <cstddef>
#include <cstdint>

#include "my_inttypes.h"
#include "mysqld.h"

#ifdef HAVE_PSI_INTERFACE
extern PSI_memory_key key_memory_ConsensusLogManager;
extern PSI_rwlock_key key_rwlock_ConsensusLog_log_cache_lock;
extern PSI_mutex_key key_fifo_cache_cleaner;
extern PSI_thread_key key_thread_cleaner;
#endif

struct ConsensusLogEntry {
  uint64 term{};
  uint64 index{};
  size_t buf_size{};
  uchar *buffer{};
  bool outer{}; /* whether created by consensus module */
  uint flag{};  /* atomic flag marked */
  uint64 checksum{};
};

enum ConsensusLogCacheResultCode {
  SUCCESS = 0,
  ALREADY_SWAP_OUT = 1,
  OUT_OF_RANGE = 2,
  FULL = 3,
  INTERRUPT = 4,
  EMPTY = 5
};

#define RESERVE_LIST_SIZE (1024 * 1024)

// Consensus fifo cache is a ring buffer to store log entries.
// Because consensus library could ask for log entry by index,
// the cache could answer the request without random IO on BINLOG file.
class ConsensusFifoCacheManager {
 public:
  ConsensusFifoCacheManager()
      : inited(false), reserve_list_size(RESERVE_LIST_SIZE) {}
  ~ConsensusFifoCacheManager() = default;
  int init(uint64 max_log_cache_size_arg);
  int cleanup();
  int add_log_to_cache(uint64 term, uint64 index, size_t buf_size,
                       uchar *buffer, bool outer, uint flag,
                       uint64 checksum = 0, bool reuse_buffer = false);
  int get_log_from_cache(uint64 index, uint64 *term, std::string &log_content,
                         bool *outer, uint *flag, uint64 *checksum);
  int trunc_log_from_cache(uint64 index);
  uint64 get_log_size_from_cache(uint64 begin_index, uint64 end_index,
                                 uint64 max_packet_size);
  uint64 get_fifo_cache_size() { return fifo_cache_size; }
  uint64 get_first_index_of_fifo_cache();
  uint64 get_fifo_cache_log_count() { return current_log_count; }
  void set_max_log_cache_size(uint64 max_log_cache_size_arg) {
    max_log_cache_size = max_log_cache_size_arg;
  }
  void set_lock_blob_index(uint64 lock_blob_index_arg);

  void clean_consensus_fifo_cache();

 private:
  bool inited;
  PSI_memory_key key_memory_cache_mem_root;
  PSI_rwlock_key key_LOCK_consensuslog_cache;
  mysql_rwlock_t LOCK_consensuslog_cache;  // used to protect log cache
  std::atomic<size_t> rleft, rright;
  // ring buffer: data range is [rleft, rright)
  ConsensusLogEntry *log_cache_list;
  std::atomic<uint64> max_log_cache_size;  // FIFO CACHE MAX SIZE
  std::atomic<uint64> fifo_cache_size;     // FIFO cache status
  uint64 lock_blob_index;
  std::atomic<uint64> current_log_count;
  std::atomic<bool> is_running;
  mysql_mutex_t cleaner_mutex;
  mysql_cond_t cleaner_cond;
  my_thread_handle cleaner_handle;

  const uint64_t reserve_list_size;  // reserve size for ring buffer
};

void *fifo_cleaner_wrapper(void *arg);

#endif  // MYSQL_CONSENSUS_FIFO_CACHE_MANAGER_H
