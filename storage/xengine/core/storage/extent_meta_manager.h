/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef XENGINE_INCLUDE_EXTENT_META_MANAGER_H_
#define XENGINE_INCLUDE_EXTENT_META_MANAGER_H_
#include <unordered_map>
#include "util/spin_rwlock.h"
#include "memory/stl_adapt_allocator.h"
#include "io_extent.h"

namespace xengine
{
namespace storage
{
struct ExtentMeta;
struct CheckpointHeader;
struct CheckpointBlockHeader;
class StorageLogger;

class ExtentMetaManager
{
public:
  ExtentMetaManager();
  ~ExtentMetaManager();
  void destroy();

  int init(StorageLogger *storage_logger);
  int write_meta(const ExtentMeta &extent_meta, bool write_log);
  int recycle_meta(const ExtentId extent_id);
  ExtentMeta *get_meta(const ExtentId &extent_id);
  int do_checkpoint(util::WritableFile *checkpoint_writer, CheckpointHeader *header);
  int load_checkpoint(util::RandomAccessFile *checkpoint_reader, CheckpointHeader *header);
  int replay(int64_t log_type, char *log_data, int64_t log_len);
  int clear_free_extent_meta();

private:
  void free_extent_meta(ExtentMeta *&extent_meta);
  int reserve_checkpoint_block_header(char *buf, int64_t buf_size, int64_t &offset, CheckpointBlockHeader *&block_header);
  int replay_extent_meta_log(char *log_data, int64_t log_length);
  int replay_write_meta(const ExtentMeta &extent_meta);

private:
  typedef std::unordered_map<int64_t, ExtentMeta*, std::hash<int64_t>, std::equal_to<int64_t>,
      memory::stl_adapt_allocator<std::pair<const int64_t, ExtentMeta*>, memory::ModId::kExtentSpaceMgr>> ExtentMetaUnorderedMap;
  static const int64_t DEFAULT_BUFFER_SIZE = 2 * 1024 * 1024;

private:
  bool is_inited_;
  StorageLogger *storage_logger_;
  util::SpinRWLock meta_mutex_;
  ExtentMetaUnorderedMap extent_meta_map_;
};

} //namespace storage
} //namespace xengine
#endif
