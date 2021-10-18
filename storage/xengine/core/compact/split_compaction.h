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

#ifndef XENGINE_STORAGE_SPLIT_COMPACTION_H_
#define XENGINE_STORAGE_SPLIT_COMPACTION_H_

#include "compaction.h"
#include "compaction_stats.h"
#include "reuse_block_merge_iterator.h"
#include "storage/storage_manager.h"
#include "table/block.h"
#include "table/two_level_iterator.h"
#include "util/aligned_buffer.h"
#include "xengine/cache.h"
#include "xengine/options.h"

namespace xengine {
namespace storage {

class SplitCompaction : public GeneralCompaction {
 public:
  SplitCompaction(const CompactionContext &context,
                  const ColumnFamilyDesc &cf,
                  memory::ArenaAllocator &arena);
  ~SplitCompaction();

  void add_split_key(const common::Slice &split_key);
  int run();
  int cleanup();

 private:
  int close_split_extent(const int64_t level);
  int split_extents(ExtSEIterator &iterator);
 private:
  std::vector<common::Slice, memory::stl_adapt_allocator<common::Slice>> split_keys_;
};
}  // namespace storage
}  // namespace xengine

#endif
