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

#ifndef XENGINE_META_DATA_H_
#define XENGINE_META_DATA_H_

#include <stdint.h>
#include <stdio.h>
#include <string.h>
#include <atomic>
#include <memory>
#include <mutex>
#include "db/memtable.h"
#include "range_iterator.h"
#include "table/internal_iterator.h"
#include "util/serialization.h"
#include "xengine/slice.h"
#include "xengine/status.h"
#include "xengine/types.h"
#include "xengine/write_batch.h"

namespace xengine {

namespace common {
class ImmutableCFOptions;
class MutableCFOptions;
}

namespace db {
class WriteBufferManager;
}

namespace storage {

class ExtentMetaValue {
 public:
  ExtentMetaValue();
  ~ExtentMetaValue();

  const common::Slice &get_start_key() const;
  void set_start_key(const common::Slice &start_key);

  const BlockPosition &get_extent_list() const;
  void set_extent_list(const BlockPosition &extents);

  void reset();

  DECLARE_SERIALIZATION();

 private:
  common::Slice start_key_;
  BlockPosition extent_list_;
};

// Key: end_key
// Value: start_key | extent_list | ref
class ExtentMetaTable {
 public:
  ExtentMetaTable();
  ~ExtentMetaTable();

  common::Status init(const common::ImmutableCFOptions &ioptions,
                      const common::MutableCFOptions &mutable_cf_options,
                      db::WriteBufferManager *write_buffer_manager);
  void destroy();
  // change as a transaction
  common::Status change(const db::WriteBatch &batch);

  common::SequenceNumber get_last_sequence() const {
    return last_sequence_.load(std::memory_order_acquire);
  }

  void set_last_sequence(const common::SequenceNumber seq) {
    last_sequence_.store(seq, std::memory_order_release);
  }

  table::InternalIterator *new_iterator(const common::ReadOptions &read_options,
                                        util::Arena *arena) {
    return table_->NewIterator(read_options, arena);
  }

 private:
  const db::InternalKeyComparator internal_comparator_;
  std::atomic<common::SequenceNumber> last_sequence_;
  std::unique_ptr<db::MemTable> table_;
  std::mutex write_mutex_;
};

}  // namespace storage
}  // namespace xengine

#endif  // XENGINE_META_DATA_H
