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
#ifndef XENGINE_STORAGE_MTEXT_COMPACTION_H_
#define XENGINE_STORAGE_MTEXT_COMPACTION_H_

#include "compact/compaction.h"
namespace xengine {
namespace storage{

class MtExtCompaction : public GeneralCompaction {
 public:
  MtExtCompaction(const CompactionContext &context,
                  const ColumnFamilyDesc &cf,
                  memory::ArenaAllocator &arena);
  ~MtExtCompaction();
  virtual int run();
  virtual int cleanup();
  int add_mem_iterators(util::autovector<table::InternalIterator *> &iters);
  int update_row_cache();
  const db::MiniTables &get_mini_tables() const{
    return flush_minitables_;
  }
  db::MiniTables &get_apply_mini_tables() {
    return mini_tables_;
  }
  void add_input_bytes(const int64_t input_bytes) {
    stats_.record_stats_.total_input_bytes += input_bytes;
  }
//  void set_schema(const common::XengineSchema *schema) {
//    schema_ = schema;
//    mini_tables_.schema = schema;
//  }
 private:
 int build_mem_se_iterators();
 int build_mem_merge_iterator(MultipleSEIterator *&merge_iterator);
  // memtable iterators
  util::autovector<table::InternalIterator *> mem_iterators_;
  MemSEIterator *mem_se_iterators_;
//  const common::XengineSchema *schema_;
  db::MiniTables flush_minitables_; // save all metas
};
} // storage
} // xengine
#endif
