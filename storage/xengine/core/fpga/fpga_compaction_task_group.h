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
#pragma once

#include <comp_aclr/comp_io.h>
#include "table/table_builder.h"
#include "compact/compaction.h"
#include <vector>
#include <memory>

using namespace xengine;
using namespace storage;
using namespace table;
using namespace common;
using namespace fpga;

namespace xengine {
namespace fpga {

  /**
   * A compaction task group contains
   * serveral compaction tasks
   * which have a global order
   * -- task 0 -- task 1 -- task 2 -- task 3 --
   */
  class FPGACompactionTaskGroup{
    public:
      FPGACompactionTaskGroup(CompactionContext context, ColumnFamilyDesc cf_desc);

      ~FPGACompactionTaskGroup();

      void add_task(CompIO *task);

      bool increase_finished_job_num();

      CompIO* get_task(size_t pos);

      size_t get_group_size();

      void set_change_info(ChangeInfo change_info);

      const ChangeInfo &get_change_info() const {return change_info_;}

      void flush();

    private:
      int open_extent();
      void build_extent();
      int close_extent();

    private:
      std::vector<CompIO *> tasks_;
      bool ready_to_build_extent_;
      std::atomic_size_t num_tasks_finished_;
      std::unique_ptr<table::TableBuilder> extent_builder_;

      // options for extent builder
      CompactionContext context_;
      ColumnFamilyDesc cf_desc_;
      db::MiniTables mini_tables_;
      std::string compression_dict_;
      std::vector<std::unique_ptr<db::IntTblPropCollectorFactory>> props_;
      ChangeInfo change_info_;
  };
}
}
