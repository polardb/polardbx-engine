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
//#include <boost/lockfree/queue.hpp>
#include <ctime>
#include <iomanip>
#include <sstream>
#include <iostream>
#include <iterator>
#include <algorithm>
#include <atomic>
#include <fpga/comp_io.h>
#include "xengine/env.h"
//#include "fpga_compaction_task_group.h"
//#include "comp_aclr/comp_aclr.h"
//#include "fpga/compaction_test_running_status.h"


namespace xengine {
namespace fpga{

  //const std::string root_dir = "/home/jason.zt/workspace/rocksdb_test_var/compaction_test";
  const std::string blk_eof_file_name = "blk_eof.hex";
  const std::string blk_len_file_name = "blk_len.hex";
  const std::string blk_off_file_name = "blk_off.hex";
  const std::string dec_input_file_name = "dec_input.hex";
  const std::string way_len_file_name = "way_len.hex";
  const std::string input_spec_file_name = "input_spec.txt";
  const std::string mock_output_file_name = "mock_output.hex";

  class FPGACompactionJob{
    public:

      static FPGACompactionJob& get_instance() {
        static FPGACompactionJob instance;
        return instance;
      }

      FPGACompactionJob(FPGACompactionJob const&) = delete;
      void operator=(FPGACompactionJob const&) = delete;
#if 0
      void add_task(CompIO *task);

      void run_fpga();

      void run_cpu();

      void run(CompactionTestRunningStatus *status);

      bool is_running();

      void add_task_group(FPGACompactionTaskGroup* group);

      void shut_down();

      void set_fpga_compaction_mode(bool mode);

      void set_task_snapshot(bool flag);

      void set_compaction_context(CompactionContext& context);

      void set_device_id(int device_id);

      void set_num_threads(uint32_t num_threads);

      const std::vector<ChangeInfo>& get_change_info_list () const {return change_info_list_;}

      static void callback_compaction_fpga(size_t level_type,
                                           uint64_t min_ref_seq_no,
                                           char **input_blocks,
                                           size_t **input_blocks_size,
                                           size_t *num_input_blocks,
                                           size_t num_ways,
                                           char *output,
                                           size_t *out_blocks_size,
                                           size_t *num_output_blocks,
                                           void *group,
                                           CompStats *stats,
                                           int rc);
#endif
      void set_data_dir(std::string path);

      void dump_task_if_fail(const CompIO * const task, bool dump_output, bool status);

      void write_content_hex(
              std::stringstream& sstream,
              char *content,
              size_t stream_size,
              std::ofstream &ofs);
#if 0
      static bool check_result(CompIO *task);
      void set_fpga_compaction_status(bool flag){fpga_compaction_status_=flag;}
      bool get_fpga_compaction_status(){return fpga_compaction_status_;}
      void init_status(){
        shutdown_ = false;
        fpga_compaction_status_ = true;
        change_info_list_.clear();
      }
#endif
    private:
      FPGACompactionJob();
    private:
      std::string data_dir_;
      std::atomic<uint64_t> total_check_case_num;
      std::atomic<uint64_t> total_success_case_num;
      std::atomic<uint64_t> total_failure_case_num;
  };
}
}
