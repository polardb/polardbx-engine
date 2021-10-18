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
#include "compaction_test_running_status.h"
#include <unistd.h>

using namespace xengine;
using namespace fpga;

namespace xengine{
namespace fpga{

  void CompactionTestRunningStatus::add_thread(std::thread *t, size_t mapped_id) {
    thread_id_.emplace(t->get_id(), mapped_id);
    status_.emplace(mapped_id, ThreadStatus());
  }

  void CompactionTestRunningStatus::add_compaction_time(std::thread::id id, double compaction_time) {
    size_t mapped_id = thread_id_[id];
    status_[mapped_id].compaction_time_ += compaction_time;
  }

  void CompactionTestRunningStatus::add_compaction_bytes(std::thread::id id, size_t compaction_bytes) {
    size_t mapped_id = thread_id_[id];
    status_[mapped_id].compaction_bytes_ += compaction_bytes;
  }

  void CompactionTestRunningStatus::add_compaction_kv_num(std::thread::id id, size_t compaction_kv_num) {
    size_t mapped_id = thread_id_[id];
    status_[mapped_id].compaction_kv_num_ += compaction_kv_num;
  }

  void CompactionTestRunningStatus::add_thread_fpga(std::thread::id id) {
    if (thread_id_fpga_.find(id) == thread_id_fpga_.end()) {
      thread_id_fpga_.emplace(id, fpga_thread_num_);
      status_fpga_.emplace(fpga_thread_num_, ThreadStatus());
      fpga_thread_num_ += 1;
    }

  }

  void CompactionTestRunningStatus::add_compaction_time_fpga(std::thread::id id, double compaction_time) {
    size_t mapped_id = thread_id_fpga_[id];
    status_fpga_[mapped_id].compaction_time_ += compaction_time;
  }

  void CompactionTestRunningStatus::add_compaction_bytes_fpga(std::thread::id id, size_t compaction_bytes) {
    size_t mapped_id = thread_id_fpga_[id];
    status_fpga_[mapped_id].compaction_bytes_ += compaction_bytes;
  }

  void CompactionTestRunningStatus::add_compaction_kv_num_fpga(std::thread::id id, size_t compaction_kv_num) {
    size_t mapped_id = thread_id_fpga_[id];
    status_fpga_[mapped_id].compaction_kv_num_ += compaction_kv_num;
  }

  void CompactionTestRunningStatus::report_status(int interval) {
    double total_throughput_bytes = 0.0;
    double total_throughput_kv_num = 0.0;
    double throughput_bytes = 0.0;
    double throughput_kv_num = 0.0;
    fpga_thread_num_ = 0;
    while (1) {
      /****************************CPU stats*******************************/
      total_throughput_bytes = 0;
      total_throughput_kv_num = 0;
      for (auto const& it : status_) {
        size_t thread_id = it.first;
        ThreadStatus status = it.second;
        if(status.compaction_time_) {
          throughput_bytes = status.compaction_bytes_/status.compaction_time_/1024/1024;
          throughput_kv_num = status.compaction_kv_num_/status.compaction_time_/10000;
          total_throughput_bytes += throughput_bytes;
          total_throughput_kv_num += throughput_kv_num;
          fprintf(stderr, "[cpu  thread %02ld]---throughput:%f Mb/s\tkv:%f w/s\n",
                  thread_id,
                  throughput_bytes,
                  throughput_kv_num);

        }

      }
      if(total_throughput_bytes) {
        fprintf(stderr, "*******************************************************************\n");
        fprintf(stderr, "[cpu  summary]---throughput:%f Mb/s\tkv:%f w/s\n", total_throughput_bytes, total_throughput_kv_num);
        fprintf(stderr, "-------------------------------------------------------------------\n");
      }

      /***************************FPGA stats******************************/
      total_throughput_bytes = 0;
      total_throughput_kv_num = 0;
      for (auto const& it : status_fpga_) {
        size_t thread_id = it.first;
        ThreadStatus status = it.second;
        if(status.compaction_time_) {
          throughput_bytes = status.compaction_bytes_/status.compaction_time_/1024/1024;
          throughput_kv_num = status.compaction_kv_num_/status.compaction_time_/10000;
          total_throughput_bytes += throughput_bytes;
          total_throughput_kv_num += throughput_kv_num;
          fprintf(stderr, "[fpga thread %02ld]---throughput:%f Mb/s\tkv:%f w/s\n",
                  thread_id,
                  throughput_bytes,
                  throughput_kv_num);
        }
      }
      if(total_throughput_bytes) {
        fprintf(stderr, "*******************************************************************\n");
        fprintf(stderr, "[fpga summary]---throughput:%f Mb/s\tkv:%f w/s\n", total_throughput_bytes, total_throughput_kv_num);
        fprintf(stderr, "-------------------------------------------------------------------\n");
      }
      sleep(interval);
    }
  }
}
}
