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
#include <thread>
#include <map>
#include <atomic>

namespace xengine {
namespace fpga {
  struct ThreadStatus{
    double compaction_time_;
    size_t compaction_bytes_;
    size_t compaction_kv_num_;

    ThreadStatus():
        compaction_time_(0.0),
        compaction_bytes_(0),
        compaction_kv_num_(0){}
  };
  class CompactionTestRunningStatus{
    public:
      void add_thread(std::thread *t, size_t mapp_id);
      void add_compaction_time(std::thread::id id, double compaction_time);
      void add_compaction_bytes(std::thread::id id, size_t compaction_bytes);
      void add_compaction_kv_num(std::thread::id id, size_t compaction_kv_num);

      void add_thread_fpga(std::thread::id id);
      void add_compaction_time_fpga(std::thread::id id, double compaction_time);
      void add_compaction_bytes_fpga(std::thread::id id, size_t compaction_bytes);
      void add_compaction_kv_num_fpga(std::thread::id id, size_t compaction_kv_num);
      void report_status(int interval);

    private:
      std::map<std::thread::id, size_t> thread_id_;
      std::map<size_t, ThreadStatus> status_;

      std::map<std::thread::id, size_t> thread_id_fpga_;
      std::map<size_t, ThreadStatus> status_fpga_;
      std::atomic_size_t fpga_thread_num_;
  };
}
}
