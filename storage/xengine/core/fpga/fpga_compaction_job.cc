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
#include "fpga_compaction_job.h"
#include <sys/time.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <fstream>
#include <mutex>

using namespace xengine;
using namespace util;

std::mutex mux;

// DECLARE_string(db);

namespace xengine {
namespace fpga {

#if 0
    void FPGACompactionJob::add_task(CompIO *task) {
      task_queue_.push(task);
    }

    void FPGACompactionJob::run(CompactionTestRunningStatus* status) {
      status_ = status;
      if (fpga_compaction_mode_) {
        run_fpga();
      }
      else {
        run_cpu();
      }
    }

    void FPGACompactionJob::run_fpga() {

      while(1) {
        CompIO *task;
        bool ret = task_queue_.pop(task);
        // 1. post compaction task to FPGA
        if (ret) {
          bool queue_full = comp_aclr_->MergeBlocks(task);
        }

        // if (!queue_full) {
          // driver's task queue full
          // push the task back to the queue
          //task_queue_.push(task);
        // }

        // 2. build extent for each group in group queue
#if 1
        FPGACompactionTaskGroup* group;
        ret = task_group_queue_.pop(group);

        if (ret) {
          if (nullptr != group) {
            group -> flush();
            change_info_list_.emplace_back(group->get_change_info());
            //delete group;
          }
        }
        if (shutdown_ && task_queue_.empty() && task_group_queue_.empty()) {
          break;
        }
#endif
      }

    }

    void FPGACompactionJob::run_cpu() {
      CompIO *task;
      bool ret = task_queue_.pop(task);
      while (1) {
        // 1. perform compaction on cpu
        if(ret) {
          struct timeval start, end;
          gettimeofday(&start, NULL);

          MultiWayBlockMerge multi_way_block_merge(task->level_type,
                                                   task->min_ref_seqno,
                                                   task->input_blocks,
                                                   task->input_blocks_size,
                                                   task->num_input_blocks,
                                                   task->num_ways,
                                                   task->output_blocks,
                                                   task->out_blocks_size,
                                                   *task->num_output_blocks,
                                                   *task->stats);
          multi_way_block_merge.MultiBlockMerge();
          CompressionType compression_type = GetCompressionType(*context_.cf_options_,
                  *context_.mutable_cf_options_,
                  1);

          if (compression_type != kNoCompression) {
            char *block_ptr = task->output_blocks;
            for (size_t i = 0; i < *task->num_output_blocks; ++i) {
              size_t block_size = task->out_blocks_size[i];
              Slice raw_block_encoding(block_ptr, block_size);
              block_ptr += block_size;
              std::string buffer;
              BlockBasedTableOptions table_options;
              // need compress data block before addBlock to extent
              raw_block_encoding = CompressBlock(raw_block_encoding,
                                                 context_.cf_options_->compression_opts,
                                                 &compression_type,
                                                 table_options.format_version,
                                                 compression_dict_,
                                                 &buffer);
            }
          }

#if 1
          gettimeofday(&end, NULL);
          double compaction_time = end.tv_sec - start.tv_sec + 1.0*(end.tv_usec - start.tv_usec)/1000000;
          status_->add_compaction_time(std::this_thread::get_id(), compaction_time);

          size_t compaction_bytes = 0;
          for (size_t i = 0; i < task->num_ways; ++i) {
            for (size_t j = 0; j < task->num_input_blocks[i]; ++j) {
              compaction_bytes += task->input_blocks_size[i][j];
            }
          }
          status_->add_compaction_bytes(std::this_thread::get_id(), compaction_bytes);
          status_->add_compaction_kv_num(std::this_thread::get_id(), task->stats->num_input_records_);
#endif
          // fprintf(stderr, "[thread-----] throughput: %f\n", compaction_bytes/compaction_time);

          delete task->stats;
          CompStats* stats = new CompStats();
          task->stats = stats;
          //delete task->output_blocks_;
          //delete task->output_blocks_size_;
          //task->output_blocks_ = new char[compaction_bytes];
          //task->output_blocks_size_ = new size_t[num_input_blocks];
          //task_queue_.push(task);

          // fprintf(stderr, "---[compaction task] total output block size: %ld---\n", total_output_bytes);
          // fprintf(stderr, "---[compaction task] time used: %f---\n", 1.0*(end - start)/CLOCKS_PER_SEC);

          // FPGACompactionTaskGroup *fpga_compaction_task_group = (FPGACompactionTaskGroup *)task->group_;
          // if (fpga_compaction_task_group->increase_finished_job_num()) {
          //  FPGACompactionJob& fpga_compaction_job = FPGACompactionJob::get_instance();
          //  fpga_compaction_job.add_task_group(fpga_compaction_task_group);
          //};
        }

        //break;
        // 2. build extent for each group in group queue
        //FPGACompactionTaskGroup* group;
        //ret = task_group_queue_.pop(group);

        //if (ret) {
          // group -> flush();
          /*
           * auto start = clock();
          group -> flush();
          auto end = clock();
          change_info_list_.emplace_back(group->get_change_info());
          fprintf(stderr, "---[flush task] time used: %f---\n", 1.0*(end - start)/CLOCKS_PER_SEC);
          */
          //delete group;
        //}

        //if (shutdown_ && task_queue_.empty() && task_group_queue_.empty()) {
        //  break;
        //}
        //break;

      }
    }

    bool FPGACompactionJob::is_running() {
      return is_running_;
    }

    void FPGACompactionJob::add_task_group(FPGACompactionTaskGroup* task_group) {
      task_group_queue_.push(task_group);
    }

    void FPGACompactionJob::shut_down() {
      shutdown_ = true;
    }

    void FPGACompactionJob::set_fpga_compaction_mode(bool fpga_compaction) {
      fpga_compaction_mode_ = fpga_compaction;
      if (fpga_compaction) {
        comp_aclr_ = new CompAclr(device_id_, num_threads_);
      }
    }

    void FPGACompactionJob::set_task_snapshot(bool flag) {
      task_snapshot_ = flag;
    }

    void FPGACompactionJob::callback_compaction_fpga(size_t level_type,
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
                                                     int rc) {

      FPGACompactionTaskGroup* fpga_compaction_task_group = (FPGACompactionTaskGroup *)group;
      if (fpga_compaction_task_group->increase_finished_job_num()) {
        FPGACompactionJob& fpga_compaction_job = FPGACompactionJob::get_instance();
        fpga_compaction_job.add_task_group((FPGACompactionTaskGroup *)group);
      };

      CompactionTestRunningStatus *status = FPGACompactionJob::get_instance().status_;
      status->add_thread_fpga(std::this_thread::get_id());
      status->add_compaction_time_fpga(std::this_thread::get_id(), 1.0*stats->elapsed_micros_fpga_/1000000);
      stats->elapsed_micros_ = 0;

      size_t compaction_bytes = 0;
      for (size_t i = 0; i < num_ways; ++i) {
        for (size_t j = 0; j < num_input_blocks[i]; ++j) {
          compaction_bytes += input_blocks_size[i][j];
        }
      }
      status->add_compaction_bytes_fpga(std::this_thread::get_id(), compaction_bytes);
      status->add_compaction_kv_num_fpga(std::this_thread::get_id(), stats->num_input_records_);
#if 1
      CompIO *task = new CompIO();
      task->level_type = level_type;
      task->min_ref_seqno = min_ref_seq_no;
      task->input_blocks = input_blocks;
      task->input_blocks_size = input_blocks_size;
      task->num_input_blocks = num_input_blocks;
      task->num_ways = num_ways;
      task->output_blocks = output;
      task->out_blocks_size = out_blocks_size;
      task->num_output_blocks = num_output_blocks;
      task->group = group;
      task->stats = stats;

      bool flag = check_result(task);
      FPGACompactionJob::get_instance().set_fpga_compaction_status(flag);
      if (!flag) {
         fprintf(stderr, "results of FPGA and CPU are inconsistent\n");
      }
#endif
      FPGACompactionJob::get_instance().shut_down();
    }

    void FPGACompactionJob::set_device_id(int device_id) {
      device_id_ = device_id;
    }

    void FPGACompactionJob::set_num_threads(uint32_t num_threads) {
      num_threads_ = num_threads;
    }

    void FPGACompactionJob::set_compaction_context (CompactionContext& context) {
      context_ = context;
    }
#endif


    FPGACompactionJob::FPGACompactionJob() {
      total_check_case_num = 0;
      total_failure_case_num = 0;
      total_success_case_num = 0;

    }

    void FPGACompactionJob::set_data_dir (std::string path) {
      Env::Default() -> CreateDir (path + "/abnormal_case");
      data_dir_ = path + "/abnormal_case/compaction_test";
    }

    void FPGACompactionJob::dump_task_if_fail(const CompIO * const task, bool dump_output, bool status) {
      //if (!task_snapshot_) {
        //return;
      //}

      mux.lock();
      ++ total_check_case_num;
      if (status) {
        fprintf(stderr, "failure case/total case:%ld/%ld\n",
                total_failure_case_num.load(),
                total_check_case_num.load());
        mux.unlock();
        return;
      }
      ++ total_failure_case_num;
      fprintf(stderr, "failure case/total case:%ld/%ld\n",
              total_failure_case_num.load(),
              total_check_case_num.load());

      std::string base_path = data_dir_ + "_" + std::to_string(total_failure_case_num);
      mux.unlock();
      //int stats = mkdir(base_path);
      //if(stats) {
      //  fprintf(stderr, "create folder error!");
      //  return;
      //}

      // system(sh.c_str());
      // int dir_err = mkdir(base_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
      //if (-1 == dir_err) {
      //  fprintf(stderr, "create dir failed\n");
      //  return;
      //}
      Env::Default() -> CreateDir(base_path);

      // level_type and ref_seq_no as global info
      std::ofstream ofs;

      // input_spec.txt
      std::string file_path = base_path + "/" + input_spec_file_name;
      ofs.open(file_path, std::ofstream::out | std::ofstream::app);


      ofs << "WAY_NUM" << std::endl;
      ofs << std::to_string(task->num_ways) << std::endl;
      ofs << "NUM_INPUT_BLOCKS" << std::endl;
      for (size_t i = 0; i < task->num_ways; ++i) {
        ofs << std::to_string(task->num_input_blocks[i]) << std::endl;
      }
      ofs << "INPUT_BLOCKS_SIZE" << std::endl;
      for (size_t i = 0; i < task->num_ways; ++i) {
        for (size_t j = 0; j < task->num_input_blocks[i]; ++j) {
          ofs << std::to_string(task->input_blocks_size[i][j]) << " ";
        }
        ofs << std::endl;
      }
      ofs << "LEVEL_TYPE" << std::endl;
      ofs << std::to_string(task->level_type) << std::endl;
      ofs << "MIN_REF_SEQ_NO" << std::endl;
      ofs << std::to_string(task->min_ref_seqno) << std::endl;
      // ofs << "min_ref_seq_no : " << task->min_ref_seq_no_;

      ofs.close();

      std::stringstream sstream;
        //fs::create_directories(way_folder_path);

      if (dump_output) {
        // also need to snapshot task output content
          std::string mock_output_file_path = base_path + "/" + mock_output_file_name;
          ofs.open(mock_output_file_path, std::ofstream::out | std::ofstream::app);
          size_t stream_size = 0;
          for (size_t i = 0; i < *task->num_output_blocks; ++i) {
            stream_size += task->out_blocks_size[i];
          }
          write_content_hex(sstream, task->output_blocks, stream_size, ofs);
          ofs.close();
      }

      sstream.str("");

      for (size_t i = 0; i < task->num_ways; ++i) {

        std::string way_folder_path = base_path + "/way_" + std::to_string(i);
        // system(sh.c_str());
        // dir_err = mkdir(way_folder_path.c_str(), S_IRWXU | S_IRWXG | S_IROTH | S_IXOTH);
        // if (-1 == dir_err) {
        //  fprintf(stderr, "create dir failed\n");
        //  return;
        // }
        Env::Default() -> CreateDir(way_folder_path);

        size_t stream_size = 0;
        std::ofstream ofs_blk_eof, ofs_blk_len, ofs_blk_off;
        file_path = way_folder_path + "/" + blk_eof_file_name;
        ofs_blk_eof.open(file_path, std::ofstream::out | std::ofstream::app);
        file_path = way_folder_path + "/" +blk_len_file_name;
        ofs_blk_len.open(file_path, std::ofstream::out | std::ofstream::app);
        file_path = way_folder_path + "/" +blk_off_file_name;
        ofs_blk_off.open(file_path, std::ofstream::out | std::ofstream::app);

        for (size_t j = 0; j < task->num_input_blocks[i]; ++j) {
          ofs_blk_eof << "1" << std::endl;

          sstream.str("");
          sstream << std::hex
                  << std::setfill('0')
                  << std::setw(8)
                  << task->input_blocks_size[i][j];
          ofs_blk_len << sstream.str() << std::endl;

          sstream.str("");

          sstream << std::hex
                  << std::setfill('0')
                  << std::setw(8)
                  << stream_size;
          ofs_blk_off << sstream.str() << std::endl;

          stream_size += task->input_blocks_size[i][j];

        }
        ofs_blk_eof << "0" << std::endl;
        ofs_blk_eof.close();
        ofs_blk_len.close();
        ofs_blk_off.close();

        file_path = way_folder_path + "/" +way_len_file_name;
        ofs.open(file_path, std::ofstream::out | std::ofstream::app);
        sstream.str("");
        sstream << std::hex
                << std::setfill('0')
                << std::setw(8)
                << stream_size;
        ofs << sstream.str() << std::endl;
        ofs.close();

        file_path = way_folder_path + "/" + dec_input_file_name;
        ofs.open(file_path, std::ofstream::out | std::ofstream::app);


        write_content_hex(sstream, task->input_blocks[i], stream_size, ofs);
        ofs.close();


      }

      fprintf(stderr, "---dump task done---\n");
      // mux.unlock();
    }

    void FPGACompactionJob::write_content_hex(
            std::stringstream& sstream,
            char *data,
            size_t stream_size,
            std::ofstream &ofs) {
        sstream.str("");
        sstream << std::hex << std::setfill('0');
        for (size_t j = 0; j < stream_size; ++j) {
          sstream << std:: setw(2) << (unsigned int)(unsigned char)(data[j]);
        }

        std::string content = sstream.str();
        size_t len = content.size();

        size_t start = 0;
        while (start + 128 < len) {
          ofs << content.substr(start, 128) << std::endl;
          start += 128;
        }

        if (start < len) {
          std::string str = content.substr(start, len - start);
          ofs << str << std::string(128 - len + start, '0');
        }
    }
#if 0
    bool FPGACompactionJob::check_result(CompIO *task) {
      CompIO *cpu_task = new CompIO();
      cpu_task->level_type = task->level_type;
      cpu_task->min_ref_seqno = task->min_ref_seqno;
      cpu_task->input_blocks = task->input_blocks;
      cpu_task->input_blocks_size = task->input_blocks_size;
      cpu_task->num_input_blocks = task->num_input_blocks;
      cpu_task->num_ways = task->num_ways;

      size_t total_input_size = 0;
      size_t total_input_blocks_num = 0;

      for (size_t i = 0;i < task->num_ways; ++i) {
        total_input_blocks_num += task->num_input_blocks[i];
        for (size_t j = 0;j < task->num_input_blocks[i]; ++j) {
          total_input_size += task->input_blocks_size[i][j];
        }
      }

      cpu_task->output_blocks = new char[total_input_size];
      cpu_task->out_blocks_size = new size_t[total_input_blocks_num];
      cpu_task->num_output_blocks = new size_t(0);
      cpu_task->group = task->group;
      cpu_task->stats = new CompStats();

      MultiWayBlockMerge multi_way_block_merge(cpu_task->level_type,
                                               cpu_task->min_ref_seqno,
                                               cpu_task->input_blocks,
                                               cpu_task->input_blocks_size,
                                               cpu_task->num_input_blocks,
                                               cpu_task->num_ways,
                                               cpu_task->output_blocks,
                                               cpu_task->out_blocks_size,
                                               *cpu_task->num_output_blocks,
                                               *cpu_task->stats);
      multi_way_block_merge.MultiBlockMerge();

      /* FPGA result is expected to be the same as CPU result
       * 1. output_blocks bytewise
       * 2. out_blocks_size
       * 3. stats
       */

      //FPGACompactionJob::get_instance().dump_task_to_file(cpu_task, true);

      if (*cpu_task->num_output_blocks != *task->num_output_blocks) {
        fprintf(stderr, "---cpu  num output blocks:%ld\n", *cpu_task->num_output_blocks);
        fprintf(stderr, "---fpga num output blocks:%ld\n", *task->num_output_blocks);
        return false;
      }

      size_t cpu_block_size = 0;
      size_t fpga_block_size = 0;
      for (size_t i = 0; i < *cpu_task->num_output_blocks; ++i) {
        if (task->out_blocks_size[i] != cpu_task->out_blocks_size[i]) {
        fprintf(stderr, "---cpu %ldth output block size:%ld\n", i, cpu_task->out_blocks_size[i]);
        fprintf(stderr, "---fpga %ldth output block size:%ld\n", i, task->out_blocks_size[i]);
          return false;
        }
        cpu_block_size += cpu_task->out_blocks_size[i];
        fpga_block_size += task->out_blocks_size[i];
      }

      std::string cpu_block_data(cpu_task->output_blocks, cpu_block_size);
      std::string fpga_block_data(task->output_blocks, fpga_block_size);

      if (cpu_block_data.compare(fpga_block_data) != 0) {
        fprintf(stderr, "---block content inconsistent\n");
        return false;
      }

      if (!task->stats->compare(*cpu_task->stats)) {
        fprintf(stderr, "---block stats inconsistent\n");
        return false;
      }

      return true;
    }
#endif
}

}
