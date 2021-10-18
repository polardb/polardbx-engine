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

#include <fstream>
#include <iostream>
#include <map>
#include <string>
#include <unordered_set>

#include <gflags/gflags.h>
#include <sys/resource.h>
#include <sys/time.h>

#include "db/column_family.h"
#include "db/dbformat.h"
#include "db/log_writer.h"
#include "db/version_edit.h"
#include "db/version_set.h"
#include "db/write_batch_internal.h"
#include "options/options_helper.h"
#include "storage/extent_space_manager.h"
#include "storage/io_extent.h"
#include "storage/storage_manager.h"
#include "table/table_reader.h"
#include "util/autovector.h"
#include "util/random.h"
#include "util/serialization.h"
#include "util/serialization.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/types.h"
#include "xengine/write_batch.h"

DEFINE_int32(threads, 1, "Num of parallel search threads");
using GFLAGS::ParseCommandLineFlags;

using namespace xengine;
using namespace common;
using namespace util;
using namespace cache;
using namespace db;
using namespace table;
using namespace memory;
using namespace logger;

namespace xengine {
namespace storage {

class StorageManagerTest {
 public:
  StorageManagerTest()
      : table_cache_(NewLRUCache(50000, 16)),
        write_buffer_manager_(db_options_.db_write_buffer_size),
        env_(Env::Default()),
        dbname_("./unittest_tmp/"),
        next_file_number_(2) {
    init(true);
  }
  void init(bool open) {
    if (open) {
      db_options_.db_paths.emplace_back(dbname_, 0);
      db_options_.create_if_missing = true;
      db_options_.fail_if_options_file_error = true;
      db_options_.create_missing_column_families = true;
      db_options_.env = env_;
      options_ = Options(db_options_, column_family_options_);
      DestroyDB(dbname_, options_);
      TryOpen({"default", "storagemeta"});
      db_options_.db_paths.emplace_back(dbname_, 0);
    }
    spacemanager_ = new ExtentSpaceManager(env_, EnvOptions(db_options_), db_options_);
    Status s;
    if (open) {
      uint64_t file_number = 0;
      //TODO:yuanfeng
      //StorageManager::parse_current_file(*env_, dbname_, file_number);
      std::string manifest_filename = DescriptorFileName(dbname_, file_number);
//      std::unique_ptr<WritableFile> descriptor_file;
      WritableFile *descriptor_file = nullptr;
      EnvOptions opt_env_opts = env_->OptimizeForManifestWrite(env_options_);
      s = NewWritableFile(env_, manifest_filename, descriptor_file, opt_env_opts);
      if (s.ok()) {
        descriptor_file->SetPreallocationBlockSize(
            db_options_.manifest_preallocation_size);

//        unique_ptr<util::ConcurrentDirectFileWriter> file_writer(
//            new util::ConcurrentDirectFileWriter(descriptor_file, opt_env_opts));
        util::ConcurrentDirectFileWriter *file_writer = new util::ConcurrentDirectFileWriter(descriptor_file, opt_env_opts);
        s = file_writer->init_multi_buffer();
        if (s.ok()) {
          descriptor_log_.reset(new db::log::Writer(file_writer, 0, false));
        }
      }
    }

    auto factory = std::make_shared<memtable::SkipListFactory>();
    options_.memtable_factory = factory;
    WriteBufferManager *wb = new WriteBufferManager(0);  // no limit space
    assert(wb != nullptr);
    // no free here ...
    db_options_.write_buffer_manager.reset(wb);

    env_->NewDirectory(dbname_, db_dir);
    //TODO:yuanfeng
    /*
    mgr = new StorageManager(*env_, dbname_, db_options_,
                             ColumnFamilyOptions(options_), env_options_,
                             db_dir.get(), table_cache_.get(), nullptr);
    */

    s = mgr->init(env_, spacemanager_, table_cache_.get());
    assert(s.ok());
  }
  void shutdown() {
    delete mgr;
    mgr = nullptr;
    // Close();
  }
  ~StorageManagerTest() {
    delete mgr;
    Close();
    Destroy();
  };

  void Close() {
    for (auto h : handles_) {
      if (h) {
        db_->DestroyColumnFamilyHandle(h);
      }
    }
    handles_.clear();
    names_.clear();
    delete db_;
    db_ = nullptr;
  }

  Status TryOpen(std::vector<std::string> cf,
                 std::vector<ColumnFamilyOptions> options = {}) {
    std::vector<ColumnFamilyDescriptor> column_families;
    names_.clear();
    for (size_t i = 0; i < cf.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(
          cf[i], options.size() == 0 ? column_family_options_ : options[i]));
      names_.push_back(cf[i]);
    }
    return DB::Open(db_options_, dbname_, column_families, &handles_, &db_);
  }

  void Destroy() {
    Close();
    DestroyDB(dbname_, Options(db_options_, column_family_options_));
  }

  StorageManager *mgr;
  ExtentSpaceManager *spacemanager_;
  std::unique_ptr<db::log::Writer> descriptor_log_;
//  std::unique_ptr<Directory> db_dir;
  Directory *db_dir;
  DBOptions db_options_;
  std::string largest_user_key_;
  std::vector<std::string> search_ikeys_;

  int64_t delta(const struct timeval &tv1, const struct timeval &tv2) {
    return (tv1.tv_sec - tv2.tv_sec) * 1000000 + tv1.tv_usec - tv2.tv_usec;
  }

  struct Record {
    std::string key;
    std::string value;
    SequenceNumber seq;
    ValueType type;
    bool operator<(const struct Record &r) const { return (seq < r.seq); }
  };

  // add the records in the order of sequence number
  int load_data(const char *filename) {
    std::ifstream file(filename);
    if (!file.is_open()) {
      fprintf(stderr, "failed to open %s\n", filename);
      return -1;
    }

    char index_buf[24];
    char cf_buf[24];
    char level_buf[24];
    char seq_buf[24];
    char key_buf[256];
    char key_seq_buf[24];
    char key_type_buf[24];
    char mem_table_seq_buf[24];
    char mem_table_type_buf[24];
    char offset_buf[24];
    char file_num_buf[24];

    std::string default_value;
    {
      //TODO:yuanfeng
      /*
      ExtentId extent_id;
      MetaValue value("fake smallest key", extent_id);
      int64_t size = value.get_serialize_size();
      default_value.resize(size);
      int64_t pos = 0;
      value.serialize(&default_value[0], size, pos);
      */
    }

    std::vector<Record> records;

    std::string line;
    while (getline(file, line)) {
      //TODO:yuanfeng
      /*
      MetaKey meta_key;
      MetaValue meta_value;
      InternalKey largest_key;
      InternalKey smallest_key;
      SequenceNumber mem_table_seq;
      ValueType mem_table_type;

      int ret =
          sscanf(line.data(),
                 "key[%[0-9]]:cf[%[0-9]], level[%[0-9]], seq[%[0-9]], "
                 "end key:[%[0-9A-Z]][%[0-9]][%[0-9]]_[%[0-9]][%[0-9]]",
                 index_buf, cf_buf, level_buf, seq_buf, key_buf, key_seq_buf,
                 key_type_buf, mem_table_seq_buf, mem_table_type_buf);
      if (ret > 3) {  // key line
        meta_key.column_family_id_ = atoi(cf_buf);
        meta_key.level_ = atoi(level_buf);
        meta_key.sequence_number_ = atol(seq_buf);
        char lbuf[512];
        util::str_to_hex(key_buf, strlen(key_buf), lbuf, sizeof(lbuf));
        largest_key.Set(Slice(lbuf, strlen(key_buf) / 2), atol(key_seq_buf),
                        static_cast<ValueType>(atoi(key_type_buf)));
        meta_key.largest_key_ = largest_key.Encode();
        int64_t size = meta_key.get_serialize_size();
        char kbuf[512];
        int64_t pos = 0;
        meta_key.serialize(kbuf, size, pos);

        mem_table_seq = atol(mem_table_seq_buf);
        mem_table_type = static_cast<ValueType>(atoi(mem_table_type_buf));

        struct Record r;
        r.key.assign(kbuf, size);
        r.seq = mem_table_seq;
        r.type = mem_table_type;
        records.push_back(r);
      } else {  // value line
        ret = sscanf(line.data(),
                     "value[%[0-9]]: start key:[%[0-9A-Z]][%[0-9]][%[0-9]], "
                     "extent[%[0-9],%[0-9]]",
                     index_buf, key_buf, key_seq_buf, key_type_buf, offset_buf,
                     file_num_buf);
        if (ret <= 5) {  // ignore the lines neither key nor value
          continue;
        }
        char sbuf[512];
        util::str_to_hex(key_buf, strlen(key_buf), sbuf, sizeof(sbuf));
        smallest_key.Set(Slice(sbuf, strlen(key_buf) / 2), atol(key_seq_buf),
                         static_cast<ValueType>(atoi(key_type_buf)));
        meta_value.smallest_key_ = smallest_key.Encode();
        meta_value.extent_id_.file_number = atoi(file_num_buf);
        meta_value.extent_id_.offset = atoi(offset_buf);
        int64_t size = meta_value.get_serialize_size();
        char vbuf[512];
        int64_t pos = 0;
        meta_value.serialize(vbuf, size, pos);

        // value line is always following the key line closely
        records.back().value.assign(vbuf, size);
      }
      */
    }

    // sort the records with sequence number
    std::sort(records.begin(), records.end());
    WriteBatch batch;
    for (auto &r : records) {
      if (r.type == ValueType::kTypeValue) {
        batch.Put(r.key, r.value.empty() ? default_value : r.value);
      } else if (r.type == kTypeDeletion) {
        batch.Delete(r.key);
      }
    }
    //TODO:yuanfeng
    //mgr->write(batch);

    return 0;
  }

  void analyze_memtable() {
    //TODO:yuanfeng
    /*
    for (auto table : {mgr->storage_meta_level1_, mgr->storage_meta_level2_}) {
      ReadOptions ro;
      Arena arena_;
      InternalIterator *iter = table->NewIterator(ro, &arena_);
      std::unordered_set<std::string> valid_keys;
      std::unordered_set<std::string> delete_keys;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ParsedInternalKey mik("", 0, ValueType::kTypeValue);
        ParseInternalKey(iter->key(), &mik);

        std::string key(mik.user_key.data(), mik.user_key.size());
        if (valid_keys.find(key) != valid_keys.end() ||
            delete_keys.find(key) != delete_keys.end()) {
          continue;
        }
        if (mik.type == ValueType::kTypeValue) {
          valid_keys.insert(key);
        } else if (mik.type == kTypeDeletion) {
          delete_keys.insert(key);
        }
      }

      std::unordered_set<int64_t> seqs;
      for (auto &key : valid_keys) {
        MetaKey meta_key;
        int64_t pos = 0;
        meta_key.deserialize(key.data(), key.size(), pos);
        seqs.insert(meta_key.sequence_number_);

        if (meta_key.largest_key_.compare(largest_user_key_) > 0) {
          largest_user_key_.assign(meta_key.largest_key_.data(),
                                   meta_key.largest_key_.size() - 8);
        }
      }
      if (table == mgr->storage_meta_level1_) {
        printf("level0 levels: %ld\n", seqs.size());
      }
    }
    */
  }

  void run_search(int num) {
    //TODO:yuanfeng
    /*
    std::vector<MetaEntry> choose;
    SnapshotImpl *s0 = new SnapshotImpl();
    s0->number_ = mgr->internal_version_;
    s0->level0_ = mgr->current_;

    int i = 0;
    while (i < num) {
      // put arena here to make sure the memory freed in time,
      // or most of the time wasted on memory allocation
      Arena arena;
      for (auto &key : search_ikeys_) {
        int32_t sorted_run = 0;
        choose.clear();
        Status s = mgr->search(key, 0, reinterpret_cast<Snapshot *>(s0),
                               choose, arena, sorted_run);
        i++;
      }
    }
    */
  }

  void do_perf_parallel(int num_threads) {
    //TODO:yuanfeng
    /*
    const int NUM = 5000000;
    {
      // generate a list of search ikeys
      int64_t largest_number = 0;
      int64_t pos = 0;
      int key_size = largest_user_key_.size();
      assert(key_size >= 8);
      util::decode_fixed_int64(largest_user_key_.data(),
                               largest_user_key_.size(), pos, largest_number);

      SequenceNumber snapshot = mgr->internal_version_;
      char buf[key_size];
      memset(buf, 0, key_size);
      // 90% keys in range, 10% out of range
      const int RANGE = 10000;
      const int IN_RANGE = 9000;
      uint64_t step = largest_number / IN_RANGE;
      assert(step >= 1);
      int num[RANGE];
      std::iota(num, num + IN_RANGE, 0);
      std::iota(num + IN_RANGE, num + RANGE, 2 * IN_RANGE);
      for (int n : num) {
        pos = 0;
        util::encode_fixed_int64(buf, key_size, pos, step * n);
        std::string key(buf, key_size);
        LookupKey lkey(key, snapshot);
        Slice ikey = lkey.internal_key();
        search_ikeys_.emplace_back(ikey.data(), ikey.size());
      }
    }

    struct timeval tv_start, tv_end;
    struct rusage ru_start, ru_end;
    getrusage(RUSAGE_SELF, &ru_start);
    gettimeofday(&tv_start, nullptr);

    std::vector<std::thread> threads;
    for (int i = 0; i < num_threads; i++) {
      threads.push_back(
          std::thread(&StorageManagerTest::run_search, this, NUM));
    }
    for (auto &th : threads) {
      th.join();
    }

    gettimeofday(&tv_end, nullptr);
    getrusage(RUSAGE_SELF, &ru_end);
    int64_t utime, stime, ttime;
    int64_t minflt, majflt, nvcsw, nivcsw;
    utime = delta(ru_end.ru_utime, ru_start.ru_utime);
    stime = delta(ru_end.ru_stime, ru_start.ru_stime);
    ttime = delta(tv_end, tv_start);
    minflt = ru_end.ru_minflt - ru_start.ru_minflt;
    majflt = ru_end.ru_majflt - ru_start.ru_majflt;
    nvcsw = ru_end.ru_nvcsw - ru_start.ru_nvcsw;
    nivcsw = ru_end.ru_nivcsw - ru_start.ru_nivcsw;
    printf("       num threads: %8d\n", num_threads);
    printf(" num search/thread: %8d\n", NUM);
    printf("         user time: %8.3f s\n", utime / 1000000.);
    printf("          sys time: %8.3f s\n", stime / 1000000.);
    printf("       passed time: %8.3f s\n", ttime / 1000000.);
    printf("       minor fault: %8ld\n", minflt);
    printf("       major fault: %8ld\n", majflt);
    printf("             nvcsw: %8ld\n", nvcsw);
    printf("            nivcsw: %8ld\n", nivcsw);
    */
  }

  void run(int num_threads) {
    analyze_memtable();
    //TODO:yuanfeng
    /*
    printf("l0 num entries/deletes: %ld %ld\n",
           mgr->storage_meta_level1_->num_entries(),
           mgr->storage_meta_level1_->num_deletes());
    printf("l1 num entries/deletes: %ld %ld\n",
           mgr->storage_meta_level2_->num_entries(),
           mgr->storage_meta_level2_->num_deletes());
    */
    // mgr->print_raw_meta();
    do_perf_parallel(num_threads);
  }

 private:
  struct LogReporter : public db::log::Reader::Reporter {
    Status *status;
    virtual void Corruption(size_t bytes, const Status &s) override {
      if (this->status->ok()) *this->status = s;
    }
  };
  EnvOptions env_options_;
  ImmutableDBOptions im_db_options_;
  MutableCFOptions mutable_cf_options_;
  std::shared_ptr<Cache> table_cache_;
  WriteController write_controller_;
  WriteBufferManager write_buffer_manager_;
  std::unique_ptr<VersionSet> versions_;
  Env *env_;
  std::vector<ColumnFamilyHandle *> handles_;
  std::vector<std::string> names_;
  std::set<std::string> keys_;
  ColumnFamilyOptions column_family_options_;
  std::string dbname_;
  Options options_;
  DB *db_ = nullptr;
  FileNumber next_file_number_;
};
}
}

int main(int argc, char **argv) {
  ParseCommandLineFlags(&argc, &argv, true);

  storage::StorageManagerTest test;
  for (int i = 1; i < argc; i++) {
    test.load_data(argv[i]);
  }
  test.run(FLAGS_threads);
}
