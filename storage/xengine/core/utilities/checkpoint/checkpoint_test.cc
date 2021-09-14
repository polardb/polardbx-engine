//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

// Syncpoint prevents us building and running tests in release
#ifndef ROCKSDB_LITE

#ifndef OS_WIN
#include <unistd.h>
#endif
#include <iostream>
#include <thread>
#include <utility>
#include "db/db_impl.h"
#include "port/port.h"
#include "port/stack_trace.h"
#include "util/sync_point.h"
#include "util/testharness.h"
#include "xengine/db.h"
#include "xengine/env.h"
#include "xengine/utilities/checkpoint.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace common;
using namespace port;
using namespace db;
using namespace table;
using namespace cache;

namespace xengine {
namespace util {
class CheckpointTest : public testing::Test {
 protected:
  // Sequence of option configurations to try
  enum OptionConfig {
    kDefault = 0,
  };
  int option_config_;

 public:
  std::string dbname_;
  std::string alternative_wal_dir_;
  Env* env_;
  DB* db_;
  Options last_options_;
  std::vector<ColumnFamilyHandle*> handles_;
  std::string backup_dir;

  CheckpointTest() : env_(Env::Default()) {
    env_->SetBackgroundThreads(1, Env::LOW);
    env_->SetBackgroundThreads(1, Env::HIGH);
    dbname_ = test::TmpDir(env_) + "/db_test";
    backup_dir = test::TmpDir(env_) + "hotbackup";
    alternative_wal_dir_ = dbname_ + "/wal";
    auto options = CurrentOptions();
    auto delete_options = options;
    delete_options.wal_dir = alternative_wal_dir_;
    EXPECT_OK(DestroyDB(dbname_, delete_options));
    // Destroy it for not alternative WAL dir is used.
    EXPECT_OK(DestroyDB(dbname_, options));
    db_ = nullptr;
    Reopen(options);
  }

  ~CheckpointTest() {
    SyncPoint::GetInstance()->DisableProcessing();
    SyncPoint::GetInstance()->LoadDependency({});
    SyncPoint::GetInstance()->ClearAllCallBacks();
    Close();
    Options options;
    options.db_paths.emplace_back(dbname_, 0);
    options.db_paths.emplace_back(dbname_ + "_2", 0);
    options.db_paths.emplace_back(dbname_ + "_3", 0);
    options.db_paths.emplace_back(dbname_ + "_4", 0);
    EXPECT_OK(DestroyDB(dbname_, options));
  }

  // Return the current option configuration.
  Options CurrentOptions() {
    Options options;
    options.env = env_;
    options.create_if_missing = true;
    return options;
  }

  void CreateColumnFamilies(const std::vector<std::string>& cfs,
                            const Options& options) {
    ColumnFamilyOptions cf_opts(options);
    size_t cfi = handles_.size();
    handles_.resize(cfi + cfs.size());
    for (auto cf : cfs) {
      ASSERT_OK(db_->CreateColumnFamily(cf_opts, cf, &handles_[cfi++]));
    }
  }

  void CreateAndReopenWithCF(const std::vector<std::string>& cfs,
                             const Options& options) {
    CreateColumnFamilies(cfs, options);
    std::vector<std::string> cfs_plus_default = cfs;
    cfs_plus_default.insert(cfs_plus_default.begin(), kDefaultColumnFamilyName);
    ReopenWithColumnFamilies(cfs_plus_default, options);
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const std::vector<Options>& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  void ReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                const Options& options) {
    ASSERT_OK(TryReopenWithColumnFamilies(cfs, options));
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const std::vector<Options>& options) {
    Close();
    EXPECT_EQ(cfs.size(), options.size());
    std::vector<ColumnFamilyDescriptor> column_families;
    for (size_t i = 0; i < cfs.size(); ++i) {
      column_families.push_back(ColumnFamilyDescriptor(cfs[i], options[i]));
    }
    DBOptions db_opts = DBOptions(options[0]);
    return DB::Open(db_opts, dbname_, column_families, &handles_, &db_);
  }

  Status TryReopenWithColumnFamilies(const std::vector<std::string>& cfs,
                                     const Options& options) {
    Close();
    std::vector<Options> v_opts(cfs.size(), options);
    return TryReopenWithColumnFamilies(cfs, v_opts);
  }

  void Reopen(const Options& options) { ASSERT_OK(TryReopen(options)); }

  void Close() {
    for (auto h : handles_) {
      delete h;
    }
    handles_.clear();
    delete db_;
    db_ = nullptr;
  }

  void DestroyAndReopen(const Options& options) {
    // Destroy using last options
    Destroy(last_options_);
    ASSERT_OK(TryReopen(options));
  }

  void Destroy(const Options& options) {
    Close();
    ASSERT_OK(DestroyDB(dbname_, options));
  }

  Status ReadOnlyReopen(const Options& options) {
    return DB::OpenForReadOnly(options, dbname_, &db_);
  }

  Status TryReopen(const Options& options) {
    Close();
    last_options_ = options;
    return DB::Open(options, dbname_, &db_);
  }

  Status Flush(int cf = 0) {
    if (cf == 0) {
      return db_->Flush(FlushOptions());
    } else {
      return db_->Flush(FlushOptions(), handles_[cf]);
    }
  }

  Status Put(const Slice& k, const Slice& v, WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, k, v);
  }

  Status Put(int cf, const Slice& k, const Slice& v,
             WriteOptions wo = WriteOptions()) {
    return db_->Put(wo, handles_[cf], k, v);
  }

  Status Delete(const std::string& k) { return db_->Delete(WriteOptions(), k); }

  Status Delete(int cf, const std::string& k) {
    return db_->Delete(WriteOptions(), handles_[cf], k);
  }

  std::string Get(const std::string& k, const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  std::string Get(int cf, const std::string& k,
                  const Snapshot* snapshot = nullptr) {
    ReadOptions options;
    options.verify_checksums = true;
    options.snapshot = snapshot;
    std::string result;
    Status s = db_->Get(options, handles_[cf], k, &result);
    if (s.IsNotFound()) {
      result = "NOT_FOUND";
    } else if (!s.ok()) {
      result = s.ToString();
    }
    return result;
  }

  int stream_extent(const char *path, int fd, int64_t offset, int dest_fd) {
    char cmd[1024];
    snprintf(cmd, 1024, "cd %s && xstream -E %ld -c %s | xstream -x -C %s", 
             dbname_.c_str(), offset/(2 * 1024 * 1024), path, backup_dir.c_str());
    int ret = system(cmd);
    return ret;
  }
};

TEST_F(CheckpointTest, hotbackup) {
  char cmd[1024]; 
  int ret = system("xstream --help > /dev/null");
  if (ret != Status::kOk) {
    fprintf(stderr, "Cant' run this test without xstream\n");
    return;
  } 
  std::function<int(const char*, int, int64_t, int)> 
  stream_function(std::bind(&CheckpointTest::stream_extent, this, 
    std::placeholders::_1, std::placeholders::_2, std::placeholders::_3, std::placeholders::_4)); 
  // Create a database
  Options options = CurrentOptions();
  std::string snapshot_name = test::TmpDir(env_) + "/snapshot/1";
  ASSERT_OK(DestroyDB(snapshot_name, options));
  env_->DeleteDir(snapshot_name);
  CreateAndReopenWithCF({}, options); 
  ASSERT_OK(Put(0, "Default", "Default"));
  Flush();
  ASSERT_OK(Put(0, "one", "one"));
  Flush();
  ASSERT_OK(Put(0, "two", "two"));
  Flush();
  ASSERT_OK(Put(0, "three", "three"));
  Flush();
  ASSERT_OK(Put(0, "four", "four"));
  Flush();
  ASSERT_OK(Put(0, "five", "five"));
  Flush();
  Checkpoint* checkpoint = nullptr;
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->manual_checkpoint(snapshot_name, 1, &stream_function, -1));
  // copy the sst
  std::vector<std::string> subchildren;
  db_->GetEnv()->GetChildren(snapshot_name, &subchildren);
  db_->GetEnv()->CreateDir(backup_dir);
  for (auto& subchild : subchildren) {
    if (subchild != "." && subchild != "..") {
      fprintf(stdout, "Copy %s phase 1\n", subchild.c_str());
      snprintf(cmd, 1024, "cd %s && xstream -c %s | xstream -x -C %s", 
               snapshot_name.c_str(), subchild.c_str(), backup_dir.c_str());
      ret = system(cmd);
      ASSERT_EQ(ret, Status::kOk);
    }
  }
  ASSERT_OK(DestroyDB(snapshot_name, options));
  env_->DeleteDir(snapshot_name);
  delete checkpoint;
  ASSERT_OK(Put(0, "Default_1", "Default_1"));
  Flush();
  ASSERT_OK(Put(0, "one_1", "one_1"));
  Flush();
  ASSERT_OK(Put(0, "two_1", "two_1"));
  Flush();
  ASSERT_OK(Put(0, "three_1", "three_1"));
  Flush();
  ASSERT_OK(Put(0, "four_1", "four_1"));
  Flush();
  ASSERT_OK(Put(0, "five_1", "five_1"));
  Flush();
  // test multi sst file
  char key[16];
  char value[16];
  for (int i = 0; i < 512; i++) {
    snprintf(key, 16, "key_%d", i);
    snprintf(value, 16, "value_%d", i);
    ASSERT_OK(Put(0, key, value));
    Flush();
  }
  snapshot_name = test::TmpDir(env_) + "/snapshot/2"; 
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->manual_checkpoint(snapshot_name, 2, &stream_function, -1));
  db_->GetEnv()->GetChildren(snapshot_name, &subchildren);
  for (auto& subchild : subchildren) {
    if (subchild != "." && subchild != "..") {
      fprintf(stdout, "Copy %s phase 2\n", subchild.c_str());
      if (subchild == "extents.inc") {
        snprintf(cmd, 1024, "cd %s && xstream -e %s -c %s | xstream -x -C %s", 
                 snapshot_name.c_str(), dbname_.c_str(), subchild.c_str(), backup_dir.c_str());
      } else {
        snprintf(cmd, 1024, "cd %s && xstream -c %s | xstream -x -C %s", 
                 snapshot_name.c_str(), subchild.c_str(), backup_dir.c_str());
      }
      ret = system(cmd);
      ASSERT_EQ(ret, Status::kOk);
    }
  }
  delete checkpoint;
  ASSERT_OK(Put(0, "Default_2", "Default_2"));
  Flush();
  ASSERT_OK(Put(0, "one_2", "one_2"));
  Flush();
  ASSERT_OK(Put(0, "two_2", "two_2"));
  Flush();
  // test input large object
  char large_value_1[2 * 1024 * 1024 + 1];
  int len = 2 * 1024 * 1024 + 1;
  for (int i = 0; i < len; i++) {
    large_value_1[i] = 'k';
  } 
  large_value_1[len] = '\0';
  ASSERT_OK(Put(0, "large_obj_1", large_value_1));
  Flush();
  char large_value_2[2 * 1024 * 1024 + 1];
  for (int i = 0; i < len; i++) {
    large_value_2[i] = 'z';
  } 
  large_value_2[len] = '\0';
  ASSERT_OK(Put(0, "large_obj_2", large_value_2));
  Flush();
  ASSERT_OK(Put(0, "three_2", "three_2"));
  ASSERT_OK(Put(0, "four_2", "four_2"));
  ASSERT_OK(Put(0, "five_2", "five_2"));
  snapshot_name = test::TmpDir(env_) + "/snapshot/3";
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->manual_checkpoint(snapshot_name, 3, &stream_function, -1));
  db_->GetEnv()->GetChildren(snapshot_name, &subchildren); 
  for (auto& subchild : subchildren) {
    if (subchild != "." && subchild != "..") {
      if (subchild.compare(subchild.find_last_of('.'), subchild.length(), ".tmp") != 0) {
        fprintf(stdout, "Copy %s phase 3\n", subchild.c_str());
        snprintf(cmd, 1024, "cd %s && xstream -c %s | xstream -x -C %s", 
               snapshot_name.c_str(), subchild.c_str(), backup_dir.c_str());
        ret = system(cmd);
        ASSERT_EQ(ret, Status::kOk);
      } 
    }
  }
  delete checkpoint;
  ASSERT_OK(Put(0, "Default_3", "Default_3"));
  Flush();
  ASSERT_OK(Put(0, "one_3", "one_3"));
  Flush();
  ASSERT_OK(Put(0, "two_3", "two_3"));
  Flush();
  ASSERT_OK(Put(0, "three_3", "three_3"));
  ASSERT_OK(Put(0, "four_3", "four_3"));
  ASSERT_OK(Put(0, "five_3", "five_3"));
  snapshot_name = test::TmpDir(env_) + "/snapshot/4";
  ASSERT_OK(Checkpoint::Create(db_, &checkpoint));
  ASSERT_OK(checkpoint->manual_checkpoint(snapshot_name, 4, &stream_function, -1)); 
  db_->GetEnv()->GetChildren(snapshot_name, &subchildren); 
  for (auto& subchild : subchildren) {
    if (subchild != "." && subchild != "..") {
      fprintf(stdout, "Copy %s phase 4\n", subchild.c_str()); 
      if (0 == subchild.compare(0, 8, "MANIFEST")) {  
        snprintf(cmd, 1024, "cd %s && xstream -a %s | xstream -x -C %s", 
               snapshot_name.c_str(), subchild.c_str(), backup_dir.c_str());
        ret = system(cmd);
        ASSERT_EQ(ret, Status::kOk);
      } else {
        if (subchild == "extents.inc") {
          snprintf(cmd, 1024, "cd %s && xstream -e %s -c %s | xstream -x -C %s", 
                   snapshot_name.c_str(), dbname_.c_str(), subchild.c_str(), backup_dir.c_str());
          ASSERT_OK(checkpoint->manual_checkpoint(snapshot_name, 5, &stream_function, -1));
        } else {
          snprintf(cmd, 1024, "cd %s && xstream -c %s | xstream -x -C %s", 
                   snapshot_name.c_str(), subchild.c_str(), backup_dir.c_str());
        }
        
        ret = system(cmd);
        ASSERT_EQ(ret, Status::kOk);
      } 
    }
  }
  ASSERT_OK(DestroyDB(snapshot_name, options));
  env_->DeleteDir(snapshot_name);
  Close();
  auto delete_options = options;
  delete_options.wal_dir = alternative_wal_dir_;
  EXPECT_OK(DestroyDB(dbname_, delete_options));
  // Destroy it for not alternative WAL dir is used.
  EXPECT_OK(DestroyDB(dbname_, options));
  db_ = nullptr;
  options.create_if_missing = false;
  BlockBasedTableOptions *table_options = (BlockBasedTableOptions *)options.table_factory->GetOptions();
  table_options->block_cache = NewLRUCache(1024 * 1024);
  ASSERT_OK(DB::Open(options, backup_dir, &db_));
  ASSERT_EQ("Default", Get("Default"));
  ASSERT_EQ("five", Get("five"));
  ASSERT_EQ("Default_1", Get("Default_1"));
  ASSERT_EQ("five_1", Get("five_1"));
  ASSERT_EQ("Default_2", Get("Default_2"));
  ASSERT_EQ("five_2", Get("five_2"));
  ASSERT_EQ(large_value_1, Get("large_obj_1"));
  ASSERT_EQ(large_value_2, Get("large_obj_2"));
  ASSERT_EQ("value_0", Get("key_0"));
  ASSERT_EQ("value_511", Get("key_511"));
  ASSERT_EQ("NOT_FOUND", Get("Default_3"));
  ASSERT_EQ("NOT_FOUND", Get("five_3"));
  Close();
  EXPECT_OK(DestroyDB(backup_dir, options));
}

}  //  namespace util
}  //  namespace xengine

int main(int argc, char** argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}

#else
#include <stdio.h>

int main(int argc, char** argv) {
  fprintf(stderr, "SKIPPED as Checkpoint is not supported in ROCKSDB_LITE\n");
  return 0;
}

#endif  // !ROCKSDB_LITE
