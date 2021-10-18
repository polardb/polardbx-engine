/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef ROCKSDB_LIT

#include "utilities/checkpoint/hotbackup_impl.h"
#include "util/testharness.h"
#include "port/stack_trace.h"
#include "storage/io_extent.h"
#include "xengine/env.h"
#define private public
#define protected public
#include "db/db_test_util.h"

namespace xengine
{
using namespace common;
using namespace db;
using namespace storage;

namespace util
{

class HotbackupTest : public DBTestBase
{
public:
  HotbackupTest() : DBTestBase("/hotbackup_test") 
  {
    backup_tmp_dir_path_ = db_->GetName() + BACKUP_TMP_DIR;
  }
  const char *backup_dir_ = "/backup_dir";
  void replay_sst_files(const std::string &backup_tmp_dir_path,
                        const std::string &extent_ids_file, 
                        const std::string &extent_file);
  void copy_sst_files();
  void copy_rest_files(const std::string &backup_tmp_dir_path);
  void copy_extents(const std::string &backup_tmp_dir_path,
                    const std::string &extent_ids_file, 
                    const std::string &extent_file);
  uint64_t last_sst_file_num_ = 0;
  std::string backup_tmp_dir_path_;
};

struct RestFileChecker
{
  RestFileChecker(const uint64_t last_sst_file_num) : last_sst_file_num_(last_sst_file_num) {}
  inline bool operator()(const util::FileType &type, const uint64_t &file_num)
  {
    return (type == util::kTableFile && file_num > last_sst_file_num_)
        || (type == util::kLogFile)
        || (type == util::kDescriptorFile)
        || (type == util::kCheckpointFile)
        || (type == kCurrentFile)
        || (type == kCurrentCheckpointFile);
  }
  uint64_t last_sst_file_num_;
};

void HotbackupTest::replay_sst_files(const std::string &backup_dir,
                                     const std::string &extent_ids_file,
                                     const std::string &extent_file)
{
//  std::unique_ptr<SequentialFile> id_reader;
//  std::unique_ptr<SequentialFile> extent_reader;
  SequentialFile *id_reader = nullptr;
  SequentialFile *extent_reader = nullptr;
  char buf[16];
  char *extent_buf = nullptr;
  EnvOptions r_opt;
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_ids_file, id_reader, r_opt));
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_file, extent_reader, r_opt));
  std::unordered_map<int32_t, int32_t> fds_map;
  int fd = -1;
  std::string fname;
  int64_t i = 0;
  Slice id_res;
  Slice extent_res;
  ASSERT_TRUE(nullptr != (extent_buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, storage::MAX_EXTENT_SIZE, memory::ModId::kDefaultMod))));
  while (id_reader->Read(8, &id_res, buf).ok() && id_res.size() == 8) {
    ExtentId extent_id(*reinterpret_cast<const int64_t*>(id_res.data()));
    // read from extent file
    ASSERT_TRUE(extent_reader->Read(storage::MAX_EXTENT_SIZE, &extent_res, extent_buf).ok() 
        && extent_res.size() == storage::MAX_EXTENT_SIZE);
    // write to sst file
    auto iter = fds_map.find(extent_id.file_number);
    if (fds_map.end() == iter) {
      fname = MakeTableFileName(backup_dir, extent_id.file_number);
      do {
        fd = open(fname.c_str(), O_WRONLY, 0644);
      } while (fd < 0 && errno == EINTR);
      ASSERT_TRUE(fd > 0) << "file: " << fname;
      fds_map[extent_id.file_number] = fd;
    } else {
      fd = iter->second;
    }
    ASSERT_OK(unintr_pwrite(fd, extent_buf, storage::MAX_EXTENT_SIZE, extent_id.offset * storage::MAX_EXTENT_SIZE));
    XENGINE_LOG(INFO, "replay extent", K(extent_id));
  }
  for (auto &iter : fds_map) {
    close(iter.second);
  }
  if (nullptr != extent_buf) {
    memory::base_memalign_free(extent_buf);
  }
}

void HotbackupTest::copy_extents(const std::string &backup_tmp_dir_path,
                                 const std::string &extent_ids_file,
                                 const std::string &extent_file)
{
  // read extent id file
//  std::unique_ptr<SequentialFile> reader;
  SequentialFile *reader = nullptr;
  EnvOptions r_opt;
  EnvOptions w_opt;
  w_opt.use_mmap_writes = false;
  w_opt.use_direct_writes = false;
  ASSERT_OK(db_->GetEnv()->NewSequentialFile(extent_ids_file, reader, r_opt));
//  std::unique_ptr<WritableFile> extent_writer;
  WritableFile *extent_writer = nullptr;
  ASSERT_OK(NewWritableFile(env_, extent_file, extent_writer, w_opt));
  char buf[16];
  char *extent_buf = nullptr;
  Slice result;
  //
  std::unordered_map<int32_t, int32_t> fds_map;
  int fd = -1;
  std::string fname;
  ASSERT_TRUE(nullptr != (extent_buf = reinterpret_cast<char *>(memory::base_memalign(PAGE_SIZE, storage::MAX_EXTENT_SIZE, memory::ModId::kDefaultMod))));
  while (reader->Read(8, &result, buf).ok() && result.size() > 0) {
    ExtentId extent_id(*reinterpret_cast<const int64_t*>(result.data()));
    auto iter = fds_map.find(extent_id.file_number);
    if (fds_map.end() == iter) {
      fname = MakeTableFileName(backup_tmp_dir_path, extent_id.file_number);
      do {
        fd = open(fname.c_str(), O_RDONLY, 0644);
      } while (fd < 0 && errno == EINTR);
      ASSERT_TRUE(fd > 0);
      fds_map[extent_id.file_number] = fd;
    } else {
      fd = iter->second;
    }
    ASSERT_OK(unintr_pread(fd, extent_buf, storage::MAX_EXTENT_SIZE, extent_id.offset * storage::MAX_EXTENT_SIZE));
    ASSERT_OK(extent_writer->Append(Slice(extent_buf, storage::MAX_EXTENT_SIZE)));
    XENGINE_LOG(INFO, "copy extent", K(extent_id));
  }
  ASSERT_OK(extent_writer->Close());
  for (auto &iter : fds_map) {
    close(iter.second);
  }
  if (nullptr != extent_buf) {
    memory::base_memalign_free(extent_buf);
  }
}

void HotbackupTest::copy_sst_files()
{
  std::vector<std::string> all_files;
  SSTFileChecker file_checker;
  ASSERT_OK(db_->GetEnv()->GetChildren(dbname_, &all_files));
  uint64_t file_num = 0;
  util::FileType type;
  char cmd[1024];
  std::string backup_path = dbname_ + backup_dir_;
  ASSERT_OK(db_->GetEnv()->CreateDir(backup_path));
  for (auto &file : all_files) {
    if (ParseFileName(file, &file_num, &type) && file_checker(type, file_num)) {
      std::string file_path = dbname_ + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", file_path.c_str(), backup_path.c_str());
      XENGINE_LOG(INFO, "copy sst file", K(cmd));
      ASSERT_EQ(0, system(cmd));
      if (file_num > last_sst_file_num_) {
        last_sst_file_num_ = file_num;
      }
    }
  }
}

void HotbackupTest::copy_rest_files(const std::string &backup_tmp_dir_path)
{
  std::vector<std::string> all_files;
  RestFileChecker file_checker(last_sst_file_num_);
  ASSERT_OK(db_->GetEnv()->GetChildren(backup_tmp_dir_path, &all_files));
  uint64_t file_num = 0;
  util::FileType type;
  char cmd[1024];
  std::string backup_path = dbname_ + backup_dir_;
  for (auto &file : all_files) {
    if ((ParseFileName(file, &file_num, &type) && file_checker(type, file_num)) 
        || file == "extent_ids.inc" || file == "extent.inc") {
      std::string file_path = backup_tmp_dir_path + "/" + file;
      snprintf(cmd, 1024, "cp %s %s", file_path.c_str(), backup_path.c_str());
      XENGINE_LOG(INFO, "copy rest sst file", K(cmd));
      ASSERT_EQ(0, system(cmd));
    }
  }
}

TEST_F(HotbackupTest, backup_job_exclusive)
{
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kInitTwice, backup_snapshot->init(db_));

  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  ASSERT_EQ(Status::kInitTwice, backup_snapshot->init(db_));

  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kInitTwice, backup_snapshot->init(db_));

  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  ASSERT_EQ(Status::kInitTwice, backup_snapshot->init(db_));
  
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));

  // with out init
  ASSERT_EQ(Status::kNotInit, backup_snapshot->do_checkpoint(db_));
  ASSERT_EQ(Status::kNotInit, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kNotInit, backup_snapshot->record_incremental_extent_ids(db_));

  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);
  Close();
  DestroyDB(backup_tmp_dir_path_, last_options_);
}

TEST_F(HotbackupTest, incremental_extent)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files();
  
  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));

  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);

  // 
  std::string backup_path = dbname_ + backup_dir_;
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  Reopen(CurrentOptions(), &backup_path); 
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, delete_cf)
{
  CreateColumnFamilies({"hotbackup", "delete_cf"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files();
  
  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));

  ASSERT_OK(Put(2, "a", "aa"));
  ASSERT_OK(Put(2, "b", "bb"));
  ASSERT_OK(Flush(2));
  DropColumnFamily(2);
  // tigger gc
  DBImpl::GCJob *gc_job = dbfull()->pop_front_gc_job();
  ASSERT_TRUE(gc_job != nullptr);
  int64_t index_id = gc_job->sub_table_->GetID();
  ASSERT_OK(gc_job->sub_table_->release_resource(false));
  ASSERT_OK(dbfull()->extent_space_manager_->unregister_subtable(gc_job->sub_table_->get_table_space_id(), index_id));
  // delete sst file
  dbfull()->extent_space_manager_->recycle_dropped_table_space();

  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_,
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);

  std::string backup_path = dbname_ + backup_dir_;
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  Reopen(CurrentOptions(), &backup_path);
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, large_object)
{
  CreateColumnFamilies({"large_object"}, CurrentOptions());
  int64_t lob_size = 5 * 1024 * 1024;
  char *lob_value = new char[lob_size];
  memset(lob_value, '0', lob_size);

  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  ASSERT_OK(Put(1, "1", Slice(lob_value, lob_size)));
  ASSERT_OK(Put(1, "2", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files();

  ASSERT_OK(Delete(1, "1"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "3", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Delete(1, "3"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "4", Slice(lob_value, lob_size)));
  ASSERT_OK(Flush(1));

  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);

  // 
  std::string backup_path = dbname_ + backup_dir_;
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  Reopen(CurrentOptions(), &backup_path); 

  ASSERT_EQ("NOT_FOUND", Get(1, "1"));
  ASSERT_EQ(lob_size, Get(1, "2").size());
  ASSERT_EQ("NOT_FOUND", Get(1, "3"));
  ASSERT_EQ(lob_size, Get(1, "4").size());

  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, multi_checkpoint)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_OK(Put(1, "1", "11"));
  ASSERT_OK(Put(1, "2", "22"));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files();
  
  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);

  // 
  std::string backup_path = dbname_ + backup_dir_;
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  Reopen(CurrentOptions(), &backup_path); 
  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, delete_manifest_before_acquire)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/MANIFEST-000005"));
  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_EQ(Status::kNotFound, backup_snapshot->record_incremental_extent_ids(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);
}

TEST_F(HotbackupTest, delete_manifest_before_acquire2)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));
  copy_sst_files();

  ASSERT_OK(Put(1, "a", "aa"));
  ASSERT_OK(Put(1, "b", "bb"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "c", "cc"));
  ASSERT_OK(Put(1, "d", "dd"));
  ASSERT_OK(Flush(1));
  db_->do_manual_checkpoint(dummy_file_num);
  ASSERT_OK(Put(1, "e", "ee"));
  ASSERT_OK(Put(1, "f", "ff"));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg"));
  ASSERT_OK(Put(1, "h", "hh"));
  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));
  ASSERT_OK(db_->GetEnv()->DeleteFile(dbname_ + "/MANIFEST-000005"));
  ASSERT_EQ(Status::kOk, backup_snapshot->record_incremental_extent_ids(db_));
  copy_extents(backup_tmp_dir_path_, 
               backup_tmp_dir_path_ + BACKUP_EXTENT_IDS_FILE,
               backup_tmp_dir_path_ + BACKUP_EXTENTS_FILE);

  copy_rest_files(backup_tmp_dir_path_);
  ASSERT_EQ(Status::kOk, backup_snapshot->release_snapshots(db_));
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);
  //
  std::string backup_path = dbname_ + backup_dir_;
  replay_sst_files(backup_path, backup_path + BACKUP_EXTENT_IDS_FILE, backup_path + BACKUP_EXTENTS_FILE);
  Reopen(CurrentOptions(), &backup_path); 
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("cc", Get(1, "c"));
  ASSERT_EQ("dd", Get(1, "d"));
  ASSERT_EQ("ee", Get(1, "e"));
  ASSERT_EQ("ff", Get(1, "f"));
  ASSERT_EQ("gg", Get(1, "g"));
  ASSERT_EQ("hh", Get(1, "h"));
  //
  Close();
  DestroyDB(backup_path, last_options_);
}

TEST_F(HotbackupTest, amber_test)
{
  CreateColumnFamilies({"hotbackup"}, CurrentOptions());
  BackupSnapshot *backup_snapshot = nullptr;
  WriteOptions wo;
  wo.disableWAL = true;
  std::string backup_path = dbname_ + backup_dir_;

  ASSERT_EQ(Status::kOk, BackupSnapshot::create(backup_snapshot));
  int32_t dummy_file_num = 0;
  ASSERT_OK(Put(1, "1", "11", wo));
  ASSERT_OK(Put(1, "2", "22", wo));
  ASSERT_OK(Flush(1));
  ASSERT_EQ(Status::kOk, backup_snapshot->init(db_, backup_path.c_str()));
  ASSERT_EQ(Status::kOk, backup_snapshot->do_checkpoint(db_));

  ASSERT_OK(Put(1, "a", "aa", wo));
  ASSERT_OK(Put(1, "b", "bb", wo));
  ASSERT_OK(Flush(1));
  
  ASSERT_EQ(Status::kOk, backup_snapshot->acquire_snapshots(db_));

  ASSERT_OK(Put(1, "c", "cc", wo));
  ASSERT_OK(Put(1, "d", "dd", wo));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "e", "ee", wo));
  ASSERT_OK(Put(1, "f", "ff", wo));
  ASSERT_OK(Flush(1));
  ASSERT_OK(Put(1, "g", "gg", wo));
  ASSERT_OK(Put(1, "h", "hh", wo));

  Reopen(CurrentOptions(), &backup_path); 

  ASSERT_EQ("11", Get(1, "1"));
  ASSERT_EQ("22", Get(1, "2"));
  ASSERT_EQ("aa", Get(1, "a"));
  ASSERT_EQ("bb", Get(1, "b"));
  ASSERT_EQ("NOT_FOUND", Get(1, "c"));
  ASSERT_EQ("NOT_FOUND", Get(1, "d"));
  ASSERT_EQ("NOT_FOUND", Get(1, "e"));
  ASSERT_EQ("NOT_FOUND", Get(1, "f"));
  ASSERT_EQ("NOT_FOUND", Get(1, "g"));
  ASSERT_EQ("NOT_FOUND", Get(1, "h"));
  //
  delete reinterpret_cast<BackupSnapshotImpl*>(backup_snapshot);
  Close();
  DestroyDB(backup_path, last_options_);
}

} // namespace util
} // namespace xengine

int main(int argc, char **argv) 
{
  xengine::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
	xengine::util::test::init_logger(__FILE__, xengine::logger::INFO_LEVEL);
  return RUN_ALL_TESTS();
}

#endif // ROCKSDB_LIT
