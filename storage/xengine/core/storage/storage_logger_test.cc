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
#include "util/testharness.h"
#include "util/testutil.h"
#include <gtest/gtest.h>
#include "xengine/status.h"
#include "storage_logger.h"

namespace xengine
{
using namespace common;
namespace storage
{

bool check_manifest_log_header(ManifestLogEntryHeader &expected_header, ManifestLogEntryHeader &actual_header)
{
  return expected_header.trans_id_ == actual_header.trans_id_
         && expected_header.log_entry_seq_ == actual_header.log_entry_seq_
         && expected_header.log_entry_type_ == actual_header.log_entry_type_
         && expected_header.log_entry_length_ == actual_header.log_entry_length_;
}

TEST(StorageLoggerBufferTest, assign_and_append)
{
  int ret = Status::kOk;
  const char *DB_DIR = "./log_test_zds/";
  common::Options options;
  options.create_if_missing = true;
  //TODO:yuanfeng
  //ret = CreateLoggerFromOptions(DB_DIR, options, &options.info_log).code();
  ASSERT_EQ(common::Status::kOk, ret);

  StorageLoggerBuffer log_buffer;
  char *buf = nullptr;
  int64_t buf_size = 2 * 1024 * 1024; //2Mb

  //invalid buffer
  ret = log_buffer.assign(buf, buf_size);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //invalid buf_size
  buf = new char[buf_size];
  ret = log_buffer.assign(buf, -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //valid param
  ret = log_buffer.assign(buf, buf_size);
  ASSERT_EQ(Status::kOk, ret);

  //append invalid log entry header
  ManifestLogEntryHeader log_entry_header;
  log_entry_header.trans_id_ = -1;
  ret = log_buffer.append_log(log_entry_header, nullptr, 0);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  
  //append invalid log data
  log_entry_header.trans_id_ = 1;
  log_entry_header.log_entry_seq_ = 1;
  log_entry_header.log_entry_length_ = 1;
  ret = log_buffer.append_log(log_entry_header, nullptr, -1);
  ASSERT_EQ(Status::kInvalidArgument, ret);
  ret = log_buffer.append_log(log_entry_header, nullptr, 1);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //append log data over the log buffer size
  log_buffer.assign(buf, 1);
  ret = log_buffer.append_log(log_entry_header, nullptr, 0);
  ASSERT_EQ(Status::kNoSpace, ret);

  //append log data success
  log_buffer.assign(buf, buf_size);
  ret = log_buffer.append_log(log_entry_header, nullptr, 0);
  ASSERT_EQ(Status::kOk, ret);

  //append log entry with invalid log entry header
  /*
  log_entry_header.trans_id_ = -1;
  ChangePartitionGroupLogEntry log_entry;
  ret = log_buffer.append_log(log_entry_header, log_entry);
  ASSERT_EQ(Status::kInvalidArgument, ret);

  //append log entry over the buffer size
  log_entry_header.trans_id_ = 1;
  log_buffer.assign(buf, 1);
  ret = log_buffer.append_log(log_entry_header, log_entry);
  ASSERT_EQ(Status::kNoSpace, ret);
  */

  //append log entry success
  /*
  ManifestLogEntryHeader log_entry_header_1;
  ChangePartitionGroupLogEntry log_entry_1;
  log_entry_header_1.trans_id_ = 1;
  log_entry_header_1.log_entry_seq_ = 1;
  log_entry_header_1.log_entry_length_ = log_entry_1.get_serialize_size();
  log_buffer.assign(buf, buf_size);
  ret = log_buffer.append_log(log_entry_header_1, log_entry_1);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(log_buffer.length(), log_entry_header_1.get_serialize_size() + log_entry_1.get_serialize_size());

  //append special log entry success
  ManifestLogEntryHeader log_entry_header_2;
  ChangePartitionGroupLogEntry log_entry_2;
  log_entry_2.partition_group_id_.table_family_id_ = 1;
  log_entry_2.partition_group_id_.partition_id_ = 2;
  log_entry_header_2.trans_id_ = 2;
  log_entry_header_2.log_entry_seq_ = 2;
  log_entry_header_2.log_entry_type_ = REDO_LOG_ADD_PARTITION_GROUP;
  log_entry_header_2.log_entry_length_ = log_entry_2.get_serialize_size();
  ret = log_buffer.append_log(log_entry_header_2, log_entry_2);
  ASSERT_EQ(Status::kOk, ret);

  //read log
  StorageLoggerBuffer read_log_buffer;
  char *log_data = nullptr;
  int64_t log_length = 0;
  int64_t pos = 0;
  ManifestLogEntryHeader read_entry_header;
  ChangePartitionGroupLogEntry read_log_entry;
  ret = read_log_buffer.assign(buf, log_buffer.length());
  ASSERT_EQ(Status::kOk, ret);
  ret = read_log_buffer.read_log(read_entry_header, log_data, log_length);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_EQ(log_data - buf, log_entry_header_1.get_serialize_size());
  ASSERT_EQ(log_length, log_entry_1.get_serialize_size());
  ret = read_log_entry.deserialize(log_data, log_length, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(check_manifest_log_header(log_entry_header_1, read_entry_header));
  ASSERT_EQ(-1, read_log_entry.partition_group_id_.table_family_id_);
  ASSERT_EQ(-1, read_log_entry.partition_group_id_.partition_id_);

  //read special log entry
  pos = 0;
  ret = read_log_buffer.read_log(read_entry_header, log_data, log_length);
  ASSERT_EQ(Status::kOk, ret);
  ret = read_log_entry.deserialize(log_data, log_length, pos);
  ASSERT_EQ(Status::kOk, ret);
  ASSERT_TRUE(check_manifest_log_header(log_entry_header_2, read_entry_header));
  ASSERT_EQ(1, read_log_entry.partition_group_id_.table_family_id_);
  ASSERT_EQ(2, read_log_entry.partition_group_id_.partition_id_);
  */
  delete[] buf;
}

}
}

int main(int argc, char **argv)
{
  ::testing::InitGoogleTest(&argc, argv);
  std::string log_path = xengine::util::test::TmpDir() + "/storage_logger_test.log";
  xengine::logger::Logger::get_log().init(log_path.c_str(), xengine::logger::WARN_LEVEL);
  return RUN_ALL_TESTS();
}
