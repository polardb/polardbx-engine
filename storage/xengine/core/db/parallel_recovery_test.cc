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

#include "db/db_impl.h"
#include "db/replay_task.h"
#include "db/replay_thread_pool.h"
#include "db/db_test_util.h"
#include "env/mock_env.h"
#include "port/port.h"
#include "util/sync_point.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace db{

class ParallelRecoveryTest : public DBTestBase {
public:
  ParallelRecoveryTest() : DBTestBase("/parallel_recovery_test") {
    SyncPoint::GetInstance()->ClearAllCallBacks();
  }
};

TEST_F(ParallelRecoveryTest, parallel_recovery_test) {
  Options options;
  //options.write_buffer_size = 4096 * 4096;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency; 
  options.parallel_wal_recovery = true;
  options.allow_2pc = false;
  CreateAndReopenWithCF({"xiaoyuan"}, options); 
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v1"));
  char buf[32768];
  memset(buf,'a',sizeof(buf));
  std::string str(buf);
  for (int i = 0; i < 20; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_OK(Put(key, str));
  }
  ReopenWithColumnFamilies({"default", "xiaoyuan"}, options);
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("v1", Get("bar"));
  for (int i = 0; i < 20; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(key));
  }
}

TEST_F(ParallelRecoveryTest, recovery_test) {
  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency; 
  options.parallel_wal_recovery = false;
  CreateAndReopenWithCF({"xiaoyuan"}, options); 
  ASSERT_OK(Put("foo", "v1"));
  ASSERT_OK(Put("bar", "v1"));
  char buf[32768];
  memset(buf,'a',sizeof(buf));
  std::string str(buf);
  for (int i = 0; i < 20; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_OK(Put(key, str));
  }
  ReopenWithColumnFamilies({"default", "xiaoyuan"}, options);
  ASSERT_EQ("v1", Get("foo"));
  ASSERT_EQ("v1", Get("bar"));
  for (int i = 0; i < 20; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(key));
  }
}

TEST_F(ParallelRecoveryTest, parallel_recovery_task_queue_full_test) {
  SyncPoint::GetInstance()->SetCallBack(
    "ReplayThreadPoolExecutor::submit_task::queue_full", [&](void* arg) {
      ReplayThreadPoolExecutor<ReplayTaskDeleter>* replay_thread_pool =
      reinterpret_cast<ReplayThreadPoolExecutor<ReplayTaskDeleter>*>(arg);
      if (replay_thread_pool->get_submit_task_num() == 7 && !replay_thread_pool->queue_full()) {
        replay_thread_pool->set_queue_full(true);
      } else {
        replay_thread_pool->set_queue_full(false);
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.create_if_missing = true;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  CreateAndReopenWithCF({"xiaoyuan", "gavin", "test"}, options);
  ASSERT_OK(Put(2, "hello", "world"));
  ASSERT_OK(Put(1, "foo0", "v0"));
  ASSERT_OK(Put(1, "foo1", "v1"));
  ASSERT_OK(Put(1, "foo2", "v2"));
  ASSERT_OK(Put(1, "foo3", "v3"));
  ASSERT_OK(Delete(1, "foo1"));
  ASSERT_OK(Put("bar0", "v0")); // 7
  ASSERT_OK(Put("bar1", "v1"));
  ASSERT_OK(Put("bar2", "v2"));
  ASSERT_OK(Put("bar3", "v3"));
  ASSERT_OK(Put(3, "hello", "world"));
  ASSERT_OK(Delete(1, "foo2")); // 12
  ReopenWithColumnFamilies({"default", "xiaoyuan", "gavin", "test"}, options);
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("v3", Get(1, "foo3"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
  ASSERT_EQ("v0", Get("bar0"));
  ASSERT_EQ("v1", Get("bar1"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("world", Get(2, "hello"));
  ASSERT_EQ("world", Get(3, "hello"));
  SyncPoint::GetInstance()->DisableProcessing();
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(ParallelRecoveryTest, pick_memtable_test) {
  Options options;
  options.dump_memtable_limit_size = 500;
  SubTableMap subtables;
  for (int i = 0; i < 7; ++i) {
    SubTable *subtable = new SubTable(options);
    RecoveryPoint rp(0, i); // for identify
    subtable->set_recovery_point(rp);
    subtable->CreateNewMemtable(*subtable->GetLatestMutableCFOptions(), 0);
    subtables.emplace(i, subtable);
  }
  SubTable *subtable0 = subtables[0];
  subtable0->mem()->Add(1, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  SubTable *subtable1 = subtables[1];
  subtable1->mem()->Add(2, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  subtable1->mem()->Add(3, kTypeValue, Slice("key1"), Slice("value"), false, nullptr);
  SubTable *subtable2 = subtables[2];
  subtable2->mem()->Add(4, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  subtable2->mem()->Add(5, kTypeValue, Slice("key1"), Slice("value"), false, nullptr);
  subtable2->mem()->Add(6, kTypeValue, Slice("key2"), Slice("value"), false, nullptr);
  SubTable *subtable3 = subtables[3];
  subtable3->mem()->Add(7, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  subtable3->mem()->Add(8, kTypeValue, Slice("key1"), Slice("value"), false, nullptr);
  subtable3->mem()->Add(9, kTypeValue, Slice("key2"), Slice("value"), false, nullptr);
  subtable3->mem()->Add(10, kTypeValue, Slice("key3"), Slice("value"), false, nullptr);
  SubTable *subtable4 = subtables[4];
  subtable4->mem()->Add(11, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  subtable4->mem()->Add(12, kTypeValue, Slice("key1"), Slice("value"), false, nullptr);
  subtable4->mem()->Add(13, kTypeValue, Slice("key2"), Slice("value"), false, nullptr);
  subtable4->mem()->Add(14, kTypeValue, Slice("key3"), Slice("value"), false, nullptr);
  subtable4->mem()->Add(15, kTypeValue, Slice("key4"), Slice("value"), false, nullptr);
  SubTable *subtable5 = subtables[5];
  subtable5->mem()->Add(16, kTypeValue, Slice("key0"), Slice("value"), false, nullptr);
  //subtable6 is empty

  DBImpl db(options, "parallel_recovery_test");
  db.TEST_avoid_flush_ = true;
  std::set<uint32_t> picked_cf_ids;
  ReplayThreadPool::SubtableMemoryUsageComparor mem_cmp;
  uint64_t picked_num;
  BinaryHeap<SubTable*, ReplayThreadPool::SubtableMemoryUsageComparor> max_sub_tables;
  ASSERT_OK(db.TEST_pick_and_switch_subtables(subtables, picked_cf_ids, 3, mem_cmp,
                                             &picked_num, &max_sub_tables));
  ASSERT_EQ(3, max_sub_tables.size());
  SubTable *subtable = max_sub_tables.top();
  max_sub_tables.pop();
  ASSERT_EQ(2, subtable->get_recovery_point().seq_);
  subtable = max_sub_tables.top();
  max_sub_tables.pop();
  ASSERT_EQ(3, subtable->get_recovery_point().seq_);
  subtable = max_sub_tables.top();
  max_sub_tables.pop();
  ASSERT_EQ(4, subtable->get_recovery_point().seq_);

  ReplayThreadPool::SubtableSeqComparor seq_cmp;
  BinaryHeap<SubTable*, ReplayThreadPool::SubtableSeqComparor> oldest_sub_tables;
  ASSERT_OK(db.TEST_pick_and_switch_subtables(subtables, picked_cf_ids, 3, seq_cmp,
                                             &picked_num, &oldest_sub_tables));
  ASSERT_EQ(3, oldest_sub_tables.size());
  subtable = oldest_sub_tables.top();
  oldest_sub_tables.pop();
  ASSERT_EQ(2, subtable->get_recovery_point().seq_);
  subtable = oldest_sub_tables.top();
  oldest_sub_tables.pop();
  ASSERT_EQ(1, subtable->get_recovery_point().seq_);
  subtable = oldest_sub_tables.top();
  oldest_sub_tables.pop();
  ASSERT_EQ(0, subtable->get_recovery_point().seq_);

  for (auto s : subtables) {
    delete s.second;
  }
}

TEST_F(ParallelRecoveryTest, parallel_recovery_switch_test) {
  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::check_if_need_switch_memtable::trigger_switch_memtable", [&](void* arg) {
      DBImpl* db_impl = reinterpret_cast<DBImpl*>(arg);
      if (db_impl->TEST_get_max_sequence_during_recovery() >= 10 && !db_impl->TEST_triggered_) {
        db_impl->TEST_trigger_switch_memtable_ = true;
        db_impl->TEST_triggered_ = true;
      } else {
        db_impl->TEST_trigger_switch_memtable_ = false;
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.write_buffer_size = 4096 * 4096;
  options.create_if_missing = false;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.db_total_write_buffer_size =  4 * 4096 * 4096;
  options.db_write_buffer_size = 2 * 4096 * 4096;
  options.dump_memtable_limit_size = 500;
  options.create_if_missing = true;
  options.allow_2pc = false;
  CreateAndReopenWithCF({"xiaoyuan", "gavin", "test"}, options);
  ASSERT_OK(Put(2, "hello", "world"));
  ASSERT_OK(Put(1, "foo0", "v0"));
  ASSERT_OK(Put(1, "foo1", "v1"));
  ASSERT_OK(Put(1, "foo2", "v2"));
  ASSERT_OK(Put(1, "foo3", "v3"));
  ASSERT_OK(Delete(1, "foo1"));
  ASSERT_OK(Put("bar0", "v0"));
  ASSERT_OK(Put(3, "hello", "world"));
  ASSERT_OK(Delete(1, "foo2"));
  char buf[32768];
  memset(buf,'a',sizeof(buf));
  std::string str(buf);
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_OK(Put(3, key, str));
  }
  ASSERT_OK(Put("bar1", "v1"));
  ASSERT_OK(Put("bar2", "v2"));
  ASSERT_OK(Put("bar3", "v3"));
  TryReopenWithColumnFamilies({"default", "xiaoyuan", "gavin", "test"}, options);
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("v3", Get(1, "foo3"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("world", Get(2, "hello"));
  ASSERT_EQ("world", Get(3, "hello"));
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(3, key));
  }
  ASSERT_OK(Delete(3, "hello"));
  Options new_options;
  new_options.write_buffer_size = 4096 * 4096;
  new_options.create_if_missing = false;
  new_options.env = env_;
  new_options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.dump_memtable_limit_size = 500;
  new_options.db_total_write_buffer_size =  4 * 4096 * 4096;
  new_options.db_write_buffer_size = 2 * 4096 * 4096;
  new_options.create_if_missing = true;
  TryReopenWithColumnFamilies({"default", "xiaoyuan", "gavin", "test"}, new_options);
  ASSERT_EQ("world", Get(2, "hello"));
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("v3", Get(1, "foo3"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("NOT_FOUND", Get(3, "hello"));
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(3, key));
  }
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(ParallelRecoveryTest, parallel_recovery_barrier_test) {
  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::check_if_need_switch_memtable::trigger_switch_memtable", [&](void* arg) {
      DBImpl* db_impl = reinterpret_cast<DBImpl*>(arg);
      if (db_impl->TEST_get_max_sequence_during_recovery() >= 10 && !db_impl->TEST_triggered_) {
        db_impl->TEST_trigger_switch_memtable_ = true;
        db_impl->TEST_triggered_ = true;
      } else {
        db_impl->TEST_trigger_switch_memtable_ = false;
      }
    });
  SyncPoint::GetInstance()->SetCallBack(
    "ReplayTask::need_before_barrier_during_replay::need_barrier", [&](void* arg) {
      bool* need_barrier = reinterpret_cast<bool*>(arg);
      *need_barrier = true;
    });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  options.write_buffer_size = 4096 * 4096;
  options.create_if_missing = false;
  options.env = env_;
  options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options.db_total_write_buffer_size =  4 * 4096 * 4096;
  options.db_write_buffer_size = 2 * 4096 * 4096;
  options.dump_memtable_limit_size = 500;
  options.create_if_missing = true;
  CreateAndReopenWithCF({"xiaoyuan", "gavin", "test"}, options);
  ASSERT_OK(Put(2, "hello", "world"));
  ASSERT_OK(Put(1, "foo0", "v0"));
  ASSERT_OK(Put(1, "foo1", "v1"));
  ASSERT_OK(Put(1, "foo2", "v2"));
  ASSERT_OK(Put(1, "foo3", "v3"));
  ASSERT_OK(Delete(1, "foo1"));
  ASSERT_OK(Put("bar0", "v0"));
  ASSERT_OK(Put(3, "hello", "world"));
  char buf[32768];
  memset(buf,'a',sizeof(buf));
  std::string str(buf);
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_OK(Put(3, key, str));
  }
  ASSERT_OK(Delete(1, "foo2"));
  ASSERT_OK(Put("bar1", "v1"));
  ASSERT_OK(Put("bar2", "v2"));
  ASSERT_OK(Put("bar3", "v3"));
  TryReopenWithColumnFamilies({"default", "xiaoyuan", "gavin", "test"}, options);
  SyncPoint::GetInstance()->ClearAllCallBacks();
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("v3", Get(1, "foo3"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("world", Get(2, "hello"));
  ASSERT_EQ("world", Get(3, "hello"));
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(3, key));
  }
  ASSERT_OK(Delete(3, "hello"));
  ASSERT_OK(Put(3, "hello", "world1"));
  Options new_options;
  new_options.write_buffer_size = 4096 * 4096;
  new_options.create_if_missing = false;
  new_options.env = env_;
  new_options.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  new_options.dump_memtable_limit_size = 500;
  new_options.db_total_write_buffer_size =  4 * 4096 * 4096;
  new_options.db_write_buffer_size = 2 * 4096 * 4096;
  new_options.create_if_missing = true;
  SyncPoint::GetInstance()->SetCallBack(
    "ReplayThreadPool::need_barrier_during_replay::need_barrier", [&](void* arg) {
      bool* need_barrier = reinterpret_cast<bool*>(arg);
      *need_barrier = true;
    });
  TryReopenWithColumnFamilies({"default", "xiaoyuan", "gavin", "test"}, new_options);
  ASSERT_EQ("world", Get(2, "hello"));
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("v3", Get(1, "foo3"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo1"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo2"));
  ASSERT_EQ("v2", Get("bar2"));
  ASSERT_EQ("v3", Get("bar3"));
  ASSERT_EQ("world1", Get(3, "hello"));
  for (int i = 0; i < 10; i++) {
    std::string key = "test" + std::to_string(i);
    ASSERT_EQ(str, Get(3, key));
  }
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(ParallelRecoveryTest, parallel_replay_error) {
  SyncPoint::GetInstance()->SetCallBack(
    "ReplayTask::replay_log::replay_error", [&](void* arg) {
      ReplayTask* replay_task = reinterpret_cast<ReplayTask*>(arg);
      auto write_batch = replay_task->get_write_batch();
      if (WriteBatchInternal::Sequence(write_batch) == 3) {
        replay_task->set_replay_error(true);
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options0;
  options0.env = env_;
  options0.wal_recovery_mode = WALRecoveryMode::kAbsoluteConsistency;
  options0.db_total_write_buffer_size =  4 * 4096 * 4096;
  options0.db_write_buffer_size = 2 * 4096 * 4096;
  options0.dump_memtable_limit_size = 500;
  options0.create_if_missing = true;
  CreateAndReopenWithCF({"xiaoyuan"}, options0);
  ASSERT_OK(Put("bar0", "v0"));
  ASSERT_OK(Put("bar1", "v1"));
  ASSERT_OK(Put(1, "foo0", "v0"));
  ASSERT_OK(Put(1, "foo1", "v1"));
  ASSERT_OK(Put(1, "foo2", "v2"));
  ASSERT_OK(Put(1, "foo3", "v3"));
  ASSERT_EQ(Status::kErrorUnexpected, TryReopen(options0).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
    "ReplayTask::replay_log::replay_error", [&](void* arg) {
      ReplayTask* replay_task = reinterpret_cast<ReplayTask*>(arg);
      auto write_batch = replay_task->get_write_batch();
      if (WriteBatchInternal::Sequence(write_batch) == 6) {
        replay_task->set_replay_error(true);
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options1;
  ASSERT_EQ(Status::kErrorUnexpected, TryReopen(options1).code());
  SyncPoint::GetInstance()->ClearAllCallBacks();
  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::read_log::read_error", [&](void* arg) {
      uint64_t *read_error_offset = reinterpret_cast<uint64_t*>(arg);
      if (*read_error_offset == UINT64_MAX) {
        *read_error_offset = 130;
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();
  Options options2;
  ASSERT_OK(TryReopen(options2));
  ASSERT_EQ("v0", Get("bar0"));
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo3"));
  SyncPoint::GetInstance()->ClearAllCallBacks();
  Options options3;
  ASSERT_OK(TryReopen(options3));
  ASSERT_EQ("v0", Get("bar0"));
  ASSERT_EQ("v0", Get(1, "foo0"));
  ASSERT_EQ("NOT_FOUND", Get(1, "foo3"));
}

} // namespace db
} // namespace xengine

int main(int argc, char** argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
