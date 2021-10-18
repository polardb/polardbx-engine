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
#include "util/testutil.h"
#include "xengine/utilities/transaction.h"
#include "xengine/utilities/transaction_db.h"

using namespace xengine;
using namespace common;
using namespace util;

namespace xengine {
namespace db {

class Parallel2PCRecoveryTest : public testing::Test {
public:
  Parallel2PCRecoveryTest() = default;
  ~Parallel2PCRecoveryTest() = default;
  void submit_2pc_trx(TransactionDB *db, const std::string &name,
                      const std::string &key, bool commit, port::Mutex *mutex,
                      bool *signaled, port::CondVar *cv) {
    {
      util::MutexLock lock_guard(mutex);
      if (!(*signaled)) {
        cv->Wait();
      }
    }
    common::WriteOptions write_options;
    Transaction *txn = db->BeginTransaction(write_options);
    txn->SetName(name);
    txn->Put(key, key);
    txn->Prepare();
    if (commit) {
      txn->Commit();
    } else {
      txn->Rollback();
    }
    delete txn;
  }
};

TEST_F(Parallel2PCRecoveryTest, parallel_replay_2pc) {
  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::check_if_need_switch_memtable::trigger_switch_memtable", [&](void* arg) {
      DBImpl* db_impl = reinterpret_cast<DBImpl*>(arg);
      if (db_impl->TEST_get_max_sequence_during_recovery() >= 3 && !db_impl->TEST_triggered_) {
        db_impl->TEST_trigger_switch_memtable_ = true;
        db_impl->TEST_triggered_ = true;
      } else {
        db_impl->TEST_trigger_switch_memtable_ = false;
      }
    });
  SyncPoint::GetInstance()->EnableProcessing();

  Options options;
  std::string dbname = test::TmpDir() + "/2pc_parallel_replay";
  DestroyDB(dbname, options);
  Options options1;
  options1.write_buffer_size = 4096 * 4096;
  options1.db_total_write_buffer_size =  4 * 4096 * 4096;
  options1.db_write_buffer_size = 2 * 4096 * 4096;
  options1.dump_memtable_limit_size = 200;
  options1.allow_2pc = true;
  options1.create_if_missing = true;
  options1.parallel_wal_recovery = true;

  TransactionDB* db;
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  common::WriteOptions write_options;
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest0"));
  ASSERT_OK(txn->Put("foo00", "v0"));
  ASSERT_OK(txn->Put("foo01", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest0"));
  ASSERT_OK(txn->Put("foo10", "v0"));
  ASSERT_OK(txn->Put("foo11", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest_rollback"));
  ASSERT_OK(txn->Put("foo20", "v0"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Rollback());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest2"));
  ASSERT_OK(txn->Put("foo20", "v0"));
  ASSERT_OK(txn->Put("foo21", "v1"));
  ASSERT_OK(txn->Delete("foo21"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest3"));
  ASSERT_OK(txn->Put("foo30", "v0"));
  ASSERT_OK(txn->Put("foo31", "v1"));
  ASSERT_OK(txn->Prepare());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest4"));
  ASSERT_OK(txn->Put("foo40", "v0"));
  ASSERT_OK(txn->Put("foo41", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Rollback());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest5"));
  ASSERT_OK(txn->Prepare());
  ReadOptions read_options;
  std::string value;
  ASSERT_OK(db->Get(read_options, "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options, "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options, "foo10", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options, "foo11", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options, "foo20", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo21", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo30", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo31", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo40", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo41", &value).code());
  delete txn;
  delete db;
  db = nullptr;
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  ReadOptions read_options1;
  ASSERT_OK(db->Get(read_options1, "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options1, "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options1, "foo10", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options1, "foo11", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options1, "foo20", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo21", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo30", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo31", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo40", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo41", &value).code());
  delete db;
  db = nullptr;
  SyncPoint::GetInstance()->DisableProcessing();
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  ReadOptions read_options2;
  ASSERT_OK(db->Get(read_options2, "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options2, "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options2, "foo10", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options2, "foo11", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options2, "foo20", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options2, "foo21", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options2, "foo30", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options2, "foo31", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options2, "foo40", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options2, "foo41", &value).code());
  delete db;
  DestroyDB(dbname, options);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(Parallel2PCRecoveryTest, parallel_replay_2pc_batch) {
  Options options;
  std::string dbname = test::TmpDir() + "/2pc_parallel_replay";
  DestroyDB(dbname, options);
  Options options1;
  options1.batch_group_max_leader_wait_time_us = 5000;
  options1.batch_group_slot_array_size = 2;
  options1.write_buffer_size = 4096 * 4096;
  options1.db_total_write_buffer_size = 4 * 4096 * 4096;
  options1.db_write_buffer_size = 2 * 4096 * 4096;
  options1.dump_memtable_limit_size = 200;
  options1.allow_2pc = true;
  options1.create_if_missing = true;
  options1.parallel_wal_recovery = true;

  TransactionDB *db;
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));

  port::Mutex mutex(false);
  bool signaled = false;
  port::CondVar cond_var(&mutex);
  std::thread threads[100];
  for (int i = 0; i < 100; i++) {
    std::stringstream transaction_name;
    transaction_name << "trx_" << i;
    std::stringstream key;
    key << "key_" << i;
    threads[i] =
        std::thread(std::bind(&Parallel2PCRecoveryTest::submit_2pc_trx, this,
                              db, transaction_name.str(), key.str(), i % 4 != 0,
                              &mutex, &signaled, &cond_var));
  }
  {
    util::MutexLock lock_guard(&mutex);
    cond_var.SignalAll();
    signaled = true;
  }
  for (int i = 0; i < 100; ++i) {
    threads[i].join();
  }
  signaled = false;
  for (int i = 100; i < 200; ++i) {
    std::stringstream transaction_name;
    transaction_name << "trx_" << i;
    std::stringstream key;
    key << "key_" << i;
    threads[i - 100] =
        std::thread(std::bind(&Parallel2PCRecoveryTest::submit_2pc_trx, this,
                              db, transaction_name.str(), key.str(), i % 4 != 0,
                              &mutex, &signaled, &cond_var));
  }
  {
    util::MutexLock lock_guard(&mutex);
    cond_var.SignalAll();
    signaled = true;
  }
  for (int i = 0; i < 100; ++i) {
    threads[i].join();
  }
  ReadOptions read_options;
  for (int i = 0; i < 200; ++i) {
    std::stringstream key_stream;
    key_stream << "key_" << i;
    std::string key = key_stream.str();
    std::string value;
    if (i % 4 == 0) {
      ASSERT_EQ(Status::kNotFound, db->Get(read_options, key, &value).code());
    } else {
      ASSERT_OK(db->Get(read_options, key, &value));
      ASSERT_EQ(key, value);
    }
  }
  delete db;
  db = nullptr;

  SyncPoint::GetInstance()->SetCallBack(
      "DBImpl::check_if_need_switch_memtable::trigger_switch_memtable",
      [&](void *arg) {
        DBImpl *db_impl = reinterpret_cast<DBImpl *>(arg);
        if (db_impl->TEST_get_max_sequence_during_recovery() >= 50 &&
            !db_impl->TEST_triggered_) {
          db_impl->TEST_trigger_switch_memtable_ = true;
          db_impl->TEST_triggered_ = true;
        } else {
          db_impl->TEST_trigger_switch_memtable_ = false;
        }
      });
  SyncPoint::GetInstance()->EnableProcessing();
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  ReadOptions read_options1;
  for (int i = 0; i < 20; ++i) {
    std::stringstream key_stream;
    key_stream << "key_" << i;
    std::string key = key_stream.str();
    std::string value;
    if (i % 4 == 0) {
      ASSERT_EQ(Status::kNotFound, db->Get(read_options1, key, &value).code());
    } else {
      ASSERT_OK(db->Get(read_options1, key, &value));
      ASSERT_EQ(key, value);
    }
  }
  delete db;
  db = nullptr;

  Options options2;
  options2.batch_group_max_leader_wait_time_us = 5000;
  options2.batch_group_slot_array_size = 2;
  options2.write_buffer_size = 4096 * 4096;
  options2.db_total_write_buffer_size = 4 * 4096 * 4096;
  options2.db_write_buffer_size = 2 * 4096 * 4096;
  options2.dump_memtable_limit_size = 200;
  options2.allow_2pc = true;
  options2.create_if_missing = true;
  options2.parallel_wal_recovery = false;
  ASSERT_OK(TransactionDB::Open(options2, TransactionDBOptions(), dbname, &db));
  ReadOptions read_options2;
  for (int i = 0; i < 20; ++i) {
    std::stringstream key_stream;
    key_stream << "key_" << i;
    std::string key = key_stream.str();
    std::string value;
    if (i % 4 == 0) {
      ASSERT_EQ(Status::kNotFound, db->Get(read_options2, key, &value).code());
    } else {
      ASSERT_OK(db->Get(read_options2, key, &value));
      ASSERT_EQ(key, value);
    }
  }
  delete db;

  DestroyDB(dbname, options);
  SyncPoint::GetInstance()->ClearAllCallBacks();
}

TEST_F(Parallel2PCRecoveryTest, serial_replay_2pc) {
  // TODO switch memtable
  Options options;
  std::string dbname = test::TmpDir() + "/2pc_parallel_replay";
  DestroyDB(dbname, options);
  Options options1;
  options1.allow_2pc = true;
  options1.create_if_missing = true;
  options1.parallel_wal_recovery = false;

  TransactionDB* db;
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  common::WriteOptions write_options;
  Transaction* txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest0"));
  ASSERT_OK(txn->Put("foo00", "v0")); //1
  ASSERT_OK(txn->Put("foo01", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest0"));
  ASSERT_OK(txn->Put("foo10", "v0")); //4
  ASSERT_OK(txn->Put("foo11", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest2"));
  ASSERT_OK(txn->Put("foo20", "v0")); //7
  ASSERT_OK(txn->Put("foo21", "v1"));
  ASSERT_OK(txn->Delete("foo21"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Commit());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest3"));
  ASSERT_OK(txn->Put("foo30", "v0")); //11
  ASSERT_OK(txn->Put("foo31", "v1"));
  ASSERT_OK(txn->Prepare());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ASSERT_OK(txn->SetName("2pctest4")); //12
  ASSERT_OK(txn->Put("foo40", "v0"));
  ASSERT_OK(txn->Put("foo41", "v1"));
  ASSERT_OK(txn->Prepare());
  ASSERT_OK(txn->Rollback());
  delete txn;
  txn = db->BeginTransaction(write_options);
  ASSERT_NE(nullptr, txn);
  ReadOptions read_options;
  std::string value;
  ASSERT_OK(db->Get(read_options, "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options, "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options, "foo10", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options, "foo11", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options, "foo20", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo21", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo30", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo31", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo40", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options, "foo41", &value).code());
  delete db;
  db = nullptr;
  ASSERT_OK(TransactionDB::Open(options1, TransactionDBOptions(), dbname, &db));
  ReadOptions read_options1;
  ASSERT_OK(db->Get(read_options1, "foo00", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options1, "foo01", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options1, "foo10", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_OK(db->Get(read_options1, "foo11", &value));
  ASSERT_EQ(value, "v1");
  ASSERT_OK(db->Get(read_options1, "foo20", &value));
  ASSERT_EQ(value, "v0");
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo21", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo30", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo31", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo40", &value).code());
  ASSERT_EQ(Status::kNotFound, db->Get(read_options1, "foo41", &value).code());
  delete db;
  DestroyDB(dbname, options);
}

} // namespace db
} // namespace xengine

int main(int argc, char** argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
