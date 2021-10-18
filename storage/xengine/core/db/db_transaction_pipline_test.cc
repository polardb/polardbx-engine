// Portions Copyright (c) 2020, Alibaba Group Holding Limited
//  Copyright (c) 2011-present, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
#include <functional>
#include <thread>
#include <future>
#include <string>
#include "db/db_test_util.h"
#include "port/stack_trace.h"

using namespace xengine;
using namespace common;
using namespace util;
using namespace table;
using namespace cache;
using namespace memtable;
using namespace monitor;
using namespace storage;

namespace xengine {
namespace db {

class DBTestTransactionPipline : public DBTestBase {
 public:
  DBTestTransactionPipline() : DBTestBase("/db_transaction_pipline_test") {}
  uint64_t count_rows(int cf) {
    uint64_t total_rows = 0;
    util::Arena arena;
    ScopedArenaIterator iter;
    auto options = CurrentOptions();
    InternalKeyComparator icmp(options.comparator);
    RangeDelAggregator range_del_agg(icmp, {} /* snapshots */);
    if (cf != 0) { 
      iter.set( dbfull()->NewInternalIterator(&arena, &range_del_agg, 
                                              get_column_family_handle(cf)));
    } else {
      iter.set(dbfull()->NewInternalIterator(&arena, &range_del_agg));
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
      iter->Next();
      total_rows++; 
    }
    return total_rows;
  }
  //sleep for millionseconds
  void m_sleep(uint32_t millionseconds) {
    std::this_thread::sleep_for(std::chrono::milliseconds(millionseconds));
  }
  void log_debug(std::string msg) {
    if (debug_) 
      std::cerr << "[thread_id=" 
                << std::this_thread::get_id() 
                << "] " << msg << std::endl;
  }
  void debug_on() {
    debug_ = true;
  }
  private:
    bool debug_ = false;
};

//fix bug #29141530
//      the following is a typical execution sequence 
//
//  [thread_id=46925190920960] begin main thread
//  [thread_id=46925190920960] main thread put the first record
//  [thread_id=46925220955904] thread 1 compelete PreprocessWrite
//  [thread_id=46925223057152] thread 2 compelete PreprocessWrite
//  [thread_id=46925220955904] thread 1 call do_flush_log_buffer and succeed
//  [thread_id=46925220955904] thread 1 put record succeed!
//  [thread_id=46925223057152] thread 2 do_flush_log_buffer and wait
//  [thread_id=46925223057152] thread 2 do_flush_log_buffer and inject error
//  [thread_id=46925223057152] thread 2 compelete do_flush_log_buffer
//  [thread_id=46925231462144] thread 3 compelete PreprocessWrite and fail
//  [thread_id=46925231462144] thread 3 put record faild!
//  [thread_id=46925223057152] thread 2 put record faild!

TEST_F(DBTestTransactionPipline, SwitchCorruptedWAL) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.concurrent_writable_file_single_buffer_size = 2 * 1024 * 1024;
  options.concurrent_writable_file_buffer_num = 8;
  options.concurrent_writable_file_buffer_switch_limit = 256 * 1024;
  options.max_total_wal_size = 10 * 1024 * 1024 * 1024U;
  options.env = env_;
  options.avoid_flush_during_shutdown = false;
  options.use_direct_write_for_wal = true;
  options.write_buffer_size = 1 * 1024 * 1024U;
  options.db_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.db_total_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.allow_concurrent_memtable_write = true;
  Reopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  uint32_t value_length = 2 * 1024 * 1024;

  //debug_on();

  //(1) first put, this will set cfd flush_requested = true
  log_debug("begin main thread");
  log_debug("main thread put the first record");
  ASSERT_OK(this->Put(1, std::to_string(0), DummyString(value_length, 'a')));


  SyncPoint::GetInstance()->EnableProcessing();

  //every thread will go through AfterPreprocessWrite Onece
  //  we use a atomic counter to distinguish different threads
  std::atomic<int> cb_count(0);
  SyncPoint::GetInstance()->SetCallBack(
    "DBimpl::WriteImplAsync::AfterPreprocessWrite",
    [&](void*arg) { 
      int previous_count = cb_count.fetch_add(1);
      if (0 == previous_count) {
        //thread id 1 ,just do all the job an dcommit
        log_debug("thread 1 compelete PreprocessWrite");
        m_sleep(100);
      } else if(1 == previous_count) {
        //thread id 2  
        log_debug("thread 2 compelete PreprocessWrite");
        m_sleep(300);
        //log_debug("thread 2 ");
      } else if(2 == previous_count) {
        //thread id 3 , this thread will try to switch wal,but do nothing
        log_debug("thread 3 compelete PreprocessWrite and fail");
      } else {
        //this is impossiable, we only have threads
        abort();
      }
  });

  std::thread::id  t1_id, t2_id, t3_id;
  std::atomic<bool> t1_flag(false), t2_flag(false), t3_flag(false);

  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::do_flush_log_buffer_job::after_flush_sync",
    [&](void*arg) { 
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id && !t1_flag.load()) {
        log_debug("thread 1 call do_flush_log_buffer and succeed");
        //thread id 2 ,just do all the job and dcommit
        t1_flag.store(true);
      } else if(t2_id == current_thread_id && !t2_flag.load()) {
        t2_flag.store(true);
        //thread id 3  
        //set inject bg_error_ and exit, this will cause write abort
        log_debug("thread 2 do_flush_log_buffer and wait");
        m_sleep(500);
        log_debug("thread 2 do_flush_log_buffer and inject error");
        dbfull()->TEST_inject_pipline_error_flush_wal_fail();
        log_debug("thread 2 compelete do_flush_log_buffer");
      } else if(t3_id == current_thread_id && !t3_flag.load()) {
        //thread 3 will try to switchmemtable and fail
        //   because we have inject error in do_flush_log_buffer stage
        //   thread 3 will never come here 
        abort();
      } else {
        //another shoot in run_pipline, do nothing, 
      }
  });


  //(2) two thread insert concurrently
  std::function<void(DBTestBase*, uint32_t, int)> f = 
    [&](DBTestBase* db, uint32_t v_len, int index){
      std::string tmp_key = std::to_string(index);
      std::string tmp_value = db->DummyString(v_len, 'a');
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id) {
        ASSERT_OK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 1 put record succeed!");
      } else if (t2_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 2 put record faild!");
      } else if (t3_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 3 put record faild!");
      } else {
        //it's impossiable come here
        ASSERT_OK(0);
      }
  };
 
  //thread 1 will compeleted the pipline and add cfd into flush_schduler 
  std::thread t1(f, this, value_length, 1);
  t1_id = t1.get_id();

  //start thread 2  50 million seconds later,this  make sure thread 2 pass 
  //      the PreprocessWrite before thread 1 compelete the pipline, 
  //thread 2 sleep for 500m seconds and  inject pipline error at 
  //     sync poit DBImpl::do_flush_log_buffer_job
  m_sleep(50);
  std::thread t2(f, this, value_length, 2);
  t2_id = t2.get_id();

  //now start thread 3
  //thread 1 has compeleted, 
  //thread 2 stuck in do_flush_log_buffer and sleep
  //  at sync point DBImpl::do_flush_log_buffer_job::after_flush_sync;
  //thread 3 find that this memtable need to be switched and try to switch
  //  this need to wait for all active thread in pipline exit(thread 2)
  m_sleep(400);
  std::thread t3(f, this, value_length, 3);
  t3_id = t3.get_id();
 
  
  t1.join();
  t2.join();
  t3.join();
  SyncPoint::GetInstance()->DisableProcessing();

  //thread 2 and thread 3 will fail, there are only 2 records in db
  ASSERT_EQ(2, this->count_rows(1));

  Reopen(options);
  //thread  4 will fail cause of error injection
  // but thread 3's wal log will be flushed when log_writer was destructed
  ASSERT_EQ(3, this->count_rows(1));
}

TEST_F(DBTestTransactionPipline, WriteMemtableFail) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.concurrent_writable_file_single_buffer_size = 2 * 1024 * 1024;
  options.concurrent_writable_file_buffer_num = 8;
  options.concurrent_writable_file_buffer_switch_limit = 256 * 1024;
  options.max_total_wal_size = 10 * 1024 * 1024 * 1024U;
  options.env = env_;
  options.avoid_flush_during_shutdown = false;
  options.use_direct_write_for_wal = true;
  options.write_buffer_size = 1 * 1024 * 1024U;
  options.db_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.db_total_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.allow_concurrent_memtable_write = true;
  Reopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  uint32_t value_length = 1 * 1024;

  //debug_on();

  //(1) first put
  log_debug("begin main thread");
  log_debug("main thread put the first record");
  ASSERT_OK(this->Put(1, std::to_string(0), DummyString(value_length, 'a')));


  SyncPoint::GetInstance()->EnableProcessing();

  //every thread will go through AfterPreprocessWrite Onece
  //  we use a atomic counter to distinguish different threads
  std::atomic<int> cb_count(0);
  SyncPoint::GetInstance()->SetCallBack(
    "DBimpl::WriteImplAsync::AfterPreprocessWrite",
    [&](void*arg) { 
      int previous_count = cb_count.fetch_add(1);
      if (0 == previous_count) {
        //thread id 1 ,just do all the job an dcommit
        log_debug("thread 1 compelete PreprocessWrite");
        m_sleep(100);
      } else if(1 == previous_count) {
        //thread id 2  
        log_debug("thread 2 compelete PreprocessWrite");
        m_sleep(300);
        //log_debug("thread 2 ");
      } else {
        //this is impossiable, we only have threads
        ASSERT_FALSE(true);
      }
  });

  std::thread::id  t1_id, t2_id;
  std::atomic<bool> t1_flag(false), t2_flag(false);

  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::do_write_memtable_job::inject_error",
    [&](void*arg) { 
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id && !t1_flag.load()) {
        log_debug("thread 1 begin write memtable and inject failure");
        //thread id 2 ,just do all the job and dcommit
        dbfull()->TEST_inject_pipline_error_write_memtable_fail();
        t1_flag.store(true);
      } else {
        //this is impossible,since we have inject pipline error
        ASSERT_FALSE(true);
      }
  });


  //(2) two thread insert concurrently
  std::function<void(DBTestBase*, uint32_t, int)> f = 
    [&](DBTestBase* db, uint32_t v_len, int index){
      std::string tmp_key = std::to_string(index);
      std::string tmp_value = db->DummyString(v_len, 'a');
      m_sleep(10);
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 1 put record failed!");
      } else if (t2_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 2 put record faild!");
      } else {
        //it's impossiable come here
        ASSERT_FALSE(true);
      }
  };
 
  //thread 1 will inject error in write memtable
  std::thread t1(f, this, value_length, 1);
  t1_id = t1.get_id();

  //start thread 2 50 million seconds later
  // thread 2 will catch pipline error  and fail
  m_sleep(50);
  std::thread t2(f, this, value_length, 2);
  t2_id = t2.get_id();


  t1.join();
  t2.join();
  SyncPoint::GetInstance()->DisableProcessing();

  //inject error after thread  1 compelete the write
  // since thread 1 already complete the write there should be 2 records
  ASSERT_EQ(2, this->count_rows(1));

  Reopen(options);
  ASSERT_EQ(2, this->count_rows(1));
}

TEST_F(DBTestTransactionPipline, CommitFail) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.concurrent_writable_file_single_buffer_size = 2 * 1024 * 1024;
  options.concurrent_writable_file_buffer_num = 8;
  options.concurrent_writable_file_buffer_switch_limit = 256 * 1024;
  options.max_total_wal_size = 10 * 1024 * 1024 * 1024U;
  options.env = env_;
  options.avoid_flush_during_shutdown = false;
  options.use_direct_write_for_wal = true;
  options.write_buffer_size = 1 * 1024 * 1024U;
  options.db_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.db_total_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.allow_concurrent_memtable_write = true;
  Reopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  uint32_t value_length = 1 * 1024;

  //debug_on();

  //(1) first put
  log_debug("begin main thread");
  log_debug("main thread put the first record");
  ASSERT_OK(this->Put(1, std::to_string(0), DummyString(value_length, 'a')));


  SyncPoint::GetInstance()->EnableProcessing();

  //every thread will go through AfterPreprocessWrite Onece
  //  we use a atomic counter to distinguish different threads
  std::atomic<int> cb_count(0);
  SyncPoint::GetInstance()->SetCallBack(
    "DBimpl::WriteImplAsync::AfterPreprocessWrite",
    [&](void*arg) { 
      int previous_count = cb_count.fetch_add(1);
      if (0 == previous_count) {
        //thread id 1 ,just do all the job an dcommit
        log_debug("thread 1 compelete PreprocessWrite");
        m_sleep(100);
      } else if(1 == previous_count) {
        //thread id 2  
        log_debug("thread 2 compelete PreprocessWrite");
        m_sleep(300);
        //log_debug("thread 2 ");
      } else {
        //this is impossiable, we only have threads
        ASSERT_FALSE(true);
      }
  });

  std::thread::id  t1_id, t2_id;
  std::atomic<bool> t1_flag(false), t2_flag(false);

  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::do_commit_job::inject_error",
    [&](void*arg) { 
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id && !t1_flag.load()) {
        log_debug("thread 1 begin commit and inject failure");
        //thread id 1 inject commit fail error
        dbfull()->TEST_inject_pipline_error_commit_fail();
        t1_flag.store(true);
      } else {
        //this is impossible,since we have inject pipline error
        ASSERT_FALSE(true);
      }
  });


  //(2) two thread insert concurrently
  std::function<void(DBTestBase*, uint32_t, int)> f = 
    [&](DBTestBase* db, uint32_t v_len, int index){
      std::string tmp_key = std::to_string(index);
      std::string tmp_value = db->DummyString(v_len, 'a');
      m_sleep(10); //sleep 10 millionseconds for t1_id be setted
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 1 put record failed!");
      } else if (t2_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 2 put record faild!");
      } else {
        //it's impossiable come here
        ASSERT_FALSE(true);
      }
  };
 
  //thread 1 will inject error in write memtable
  std::thread t1(f, this, value_length, 1);
  t1_id = t1.get_id();

  //start thread 2 50 million seconds later
  // thread 2 will catch pipline error and fail
  m_sleep(50);
  std::thread t2(f, this, value_length, 2);
  t2_id = t2.get_id();


  t1.join();
  t2.join();
  SyncPoint::GetInstance()->DisableProcessing();

  //inject error after thread  1 compelete the write
  //so there should be 2 rows
  ASSERT_EQ(2, this->count_rows(1));

  Reopen(options);
  ASSERT_EQ(2, this->count_rows(1));
}

TEST_F(DBTestTransactionPipline, CopyLogFail) {
  Options options = CurrentOptions();
  options.create_if_missing = true;
  options.concurrent_writable_file_single_buffer_size = 2 * 1024 * 1024;
  options.concurrent_writable_file_buffer_num = 8;
  options.concurrent_writable_file_buffer_switch_limit = 256 * 1024;
  options.max_total_wal_size = 10 * 1024 * 1024 * 1024U;
  options.env = env_;
  options.avoid_flush_during_shutdown = false;
  options.use_direct_write_for_wal = true;
  options.write_buffer_size = 1 * 1024 * 1024U;
  options.db_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.db_total_write_buffer_size = 10 * 1024 * 1024 * 1024U;
  options.allow_concurrent_memtable_write = true;
  Reopen(options);
  CreateAndReopenWithCF({"pikachu"}, options);

  uint32_t value_length = 1 * 1024 * 1024;

  //debug_on();

  //(1) first put
  log_debug("begin main thread");
  log_debug("main thread put the first record");
  ASSERT_OK(this->Put(1, std::to_string(0), DummyString(value_length, 'a')));


  SyncPoint::GetInstance()->EnableProcessing();

  //every thread will go through AfterPreprocessWrite Onece
  //  we use a atomic counter to distinguish different threads
  std::atomic<int> cb_count(0);
  SyncPoint::GetInstance()->SetCallBack(
    "DBimpl::WriteImplAsync::AfterPreprocessWrite",
    [&](void*arg) { 
      int previous_count = cb_count.fetch_add(1);
      if (0 == previous_count) {
        //thread id 1 ,just do all the job an dcommit
        log_debug("thread 1 compelete PreprocessWrite");
        m_sleep(100);
      } else if(1 == previous_count) {
        //thread id 2  
        log_debug("thread 2 compelete PreprocessWrite");
        m_sleep(300);
        //log_debug("thread 2 ");
      } else {
        //this is impossiable, we only have threads
        ASSERT_FALSE(true);
      }
  });

  std::thread::id  t1_id, t2_id;
  std::atomic<bool> t1_flag(false), t2_flag(false);

  SyncPoint::GetInstance()->SetCallBack(
    "DBImpl::do_copy_log_buffer_job::inject_error",
    [&](void*arg) { 
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id && !t1_flag.load()) {
        log_debug("thread 1 begin copy log buffer and inject failure");
        //thread id 1 inject commit fail error
        dbfull()->TEST_inject_pipline_error_copy_log_fail();
        t1_flag.store(true);
      } else {
        //this is impossible,since we have inject pipline error
        ASSERT_FALSE(true);
      }
  });


  //(2) two thread insert concurrently
  std::function<void(DBTestBase*, uint32_t, int)> f = 
    [&](DBTestBase* db, uint32_t v_len, int index){
      std::string tmp_key = std::to_string(index);
      std::string tmp_value = db->DummyString(v_len, 'a');
      m_sleep(10); //sleep 10 millionseconds for t1_id be setted
      std::thread::id current_thread_id = std::this_thread::get_id();
      if (t1_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 1 put record failed!");
      } else if (t2_id == current_thread_id) {
        ASSERT_NOK(db->Put(1, tmp_key, tmp_value));
        log_debug("thread 2 put record faild!");
      } else {
        //it's impossiable come here
        ASSERT_FALSE(true);
      }
  };
 
  //thread 1 will inject error in write memtable
  std::thread t1(f, this, value_length, 1);
  t1_id = t1.get_id();

  //start thread 2 50 million seconds later
  // thread 2 will catch pipline error and fail
  m_sleep(50);
  std::thread t2(f, this, value_length, 2);
  t2_id = t2.get_id();


  t1.join();
  t2.join();
  SyncPoint::GetInstance()->DisableProcessing();

  //no write will succeed since we inject error while writing wal
  ASSERT_EQ(1, this->count_rows(1));

  Reopen(options);
  //thread 1's wal was flushed while log_writer destructe
  // so thread 1's put will be recovered from wal after reopen()
  ASSERT_EQ(2, this->count_rows(1));
}
}
}  // namespace xengine

int main(int argc, char** argv) {
  port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  xengine::util::test::init_logger(__FILE__);
  return RUN_ALL_TESTS();
}
