/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2016, Facebook, Inc.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

#pragma once

/* C++ standard header files */
#include <atomic>
#include <condition_variable>
#include <mutex>
#include <queue>
#include <string>
#include <thread>
#include <vector>

/* XENGINE header files */
#include "xengine/db.h"
#include "xengine/sst_file_writer.h"

// define XDB_SST_INFO_USE_THREAD /* uncomment to use threads */
namespace xengine {
struct MiniTables;
}

namespace myx {

class Xdb_sst_file {
private:
  Xdb_sst_file(const Xdb_sst_file &p) = delete;
  Xdb_sst_file &operator=(const Xdb_sst_file &p) = delete;

  xengine::db::DB *const m_db;
  xengine::db::ColumnFamilyHandle *const m_cf;
  const xengine::common::DBOptions &m_db_options;
  xengine::table::SstFileWriter *m_sst_file_writer;
  const std::string m_name;
  const bool m_tracing;
  xengine::db::MiniTables *mtables_;

  std::string generateKey(const std::string &key);
  bool rollbacked;

public:
  Xdb_sst_file(xengine::db::DB *const db, xengine::db::ColumnFamilyHandle *const cf,
               const xengine::common::DBOptions &db_options, const std::string &name,
               const bool tracing, xengine::db::MiniTables* mtables = nullptr);
  ~Xdb_sst_file();

  xengine::common::Status open();
  xengine::common::Status put(const xengine::common::Slice &key, const xengine::common::Slice &value);
  xengine::common::Status commit();
  const std::string get_name() const { return m_name; }
  int rollback();
};

class Xdb_sst_info {
private:
  Xdb_sst_info(const Xdb_sst_info &p) = delete;
  Xdb_sst_info &operator=(const Xdb_sst_info &p) = delete;

  xengine::db::DB *const m_db;
  xengine::db::ColumnFamilyHandle *const m_cf;
  const xengine::common::DBOptions &m_db_options;
  uint64_t m_curr_size;
  uint64_t m_max_size;
  uint m_sst_count;
  std::string m_error_msg;
  std::string m_prefix;
  static std::atomic<uint64_t> m_prefix_counter;
  static std::string m_suffix;
#if defined(XDB_SST_INFO_USE_THREAD)
  std::queue<Xdb_sst_file *> m_queue;
  std::mutex m_mutex;
  std::condition_variable m_cond;
  std::thread *m_thread;
  bool m_finished;
#endif
  Xdb_sst_file *m_sst_file;
  const bool m_tracing;
  xengine::db::MiniTables *mtables_;

  int open_new_sst_file();
  void close_curr_sst_file();
  void set_error_msg(const std::string &sst_file_name, const std::string &msg);

#if defined(XDB_SST_INFO_USE_THREAD)
  void run_thread();

  static void thread_fcn(void *object);
#endif

public:
  Xdb_sst_info(xengine::db::DB *const db, const std::string &tablename,
               const std::string &indexname,
               xengine::db::ColumnFamilyHandle *const cf,
               const xengine::common::DBOptions &db_options, const bool &tracing,
               xengine::db::MiniTables* mtables = nullptr,
               xengine::db::MiniTables* mtables_c = nullptr);
  ~Xdb_sst_info();

  int put(const xengine::common::Slice &key, const xengine::common::Slice &value);
  int commit();

  // if add index failed
  int rollback();

  const std::string &error_message() const { return m_error_msg; }

  static void init(const xengine::db::DB *const db);
};

} // namespace myx
