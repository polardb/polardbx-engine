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

#pragma once
#include <iostream>
#include <string>
#include "xdb_datadic.h"

namespace myx {

using namespace xengine;
using namespace std;

#define DDL_RECORD_MAX_KEY_LENGTH (1024)
#define DDL_RECORD_MAX_VALUE_LENGTH (1024)

/** all type of ddl-log key start (log_dict_type(4bytes), thread_id(8bytes), seqno(4bytes) */
#define DDL_LOG_PREFIX_KEY_LEN  (4 + 8 + 4)
#define DDL_LOG_DICT_PREFIX_KEY_LEN  (4)

/** print action of ddl-record */
#define DDL_LOG_INSERT "DDL LOG INSERT"
#define DDL_LOG_DELETE "DDL LOG DELETE"
#define DDL_LOG_REPLAY "DDL LOG REPLAY"

enum Log_Type: uint32_t {
  DDL_INVALID_LOG = 1,

  DDL_SMALLEST_LOG = 2,

  DDL_DROP_SUBTABLE_LOG = 2,

  DDL_REMOVE_CACHE_LOG = 3,

  DDL_RENAME_CACHE_LOG = 4,

  DDL_BIGGEST_LOG = DDL_RENAME_CACHE_LOG
};

class DDL_Record {
public:
  DDL_Record()
      : m_type(DDL_INVALID_LOG), m_thread_id(-1), m_seqno(-1), 
        m_subtable_id(-1), m_table_name(""), m_dst_table_name(""),
        m_key(""), m_value("") {}

  ~DDL_Record() {}

  DDL_Record(common::Slice &key, common::Slice &value);
  void initialize();

  bool convert_to_kv();

  common::Slice& get_key() { return m_key; }

  common::Slice& get_value() { return m_value; }

  void set_type(Log_Type type) { m_type = type; }
  Log_Type get_type() { return m_type; }

  void set_thread_id(ulong thread_id) { m_thread_id = thread_id; }
  ulong get_thread_id() { return m_thread_id; }

  void set_seqno(uint32_t seqno) { m_seqno = seqno; }
  uint32_t get_seqno() { return m_seqno; }

  void set_subtable_id(uint subtable_id) { m_subtable_id = subtable_id;}
  uint get_subtable_id() { return m_subtable_id; }

  /** set/get subtable name */
  void set_subtable_name(std::string table_name) { m_subtable_name = table_name; }
  void set_subtable_name(const char* buf, size_t len) { set_subtable_name(std::string(buf, len)); }
  std::string& get_subtable_name() { return m_subtable_name; }

  /** set/get table name */
  void set_table_name(std::string table_name) { m_table_name = table_name; }
  void set_table_name(const char* buf, size_t len) { set_table_name(std::string(buf, len)); }
  std::string& get_table_name() { return m_table_name; }

  /** set/get dest table name for rename case */
  void set_dst_table_name(std::string table_name) { m_dst_table_name = table_name; }
  void set_dst_table_name(const char* buf, size_t len) { set_dst_table_name(std::string(buf, len)); }
  std::string& get_dst_table_name() { return m_dst_table_name; }

  /** Print the DDL record to specified output stream
  @param[in,out]	out	output stream
  @return output stream */
  std::ostream &print(std::ostream &out) const;

private:
  Log_Type m_type;

  ulong m_thread_id;

  uint m_seqno; //make sure ddl_log is orderly, we need replay reversely

  uint m_subtable_id;

  uint m_index_number; //just for test
 
  std::string m_subtable_name;

  std::string m_table_name;

  uchar key_buf[DDL_RECORD_MAX_KEY_LENGTH];
  uchar val_buf[DDL_RECORD_MAX_VALUE_LENGTH];

  /** only used for rename record */
  std::string m_dst_table_name;

  common::Slice m_key;

  common::Slice m_value;
};

/** Array of DDL records */
using DDL_Records = std::vector<DDL_Record *>;
using DDL_Records_iter = std::vector<DDL_Record *>::iterator;

class Xdb_dict_manager;
class Xdb_ddl_manager;
class Xdb_cf_manager;
class Xdb_drop_index_thread;

class Xdb_ddl_log_manager {
private:
  Xdb_dict_manager *m_dict;
  Xdb_ddl_manager *m_ddl_manager;
  Xdb_cf_manager *m_cf_manager;
  Xdb_drop_index_thread *m_drop_subtable_thread;

public:
  Xdb_ddl_log_manager()
      : m_dict(nullptr), m_ddl_manager(nullptr), m_cf_manager(nullptr),
        m_drop_subtable_thread(nullptr) {}

  ~Xdb_ddl_log_manager() {}

  bool init(Xdb_dict_manager *dict_manager, Xdb_ddl_manager *ddl_manager,
            Xdb_cf_manager *cf_manager,
            Xdb_drop_index_thread *drop_subtable_thread) {
    m_dict = dict_manager;

    m_ddl_manager = ddl_manager;

    m_cf_manager = cf_manager;

    m_drop_subtable_thread = drop_subtable_thread;

    return false;
  }

  bool write_drop_subtable_log_test(xengine::db::WriteBatch *const xa_batch,
                                    string &subtable_name, uint subtable_id,
                                    uint index_number, ulong thread_id,
                                    bool is_drop);

  bool write_drop_subtable_log(xengine::db::WriteBatch *const xa_batch,
                               uint subtable_id, ulong thread_id, bool is_drop);

  bool write_remove_cache_log(xengine::db::WriteBatch *const xa_batch,
                              const std::string &table_name, ulong thread_id);

  bool write_rename_cache_log(xengine::db::WriteBatch *const xa_batch,
                              std::string &table_name,
                              std::string &dst_table_name, ulong thread_id);

  bool search_all(DDL_Records &records);

  bool search_by_thread_id(ulong thread_id, DDL_Records &records);

  bool replay_all();

  bool replay_by_thread_id(ulong thread_id);

  bool replay(DDL_Record *record);

  bool replay_drop_subtable(uint subtable_id);

  bool replay_remove_cache(string &table_name);

  bool replay_rename_cache(string &table_name, string &dst_table_name);

  bool post_ddl(THD *thd);

  bool recover();

  void delete_records(DDL_Records &records, bool all = false);
};

} //namespace myx
