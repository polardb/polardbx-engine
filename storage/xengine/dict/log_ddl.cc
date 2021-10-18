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

#include <debug_sync.h>
#include "./log_ddl.h"
#include "./xdb_buff.h"
#include "./xdb_utils.h"
#include "./xdb_threads.h"
#include "./xdb_cf_manager.h"
#include "./ha_xengine.h"
#include "sql_thd_internal_api.h"


using namespace std;
using namespace xengine;
using namespace common;

namespace myx {

extern xengine::util::TransactionDB *xdb;

/** Display a DDL record
@param[in]	record	DDL record to display
@param[in]	insert_replay_flag DDL_LOG_INSERT:insert record, DDL_LOG_REPLAY:replay record
@return  */
static void print_record(DDL_Record &record, string insert_replay_flag) {
  Log_Type type = record.get_type();
  assert(type >= DDL_SMALLEST_LOG);
  assert(type <= DDL_BIGGEST_LOG);

  if (!xengine_enable_print_ddl_log) {
    return;
  }

  ulong thread_id = record.get_thread_id();

  switch (type) {
  case DDL_DROP_SUBTABLE_LOG: {
    uint subtable_id = record.get_subtable_id();
    sql_print_information("%s, [DDL record: DROP SUBTABLE, thread_id(%ld), "
                          "subtable_id(%d)]",
                          insert_replay_flag.c_str(), thread_id, subtable_id);
    break;
  }
  case DDL_REMOVE_CACHE_LOG: {
    sql_print_information("%s, [DDL record: REMOVE CACHE, "
                          "thread_id(%ld), table_name(%s)]",
                          insert_replay_flag.c_str(), thread_id,
                          record.get_table_name().c_str());
    break;
  }

  case DDL_RENAME_CACHE_LOG: {
    sql_print_information("%s, [DDL record: RENAME CACHE, thread_id(%ld), "
                          "table_name(%s), rename to table_name(%s)]",
                          insert_replay_flag.c_str(), thread_id, record.get_table_name().c_str(),
                          record.get_dst_table_name().c_str());
    break;
  }
  default:
    assert(false);
  }
}

/**
@param[in] xa_batch, make ddl_log dictionary-operatiron be part of xa_batch
@param[in] subtable_id
@param[in] thread_id
@param[in] is_drop, if drop_table true, else if create_table/create_index false
@return success or fail
*/
bool Xdb_ddl_log_manager::write_drop_subtable_log(
    xengine::db::WriteBatch *const xa_batch, uint subtable_id, ulong thread_id,
    bool is_drop) {
  DDL_Record record;
  record.set_type(DDL_DROP_SUBTABLE_LOG);
  record.set_subtable_id(subtable_id);
  record.set_thread_id(thread_id);
  record.set_seqno(xa_batch->Count());

  bool internal_trx = is_drop ? false : true;

  /** generate ddl_dict_kv */
  if (record.convert_to_kv()) {
    sql_print_error("generate ddl_record error, subtable_id(%ld), thread_id(%d), is_drop(%d)", subtable_id, thread_id, is_drop);
    return HA_EXIT_FAILURE;
  }

  if (internal_trx) {
    const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
    xengine::db::WriteBatch *const batch = wb.get();

    DBUG_ASSERT(batch != nullptr);
    m_dict->put_key(batch, record.get_key(), record.get_value());
    print_record(record, DDL_LOG_INSERT);
    m_dict->commit(batch);

    m_dict->delete_key(xa_batch, record.get_key());
    print_record(record, DDL_LOG_DELETE);
  } else {
    m_dict->put_key(xa_batch, record.get_key(), record.get_value());
    print_record(record, DDL_LOG_INSERT);
  }

  return HA_EXIT_SUCCESS;
}


/** rollback useless xdb_table_def if necessary, this type ddl-log is always internal-trx */
bool Xdb_ddl_log_manager::write_remove_cache_log(
    xengine::db::WriteBatch *const xa_batch, const string &table_name,
    ulong thread_id) {

  DDL_Record record;
  record.set_type(DDL_REMOVE_CACHE_LOG);
  record.set_table_name(table_name);
  record.set_thread_id(thread_id);
  record.set_seqno(xa_batch->Count());

  /** generate ddl_dict_kv */
  if (record.convert_to_kv()) {
    sql_print_error("generate ddl_record error, type(%s), thread_id(%d), table_name(%s)", "DDL_REMOVE_CACHE_LOG", thread_id, table_name.c_str());
    return HA_EXIT_FAILURE;
  }

  const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
  xengine::db::WriteBatch *const batch = wb.get();
  m_dict->put_key(batch, record.get_key(), record.get_value());
  print_record(record, DDL_LOG_INSERT);
  m_dict->commit(batch);

  m_dict->delete_key(xa_batch, record.get_key());
  print_record(record, DDL_LOG_DELETE);

  return HA_EXIT_SUCCESS;
}

bool Xdb_ddl_log_manager::write_rename_cache_log(
    xengine::db::WriteBatch *const xa_batch, std::string &table_name,
    std::string &dst_table_name, ulong thread_id) {
  DDL_Record record;
  record.set_type(DDL_RENAME_CACHE_LOG);
  record.set_table_name(table_name);
  record.set_dst_table_name(dst_table_name);
  record.set_thread_id(thread_id);
  record.set_seqno(xa_batch->Count());

  /** generate ddl_dict_kv */
  if (record.convert_to_kv()) {
    sql_print_error("generate ddl_record error, type(%s), thread_id(%d), table_name(%s), dst_table_name(%s)", "DDL_RENAME_CACHE_LOG", thread_id, table_name.c_str(), dst_table_name.c_str());
    return HA_EXIT_FAILURE;
  }

  const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
  xengine::db::WriteBatch *const batch = wb.get();
  m_dict->put_key(batch, record.get_key(), record.get_value());
  print_record(record, DDL_LOG_INSERT);
  m_dict->commit(batch);

  m_dict->delete_key(xa_batch, record.get_key());
  print_record(record, DDL_LOG_DELETE);

  return HA_EXIT_SUCCESS;
}

bool Xdb_ddl_log_manager::replay(DDL_Record *record) {
  Log_Type type;
  type = record->get_type();

  if (type < DDL_SMALLEST_LOG || type > DDL_BIGGEST_LOG) {
    sql_print_error("replay failed, unexpected ddl_log_type, type(%d)", type);
    return true;
  }

  print_record(*record, DDL_LOG_REPLAY);

  switch(type) {
    case DDL_DROP_SUBTABLE_LOG: {
      replay_drop_subtable(record->get_subtable_id());
      break;
    };

    case DDL_REMOVE_CACHE_LOG: {
      replay_remove_cache(record->get_table_name());
      break;
    };

    case DDL_RENAME_CACHE_LOG: {
      replay_rename_cache(record->get_dst_table_name(), record->get_table_name());
      break;
    };

    default: {
      assert(false);
    }
  }

  return false;
}

bool Xdb_ddl_log_manager::replay_drop_subtable(uint subtable_id) {
  GL_INDEX_ID gl_index_id = {.cf_id = subtable_id, .index_id = subtable_id};

#if 0
  /** make sure we can replay idempotently */
  uint16 index_dict_version = 0;
  uchar index_type = 0;
  uint16 kv_version = 0;
  if (!m_dict->get_index_info(gl_index_id, &index_dict_version, &index_type,
                      &kv_version)) {
    sql_print_information("drop subtable record was already replayed");

    /** in the case, maybe index_info is not exist in the dictionary, but subtable has already built, we need cleanup subtable */
    assert(xdb != nullptr);
    m_cf_manager->drop_cf(xdb, subtable_id);
    return false;
  }
#endif

  const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
  xengine::db::WriteBatch *const batch = wb.get();

#if 0
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  gl_index_ids.insert(gl_index_id);
  m_dict->add_drop_index(gl_index_ids, batch);
#endif
  m_dict->delete_index_info(batch, gl_index_id);
  m_dict->commit(batch);
  m_cf_manager->drop_cf(xdb, subtable_id);

  //DEBUG_SYNC(ha_thd(), "before_remove_drop_ongoing");
  m_drop_subtable_thread->signal();

  return false;
}

/** clear rubbish xdb_table_def cache in ddl_table_manager */
bool Xdb_ddl_log_manager::replay_remove_cache(string &table_name) {
  m_ddl_manager->remove_cache(table_name);

  return false;
}

bool Xdb_ddl_log_manager::replay_rename_cache(string &table_name,
                                                  string &dst_table_name) {
  return m_ddl_manager->rename_cache(table_name, dst_table_name);
}

/**
  collect records with reverse order
@param[in] thread_id, collect records for one session
@param[in/out] records, collected records
*/
bool Xdb_ddl_log_manager::search_by_thread_id(ulong thread_id, DDL_Records &records) {
  uchar ddl_log_entry[DDL_LOG_PREFIX_KEY_LEN];
  xdb_netbuf_store_index(ddl_log_entry, Xdb_key_def::DDL_OPERATION_LOG);
  xdb_netbuf_store_uint64(ddl_log_entry + Xdb_key_def::INDEX_NUMBER_SIZE, thread_id);
  xdb_netbuf_store_uint32(ddl_log_entry + Xdb_key_def::INDEX_NUMBER_SIZE + sizeof(ulong), 0);
  const common::Slice ddl_log_slice(reinterpret_cast<char*>(ddl_log_entry), DDL_LOG_PREFIX_KEY_LEN);

  db::Iterator *it = m_dict->new_iterator();

  for (it->Seek(ddl_log_slice); it->Valid(); it->Next()) {
    common::Slice key = it->key();
    common::Slice val = it->value();

    // only check records with type=DDL_OPERATION_LOG
    if (key.size() < DDL_LOG_DICT_PREFIX_KEY_LEN ||
        memcmp(key.data(), ddl_log_entry, DDL_LOG_DICT_PREFIX_KEY_LEN))
      break;

    //only collect one session ddl_logs.
    if (key.size() >= DDL_LOG_PREFIX_KEY_LEN &&
        memcmp(key.data(), ddl_log_entry, DDL_LOG_PREFIX_KEY_LEN - sizeof(uint32_t)))
      break;

    if (key.size() <= DDL_LOG_PREFIX_KEY_LEN) {
      sql_print_error("XEngine: Table_store: key has length %d (corruption?)",
                      (int)key.size());
      MOD_DELETE_OBJECT(Iterator, it);
      return true;
    }

    DDL_Records_iter iter = records.begin();
    DDL_Record* record = new DDL_Record(key, val);
    records.insert(iter, record);
  }

  MOD_DELETE_OBJECT(Iterator, it);
  return false;
}

bool Xdb_ddl_log_manager::search_all(DDL_Records &records) {
  uchar ddl_log_entry[DDL_LOG_DICT_PREFIX_KEY_LEN];
  xdb_netbuf_store_index(ddl_log_entry, Xdb_key_def::DDL_OPERATION_LOG);
  const common::Slice ddl_log_slice(reinterpret_cast<char*>(ddl_log_entry), DDL_LOG_DICT_PREFIX_KEY_LEN);

  db::Iterator *it = m_dict->new_iterator();

  for (it->Seek(ddl_log_slice); it->Valid(); it->Next()) {
    common::Slice key = it->key();
    common::Slice val = it->value();

    // only check records with type=DDL_OPERATION_LOG
    if (key.size() < DDL_LOG_DICT_PREFIX_KEY_LEN ||
        memcmp(key.data(), ddl_log_entry, DDL_LOG_DICT_PREFIX_KEY_LEN))
      break;

    if (key.size() <= DDL_LOG_PREFIX_KEY_LEN) {
      sql_print_error("XEngine: Table_store: key has length %d (corruption?)",
                      (int)key.size());
      MOD_DELETE_OBJECT(Iterator, it);
      return true;
    }

    DDL_Records_iter iter = records.begin();
    DDL_Record* record = new DDL_Record(key, val);
    records.insert(iter, record);
  }

  MOD_DELETE_OBJECT(Iterator, it);
  return false;
}

/**
@param[in] records, collect records to replay
@param[in] all, if recover phase, delete all records replayed
*/
void Xdb_ddl_log_manager::delete_records(DDL_Records &records, bool all) {
  const std::unique_ptr<xengine::db::WriteBatch> wb = m_dict->begin();
  xengine::db::WriteBatch *const batch = wb.get();
  DBUG_ASSERT(batch != nullptr);

  for (auto record : records) {
    m_dict->delete_key(batch, record->get_key());
  }

  m_dict->commit(batch);

  //free memory
  for (auto record : records) {
    delete record;
  }

  return;
}

bool Xdb_ddl_log_manager::replay_by_thread_id(ulong thread_id) {
  DDL_Records records;

  if (search_by_thread_id(thread_id, records)) {
    sql_print_error("search ddl-log record error, %ld", thread_id);
    return true;
  }

  for (auto record : records) {
    if (replay(record)) {
      sql_print_error("replay ddl-log record error, %ld", thread_id);
      return true;
    } else {
      sql_print_information("replay ddl-log record successfully, %ld", thread_id);
    }

#ifndef NDEBUG
  static int replay_func_enter_counts = 0;
  DBUG_EXECUTE_IF("ddl_log_crash_replay_funcs", { replay_func_enter_counts++; };);
  if (replay_func_enter_counts > 1) {
    DBUG_SUICIDE();
  }
#endif
  }

  delete_records(records);

  return false;
}

bool Xdb_ddl_log_manager::replay_all() {
  DDL_Records records;

  if (search_all(records)) {
    sql_print_error("search all ddl-log record error");
    return true;
  }

  for (auto record : records) {
    if (replay(record)) {
      sql_print_error("replay ddl-log record error, %ld", record->get_thread_id());
      return true;
    }
  }

  delete_records(records, true);

  return false;
}

bool Xdb_ddl_log_manager::post_ddl(THD *thd) {
  ulong thread_id = thd_thread_id(thd);

  DBUG_EXECUTE_IF("ddl_log_crash_before_post_ddl_phase", DBUG_SUICIDE();); 

  sql_print_information("DDL log post ddl : begin for thread id : %ld", thread_id);

  if (replay_by_thread_id(thread_id)) {
    sql_print_error("DDL log post ddl : replay error thread id : %ld", thread_id);
  }

  sql_print_information("DDL log post ddl : end for thread id : %ld", thread_id);
  
  return false;
}

bool Xdb_ddl_log_manager::recover() {
  sql_print_information("DDL log recovery : begin");

  if (replay_all()) {
    sql_print_error("DDL log recovery : replay error");
  }

  sql_print_information("DDL log recovery : end");

  return false;
}

void DDL_Record::initialize() {
  const uchar *key_ptr;
  const uchar *val_ptr;

  key_ptr = reinterpret_cast<const uchar *>(m_key.data());
  val_ptr = reinterpret_cast<const uchar *>(m_value.data());

  const uchar *thread_ptr = key_ptr + Xdb_key_def::INDEX_NUMBER_SIZE;
  m_thread_id =
      xdb_netbuf_read_uint64(&thread_ptr);

  const uchar *seq_ptr = key_ptr + Xdb_key_def::INDEX_NUMBER_SIZE + sizeof(ulong);
  m_seqno = xdb_netbuf_read_uint32(&seq_ptr);

  m_type = (Log_Type)(*(key_ptr + DDL_LOG_PREFIX_KEY_LEN));

  uint16_t version;
  version = xdb_netbuf_read_uint16(&val_ptr);
  if (version != Xdb_key_def::DDL_OPERATION_LOG_VERSION) {
    sql_print_error("Unexpected ddl_log_version, %d", version);
    assert(false);
    return;
  }

  switch(m_type) {
    case DDL_DROP_SUBTABLE_LOG: {
      const uchar *subtable_ptr = key_ptr + DDL_LOG_PREFIX_KEY_LEN + sizeof(char);
      m_subtable_id =
        xdb_netbuf_read_uint32(&subtable_ptr);

      break;
    };

    case DDL_REMOVE_CACHE_LOG: {
      uint key_size = m_key.size();
      uint tablename_len = key_size - (DDL_LOG_PREFIX_KEY_LEN + sizeof(char));
      set_table_name(reinterpret_cast<const char*>(key_ptr + DDL_LOG_PREFIX_KEY_LEN + sizeof(char)), tablename_len);

      break;
    };

    case DDL_RENAME_CACHE_LOG: {
      uint key_size = m_key.size();
      uint val_size = m_value.size();

      uint tablename_len = key_size - (DDL_LOG_PREFIX_KEY_LEN + sizeof(char));
      set_table_name(reinterpret_cast<const char*>(key_ptr + DDL_LOG_PREFIX_KEY_LEN + sizeof(char)), tablename_len);

      uint dst_tablename_len = val_size - sizeof(uint16_t);
      set_dst_table_name(reinterpret_cast<const char*>(val_ptr), dst_tablename_len);

      break;
    }

    default: {
      sql_print_error("Unexpected log type %d", m_type);
    } 
  }

  return;
}

DDL_Record::DDL_Record(common::Slice &key, common::Slice &value) {
  memcpy(key_buf, key.data(), key.size());
  memcpy(val_buf, value.data(), value.size());

  m_key = common::Slice(reinterpret_cast<const char*>(key_buf), key.size());
  m_value = common::Slice(reinterpret_cast<const char*>(val_buf), value.size());

  initialize();
}

bool DDL_Record::convert_to_kv() {
  if (m_type < DDL_SMALLEST_LOG || m_type > DDL_BIGGEST_LOG) {
    sql_print_error("unexpected ddl_log_type, mtype(%d)", m_type);
    return HA_EXIT_FAILURE;
  }

  switch (m_type) {
    case DDL_DROP_SUBTABLE_LOG:
    {
      xdb_netbuf_store_uint32(key_buf, Xdb_key_def::DDL_OPERATION_LOG);

      xdb_netbuf_store_uint64(key_buf + DDL_LOG_DICT_PREFIX_KEY_LEN , m_thread_id);
      
      xdb_netbuf_store_uint32(key_buf + (DDL_LOG_PREFIX_KEY_LEN - sizeof(uint32_t)), m_seqno);

      xdb_netbuf_store_byte(key_buf + DDL_LOG_PREFIX_KEY_LEN,
                            DDL_DROP_SUBTABLE_LOG);

      xdb_netbuf_store_uint32(key_buf + (DDL_LOG_PREFIX_KEY_LEN + sizeof(char)),
                              m_subtable_id);

      xdb_netbuf_store_uint16(val_buf, Xdb_key_def::DDL_OPERATION_LOG_VERSION);

      m_key = common::Slice(reinterpret_cast<char *>(key_buf),
                            (DDL_LOG_PREFIX_KEY_LEN +
                             Xdb_key_def::INDEX_NUMBER_SIZE + sizeof(char)));


      m_value =
          common::Slice(reinterpret_cast<char *>(val_buf), sizeof(uint16_t));
      break;
    }

    case DDL_REMOVE_CACHE_LOG:
    {
      uint key_len;

      xdb_netbuf_store_uint32(key_buf, Xdb_key_def::DDL_OPERATION_LOG);

      xdb_netbuf_store_uint64(key_buf + DDL_LOG_DICT_PREFIX_KEY_LEN, m_thread_id);
      
      xdb_netbuf_store_uint32(key_buf + DDL_LOG_PREFIX_KEY_LEN - sizeof(uint32_t), m_seqno);

      xdb_netbuf_store_byte(key_buf + DDL_LOG_PREFIX_KEY_LEN,
                            DDL_REMOVE_CACHE_LOG);

      memcpy(key_buf + (DDL_LOG_PREFIX_KEY_LEN + sizeof(char)),
             m_table_name.data(), m_table_name.size());

      xdb_netbuf_store_uint16(val_buf, Xdb_key_def::DDL_OPERATION_LOG_VERSION);

      key_len = (DDL_LOG_PREFIX_KEY_LEN + sizeof(char) + m_table_name.size());

      m_key = common::Slice(reinterpret_cast<char *>(key_buf), key_len);

      m_value =
          common::Slice(reinterpret_cast<char *>(val_buf), sizeof(uint16_t));
      break;
    }

    case DDL_RENAME_CACHE_LOG:
    {
      uint key_len, val_len;
      xdb_netbuf_store_uint32(key_buf, Xdb_key_def::DDL_OPERATION_LOG);

      xdb_netbuf_store_uint64(key_buf + DDL_LOG_DICT_PREFIX_KEY_LEN, m_thread_id);
      
      xdb_netbuf_store_uint32(key_buf + (DDL_LOG_PREFIX_KEY_LEN - sizeof(uint32_t)), m_seqno);

      xdb_netbuf_store_byte(key_buf + DDL_LOG_PREFIX_KEY_LEN, DDL_RENAME_CACHE_LOG);
 
      memcpy(key_buf + (DDL_LOG_PREFIX_KEY_LEN + sizeof(char)), m_table_name.data(), m_table_name.size());

      key_len = (DDL_LOG_PREFIX_KEY_LEN + sizeof(char) + m_table_name.size());

      m_key = common::Slice(reinterpret_cast<char *>(key_buf), key_len);

      xdb_netbuf_store_uint16(val_buf, Xdb_key_def::DDL_OPERATION_LOG_VERSION);

      memcpy(val_buf + sizeof(uint16_t), m_dst_table_name.data(), m_dst_table_name.size());
      val_len = sizeof(uint16_t) + m_dst_table_name.size();

      m_value =
          common::Slice(reinterpret_cast<char *>(val_buf), val_len);

      break;
    }

    default:
    {
      assert(false);
    }
 }

 return HA_EXIT_SUCCESS;
}

}
