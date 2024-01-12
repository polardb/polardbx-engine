/* Copyright (c) 2000, 2018, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "include/mysql/components/services/bits/psi_mutex_bits.h"  // PSI_mutex_key
#include "include/mysql/components/services/bits/psi_rwlock_bits.h"  // PSI_rwlock_key
#include "my_murmur3.h"  // my_murmur3_32
#include "my_sys.h"      // MY_WME
#include "mysql/psi/mysql_memory.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_rwlock.h"
#include "mysql/service_mysql_alloc.h"  // my_malloc
#include "sql/auth/auth_acls.h"         // SELECT_ACL
#include "sql/auth/auth_common.h"       // check_access
#include "sql/field.h"                  // store
#include "sql/handler.h"                // update_statistics
#include "sql/log.h"
#include "sql/psi_memory_key.h"

#include "sql/table.h"

#include "sql/sql_show.h"
#include "sql/sql_statistics.h"

bool opt_tablestat = true;
bool opt_indexstat = true;

/* Max retry times when generate object stats */
static uint object_statistics_retry_max_times = 10;

#ifdef HAVE_PSI_INTERFACE
static PSI_memory_key key_memory_object_stats;
static PSI_rwlock_key key_LOCK_object_stats;
#endif

ST_FIELD_INFO table_stats_fields_info[] = {
    {"TABLE_SCHEMA", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, "Table_schema", 0},
    {"TABLE_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, "Table_name", 0},
    {"ROWS_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Rows_read", 0},
    {"ROWS_CHANGED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Rows_changed", 0},
    {"ROWS_CHANGED_X_INDEXES", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, "Rows_changed_x_#indexes", 0},
    {"ROWS_INSERTED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Rows_inserted", 0},
    {"ROWS_DELETED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Rows_deleted", 0},
    {"ROWS_UPDATED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     "Rows_updated", 0},
    {"ROWS_READ_DELETE_MARK", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, "Rows_read_delete_mark", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO index_stats_fields_info[] = {
    {"TABLE_SCHEMA", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, "Table_schema", 0},
    {"TABLE_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, "Table_name", 0},
    {"INDEX_NAME", NAME_LEN, MYSQL_TYPE_STRING, 0, 0, "Index_name", 0},
    {"ROWS_READ", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0,
     "Rows_read", 0},
    {"SCAN_USED", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONG, 0, 0,
     "Scan_used", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

/**
  We want to accumulate the memory, so define a new allocator.
*/
void *String_stats_alloc::operator()(size_t s) const {
  return my_malloc(key_memory_object_stats, s, MYF(MY_WME | ME_FATALERROR));
}

/**
  If String_stats_type is treated as key, here should realize the hash function.
  the std::hash template specialization.
*/
namespace std {

size_t hash<String_stats_type>::operator()(const String_stats_type &s) const {
  return murmur3_32(reinterpret_cast<const uchar *>(s.c_str()), s.size(), 0);
}

}  // namespace std

template String_stats_type::~basic_string();

/**
  Allocate and init a new Object_stats.

  @retval       Object_stats  New allocated object
*/
template <typename T>
T *object_stats_allocator() {
  void *ptr = nullptr;
  T *stats = nullptr;

  ptr = my_malloc(key_memory_object_stats, sizeof(T),
                  MYF(MY_WME | ME_FATALERROR));

  if (ptr) stats = new (ptr) T();

  return stats;
}

Table_stats::Table_stats() {
  rows_read = 0;
  rows_changed = 0;
  rows_deleted = 0;
  rows_inserted = 0;
  rows_updated = 0;
  rows_changed_x_indexes = 0;
  rds_rows_read_del_mark = 0;
}

/**
  Accumulate the data from Stats_data;

  @param[in]      data        Stats_data
  @param[in]      key_number  How many keys
*/
void Table_stats::accumulate(Stats_data &data, size_t key_number) {
  rows_read += data.rows_read;
  rows_changed += data.rows_changed;
  rows_deleted += data.rows_deleted;
  rows_inserted += data.rows_inserted;
  rows_updated += data.rows_updated;
  rows_changed_x_indexes += data.rows_changed * (key_number ? key_number : 1);
  rds_rows_read_del_mark += data.rds_rows_read_del_mark;
}

Index_stats::Index_stats() {
  rows_read = 0;
  scan_used = 0;
}

/**
  Accumulate the data from Stats_data;

  @param[in]      data        Stats_data
  @param[in]      key_number  Which key
*/
void Index_stats::accumulate(Stats_data &data, size_t key_number) {
  if (key_number < MAX_KEY) rows_read += data.index_rows_read[key_number];
  if (key_number < MAX_KEY) scan_used += data.index_scan_used[key_number];
}

/* Table_stats template specialization */
template Table_stats *object_stats_allocator<Table_stats>();

/* Index_stats template specialization */
template Index_stats *object_stats_allocator<Index_stats>();

/**
  Constructor of Object stats cache
*/
Object_stats_cache::Object_stats_cache()
    : m_table_map(key_memory_object_stats),
      m_index_map(key_memory_object_stats) {
  init();
}

/**
  The singleton instance entry.
*/
Object_stats_cache *Object_stats_cache::instance() {
  static Object_stats_cache cache;
  return &cache;
}

void Object_stats_cache::init() {
  mysql_rwlock_init(key_LOCK_object_stats, &m_table_lock);
  mysql_rwlock_init(key_LOCK_object_stats, &m_index_lock);
}

Object_stats_cache::~Object_stats_cache() {}

void Object_stats_cache::destroy() {
  mysql_rwlock_destroy(&m_table_lock);
  mysql_rwlock_destroy(&m_index_lock);
  destroy_map<Table_stats>(map<Table_stats>());
  destroy_map<Index_stats>(map<Index_stats>());
}

/**
  Accumulate the table statistics

  @param[in]      table     TABLE object
  @param[in]      data      stats_data reference
*/
void Object_stats_cache::update_table_stats(TABLE_SHARE *share) {
  Stats_data &data = share->stats_data;
  Object_stats *stats = nullptr;

  if ((!data.rows_read && !data.rows_changed && !data.rds_rows_read_del_mark) ||
      !share->db.str || !share->table_name.str)
    return;

  String_stats_type key(share->table_cache_key.str,
                        share->table_cache_key.length);

  RWlock_helper helper(&m_table_lock, false);
  assert(helper.effect());
  uint retrys = 0;

retry:
  if (retrys++ > object_statistics_retry_max_times) {
    sql_print_warning("Table statistics retry %d times.", retrys);
    return;
  }
  helper.rdlock();
  if (!(stats = lookup<Table_stats>(key))) {
    helper.unlock();
    helper.wrlock();

    std::pair<Table_stats *, bool> res = generate_stats<Table_stats>(key);
    if (!res.second) {
      helper.unlock();
      goto retry;
    } else {
      stats = res.first;
    }
  }

  /* TODO: Move it later if support flushing in future. */
  helper.unlock();
  if (stats) {
    stats->accumulate(data, share->keys);
    data.reset_table();
  }
}

/**
  Accumulate the index statistics

  @param[in]      table     TABLE object
  @param[in]      data      stats_data reference
*/
void Object_stats_cache::update_index_stats(TABLE_SHARE *share) {
  char buf[NAME_LEN * 3 + 3];
  size_t length;
  KEY *info;
  Object_stats *stats = nullptr;
  Stats_data &data = share->stats_data;

  for (size_t i = 0; i < share->keys; i++) {
    stats = nullptr;
    if (data.index_rows_read[i] > 0) {
      info = share->key_info + i;
      length = snprintf(buf, NAME_LEN * 3 + 3, "%s%c%s%c%s", share->db.str,
                        '\0', share->table_name.str, '\0', info->name);
      String_stats_type key(buf, length);

      RWlock_helper helper(&m_index_lock, false);
      assert(helper.effect());
      uint retrys = 0;

    retry:
      if (retrys++ > object_statistics_retry_max_times) {
        sql_print_warning("Index statistics retry %d times.", retrys);
        return;
      }
      helper.rdlock();
      if (!(stats = lookup<Index_stats>(key))) {
        helper.unlock();
        helper.wrlock();
        std::pair<Index_stats *, bool> res = generate_stats<Index_stats>(key);

        if (!res.second) {
          helper.unlock();
          goto retry;
        } else {
          stats = res.first;
        }
      }
      /* TODO: Move it later if support flushing in future. */
      helper.unlock();
      if (stats) {
        stats->accumulate(data, i);
        data.reset_index(i);
      }
    }
  }
}

/**
  Fill the statistics into table_statisitcs table.

  @param[in]      thd       User connection context
  @param[in]      table     Current opened schema table
*/
int Object_stats_cache::fill_table_stats(THD *thd, TABLE *table) {
  const char *table_schema;
  const char *table_name;

  RWlock_helper helper(&m_table_lock, true);
  assert(helper.effect());
  helper.rdlock();
  for (auto it = map<Table_stats>().cbegin(); it != map<Table_stats>().cend();
       it++) {
    restore_record(table, s->default_values);

    table_schema = (it->first).c_str();
    table_name = table_schema + strlen(table_schema) + 1;

    const Table_stats *table_stats = it->second;
    Table_ref tmp_table;

    memset((char *)&tmp_table, 0, sizeof(tmp_table));
    tmp_table.table_name = table_name;
    tmp_table.db = table_schema;
    tmp_table.grant.privilege = 0;

    if (check_access(thd, SELECT_ACL, tmp_table.db, &tmp_table.grant.privilege,
                     0, 0, is_infoschema_db(table_schema)) ||
        check_grant(thd, SELECT_ACL, &tmp_table, 1, UINT_MAX, 1))
      continue;

    table->field[0]->store(table_schema, strlen(table_schema),
                           system_charset_info);
    table->field[1]->store(table_name, strlen(table_name), system_charset_info);
    table->field[2]->store((longlong)table_stats->rows_read, true);
    table->field[3]->store((longlong)table_stats->rows_changed, true);
    table->field[4]->store((longlong)table_stats->rows_changed_x_indexes, true);

    table->field[5]->store((longlong)table_stats->rows_inserted, true);
    table->field[6]->store((longlong)table_stats->rows_deleted, true);
    table->field[7]->store((longlong)table_stats->rows_updated, true);
    table->field[8]->store((longlong)table_stats->rds_rows_read_del_mark, true);

    if (schema_table_store_record(thd, table)) return 1;
  }
  return 0;
}

/**
  Fill the statistics into index_statisitcs table.

  @param[in]      thd       User connection context
  @param[in]      table     Current opened schema table
*/
int Object_stats_cache::fill_index_stats(THD *thd, TABLE *table) {
  const char *table_schema;
  const char *table_name;
  const char *index_name;

  RWlock_helper helper(&m_index_lock, true);
  assert(helper.effect());

  helper.rdlock();
  for (auto it = map<Index_stats>().cbegin(); it != map<Index_stats>().cend();
       it++) {
    restore_record(table, s->default_values);

    table_schema = (it->first).c_str();
    table_name = table_schema + strlen(table_schema) + 1;
    index_name = table_name + strlen(table_name) + 1;

    const Index_stats *index_stats = it->second;
    Table_ref tmp_table;

    memset((char *)&tmp_table, 0, sizeof(tmp_table));
    tmp_table.table_name = table_name;
    tmp_table.db = table_schema;
    tmp_table.grant.privilege = 0;

    if (check_access(thd, SELECT_ACL, tmp_table.db, &tmp_table.grant.privilege,
                     0, 0, is_infoschema_db(table_schema)) ||
        check_grant(thd, SELECT_ACL, &tmp_table, 1, UINT_MAX, 1))
      continue;

    table->field[0]->store(table_schema, strlen(table_schema),
                           system_charset_info);
    table->field[1]->store(table_name, strlen(table_name), system_charset_info);
    table->field[2]->store(index_name, strlen(index_name), system_charset_info);
    table->field[3]->store((longlong)index_stats->rows_read, true);
    table->field[4]->store((longlong)index_stats->scan_used, true);

    if (schema_table_store_record(thd, table)) return 1;
  }
  return 0;
}

void handler::update_table_statistics() {
  if (!table || !table->s ||
      (!stats_data.rows_read && !stats_data.rows_changed &&
       !stats_data.rds_rows_read_del_mark) ||
      !table->s->db.str || !table->s->table_name.str)
    return;

  table->s->stats_data.rows_read += stats_data.rows_read;
  table->s->stats_data.rows_changed += stats_data.rows_changed;
  table->s->stats_data.rows_inserted += stats_data.rows_inserted;
  table->s->stats_data.rows_deleted += stats_data.rows_deleted;
  table->s->stats_data.rows_updated += stats_data.rows_updated;
  table->s->stats_data.rds_rows_read_del_mark +=
      stats_data.rds_rows_read_del_mark;
}

void handler::update_index_statistics() {
  if (!table || !table->s || !table->s->table_cache_key.str ||
      !table->s->table_name.str)
    return;

  for (uint x = 0; x < table->s->keys; ++x) {
    if (stats_data.index_rows_read[x]) {
      table->s->stats_data.index_rows_read[x] += stats_data.index_rows_read[x];
      table->s->stats_data.index_scan_used[x]++;
    }
  }
}

/**
  Accumulate the operated rows once statement end.
*/
void handler::update_statistics() {
  if (opt_tablestat) update_table_statistics();

  if (opt_indexstat) update_index_statistics();

  stats_data.reset();
}

/**
  Send the table stats back to the client.
*/
int fill_schema_table_stats(THD *thd, Table_ref *tables,
                            Item *__attribute__((unused))) {
  TABLE *table = tables->table;
  DBUG_ENTER("fill_schema_table_stats");

  mysql_mutex_lock(&LOCK_open);

  for (const auto &key_and_value : *table_def_cache) {
    TABLE_SHARE *share = key_and_value.second.get();
    Object_stats_cache::instance()->update_table_stats(share);
  }

  mysql_mutex_unlock(&LOCK_open);

  DBUG_RETURN(Object_stats_cache::instance()->fill_table_stats(thd, table));
}

/**
  Send the index stats back to the client.
*/
int fill_schema_index_stats(THD *thd, Table_ref *tables,
                            Item *__attribute__((unused))) {
  TABLE *table = tables->table;
  DBUG_ENTER("fill_schema_index_stats");

  mysql_mutex_lock(&LOCK_open);

  for (const auto &key_and_value : *table_def_cache) {
    TABLE_SHARE *share = key_and_value.second.get();
    Object_stats_cache::instance()->update_index_stats(share);
  }

  mysql_mutex_unlock(&LOCK_open);

  DBUG_RETURN(Object_stats_cache::instance()->fill_index_stats(thd, table));
}

#ifdef HAVE_PSI_INTERFACE
static PSI_rwlock_info object_statistics_rwlocks[] = {
    {&key_LOCK_object_stats, "LOCK_sql_statistics", PSI_FLAG_SINGLETON, 0,
     PSI_DOCUMENT_ME}};

static PSI_memory_info object_statistics_memory[] = {
    {&key_memory_object_stats, "object_statistics", PSI_FLAG_ONLY_GLOBAL_STAT,
     0, PSI_DOCUMENT_ME}};

static void init_object_statistics_psi_keys() {
  const char *category = "sql";
  int count;

  count = static_cast<int>(array_elements(object_statistics_rwlocks));
  mysql_rwlock_register(category, object_statistics_rwlocks, count);

  count = static_cast<int>(array_elements(object_statistics_memory));
  mysql_memory_register(category, object_statistics_memory, count);
}
#endif
/**
  Init the object statistics context when booting, since the singleton instance
  is not thread safe.
*/
void object_statistics_context_init() {
#ifdef HAVE_PSI_INTERFACE
  init_object_statistics_psi_keys();
#endif

  Object_stats_cache::instance();
}

void object_statistics_context_destroy() {
  Object_stats_cache::instance()->destroy();
}
