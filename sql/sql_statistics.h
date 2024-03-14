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

#ifndef SQL_STATISTICS_INCLUDED
#define SQL_STATISTICS_INCLUDED

#include <string>
#include "map_helpers.h"             // malloc_unordered_map
#include "mysql/psi/mysql_mutex.h"   // mysql_mutex_t
#include "mysql/psi/mysql_rwlock.h"  // mysql_rwlock_t
#include "sql/common/component.h"
#include "sql/sql_base.h"
#include "sql/sql_const.h"
#include "sql/sql_statistics_common.h"
#include "sql/stateless_allocator.h"  // Stateless_allocator

/**
  Memory usage statistics helper.

  Since these table/index statistics will saved within hash table, maybe it
  costs unsure memories if object count is different. so we must calculate
  memory by these helper.

  See the PFS memory.
*/
struct String_stats_alloc {
  void *operator()(size_t s) const;
};

/* String type for object statistics key */
typedef std::basic_string<char, std::char_traits<char>,
                          Stateless_allocator<char, String_stats_alloc>>
    String_stats_type;

namespace std {

/* The template std::hash specialization for the String_stats_type */
template <>
struct hash<String_stats_type> {
  typedef String_stats_type argument_type;
  typedef size_t result_type;

  size_t operator()(const String_stats_type &s) const;
};

}  // namespace std

/**
  The abstract class for object statistics.
*/
class Object_stats {
 public:
  Object_stats() {}
  virtual ~Object_stats() {}

  /**
    Accumulate the data from Stats_data;

    @param[in]      data        Stats_data
    @param[in]      key_number  How many keys in Table_stats
                                which key in Index_stats
  */
  virtual void accumulate(Stats_data &data, size_t key_number) = 0;

 private:
  /* All the derived class cann't have copy and assign function */
  Object_stats(const Object_stats &);
  Object_stats &operator=(const Object_stats &);
};

/**
  Table statistics
*/
class Table_stats : public Object_stats {
 public:
  Table_stats();

  ~Table_stats() override {}

  /**
    Accumulate the data from Stats_data;

    @param[in]      data        Stats_data
    @param[in]      key_number  How many keys
  */
  virtual void accumulate(Stats_data &data, size_t key_number) override;

 public:
  ulonglong rows_read;
  ulonglong rows_changed;
  ulonglong rows_deleted;
  ulonglong rows_inserted;
  ulonglong rows_updated;
  ulonglong rows_changed_x_indexes;
  ulonglong rds_rows_read_del_mark;
};

/**
  Index statistics
*/
class Index_stats : public Object_stats {
 public:
  Index_stats();

  ~Index_stats() override {}

  /**
    Accumulate the data from Stats_data;

    @param[in]      data        Stats_data
    @param[in]      key_number  Which key
  */
  virtual void accumulate(Stats_data &data, size_t key_number) override;

 public:
  ulonglong rows_read;

  /** number of statements have been used to scan with this key. */
  ulonglong scan_used;
};

/**
  Allocate and init a new Object_stats.

  @param[in]    key           Object statistics key

  @retval       Object_stats  New allocated object
*/
template <typename T>
extern T *object_stats_allocator();

template <typename T>
void object_stats_destroy(T *ptr) {
  if (ptr) ptr->~T();
  my_free(ptr);
}

template <typename T>
void destroy_map(malloc_unordered_map<String_stats_type, T *> &map) {
  for (auto it = map.cbegin(); it != map.cend(); it++) {
    object_stats_destroy(it->second);
  }
  map.clear();
}

/**
  Global shared object statistic cache. it's singleton.
*/
class Object_stats_cache {
 public:
  typedef malloc_unordered_map<String_stats_type, Table_stats *>
      Table_stats_map;
  typedef malloc_unordered_map<String_stats_type, Index_stats *>
      Index_stats_map;

  class Mutex_helper : public im::Disable_unnamed_object {
   public:
    explicit Mutex_helper(mysql_mutex_t *mutex) : m_mutex(mutex) {
      mysql_mutex_lock(m_mutex);
    }
    ~Mutex_helper() { mysql_mutex_unlock(m_mutex); }

   private:
    mysql_mutex_t *m_mutex;
  };

  class RWlock_helper : public im::Disable_unnamed_object {
   public:
    explicit RWlock_helper(mysql_rwlock_t *lock, bool auto_release)
        : m_lock(lock), m_auto_release(auto_release) {}

    void wrlock() { mysql_rwlock_wrlock(m_lock); }
    void rdlock() { mysql_rwlock_rdlock(m_lock); }

    void unlock() { mysql_rwlock_unlock(m_lock); }

    ~RWlock_helper() {
      if (m_auto_release) mysql_rwlock_unlock(m_lock);
    }

   private:
    mysql_rwlock_t *m_lock;
    bool m_auto_release;
  };

  template <typename T>
  struct Type_selector {};

  Table_stats_map &get_map(Type_selector<Table_stats>) { return m_table_map; }

  Index_stats_map &get_map(Type_selector<Index_stats>) { return m_index_map; }
  /**
    Get the table or index map.

    @retval   malloc_unordered_map    table or index map.
  */
  template <typename T>
  malloc_unordered_map<String_stats_type, T *> &map() {
    return get_map(Type_selector<T>());
  }

  /**
    Look up the object statistics.

    @param[in]      key       Table key or index key

    @retval         Object    Table_stats or Index_stats
  */
  template <typename T>
  T *lookup(String_stats_type &key) {
    typename malloc_unordered_map<String_stats_type, T *>::const_iterator it;

    it = map<T>().find(key);
    if (it == map<T>().end())
      return nullptr;
    else
      return it->second;
  }

  /**
    Save the stats object into map.

    @param[in]      key       Table key or index key
    @param[in]      stats     Table_stats or Index_stats

    @retval         True      Success
    @retval         False     Failure
  */
  template <typename T>
  bool insert(String_stats_type &key, T *stats) {
    std::pair<typename malloc_unordered_map<String_stats_type, T *>::iterator,
              bool>
        it;

    it = map<T>().insert(std::pair<String_stats_type, T *>(key, stats));
    return it.second;
  }

  /**
    Allocate object stats and insert into map.

    @param[in]      key       Table key or index key

    @retval         pair[stats *, bool]
                              stats = nullptr and false if failure
                              stats != nullptr and true if success
  */
  template <typename T>
  std::pair<T *, bool> generate_stats(String_stats_type &key) {
    T *stats = nullptr;
    bool res = false;

    if ((stats = object_stats_allocator<T>())) {
      if (!(res = insert<T>(key, stats))) {
        object_stats_destroy<T>(stats);
        stats = nullptr;
      }
    }
    return std::pair<T *, bool>(stats, res);
  }

  /**
    Accumulate the table statistics

    @param[in]      share     TABLE_SHARE object
  */
  void update_table_stats(TABLE_SHARE *share);
  /**
    Accumulate the index statistics

    @param[in]      share     TABLE_SHARE object
  */
  void update_index_stats(TABLE_SHARE *share);

  /**
    Fill the statistics into table_statisitcs table.

    @param[in]      thd       User connection context
    @param[in]      table     Current opened schema table
  */
  int fill_table_stats(THD *thd, TABLE *table);

  /**
    Fill the statistics into index_statisitcs table.

    @param[in]      thd       User connection context
    @param[in]      table     Current opened schema table
  */
  int fill_index_stats(THD *thd, TABLE *table);

  Object_stats_cache();
  virtual ~Object_stats_cache();

  /* The global singleton */
  static Object_stats_cache *instance();

  void init();
  void destroy();

  void update_table_stats();
  void update_index_stats();

  /* Disable the copy constructor and assign function */
  Object_stats_cache(const Object_stats_cache &) = delete;
  Object_stats_cache &operator=(const Object_stats_cache &) = delete;

 private:
  mysql_rwlock_t m_table_lock;
  mysql_rwlock_t m_index_lock;
  Table_stats_map m_table_map;
  Index_stats_map m_index_map;
};

#endif
