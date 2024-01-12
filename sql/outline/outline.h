/* Copyright (c) 2018, 2019, Alibaba and/or its affiliates. All rights reserved.

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

#ifndef SQL_OUTLINE_OUTLINE_INCLUDED
#define SQL_OUTLINE_OUTLINE_INCLUDED

#include "sql/outline/outline_common.h"
#include "sql/outline/outline_table_common.h"

struct TABLE_LIST;

namespace im {

/* Outline statistics */
struct Outline_stats {
  ulonglong hit;
  ulonglong overflow;

 public:
  Outline_stats() {
    hit = 0;
    overflow = 0;
  }
  void aggregate(const Outline_stats &stats) {
    hit += stats.hit;
    overflow += stats.overflow;
  }
};

/**
  Outline object definition.
*/
class Outline {
  using Index_array = Prealloced_array<String_outline, 10>;

  /* Separator of indexs in hint column */
  static constexpr const char *INDEX_HINT_SEPARATOR = ",";

 public:
  explicit Outline(Outline_record *record)
      : m_id(record->id),
        m_schema(record->schema.str ? record->schema.str : ""),
        m_digest(record->digest.str),
        m_digest_text(record->digest_text.str ? record->digest_text.str : ""),
        m_type(record->type),
        m_scope(record->scope),
        m_state(record->state),
        m_pos(record->pos),
        m_hint(record->hint.str ? record->hint.str : ""),
        m_indexes(key_memory_outline),
        m_stats() {
    /* Build the index hint array if outline_type is INDEX */
    build_index_hint();
  }

  explicit Outline(const Outline &other)
      : m_id(other.m_id),
        m_schema(other.m_schema),
        m_digest(other.m_digest),
        m_digest_text(other.m_digest_text),
        m_type(other.m_type),
        m_scope(other.m_scope),
        m_state(other.m_state),
        m_pos(other.m_pos),
        m_hint(other.m_hint),
        m_indexes(key_memory_outline),
        m_stats() {
    build_index_hint();
  }

  virtual ~Outline() { m_indexes.clear(); }

  Outline_type get_type() const { return m_type; }
  ulonglong get_pos() const { return m_pos; }
  const String_outline &get_digest() const { return m_digest; }
  const String_outline &get_digest_text() const { return m_digest_text; }
  const String_outline &get_schema() const { return m_schema; }
  const String_outline &get_hint() const { return m_hint; }
  ulonglong get_id() const { return m_id; }
  const Outline_stats &get_stats() const { return m_stats; }
  Outline_scope get_scope() const { return m_scope; }

  void inc_hit() { m_stats.hit++; }
  void inc_overflow() { m_stats.overflow++; }
  /**
    Evoke index hint instance, and add into list.

    @param[in]        mem_root    memory allocator
    @param[in/out]    list        container
  */
  void evoke_index_hint(MEM_ROOT *mem_root, List<Index_hint> *list);
  /**
    Evoke optimizer hint instance, and add into list.

    @param[in]        mem_root    memory allocator
    @param[in/out]    list        container
  */
  void evoke_optimizer_hint(MEM_ROOT *mem_root, List<Lex_optimizer_hint> *list);

 private:
  /**
    Build the index array by hint column in table.
  */
  void build_index_hint();

 private:
  ulonglong m_id;
  String_outline m_schema;
  String_outline m_digest;
  String_outline m_digest_text;
  Outline_type m_type;
  Outline_scope m_scope;
  Outline_state m_state;
  ulonglong m_pos;
  String_outline m_hint;

  /* Generated attributes */
  Index_array m_indexes;

  /* All metrics */
 private:
  Outline_stats m_stats;
};

/* Outline show result structure */
struct Outline_show_result {
  ulonglong id;
  String_outline schema;
  String_outline digest;
  Outline_type type;
  Outline_scope scope;
  ulonglong pos;
  String_outline hint;

  Outline_stats stats;

  String_outline digest_text;

 public:
  explicit Outline_show_result(const Outline *outline)
      : id(outline->get_id()),
        schema(outline->get_schema()),
        digest(outline->get_digest()),
        type(outline->get_type()),
        scope(outline->get_scope()),
        pos(outline->get_pos()),
        hint(outline->get_hint()),
        stats(),
        digest_text(outline->get_digest_text()) {
    stats.aggregate(outline->get_stats());
  }

  explicit Outline_show_result(const Outline_show_result &other)
      : id(other.id),
        schema(other.schema),
        digest(other.digest),
        type(other.type),
        scope(other.scope),
        pos(other.pos),
        hint(other.hint),
        stats(other.stats),
        digest_text(other.digest_text) {}

  void aggregate(const Outline *outline) {
    stats.aggregate(outline->get_stats());
  }
};

using Outline_show_result_container =
    malloc_unordered_map<ulonglong, Outline_show_result>;

struct Outline_preview_result {
  String_outline schema;
  String_outline digest;
  String_outline block_type;
  String_outline block_name;
  ulonglong block;
  String_outline hint_text;

 public:
  explicit Outline_preview_result(const char *schema_arg,
                                  const char *digest_arg,
                                  const char *block_type_arg,
                                  const char *block_name_arg,
                                  ulonglong block_arg,
                                  const char *hint_text_arg)
      : schema(schema_arg),
        digest(digest_arg),
        block_type(block_type_arg),
        block_name(block_name_arg),
        block(block_arg),
        hint_text(hint_text_arg) {}
};
using Outline_preview_result_container =
    Malloc_outline_vector<Outline_preview_result *>;
/**
  Statement outlines.

  Outline array within statement.

  Statement outline structure:
    1) schema, digest [outline key]
    2) outline array
*/
class Statement_outline : public Disable_copy_base {
 public:
  using Outline_vector = Malloc_outline_vector<Outline *>;

 public:
  explicit Statement_outline(const char *schema, const char *digest)
      : m_schema(schema), m_digest(digest), m_outlines() {}
  /**
    Insert outline into statement_outline.

    @param[in]      outline       outline
  */
  void insert(Outline *outline);
  /**
    Delete the outline from array by id;

    @param[in]        outline id
    @retval           affected rows
  */
  size_t delete_by_id(ulonglong id);
  virtual ~Statement_outline();

  const Outline_vector &outlines() const { return m_outlines; }

 private:
  String_outline m_schema;
  String_outline m_digest;
  Outline_vector m_outlines;
};

typedef Statement_outline::Outline_vector Statement_outline_vector;

/**
  System outline cache.

  Structure:
    Statement_outline map array;

    map structure: { [schema, digest] => Statement_outline}

    1) statement index outline
    map[0]
    map[1]
    ...

    2) statement optimizer outline
    map[m_partition]
    ...

    map[m_elements - 1]
*/
class System_outline {
  using Statement_outline_map =
      Pair_key_unordered_map<String_outline, String_outline, Statement_outline>;

  template <typename T>
  using Container = Prealloced_array<T, 32>;

  /* Lock helper class */
  class Lock_helper : public Disable_unnamed_object {
   public:
    explicit Lock_helper(mysql_rwlock_t *lock, bool exclusive)
        : m_locked(false), m_lock(lock) {
      if (exclusive)
        mysql_rwlock_wrlock(m_lock);
      else
        mysql_rwlock_rdlock(m_lock);

      m_locked = true;
    }

    void unlock() {
      mysql_rwlock_unlock(m_lock);
      m_locked = false;
    }

    ~Lock_helper() {
      if (m_locked) mysql_rwlock_unlock(m_lock);
    }

   private:
    bool m_locked;
    mysql_rwlock_t *m_lock;
  };

 public:
  /* Index or optimizer */
  enum class Category {
    CATEGORY_INDEX = 0,
    CATEGORY_OPTIMIZER,
    CATEGORY_COUNT
  };

 public:
  explicit System_outline(size_t partition);

  virtual ~System_outline();

  size_t get_partition() { return m_partition; }
  /**
    Fill the outlines into maps, clear all if force_clean is true.

    @param[in]      records       outline records container.
    @param[in]      force_clean   Whether clear all maps.
  */
  void fill_records(Conf_records *records, bool force_clean);
  /**
    Find the outlines by schema and digest, and create hint
    add into TABLE_LIST or SELECT_LEX if found.

    @param[in]      thd           thread context
    @param[in/out]  T             TABLE_LISTS or SELECT_LEX list
    @param[in]      schema        schema name
    @param[in]      digest        statement digest
  */
  template <typename T, Category outline_category>
  void find_and_fill_hints(THD *thd, T *all_objects, String_outline &schema,
                           String_outline &digest);
  /**
    Delete the outline from all maps by id

    @param[in]        outline id

    @retval           affected rows
  */
  size_t delete_outline(ulonglong id);
  /**
    Aggregate the outlines that has the same outline id within
    all partition maps.

    @param[out]   container       Result container
  */
  void aggregate_outline(Outline_show_result_container *container);

  /* The system outline singleton instance */
  static System_outline *m_system_outline;
  static System_outline *instance() { return m_system_outline; }

 private:
  /**
    Get the suitable position by thread id and category type

    @param[in]      type        Category type
    @param[in]      thread id   Thread id

    @retval         position
  */
  template <Category type>
  size_t get_pos(ulonglong thread_id) {
    return (thread_id % m_partition) +
           (static_cast<size_t>(type) * m_partition);
  }
  /**
    Get the begin position by category type

    There will be m_partition for every kind of category.
  */
  size_t get_begin(Category type) {
    return static_cast<size_t>(type) * m_partition;
  }
  /**
    Add the outling into m_maps at pos.

    @param[in]      pos       partition position
    @param[in]      outline   outline object

    @retval         true      Failure
    @retval         false     Success
  */
  bool add_outline(size_t pos, const Outline *outline);

  /**
    Clear the statement outline objects from maps at pos.

    @param[in]      pos       partition position
  */
  void clear(size_t pos);
  /**
    Find the statement outline by schema and digest.

    @param[in]      pos       position
    @param[in]      schema    schema name
    @param[in]      digest    statement digest
  */
  Statement_outline *find(size_t pos, const String_outline &schema,
                          const String_outline &digest) {
    typename Statement_outline_map::const_iterator it;
    it = m_maps[pos]->find(Statement_outline_map::key_type(schema, digest));
    if (it == m_maps[pos]->end())
      return nullptr;
    else
      return const_cast<Statement_outline *>(it->second);
  }

 private:
  /* The number of partition */
  size_t m_partition;
  /* The number of element. */
  size_t m_elements;
  /* statement outline map's array */
  Container<Statement_outline_map *> m_maps;
  /* statement outline size's array */
  Container<std::atomic_ullong *> m_sizes;
  /* locks array */
  Container<mysql_rwlock_t *> m_locks;
};

typedef System_outline::Category Outline_category;

/**
  Structure of two category containers
*/
struct Outline_group {
  std::vector<Outline *> index_outlines;
  std::vector<Outline *> optimizer_outlines;

 public:
  Outline_group() {}
  /**
    Classify the different hint type, and fill into vector.

    1) One is index hint.
    2) The other is optimizer hint.

    @param[in]    records     All kinds of outline
   */
  void classify_hint(Conf_records *records);
  /**
    Destructor of two container

    Should destroy the outlines from allocate_outline_object();
  */
  ~Outline_group();
};

/* Struct within SELECT_LEX */
class Lex_optimizer_hint {
 public:
  ulonglong query_block;
  LEX_CSTRING hint;

  explicit Lex_optimizer_hint() {
    query_block = 0;
    hint = NULL_CSTR;
  }

  explicit Lex_optimizer_hint(ulonglong pos, char *hint_arg, size_t len) {
    query_block = pos;
    hint.str = hint_arg;
    hint.length = len;
  }
};

} /*namespace im */

#endif
