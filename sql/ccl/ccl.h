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

#ifndef SQL_CCL_CCL_INCLUDED
#define SQL_CCL_CCL_INCLUDED

#include <atomic>

#include "my_sqlcommand.h"
#include "mysql/psi/mysql_rwlock.h"
#include "sql/ccl/ccl_bucket.h"
#include "sql/ccl/ccl_common.h"
#include "sql/ccl/ccl_table_common.h"

/**
  Concurrency control module.
*/

/* Every wait time interval (second) */
#define CCL_RETRY_INTERVAL 5

/* The default wait time (second) */
#define CCL_LONG_WAIT ((ulong)3600L * 24L)

/* The default max wait thread */
#define CCL_DEFAULT_WAITING_COUNT ((ulonglong)0L)

class Table_ref;
class THD;
struct LEX;
class Item;

namespace im {
class Ccl_slot;
class Ccl_queue_hint;

/* The max timeout when concurrency control */
extern ulong ccl_wait_timeout;

/* The max waiting count when concurrency control */
extern ulonglong ccl_max_waiting_count;

/* Result structure for show_ccl_rule */
typedef struct Ccl_show_result {
  ulonglong id;
  LEX_CSTRING type;
  LEX_STRING schema;
  LEX_STRING table;
  ulonglong concurrency_count;
  LEX_STRING state;
  LEX_STRING ordered;
  ulonglong matched;
  ulonglong running;
  ulonglong waiting;
  LEX_STRING keywords;

 public:
  Ccl_show_result() {
    id = 0;
    type = {nullptr, 0};
    schema = {nullptr, 0};
    table = {nullptr, 0};
    concurrency_count = 0;
    matched = 0;
    running = 0;
    waiting = 0;
    keywords = {nullptr, 0};
  }
} Ccl_show_result;
/**
  Concurrency control thread local context.
*/
class Ccl_comply {
 public:
  explicit Ccl_comply(THD *thd) : m_thd(thd), m_slot(nullptr), m_version(0) {}

  ~Ccl_comply();

  /**
    Comply the rule when execute the statement.

    @param[in]    sql_command   Query command
    @param[in]    all_tables    Table list
    @param[in]    query         Statement string

    @retval       true          Rule worked
    @retval       false         Skipped
  */
  bool comply_rule(enum_sql_command sql_command, Table_ref *all_tables,
                   const LEX_CSTRING &query);

  /**
    Comply the queue hint when execute the statement.

    @param[in]    lex           Lex context
    @param[in]    hint          Queue hint

    @retval       true          Rule worked
    @retval       false         Skipped
  */
  bool comply_queue(THD *thd, LEX *lex, Ccl_queue_hint *hint);

  /**
    Comply the where cond when execute the statement.

    @param[in]    thd           Thread context
    @param[in]    cond          Where cond
  */
  bool comply_cond(THD *thd, Item *cond);
  /**
    End the statement if comply some rule.
  */
  void comply_end();

 private:
  THD *m_thd;
  Ccl_slot *m_slot;
  ulonglong m_version;
};

/**
  The keywords within a rule
*/
class Ccl_keywords : public PSI_memory_base, public Disable_copy_base {
  /* Normally no more than 3 keywords in a rule. */
  static constexpr const size_t PREALLOC = 3;

  typedef Keyword_array_type<PREALLOC> Keyword_array;

  /* Original keyword string is separated by ";" */
  static constexpr const char *SEPARATOR = ";";

 public:
  Ccl_keywords(const char *source, PSI_memory_key key)
      : PSI_memory_base(key), m_keyword_array(key) {
    build_keyword(source);
  }

  virtual ~Ccl_keywords() { m_keyword_array.clear(); }
  /**
    Build the keyword array from string

    @param[in]    source string
  */
  void build_keyword(const char *source);

  size_t size() const { return m_keyword_array.size(); }

  /**
     Whether the query string match the keywords.

     @param[in]     query       Statement
     @param[in]     ordered     Match the keyword orderly or not

     @retval        true        Matched
     @retval        false       Not matched
  */
  bool match(const LEX_CSTRING &query, bool ordered) const;

 private:
  /* keywords array */
  Keyword_array m_keyword_array;
};

/**
  The concurrency control rule definition.
*/
class Ccl_rule : public PSI_memory_base, public Disable_copy_base {
 public:
  explicit Ccl_rule(const Ccl_record *record, PSI_memory_key key)
      : PSI_memory_base(key),
        m_rule_id(record->id),
        m_type(record->type),
        m_schema(record->schema_name ? record->schema_name : ""),
        m_table(record->table_name ? record->table_name : ""),
        m_concurrency_count(record->concurrency_count),
        m_ccl_keywords(record->keywords ? record->keywords : "", key),
        m_state(record->state),
        m_order(record->ordered),
        m_slot(nullptr),
        m_original_keywords(record->keywords ? record->keywords : ""),
        m_matched(0) {}

  explicit Ccl_rule(ulonglong bucket_size, PSI_memory_key key)
      : PSI_memory_base(key),
        m_rule_id(0),
        m_type(Ccl_rule_type::RULE_QUEUE),
        m_schema(""),
        m_table(""),
        m_concurrency_count(bucket_size),
        m_ccl_keywords("", key),
        m_state(Ccl_rule_state::RULE_ACTIVE),
        m_order(Ccl_rule_ordered::RULE_DISORDER),
        m_slot(nullptr),
        m_original_keywords(""),
        m_matched(0) {}

  /* Except of slot, Other elements will be destructed automatically */
  virtual ~Ccl_rule() { unbind_slot(); }

  /**
    Search an available slot for rule.
    report error message by level if failed.

    @param[in]      level           error level

    @retval   true      Failure
    @retval   false     Success
  */
  bool bind_slot(Ccl_error_level level);

  /**
    Release the slot back to ring_buffer,
    Call it when destruct rule.
  */
  void unbind_slot();

  const String_ccl &get_schema() const { return m_schema; }
  const String_ccl &get_table() const { return m_table; }
  const String_ccl &get_keywords() const { return m_original_keywords; }
  Ccl_slot *get_slot() const { return m_slot; }
  ulonglong get_concurrency_count() const { return m_concurrency_count; }
  ulonglong get_rule_id() const { return m_rule_id; }
  Ccl_rule_state get_state() const { return m_state; }
  Ccl_rule_type get_type() const { return m_type; }
  Ccl_rule_ordered get_ordered() const { return m_order; }
  ulonglong get_matched() const { return m_matched; }

  bool keyword_is_null() const { return m_ccl_keywords.size() == 0; }

  bool match_keyword(const LEX_CSTRING &query) const;

  Ccl_slot *retrieve_slot(ulonglong *version);

  void set_id(ulonglong value) { m_rule_id = value; }

 private:
  ulonglong m_rule_id;
  Ccl_rule_type m_type;
  String_ccl m_schema;
  String_ccl m_table;
  ulonglong m_concurrency_count;
  Ccl_keywords m_ccl_keywords;
  Ccl_rule_state m_state;
  Ccl_rule_ordered m_order;
  Ccl_slot *m_slot;

  String_ccl m_original_keywords;
  ulonglong m_matched;
};

/**
  Rule set for every kind of rule type
*/
class Ccl_rule_set : public PSI_memory_base, public Disable_copy_base {
 public:
  static constexpr const size_t PREALLOC = 10;
  /**
    Concurrency control containe three category of rules:

    1) schema + table + keyword(optional)

      - Firstly, search rules according to [schema + table],
        and match keyword if has.

    2) keywords
      - Secondly, search the rules according keyword.

    3) command
      - Last, only one rule by sql command.

   So it corresponds three containers:

    1) Hash on Intact key [schema + table]

    2) Array

    3) Ccl_rule
  */

  /**
   Revision History:
   -----------------
   1. Add ccl queue strategy
      -- It allocated several rules according to variable
   ccl_queue_bucket_count, Every bucket controled the concurrency count by
   ccl_queue_bucket_size.

         So it use array to save all queue rules, and mapped the position by
   hash value.
  */
  using Intact_object_rules = Ccl_rule_map_type<Ccl_rule>;
  using Intact_value_type = Intact_object_rules::value_type;
  using Intact_key_type = Intact_object_rules::key_type;

  /* Array of keyword rules. element type is ccl_rule pointer */
  using Keyword_rules = Ccl_rule_list_type<Ccl_rule *, PREALLOC>;

  /* Array of queue rules. */
  using Queue_buckets =
      Ccl_rule_list_type<Ccl_rule *, CCL_QUEUE_BUCKET_COUNT_MAX>;

  class Rule_lock_helper : public Disable_unnamed_object {
   public:
    explicit Rule_lock_helper(mysql_rwlock_t *lock, bool exclusive)
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

    ~Rule_lock_helper() {
      if (m_locked) mysql_rwlock_unlock(m_lock);
    }

   private:
    bool m_locked;
    mysql_rwlock_t *m_lock;
  };

 public:
  Ccl_rule_set(Ccl_rule_type type, PSI_memory_key key)
      : PSI_memory_base(key),
        m_intact_rules(key),
        m_intact_size(0),
        m_keyword_rules(key),
        m_keyword_size(0),
        m_command_rule(nullptr),
        m_command_size(0),
        m_queue_buckets(key),
        m_queue_size(0),
        m_type(type) {
    mysql_rwlock_init(key_rwlock_rule_container, &m_intact_lock);
    mysql_rwlock_init(key_rwlock_rule_container, &m_keyword_lock);
    mysql_rwlock_init(key_rwlock_rule_container, &m_command_lock);
    mysql_rwlock_init(key_rwlock_rule_container, &m_queue_lock);
  }

  virtual ~Ccl_rule_set() {
    clear_rules();
    clear_queue_buckets(true);
    mysql_rwlock_destroy(&m_intact_lock);
    mysql_rwlock_destroy(&m_keyword_lock);
    mysql_rwlock_destroy(&m_command_lock);
    mysql_rwlock_destroy(&m_queue_lock);
  }

  /**
    Comply the rules, the matching order is : intact rule, keyword rule,
    command rule.

    @param[in]    all_tables    Table list
    @param[in]    query         Statement string
    @param[out]   version       the matched slot version

    @retval       slot
  */
  Ccl_slot *comply_rule(Table_ref *all_tables, const LEX_CSTRING &query,
                        ulonglong *version);

  /**
    Comply the queue rules.

    @param[in]    hash          hash value
    @param[out]   version       the matched slot version

    @retval       slot
  */
  Ccl_slot *comply_queue(ulonglong hash, ulonglong *version);
  /**
    Refresh the rules.
    Report the error message by level.

    @param[in]      group           rule group
    @param[in]      force_clearn    whether clear cache
    @param[in]      level           error level

    @retval         added rule count
  */
  size_t refresh_rules(Ccl_record_group &group, bool force_clean,
                       Ccl_error_level level);
  /**
    Collect all active queue buckets.

    @param[in]      mem_root      memory pool
    @param[out]     results       rule result container
  */
  void collect_queue_buckets(MEM_ROOT *mem_root,
                             std::vector<Ccl_show_result *> &results);
  /**
    Collect all active rules.

    @param[in]      mem_root      memory pool
    @param[out]     results       rule result container
  */
  void collect_rules(MEM_ROOT *mem_root,
                     std::vector<Ccl_show_result *> &results);
  /**
    Insert the intact rules into cache.
    Report corressponding error message by level.

    @param[in]      rules     Rule set
    @param[in]      level     Error level

    @retval         affected rule count
  */
  size_t insert_intact_rules(std::vector<Ccl_record *> &rules,
                             Ccl_error_level level);
  /**
    Insert the keyword rules into cache.
    Report corressponding error message by level.

    @param[in]      rules     Rule set
    @param[in]      level     Error level

    @retval         affected rule count
  */
  size_t insert_keyword_rules(std::vector<Ccl_record *> &rules,
                              Ccl_error_level level);
  /**
    Insert the command rules into cache.
    Report corressponding error message by level.

    @param[in]      rules     Rule set
    @param[in]      level     Error level

    @retval         affected rule count
  */
  size_t insert_command_rules(std::vector<Ccl_record *> &rules,
                              Ccl_error_level level);
  /**
    Clear the rule cache by rule_id.

    @param[in]      rule_id     rule id

    @retval         affected rows
  */
  size_t delete_rule(ulonglong rule_id);

  /**
    Clear all rules from intact, keyword, command container.
  */
  void clear_rules();

  /**
    Clear all the queue bucket.
  */
  void clear_queue_buckets(bool need_lock);
  /**
    Init the queue buckets

    @param[in]    bucket_count      The bucket count
    @param[in]    bucket_size       The bucket size
    @param[in]    level             Error level

    @retval       successful item count
  */
  size_t init_queue_buckets(ulonglong bucket_count, ulonglong bucket_size,
                            Ccl_error_level level);

 private:
  /* Clear intact container */
  void clear_intact_rules();
  /* Clear keyword container */
  void clear_keyword_rules();
  /* Clear command container */
  void clear_command_rule();

  /**
    clear the rule from intact container by rule_id.

    @param[in]        rule_id       rule id

    @retval           affected rows
  */
  size_t delete_intact_rule(ulonglong rule_id);
  /**
    clear the rule from keyword container by rule_id.

    @param[in]        rule_id       rule id

    @retval           affected rows
  */
  size_t delete_keyword_rule(ulonglong rule_id);
  /**
    clear the rule from command container by rule_id.

    @param[in]        rule_id       rule id

    @retval           affected rows
  */
  size_t delete_command_rule(ulonglong rule_id);

 private:
  /* Hash on [schema + table] */
  Intact_object_rules m_intact_rules;
  mysql_rwlock_t m_intact_lock;
  std::atomic_ullong m_intact_size;

  /* Array on [keywords] */
  Keyword_rules m_keyword_rules;
  mysql_rwlock_t m_keyword_lock;
  std::atomic_ullong m_keyword_size;

  /* Single command rule */
  Ccl_rule *m_command_rule;
  mysql_rwlock_t m_command_lock;
  std::atomic_ullong m_command_size;

  Queue_buckets m_queue_buckets;
  mysql_rwlock_t m_queue_lock;
  std::atomic_ullong m_queue_size;

  Ccl_rule_type m_type;
};

/**
  Singleton concurrency control system

  Prepare a rule set container for every kind of rule type
    (SELECT, UPDATE, INSERT, DELETE).
  Rule set container initilized when ccl_init(), and didn't support to
  add new rule type dynamically, so it's not necessary to be protected
  by lock.
*/
class System_ccl : public PSI_memory_base, public Disable_copy_base {
 public:
  System_ccl(PSI_memory_key key)
      : PSI_memory_base(key),
        m_select_rule_set(Ccl_rule_type::RULE_SELECT, key),
        m_update_rule_set(Ccl_rule_type::RULE_UPDATE, key),
        m_insert_rule_set(Ccl_rule_type::RULE_INSERT, key),
        m_delete_rule_set(Ccl_rule_type::RULE_DELETE, key),
        m_queue_bucket_set(Ccl_rule_type::RULE_QUEUE, key) {}

  /**
    Refresh the rules.
    Report the error message by level.

    @param[in]      group           rule group
    @param[in]      type            rule type
    @param[in]      force_clearn    whether clear cache
    @param[in]      level           error level

    @retval         added rule count
  */
  size_t refresh_rules(Ccl_record_group &group, Ccl_rule_type type,
                       bool force_clean, Ccl_error_level level);

  /* Clear all active rules */
  void clear_rules();
  /**
    Collect all active rules.

    @param[in]      mem_root      memory pool
    @param[out]     results       rule result container
  */
  void collect_rules(MEM_ROOT *mem_root,
                     std::vector<Ccl_show_result *> &results);
  /**
    Collect all active queue buckets.

    @param[in]      mem_root      memory pool
    @param[out]     results       rule result container
  */
  void collect_queue_buckets(MEM_ROOT *mem_root,
                             std::vector<Ccl_show_result *> &results);
  /**
    Delete the rule by rule id.

    @param[in]      rule_id         rule id

    @retval         affected rule count
  */
  size_t delete_rule(ulonglong rule_id);

  /**
    Get the corresponding rule set by type.
  */
  Ccl_rule_set *get_rule_set(Ccl_rule_type type) {
    DBUG_ENTER("get_rule_set");
    switch (type) {
      case Ccl_rule_type::RULE_SELECT:
        DBUG_RETURN(&m_select_rule_set);
      case Ccl_rule_type::RULE_UPDATE:
        DBUG_RETURN(&m_update_rule_set);
      case Ccl_rule_type::RULE_INSERT:
        DBUG_RETURN(&m_insert_rule_set);
      case Ccl_rule_type::RULE_DELETE:
        DBUG_RETURN(&m_delete_rule_set);
      /* Pls use get_queue_buckets() get queue_buckets */
      case Ccl_rule_type::RULE_QUEUE:
      default:
        assert(0);
        DBUG_RETURN(nullptr);
    }
  }

  /* Get queue buckets */
  Ccl_rule_set *get_queue_buckets() { return &m_queue_bucket_set; }

 public:
  /* Singleton instance */
  static System_ccl *instance() { return m_system_ccl; }

  /* Singleton object */
  static System_ccl *m_system_ccl;

  /* Destructor */
  virtual ~System_ccl() {}

 private:
  /* SELECT rule set */
  Ccl_rule_set m_select_rule_set;
  /* UPDATE rule set */
  Ccl_rule_set m_update_rule_set;
  /* INSERT rule set */
  Ccl_rule_set m_insert_rule_set;
  /* DELETE rule set */
  Ccl_rule_set m_delete_rule_set;
  /* Queue buckets */
  Ccl_rule_set m_queue_bucket_set;
};

/**
  Ccl rule only keeped the static configure.
  Every valid rule will has corresponding slot which maintained
  dynamic running state.
*/
class Ccl_slot {
 public:
  Ccl_slot();
  virtual ~Ccl_slot();
  /**
    Acquire this slot;

    @param[in]  version       Unique version
    @param[in]  cc_count      Concurrency count

    @retval     true          Success
    @retval     false         Failure
  */
  bool acquire(ulonglong version, ulonglong cc_count);

  /**
    Release this slot;
    Should call it if return the slot back to ring_buffer.
  */
  void release();
  /**
    Enter the condition.

    @param[in]    thd         thread context.
    @param[in]    version     the thread local version.
  */
  void enter_cond(THD *thd, ulonglong version);

  /**
    Exit the condition.
  */
  void exit_cond();

  void set_concurrency(ulonglong value) { m_concurrency_count = value; }
  void reset_concurrency() { m_concurrency_count = 0; }
  ulonglong get_version() const { return m_version; }
  ulonglong get_running() const { return m_running; }
  ulonglong get_waiting() const { return m_waiting; }

  void lock();
  void unlock();

 private:
  mysql_mutex_t m_mutex;
  mysql_cond_t m_cond;
  /* Concurrency count from ccl rule */
  ulonglong m_concurrency_count;
  /* Running count */
  ulonglong m_running;
  /* Waiting count */
  ulonglong m_waiting;
  /* Every rule should has unique version when assign a slot */
  ulonglong m_version;
  /* if available or not */
  std::atomic<bool> m_state;
};

/**
  Pre-allocateded array as the cycle buffer, every slot can be used
  from head to tail and again.
*/
template <typename Element_type, size_t Prealloc>
class Ring_buffer : public PSI_memory_base, public Disable_copy_base {
 public:
  /* Array structure */
  using Buffer = Prealloced_array<Element_type *, Prealloc>;

  explicit Ring_buffer(PSI_memory_key key)
      : PSI_memory_base(key),
        m_buffer(key),
        m_pos(0),
        m_size(Prealloc),
        m_global_version(1) {}

  /* Destructor of Ring_buffer */
  virtual ~Ring_buffer() {
    for (typename Buffer::const_iterator it = m_buffer.cbegin();
         it != m_buffer.cend(); it++) {
      destroy_object<Element_type>(*it);
    }
    m_buffer.clear();
  }
  /* Get slots */
  Buffer &get_slots() { return m_buffer; }
  /**
    Acquire an available slot, return null if still unavailable
    after Prealloc times.

    @param[in]  cc_count        Concurrency count

    @retval     Element_type    Available slot
    @retval     nullptr         Unavailable slot
  */
  Element_type *acquire_slot(ulonglong cc_count) {
    ulonglong begin_pos;
    ulonglong version;
    size_t loop = 0;
    DBUG_ENTER("Ring_buffer::acquire_slot");
    begin_pos = m_pos.load();
    version = m_global_version.fetch_add(1);
    while (!(m_buffer[begin_pos % m_size]->acquire(version, cc_count)) &&
           (loop <= m_size)) {
      begin_pos++;
      loop++;
    }
    /* Still unavailable slot retry loop times */
    if (loop > m_size) {
      DBUG_RETURN(nullptr);
    } else {
      /* Set next begin position to improve efficiency */
      m_pos.store(begin_pos + 1);
      DBUG_RETURN(m_buffer[begin_pos % m_size]);
    }
  }

  /* Singleton instance */
  static Ring_buffer *instance();
  static Ring_buffer *m_ring_buffer;

 private:
  /* Slot array */
  Buffer m_buffer;
  /* Last available position + 1 */
  std::atomic_ullong m_pos;
  /* Array size */
  size_t m_size;
  /* Global version for every acquire_slot */
  std::atomic_ullong m_global_version;
};

/**
  Concurrency control system allowed max slot size setting.

  1) 64
      The max rule records in mysql.concurrency_control

  2) CCL_QUEUE_BUCKET_COUNT_MAX
      The max queue bucket count
*/
#define CCL_SLOT_SIZE (64 + CCL_QUEUE_BUCKET_COUNT_MAX)

/* Instantiation of slots */
using Ccl_ring_slots =
    Ring_buffer<Ccl_slot, CCL_SLOT_SIZE>;

} /*namespace im */

#endif
