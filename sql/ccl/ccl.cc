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

#include "sql/ccl/ccl.h"
#include "errmsg.h"
#include "mysql/plugin.h"
#include "mysqld_error.h"
#include "mysys_err.h"
#include "sql/ccl/ccl_common.h"
#include "sql/ccl/ccl_hint.h"
#include "sql/current_thd.h"
#include "sql/derror.h"  // ER_THD
#include "sql/mysqld_thd_manager.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "sql/table.h"

/**
  Concurrency control module.
*/

namespace im {

/* The max timeout when concurrency control */
ulong ccl_wait_timeout = CCL_LONG_WAIT;

/* The max waiting count when concurrency control */
ulonglong ccl_max_waiting_count = CCL_DEFAULT_WAITING_COUNT;

/**
  Comply the rule when execute the statement.

  @param[in]    sql_command   Query command
  @param[in]    all_tables    Table list
  @param[in]    query         Statement string

  @retval       true          Rule worked
  @retval       false         Skipped
*/
bool Ccl_comply::comply_rule(enum_sql_command sql_command,
                             Table_ref *all_tables, const LEX_CSTRING &query) {
  Ccl_rule_type rule_type;
  Ccl_rule_set *rule_set;
  ulonglong version;
  Ccl_slot *slot;
  DBUG_ENTER("Ccl_comply::comply_rule");

  /** Already comply some rule */
  if (m_slot != nullptr) DBUG_RETURN(false);

  assert(m_slot == nullptr && m_version == 0);

  rule_type = sql_command_to_ccl_type(sql_command);

  if (rule_type == Ccl_rule_type::RULE_UNKNOWN) DBUG_RETURN(false);

  rule_set = System_ccl::instance()->get_rule_set(rule_type);
  assert(rule_set);

  /* The version value is retrieved under lock protecting */
  if ((slot = rule_set->comply_rule(all_tables, query, &version))) {
    m_version = version;
    m_slot = slot;
    m_slot->enter_cond(m_thd, m_version);
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}

/**
  Comply the queue hint when execute the statement.

  @param[in]    thd           Thread context
  @param[in]    lex           Lex context
  @param[in]    hint          Queue hint

  @retval       true          Rule worked
  @retval       false         Skipped
*/
bool Ccl_comply::comply_queue(THD *thd, LEX *lex, Ccl_queue_hint *hint) {
  Ccl_rule_set *rule_set;
  ulonglong version;
  Ccl_slot *slot;
  ulonglong hash_value;
  DBUG_ENTER("Ccl_comply::comply_queue");

  /** Already comply some rule */
  if (m_slot != nullptr) DBUG_RETURN(false);

  assert(m_slot == nullptr && m_version == 0);

  hash_value = hint->hash_value(thd, lex->query_block);
  rule_set = System_ccl::instance()->get_queue_buckets();
  assert(rule_set);

  /* The version value is retrieved under lock protecting */
  if ((slot = rule_set->comply_queue(hash_value, &version))) {
    m_version = version;
    m_slot = slot;
    m_slot->enter_cond(m_thd, m_version);
    DBUG_RETURN(true);
  }

  DBUG_RETURN(false);
}

/**
  End the statement if comply some rule.
*/
void Ccl_comply::comply_end() {
  if (m_slot) {
    m_slot->exit_cond();
    m_slot = nullptr;
    m_version = 0;
  }
}

/* Destructor */
Ccl_comply::~Ccl_comply() {
  DBUG_ENTER("Ccl_comply::~Ccl_comply");
  assert(m_slot == nullptr && m_version == 0);
  DBUG_VOID_RETURN;
}

/**
   Whether the query string match the keywords.

   @param[in]     query       Statement
   @param[in]     ordered     Match the keyword orderly or not

   @retval        true        Matched
   @retval        false       Not matched
*/
bool Ccl_keywords::match(const LEX_CSTRING &query, bool ordered) const {
  std::string target(query.str);
  std::string::size_type pos = 0;
  std::string::size_type sub_pos = 0;

  for (Keyword_array::const_iterator it = m_keyword_array.cbegin();
       it != m_keyword_array.cend(); it++) {
    if ((sub_pos = target.find(it->c_str(), pos)) != std::string::npos) {
      if (ordered) pos = sub_pos;
    } else
      return false;
  }
  return true;
}

/**
  Build the keyword array from string

  @param[in]    source string
*/
void Ccl_keywords::build_keyword(const char *str) {
  std::string::size_type pos1, pos2;
  DBUG_ENTER("Ccl_keywords::build_keyword");
  assert(m_keyword_array.size() == 0);

  if (str == nullptr || str[0] == '\0') DBUG_VOID_RETURN;

  String_ccl t_str(str);
  pos1 = 0;
  pos2 = t_str.find(SEPARATOR);
  while (pos2 != std::string::npos) {
    m_keyword_array.push_back(t_str.substr(pos1, pos2 - pos1));

    pos1 = pos2 + strlen(SEPARATOR);
    pos2 = t_str.find(SEPARATOR, pos1);
  }
  m_keyword_array.push_back(t_str.substr(pos1));
  DBUG_VOID_RETURN;
}

/**
  Search an available slot for rule.
  report error message by level if failed.

  @param[in]      level           error level

  @retval   true      Failure
  @retval   false     Success
*/
bool Ccl_rule::bind_slot(Ccl_error_level level) {
  DBUG_ENTER("Ccl_rule::bind_slot");
  assert(m_slot == nullptr);

  Ccl_slot *slot =
      Ccl_ring_slots::instance()->acquire_slot(m_concurrency_count);
  if (slot) {
    m_slot = slot;
    DBUG_RETURN(false);
  } else {
    if (level == Ccl_error_level::CCL_CRITICAL) {
      my_error(ER_CCL_UNAVAILABLE_SLOT, MYF(0), m_rule_id);
    } else {
      push_warning_printf(
          current_thd, Sql_condition::SL_WARNING, ER_CCL_UNAVAILABLE_SLOT,
          ER_THD(current_thd, ER_CCL_UNAVAILABLE_SLOT), m_rule_id);
    }
    DBUG_RETURN(true);
  }
}

/**
  Release the slot back to ring_buffer,
  Call it when destruct rule.
*/
void Ccl_rule::unbind_slot() {
  DBUG_ENTER("Ccl_rule::unbind_slot");
  if (m_slot) {
    m_slot->release();
    m_slot = nullptr;
  }
  DBUG_VOID_RETURN;
}

bool Ccl_rule::match_keyword(const LEX_CSTRING &query) const {
  if (m_ccl_keywords.size() == 0 || query.length == 0) return false;

  return m_ccl_keywords.match(query, (m_order == Ccl_rule_ordered::RULE_ORDER));
}

/* Clear intact container */
void Ccl_rule_set::clear_intact_rules() {
  m_intact_size.store(0);
  /* Clear Intact table rules */
  for (Intact_object_rules::const_iterator it = m_intact_rules.cbegin();
       it != m_intact_rules.cend(); it++) {
    destroy_object<Ccl_rule>(const_cast<Ccl_rule *>(it->second));
  }
  m_intact_rules.clear();
}

/**
  clear the rule from intact container by rule_id.

  @param[in]        rule_id       rule id

  @retval           affected rows
*/
size_t Ccl_rule_set::delete_intact_rule(ulonglong rule_id) {
  size_t num = 0;
  /* delete Intact table rules */
  for (Intact_object_rules::const_iterator it = m_intact_rules.cbegin();
       it != m_intact_rules.cend();) {
    const Ccl_rule *rule = it->second;
    if (rule->get_rule_id() == rule_id) {
      destroy_object<Ccl_rule>(const_cast<Ccl_rule *>(rule));
      it = m_intact_rules.erase(it);
      num += 1;
    } else
      it++;
  }
  m_intact_size.store(m_intact_rules.size());
  return num;
}

/* Clear keyword container */
void Ccl_rule_set::clear_keyword_rules() {
  m_keyword_size.store(0);
  /* Clear keyword rules */
  for (Keyword_rules::const_iterator it = m_keyword_rules.cbegin();
       it != m_keyword_rules.cend(); it++) {
    destroy_object<Ccl_rule>(*it);
  }
  m_keyword_rules.clear();
}
/**
  clear the rule from keyword container by rule_id.

  @param[in]        rule_id       rule id

  @retval           affected rows
*/
size_t Ccl_rule_set::delete_keyword_rule(ulonglong rule_id) {
  size_t num = 0;
  for (Keyword_rules::const_iterator it = m_keyword_rules.cbegin();
       it != m_keyword_rules.cend();) {
    const Ccl_rule *rule = *it;
    if (rule->get_rule_id() == rule_id) {
      destroy_object<Ccl_rule>(const_cast<Ccl_rule *>(rule));
      m_keyword_rules.erase(it);
      num += 1;
    } else
      it++;
  }
  m_keyword_size.store(m_keyword_rules.size());
  return num;
}

/**
  clear the rule from command container by rule_id.

  @param[in]        rule_id       rule id

  @retval           affected rows
*/
size_t Ccl_rule_set::delete_command_rule(ulonglong rule_id) {
  size_t num = 0;
  if (m_command_rule && m_command_rule->get_rule_id() == rule_id) {
    m_command_size.store(0);
    destroy_object<Ccl_rule>(m_command_rule);
    m_command_rule = nullptr;
    num += 1;
  }

  return num;
}

/* Clear command container */
void Ccl_rule_set::clear_command_rule() {
  m_command_size.store(0);
  if (m_command_rule) destroy_object<Ccl_rule>(m_command_rule);
  ;
  m_command_rule = nullptr;
}

/**
  Clear all rules from intact, keyword, command container.
*/
void Ccl_rule_set::clear_rules() {
  Rule_lock_helper intact_lock(&m_intact_lock, true);
  assert(intact_lock.effect());
  clear_intact_rules();
  intact_lock.unlock();

  Rule_lock_helper keyword_lock(&m_keyword_lock, true);
  assert(keyword_lock.effect());
  clear_keyword_rules();
  keyword_lock.unlock();

  Rule_lock_helper command_lock(&m_command_lock, true);
  assert(command_lock.effect());
  clear_command_rule();
  command_lock.unlock();
}

/**
  Clear all the queue bucket.
*/
void Ccl_rule_set::clear_queue_buckets(bool need_lock) {
  if (need_lock) mysql_rwlock_wrlock(&m_queue_lock);

  m_queue_size.store(0);
  for (Queue_buckets::const_iterator it = m_queue_buckets.cbegin();
       it != m_queue_buckets.cend(); it++) {
    destroy_object<Ccl_rule>(*it);
  }
  m_queue_buckets.clear();

  if (need_lock) mysql_rwlock_unlock(&m_queue_lock);
}

/**
  Init the queue buckets

  @param[in]    bucket_count      The bucket count
  @param[in]    bucket_size       The bucket size
  @param[in]    level             Error level

  @retval       successful item count
*/
size_t Ccl_rule_set::init_queue_buckets(ulonglong bucket_count,
                                        ulonglong bucket_size,
                                        Ccl_error_level level) {
  size_t num = 0;
  Rule_lock_helper queue_lock(&m_queue_lock, true);
  assert(queue_lock.effect());
  clear_queue_buckets(false);

  for (ulonglong i = 0; i < bucket_count; i++) {
    Ccl_rule *rule = allocate_ccl_object<Ccl_rule>(bucket_size, key_memory_ccl);
    if (rule->bind_slot(level)) {
      destroy_object<Ccl_rule>(rule);
      continue;
    }
    /* Ccl queue bucket rule id is sequenced internally */
    rule->set_id(i + 1);
    m_queue_buckets.push_back(rule);
    num += 1;
  }
  m_queue_size.store(m_queue_buckets.size());
  return num;
}

/**
  Clear the rule cache by rule_id.

  @param[in]      rule_id     rule id

  @retval         affected rows
*/
size_t Ccl_rule_set::delete_rule(ulonglong rule_id) {
  size_t num = 0;
  Rule_lock_helper intact_lock(&m_intact_lock, true);
  assert(intact_lock.effect());
  num += delete_intact_rule(rule_id);
  intact_lock.unlock();

  Rule_lock_helper keyword_lock(&m_keyword_lock, true);
  assert(keyword_lock.effect());
  num += delete_keyword_rule(rule_id);
  keyword_lock.unlock();

  Rule_lock_helper command_lock(&m_command_lock, true);
  assert(command_lock.effect());
  num += delete_command_rule(rule_id);
  command_lock.unlock();

  return num;
}

/**
  Refresh the rules.
  Report the error message by level.

  Immediate break off insert rules if error_level is critical,
  and insert_xxx_rules function should has report my_error.
  For example: add_ccl_rule()

  Continue to insert rules if error_level is warning,
  and insert_xxx_rules function maybe has pushed warning message.
  For example: flush_ccl_rule()

  @param[in]      group           rule group
  @param[in]      force_clean     whether clear cache
  @param[in]      level           error level

  @retval         added rule count
*/
size_t Ccl_rule_set::refresh_rules(Ccl_record_group &group, bool force_clean,
                                   Ccl_error_level level) {
  size_t num = 0;
  DBUG_ENTER("Ccl_rule_set::refresh_rules");

  if (group.intact_rules.size() > 0) {
    Rule_lock_helper intact_lock(&m_intact_lock, true);
    assert(intact_lock.effect());
    if (force_clean) clear_intact_rules();
    size_t intact_num = insert_intact_rules(group.intact_rules, level);
    intact_lock.unlock();
    num += intact_num;
    if (level == Ccl_error_level::CCL_CRITICAL &&
        intact_num != group.intact_rules.size()) {
      goto end;
    }
  }

  if (group.keyword_rules.size() > 0) {
    Rule_lock_helper keyword_lock(&m_keyword_lock, true);
    assert(keyword_lock.effect());
    if (force_clean) clear_keyword_rules();
    size_t keyword_num = insert_keyword_rules(group.keyword_rules, level);
    keyword_lock.unlock();
    num += keyword_num;
    if (level == Ccl_error_level::CCL_CRITICAL &&
        keyword_num != group.keyword_rules.size()) {
      goto end;
    }
  }

  if (group.command_rules.size() > 0) {
    Rule_lock_helper command_lock(&m_command_lock, true);
    assert(command_lock.effect());
    if (force_clean) clear_command_rule();
    size_t command_num = insert_command_rules(group.command_rules, level);
    command_lock.unlock();
    num += command_num;
    if (level == Ccl_error_level::CCL_CRITICAL &&
        command_num != group.command_rules.size()) {
      goto end;
    }
  }

end:
  DBUG_RETURN(num);
}

/**
  Insert the intact rules into cache.
  Report corressponding error message by level.

  @param[in]      rules     Rule set
  @param[in]      level     Error level

  @retval         affected rule count
*/
size_t Ccl_rule_set::insert_intact_rules(std::vector<Ccl_record *> &rules,
                                         Ccl_error_level level) {
  size_t num = 0;
  Ccl_rule *rule = nullptr;
  DBUG_ENTER("Ccl_rule_set::insert_intact_rules");

  for (auto it = rules.cbegin(); it != rules.cend(); ++it) {
    Ccl_record *record = *it;
    rule = allocate_ccl_object<Ccl_rule>(record, key_memory_ccl);

    /* Bind the slot, break off if critical */
    if (rule->bind_slot(level)) {
      destroy_object<Ccl_rule>(rule);
      if (level == Ccl_error_level::CCL_CRITICAL)
        break;
      else
        continue;
    }

    if (!(m_intact_rules
              .insert(Intact_value_type(
                  Intact_key_type(rule->get_schema(), rule->get_table()), rule))
              .second)) {
      if (level == Ccl_error_level::CCL_CRITICAL) {
        my_error(ER_CCL_DUPLICATE_RULE, MYF(0), rule->get_rule_id(),
                 "load intact rule");
        destroy_object<Ccl_rule>(rule);
        break;
      } else {
        push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                            ER_CCL_DUPLICATE_RULE,
                            ER_THD(current_thd, ER_CCL_DUPLICATE_RULE),
                            rule->get_rule_id(), "load intact rule");
        destroy_object<Ccl_rule>(rule);
      }
    } else
      num += 1;
  }
  m_intact_size.store(m_intact_rules.size());
  DBUG_RETURN(num);
}
/**
  Insert the intact rules into cache.
  Report corressponding error message by level.

  @param[in]      rules     Rule set
  @param[in]      level     Error level

  @retval         affected rule count
*/
size_t Ccl_rule_set::insert_keyword_rules(std::vector<Ccl_record *> &rules,
                                          Ccl_error_level level) {
  size_t num = 0;
  Ccl_rule *rule = nullptr;
  DBUG_ENTER("Ccl_rule_set::insert_keyword_rules");
  for (auto it = rules.cbegin(); it != rules.cend(); ++it) {
    Ccl_record *record = *it;
    rule = allocate_ccl_object<Ccl_rule>(record, key_memory_ccl);
    if (!rule->bind_slot(level)) {
      m_keyword_rules.push_back(rule);
      num += 1;
    } else {
      destroy_object<Ccl_rule>(rule);
      if (level == Ccl_error_level::CCL_CRITICAL) break;
    }
  }
  m_keyword_size.store(m_keyword_rules.size());
  DBUG_RETURN(num);
}

/**
  Insert the command rules into cache.
  report error message by level if failed.

  @param[in]      rules     Rule set
  @param[in]      level     Error level

  @retval         affected rule count
*/
size_t Ccl_rule_set::insert_command_rules(std::vector<Ccl_record *> &rules,
                                          Ccl_error_level level) {
  size_t num = 0;
  Ccl_rule *rule = nullptr;
  DBUG_ENTER("Ccl_rule_set::insert_command_rules");
  for (auto it = rules.cbegin(); it != rules.cend(); ++it) {
    Ccl_record *record = *it;
    if (m_command_rule != nullptr) {
      if (level == Ccl_error_level::CCL_CRITICAL) {
        my_error(ER_CCL_DUPLICATE_RULE, MYF(0), record->id,
                 "load command rule");
        break;
      } else {
        push_warning_printf(current_thd, Sql_condition::SL_WARNING,
                            ER_CCL_DUPLICATE_RULE,
                            ER_THD(current_thd, ER_CCL_DUPLICATE_RULE),
                            record->id, "load command rule");
        continue;
      }
    }
    rule = allocate_ccl_object<Ccl_rule>(record, key_memory_ccl);
    if ((!rule->bind_slot(level))) {
      m_command_rule = rule;
      num += 1;
    } else {
      destroy_object<Ccl_rule>(rule);
      if (level == Ccl_error_level::CCL_CRITICAL) break;
    }
  }
  m_command_size.store(m_command_rule == nullptr ? 0 : 1);
  DBUG_RETURN(num);
}

Ccl_slot *Ccl_rule::retrieve_slot(ulonglong *version) {
  *version = m_slot->get_version();
  m_matched++;
  return m_slot;
}

static Ccl_show_result *get_ccl_result(MEM_ROOT *mem_root,
                                       const Ccl_rule *rule) {
  Ccl_show_result *result = new (mem_root) Ccl_show_result();
  result->id = rule->get_rule_id();
  const char *type = ccl_rule_type_str[static_cast<size_t>(rule->get_type())];
  result->type.str = type;
  result->type.length = strlen(type);

  result->schema.str = strmake_root(mem_root, rule->get_schema().c_str(),
                                    rule->get_schema().length());
  result->schema.length = rule->get_schema().length();

  result->table.str = strmake_root(mem_root, rule->get_table().c_str(),
                                   rule->get_table().length());
  result->table.length = rule->get_table().length();

  char state = ccl_rule_state_str[static_cast<size_t>(rule->get_state())];
  result->state.str = strmake_root(mem_root, &state, 1);
  result->state.length = 1;

  char ordered = ccl_rule_ordered_str[static_cast<size_t>(rule->get_ordered())];
  result->ordered.str = strmake_root(mem_root, &ordered, 1);
  result->ordered.length = 1;

  result->concurrency_count = rule->get_concurrency_count();
  result->matched = rule->get_matched();
  result->running = rule->get_slot()->get_running();
  result->waiting = rule->get_slot()->get_waiting();

  result->keywords.str = strmake_root(mem_root, rule->get_keywords().c_str(),
                                      rule->get_keywords().length());
  result->keywords.length = rule->get_keywords().length();

  return result;
}

/**
  Collect all active queue buckets.

  @param[in]      mem_root      memory pool
  @param[out]     results       rule result container
*/
void Ccl_rule_set::collect_queue_buckets(
    MEM_ROOT *mem_root, std::vector<Ccl_show_result *> &results) {
  Ccl_show_result *result;

  Rule_lock_helper queue_lock(&m_queue_lock, false);
  assert(queue_lock.effect());
  for (Queue_buckets::const_iterator it = m_queue_buckets.cbegin();
       it != m_queue_buckets.cend(); it++) {
    if ((result = get_ccl_result(mem_root, *it))) results.push_back(result);
  }
}

/**
  Collect all active rules.

  @param[in]      mem_root      memory pool
  @param[out]     results       rule result container
*/
void Ccl_rule_set::collect_rules(MEM_ROOT *mem_root,
                                 std::vector<Ccl_show_result *> &results) {
  Ccl_show_result *result;
  Rule_lock_helper intact_lock(&m_intact_lock, false);
  assert(intact_lock.effect());
  for (Intact_object_rules::const_iterator it = m_intact_rules.cbegin();
       it != m_intact_rules.cend(); it++) {
    if ((result = get_ccl_result(mem_root, it->second)))
      results.push_back(result);
  }
  intact_lock.unlock();

  Rule_lock_helper keyword_lock(&m_keyword_lock, false);
  assert(keyword_lock.effect());
  for (Keyword_rules::const_iterator it = m_keyword_rules.cbegin();
       it != m_keyword_rules.cend(); it++) {
    if ((result = get_ccl_result(mem_root, *it))) results.push_back(result);
  }
  keyword_lock.unlock();

  Rule_lock_helper command_lock(&m_command_lock, false);
  assert(command_lock.effect());
  if (m_command_rule) {
    if ((result = get_ccl_result(mem_root, m_command_rule)))
      results.push_back(result);
  }
  command_lock.unlock();
}
/**
  Comply the queue rules.

  @param[in]    hash          hash value
  @param[out]   version       the matched slot version

  @retval       slot
*/
Ccl_slot *Ccl_rule_set::comply_queue(ulonglong hash, ulonglong *version) {
  DBUG_ENTER("Ccl_rule_set::comply_queue");
  if (m_queue_size.load() > 0) {
    Rule_lock_helper queue_lock(&m_queue_lock, false);
    assert(queue_lock.effect());
    assert(m_queue_size.load() == m_queue_buckets.size());
    Ccl_rule *rule = m_queue_buckets[hash % m_queue_size.load()];
    if (rule) DBUG_RETURN(rule->retrieve_slot(version));
  }
  DBUG_RETURN(nullptr);
}
/**
  Comply the rules, the matching order is : intact rule, keyword rule,
  command rule.

  @param[in]    all_tables    Table list
  @param[in]    query         Statement string
  @param[out]   version       the matched slot version

  @retval       slot
*/
Ccl_slot *Ccl_rule_set::comply_rule(Table_ref *all_tables,
                                    const LEX_CSTRING &query,
                                    ulonglong *version) {
  /* 1. check the intact rules */
  if (all_tables && m_intact_size.load() > 0) {
    Rule_lock_helper intact_lock(&m_intact_lock, false);
    assert(intact_lock.effect());
    Table_ref *table;
    for (table = all_tables; table; table = table->next_global) {
      typename Intact_object_rules::const_iterator it;
      it = m_intact_rules.find(std::pair<String_ccl, String_ccl>(
          String_ccl(table->db), String_ccl(table->table_name)));

      if (it == m_intact_rules.end())
        continue;
      else {
        Ccl_rule *rule = const_cast<Ccl_rule *>(it->second);
        if (rule->keyword_is_null() || rule->match_keyword(query)) {
          return rule->retrieve_slot(version);
        }
      }
    }
  }

  /* 2. check the keyword rules. */

  if (m_keyword_size.load() > 0) {
    Rule_lock_helper keyword_lock(&m_keyword_lock, false);
    assert(keyword_lock.effect());

    for (Keyword_rules::const_iterator it = m_keyword_rules.cbegin();
         it != m_keyword_rules.cend(); it++) {
      Ccl_rule *rule = *it;

      if (!rule->keyword_is_null() && rule->match_keyword(query))
        return rule->retrieve_slot(version);
    }
  }

  /* 3. check the command rules. */

  if (m_command_size.load() == 1) {
    Rule_lock_helper command_lock(&m_command_lock, false);
    assert(command_lock.effect());
    if (m_command_rule) return m_command_rule->retrieve_slot(version);
  }

  /* not match */
  return nullptr;
}
/**
  Collect all active rules.

  @param[in]      mem_root      memory pool
  @param[out]     results       rule result container
*/
void System_ccl::collect_rules(MEM_ROOT *mem_root,
                               std::vector<Ccl_show_result *> &results) {
  m_select_rule_set.collect_rules(mem_root, results);
  m_update_rule_set.collect_rules(mem_root, results);
  m_insert_rule_set.collect_rules(mem_root, results);
  m_delete_rule_set.collect_rules(mem_root, results);
}

/**
  Collect all active queue buckets.

  @param[in]      mem_root      memory pool
  @param[out]     results       rule result container
*/
void System_ccl::collect_queue_buckets(
    MEM_ROOT *mem_root, std::vector<Ccl_show_result *> &results) {
  m_queue_bucket_set.collect_queue_buckets(mem_root, results);
}

/* Clear all rules */
void System_ccl::clear_rules() {
  m_select_rule_set.clear_rules();
  m_update_rule_set.clear_rules();
  m_insert_rule_set.clear_rules();
  m_delete_rule_set.clear_rules();
}
/**
  Refresh the rules.
  Report the error message by level.

  @param[in]      group           rule group
  @param[in]      type            rule type
  @param[in]      force_clearn    whether clear cache
  @param[in]      level           error level

  @retval         added rule count
*/
size_t System_ccl::refresh_rules(Ccl_record_group &group, Ccl_rule_type type,
                                 bool force_clean, Ccl_error_level level) {
  DBUG_ENTER("System_ccl::refresh_rules");
  DBUG_RETURN(get_rule_set(type)->refresh_rules(group, force_clean, level));
}

/**
  Delete the rule by rule id.

  @param[in]      rule_id         rule id

  @retval         affected rule count
*/
size_t System_ccl::delete_rule(ulonglong rule_id) {
  DBUG_ENTER("System_ccl::delete_rule");
  DBUG_RETURN(m_select_rule_set.delete_rule(rule_id) +
              m_update_rule_set.delete_rule(rule_id) +
              m_insert_rule_set.delete_rule(rule_id) +
              m_delete_rule_set.delete_rule(rule_id));
}
/**
  Constructor of Ccl_slot.
*/
Ccl_slot::Ccl_slot() {
  mysql_mutex_init(key_LOCK_ccl_slot, &m_mutex, MY_MUTEX_INIT_FAST);
  mysql_cond_init(key_COND_ccl_slot, &m_cond);

  m_concurrency_count = 0;
  m_running = 0;
  m_waiting = 0;
  m_version = 0;
  m_state.store(false);
}

/**
  Destructor of Ccl_slot.
*/
Ccl_slot::~Ccl_slot() {
  DBUG_ENTER("Ccl_slot::~Ccl_slot");
  assert(m_running == 0 && m_waiting == 0);
  assert(m_state.load() == false);
  mysql_mutex_destroy(&m_mutex);
  mysql_cond_destroy(&m_cond);
  DBUG_VOID_RETURN;
}
void Ccl_slot::lock() { mysql_mutex_lock(&m_mutex); }
void Ccl_slot::unlock() { mysql_mutex_unlock(&m_mutex); }
/**
  Acquire available slot;

  @param[in]  version       Unique version
  @param[in]  cc_count      Concurrency count

  @retval     true          Success
  @retval     false         Failure
*/
bool Ccl_slot::acquire(ulonglong version, ulonglong cc_count) {
  bool old_value = false;
  bool new_value = true;
  bool success;
  DBUG_ENTER("Ccl_slot::acquire");
  if ((success =
           atomic_compare_exchange_strong(&m_state, &old_value, new_value))) {
    lock();
    m_version = version;
    set_concurrency(cc_count);
    /**
      Maybe some waiters still wait last rule that has been destroyed,
      and this slot is being assigned new rule.
    */
    mysql_cond_broadcast(&m_cond);
    unlock();
  }
  DBUG_RETURN(success);
}

/**
  Enter the condition.

  @param[in]    version     the thread local version.
*/
void Ccl_slot::enter_cond(THD *thd, ulonglong version) {
  int wait_result = 0;
  struct timespec abs_timeout;
  ulonglong timeout_cnt = 0;
  DBUG_ENTER("Ccl_slot::enter_cond");
  ulonglong max_times = ccl_wait_timeout / CCL_RETRY_INTERVAL;
  const char *save_proc_info = thd->proc_info();
  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();

  thd_manager->dec_thread_running();
  thd_proc_info(thd, "Concurrency control waiting");
  lock();

  m_running++;
  /* Listen to the wakeup by other thread */
  thd_enter_cond(thd, &m_cond, &m_mutex, NULL, NULL, __func__, __FILE__,
                 __LINE__);
  thd_wait_begin(thd, THD_WAIT_SLEEP);

  if (m_concurrency_count == 0) {
    my_error(ER_CCL_REFUSE_QUERY, MYF(0));
    goto end;
  }

  if (ccl_max_waiting_count != 0 && m_waiting >= ccl_max_waiting_count) {
    my_error(ER_CCL_MAX_WAITING_COUNT, MYF(0));
    goto end;
  }
  /**
    Wait requirement:
    1) retry times is not more than ccl_wait_timeout.
    2) slot is active.
    3) version is equal thread local version. it demostrate rule is not changed.
    4) running thread is more than concurrency control limit.
    5) thd is not killed.
  */
  while (timeout_cnt < max_times && m_state.load() == true &&
         version == m_version && m_running > m_concurrency_count &&
         !thd->is_killed()) {
    /* Decrease the running and increase the waiting */
    m_running--;
    m_waiting++;
    set_timespec(&abs_timeout, 5);
    wait_result = mysql_cond_timedwait(&m_cond, &m_mutex, &abs_timeout);
    /* Increase the timeout times */
    if (is_timeout(wait_result)) timeout_cnt++;
    m_waiting--;
    m_running++;
  }

end:
  thd_wait_end(thd);

  unlock();
  thd_exit_cond(thd, NULL, __func__, __FILE__, __LINE__);

  thd_proc_info(thd, save_proc_info);
  thd_manager->inc_thread_running();

  DBUG_EXECUTE_IF("cond_wait_sleep_10", {
    DBUG_SET("-d, cond_wait_sleep_10");
    my_sleep(10 * 1000 * 1000);
  });

  DBUG_VOID_RETURN;
}
/**
  Exit the condition.
*/
void Ccl_slot::exit_cond() {
  DBUG_ENTER("Ccl_slot::exit_cond");
  lock();
  m_running--;
  mysql_cond_broadcast(&m_cond);
  unlock();
  DBUG_VOID_RETURN;
}

/**
  Release the slot;
*/
void Ccl_slot::release() {
  bool old_value = true;
  bool new_value = false;
  bool success;
  DBUG_ENTER("Ccl_slot::release");
  if ((success =
           atomic_compare_exchange_strong(&m_state, &old_value, new_value))) {
    lock();
    reset_concurrency();
    /**
       Wakeup the waiters mainly release will reset
       concurrency count condition
    */
    mysql_cond_broadcast(&m_cond);
    unlock();
  }
  DBUG_VOID_RETURN;
}

} /* namespace im */
