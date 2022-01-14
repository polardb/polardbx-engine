/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/Apsara GalaxyStore hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/Apsara GalaxyStore.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/**
  @file

  Implementation of SEQUENCE shared structure or function.
*/

#include "mysql/psi/mysql_memory.h"
#include "mysql_com.h"                       // NOT NULL
#include "mysqld_error.h"                    // ER_SEQUENCE_INVALID
#include "sql/create_field.h"                // Create_field
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_routine.h"               // dd routine methods.
#include "sql/dd/impl/types/object_table_definition_impl.h"
#include "sql/dd/types/function.h"  // dd::Function
#include "sql/field.h"              // Create_field
#include "sql/handler.h"            // DB_TYPE_SEQUENCE_DB
#include "sql/parse_tree_items.h"   // PTI_xxx items
#include "sql/sp_head.h"            // sp_head
#include "sql/sql_alter.h"          // Alter_info
#include "sql/sql_lex.h"            // TL_OPTION_SEQUENCE
#include "sql/sql_plugin.h"         // plugin_unlock
#include "sql/sql_thd_internal_api.h"
#include "sql/table.h"  // bitmap

#include "sql/sequence_common.h"

/**
  @addtogroup Sequence Engine

  Sequence Engine shared structure or function implementation.

  @{
*/

PSI_memory_key key_memory_sequence_last_value;
PSI_thread_key key_thread_sequence;

st_sequence_field_info seq_fields[] = {
    {"currval",
     "21",
     Sequence_field::FIELD_NUM_CURRVAL,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("current value")}},
    {"nextval",
     "21",
     Sequence_field::FIELD_NUM_NEXTVAL,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("next value")}},
    {"minvalue",
     "21",
     Sequence_field::FIELD_NUM_MINVALUE,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("min value")}},
    {"maxvalue",
     "21",
     Sequence_field::FIELD_NUM_MAXVALUE,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("max value")}},
    {"start",
     "21",
     Sequence_field::FIELD_NUM_START,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("start value")}},
    {"increment",
     "21",
     Sequence_field::FIELD_NUM_INCREMENT,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("increment value")}},
    {"cache",
     "21",
     Sequence_field::FIELD_NUM_CACHE,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("cache size")}},
    {"cycle",
     "21",
     Sequence_field::FIELD_NUM_CYCLE,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("cycle state")}},
    {"round",
     "21",
     Sequence_field::FIELD_NUM_ROUND,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("already how many round")}},
    {NULL,
     NULL,
     Sequence_field::FIELD_NUM_END,
     MYSQL_TYPE_LONGLONG,
     {C_STRING_WITH_LEN("")}}};
/**
   Global variables for sequence engine and sequence base engine, in order to
   get the engine plugin through these engine name.
*/
const LEX_CSTRING SEQUENCE_ENGINE_NAME = {C_STRING_WITH_LEN("Sequence")};
const LEX_CSTRING SEQUENCE_BASE_ENGINE_NAME = {C_STRING_WITH_LEN("InnoDB")};

/**
  Resolve the sequence engine plugin.

  @param[in]    thd           user connection

  @retval       plugin_ref    sequence engine plugin.
*/
plugin_ref ha_resolve_sequence(const THD *thd) {
  return ha_resolve_by_name(const_cast<THD *>(thd), &SEQUENCE_ENGINE_NAME,
                            false);
}

/**
  Resolve the sequence base engine plugin.

  @param[in]    thd           user connection

  @retval       plugin_ref    sequence base engine plugin.
*/
plugin_ref ha_resolve_sequence_base(const THD *thd) {
  return ha_resolve_by_name(const_cast<THD *>(thd), &SEQUENCE_BASE_ENGINE_NAME,
                            false);
}

/**
  Assign initial default values of sequence fields

  @retval   void
*/
void Sequence_info::init_default() {
  DBUG_ENTER("Sequence_info::init_default");
  values[FIELD_NUM_CURRVAL] = 0;
  values[FIELD_NUM_NEXTVAL] = 0;
  values[FIELD_NUM_MINVALUE] = 1;
  values[FIELD_NUM_MAXVALUE] = ULLONG_MAX;
  values[FIELD_NUM_START] = 1;
  values[FIELD_NUM_INCREMENT] = 1;
  values[FIELD_NUM_CACHE] = DEF_CACHE_DIGITAL;
  values[FIELD_NUM_CYCLE] = 0;
  values[FIELD_NUM_ROUND] = 0;

  base_db_type = NULL;
  db = NULL;
  table_name = NULL;

  type = Sequence_type::DIGITAL;
  flags = 0;
  DBUG_VOID_RETURN;
}

/**
  Assign the initial values for all sequence fields
*/
Sequence_info::Sequence_info() { init_default(); }

/**
  Sequence field setting function

  @param[in]    field_num   Sequence field number
  @param[in]    value       Sequence field value

  @retval       void
*/
void Sequence_info::init_value(const Fields field_num, const ulonglong value) {
  DBUG_ENTER("Sequence_info::init_value");
  values[field_num] = value;
  flags |= 1 << field_num;
  DBUG_VOID_RETURN;
}

/**
  Validate sequence values
  Require:
    1. max value >= min value
    2. start >= min value
    3. increment >= 1
    4. max value >= start

  @param[in]    item    field value

  @retval       false   valid
  @retval       true    invalid
*/
bool check_sequence_values_valid(Sequence_type type, const ulonglong *items) {
  DBUG_ENTER("check_sequence_values_valid");
  if (type == Sequence_type::DIGITAL) {
    if (items[Sequence_field::FIELD_NUM_MAXVALUE] >=
            items[Sequence_field::FIELD_NUM_MINVALUE] &&
        items[Sequence_field::FIELD_NUM_START] >=
            items[Sequence_field::FIELD_NUM_MINVALUE] &&
        items[Sequence_field::FIELD_NUM_INCREMENT] >= 1 &&
        items[Sequence_field::FIELD_NUM_MAXVALUE] >=
            items[Sequence_field::FIELD_NUM_START])
      DBUG_RETURN(false);
  } else {
    /** Cache must be between 1s and 300s */
    if (items[Sequence_field::FIELD_NUM_CACHE] >=
            Sequence_info::MIN_CACHE_TIMESTAMP &&
        items[Sequence_field::FIELD_NUM_CACHE] <=
            Sequence_info::MAX_CACHE_TIMESTAMP)
      DBUG_RETURN(false);
  }

  DBUG_RETURN(true);
}

/**
  Only cache size is valid for timestamp squence, and
  increment size 0 means that it's a timestamp sequence.
*/
void overwrite_timestamp_sequence_values(ulonglong *values) {
  DBUG_ENTER("overwrite_timestamp_sequence_values");

  values[Sequence_field::FIELD_NUM_CURRVAL] = 0;
  values[Sequence_field::FIELD_NUM_NEXTVAL] = 0;
  values[Sequence_field::FIELD_NUM_MINVALUE] = 1;
  values[Sequence_field::FIELD_NUM_MAXVALUE] = ULLONG_MAX;
  values[Sequence_field::FIELD_NUM_START] = 1;
  values[Sequence_field::FIELD_NUM_INCREMENT] = 0;
  values[Sequence_field::FIELD_NUM_CYCLE] = 0;
  values[Sequence_field::FIELD_NUM_ROUND] = 0;

  DBUG_VOID_RETURN;
}

Sequence_type convert_sequence_type_by_increment(ulonglong value) {
  if (value == 0) {
    return Sequence_type::TIMESTAMP;
  } else {
    return Sequence_type::DIGITAL;
  }
}

void Sequence_info::init_type(Types value) { type = value; }

/** Assign default value to fields that are meaningless for timestamp sequence
 */
void Sequence_info::fix_timestamp() {
  DBUG_ENTER("Sequence_info::fix_timestamp");

  assert(type == Sequence_type::TIMESTAMP);

  overwrite_timestamp_sequence_values(values);

  if (!(flags & F_CACHE)) {
    values[Sequence_field::FIELD_NUM_CACHE] = DEF_CACHE_TIMESTAMP;
  }

  DBUG_VOID_RETURN;
}

/*
  Check whether inited values are valid through
  syntax:
    CREATE SEQUENCE [IF NOT EXISTS] schema.seqName
     [START WITH <constant>]
     [MINVALUE <constant>]
     [MAXVALUE <constant>]
     [INCREMENT BY <constant>]
     [CACHE <constant> | NOCACHE]
     [CYCLE | NOCYCLE]
    ;

  @retval       true        Invalid
  @retval       false       valid
*/
bool Sequence_info::check_valid() {
  DBUG_ENTER("Sequence_info::check_valid");

  if (type == TIMESTAMP) {
    fix_timestamp();
  }

  if (check_sequence_values_valid(type, values)) {
    my_error(ER_SEQUENCE_INVALID, MYF(0), db, table_name);
    DBUG_RETURN(true);
  }
  DBUG_RETURN(false);
}

/**
  Sequence field getting function

  @param[in]    field_num   Sequence field number

  @retval       ulonglong   Sequence field value
*/
ulonglong Sequence_info::get_value(const Fields field_num) const {
  DBUG_ENTER("Sequence_info::get_value");
  assert(field_num < FIELD_NUM_END);
  DBUG_RETURN(values[field_num]);
}

/**
  Release the ref count if locked
*/
Sequence_property::~Sequence_property() {
  if (m_plugin) plugin_unlock(NULL, m_plugin);
}

/**
  Configure the sequence flags and base db_type when open_table_share.

  @param[in]    plugin      Storage engine plugin
*/
void Sequence_property::configure(plugin_ref plugin) {
  handlerton *hton;
  if (plugin && ((hton = plugin_data<handlerton *>(plugin))) &&
      hton->db_type == DB_TYPE_SEQUENCE_DB) {
    if ((m_plugin = ha_resolve_sequence_base(NULL))) {
      base_db_type = plugin_data<handlerton *>(m_plugin);
      m_sequence = true;
    }
  }
}
/**
  Judge the sequence iteration type according to the query string.

  @param[in]    table         TABLE object

  @retval       iteration mode
*/
Sequence_iter_mode sequence_iteration_type(TABLE *table) {
  DBUG_ENTER("sequence_iteration_type");
  if (bitmap_is_set(table->read_set, Sequence_field::FIELD_NUM_NEXTVAL))
    DBUG_RETURN(Sequence_iter_mode::IT_NEXTVAL);

  DBUG_RETURN(Sequence_iter_mode::IT_NON_NEXTVAL);
}

/**
  Check the sequence table fields validation

  @param[in]    alter info    The alter information

  @retval       true          Failure
  @retval       false         Success

*/
bool check_sequence_fields_valid(Alter_info *alter_info) {
  Create_field *field;
  List_iterator<Create_field> it(alter_info->create_list);
  size_t field_count;
  size_t field_no;
  DBUG_ENTER("check_sequence_fields_valid");

  field_count = alter_info->create_list.elements;
  field_no = 0;
  if (field_count != Sequence_field::FIELD_NUM_END ||
      alter_info->key_list.size() > 0)
    DBUG_RETURN(true);

  while ((field = it++)) {
    if (my_strcasecmp(system_charset_info, seq_fields[field_no].field_name,
                      field->field_name) ||
        (field->flags != (NOT_NULL_FLAG | NO_DEFAULT_VALUE_FLAG)) ||
        (field->sql_type != seq_fields[field_no].field_type))
      DBUG_RETURN(true);

    field_no++;
  }
  DBUG_RETURN(false);
}

/**
  Whether it is SEQUENCE object creation;

  @retval   true    CREATE SEQUENCE  or
                    CREATE TABLE ...sequence engine
  @retval   false   Not
*/
bool sequence_create_info(HA_CREATE_INFO *create_info) {
  /* CREATE SEQUENCE syntax */
  if (create_info->sequence_info) return true;

  /* CREATE TABLE ... ENGINE = SEQUENCE syntax */
  if (create_info->db_type &&
      create_info->db_type->db_type == DB_TYPE_SEQUENCE_DB)
    return true;

  return false;
}

/**
  Builder for 'NEXTVAL'.
*/
class Create_next_func : public Create_func {
 public:
  static Create_next_func s_singleton;

  Item *create_func(THD *thd, LEX_STRING name MY_ATTRIBUTE((unused)),
                    PT_item_list *item_list) override {
    return new (thd->mem_root) Item_func_nextval(POS(), thd, item_list);
  }

 protected:
  /* Single instance */
  Create_next_func() {}
  ~Create_next_func() override {}
};

/**
  Builder for 'CURRTVAL'.
*/
class Create_curr_func : public Create_func {
 public:
  static Create_curr_func s_singleton;

  Item *create_func(THD *thd, LEX_STRING name MY_ATTRIBUTE((unused)),
                    PT_item_list *item_list) override {
    return new (thd->mem_root) Item_func_currval(POS(), thd, item_list);
  }

 protected:
  /* Single instance */
  Create_curr_func() {}
  ~Create_curr_func() override {}
};

/**
  Builder for 'NEXTVAL_SKIP'.
*/
class Create_next_skip_func : public Create_func {
 public:
  static Create_next_skip_func s_singleton;

  Item *create_func(THD *thd, LEX_STRING name MY_ATTRIBUTE((unused)),
                    PT_item_list *item_list) override {
    return new (thd->mem_root) Item_func_nextval_skip(POS(), thd, item_list);
  }

 protected:
  /* Single instance */
  Create_next_skip_func() {}
  ~Create_next_skip_func() override {}
};

/**
  Builder for 'NEXTVAL_SHOW'.
*/
class Create_next_show_func : public Create_func {
 public:
  static Create_next_show_func s_singleton;

  Item *create_func(THD *thd, LEX_STRING name MY_ATTRIBUTE((unused)),
                    PT_item_list *item_list) override {
    return new (thd->mem_root) Item_func_nextval_show(POS(), thd, item_list);
  }

 protected:
  /* Single instance */
  Create_next_show_func() {}
  ~Create_next_show_func() override {}
};

Create_next_func Create_next_func::s_singleton;
Create_curr_func Create_curr_func::s_singleton;
Create_next_skip_func Create_next_skip_func::s_singleton;
Create_next_show_func Create_next_show_func::s_singleton;

/**
  Return the item builder of 'NEXTVAL' and 'CURRVAL'

  @retval   Create_func   Builder
  @retval   NULL          Not surpported function
*/
Create_func *find_sequence_function_builder(const LEX_STRING &f_name) {
  if (is_seq_nextval_func_name(f_name.str))
    return &Create_next_func::s_singleton;
  else if (is_seq_currval_func_name(f_name.str))
    return &Create_curr_func::s_singleton;
  else if (is_seq_nextval_skip_func_name(f_name.str))
    return &Create_next_skip_func::s_singleton;
  else if (is_seq_nextval_show_func_name(f_name.str))
    return &Create_next_show_func::s_singleton;
  else
    return nullptr;
}

/** Stores sequence sp status for every schema */
using db_sp_set = malloc_unordered_set<dd::String_type>;

static db_sp_set *user_seq_sp_set = nullptr;
static mysql_rwlock_t user_seq_sp_set_lock;
static PSI_rwlock_key user_seq_sp_set_lock_key;
static PSI_memory_key user_seq_sp_set_mem_key;

static const dd::String_type nextval_name = "nextval";
static const dd::String_type currval_name = "currval";

static const dd::String_type nextval_skip_name = "nextval_skip";
static const dd::String_type nextval_show_name = "nextval_show";

extern bool opt_initialize;

/**
  Helper class and functions.
*/
struct AutoTHD {
  THD *m_thd;
  AutoTHD() { m_thd = create_thd(false, true, true, key_thread_sequence, 0); }
  ~AutoTHD() { destroy_thd(m_thd); }
};

class AutoTicket {
  THD *m_thd;
  MDL_ticket *m_ticket;

 public:
  AutoTicket(THD *thd, MDL_ticket *ticket) : m_thd(thd), m_ticket(ticket) {}
  ~AutoTicket() { m_thd->mdl_context.release_lock(m_ticket); }
};

class AutoLock {
  mysql_rwlock_t *m_lock;

 public:
  AutoLock(mysql_rwlock_t *lock, bool wr) : m_lock(lock) {
    if (wr)
      mysql_rwlock_wrlock(m_lock);
    else
      mysql_rwlock_rdlock(m_lock);
  }
  ~AutoLock() { mysql_rwlock_unlock(m_lock); }
};

static bool load_sp(THD *thd, const dd::String_type &db,
                    const dd::String_type &name) {
  /* First we should acquire MDL */
  MDL_key mdl_key;
  dd::Function::create_mdl_key(db, name, &mdl_key);

  MDL_request request;
  MDL_REQUEST_INIT_BY_KEY(&request, &mdl_key, MDL_SHARED, MDL_EXPLICIT);

  if (thd->mdl_context.acquire_lock(&request, thd->variables.lock_wait_timeout))
    return true;

  assert(request.ticket);
  AutoTicket ticket(thd, request.ticket);

  /* Acquire function from DD cache */
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
  const dd::Function *sr = nullptr;

  if (thd->dd_client()->acquire<dd::Function>(db, name, &sr)) return true;

  /* Update sp status in set */
  if (sr) {
    /* Both db and name are normalized already */
    dd::String_type db_sp_name(db);
    db_sp_name.append(name);
    user_seq_sp_set->insert(db_sp_name);
  }

  return false;
}

/**
  Init the existence of sequence SP.

  @retval   true   Internal error
  @retval   false  Success
*/
bool user_seq_sp_init() {
  if (opt_initialize) return false;

#ifdef HAVE_PSI_INTERFACE
  const char *category = "sequence";

  PSI_rwlock_info lock_info[] = {{&user_seq_sp_set_lock_key,
                                  "LOCK_user_seq_sp_cache", 0, 0,
                                  PSI_DOCUMENT_ME}};
  mysql_rwlock_register(category, lock_info, 1);

  PSI_memory_info memory_info[] = {{&user_seq_sp_set_mem_key,
                                    "user_seq_sp_cache_memory", 0, 0,
                                    PSI_DOCUMENT_ME}};
  mysql_memory_register(category, memory_info, 1);
#endif

  mysql_rwlock_init(user_seq_sp_set_lock_key, &user_seq_sp_set_lock);

  void *ptr = my_malloc(user_seq_sp_set_mem_key, sizeof(db_sp_set),
                        MYF(MY_WME | ME_FATALERROR));
  user_seq_sp_set = new (ptr) db_sp_set(user_seq_sp_set_mem_key);

  /** Load all user defined sequence sp */
  AutoTHD auto_thd;
  THD *thd = auto_thd.m_thd;
  dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());

  std::vector<const dd::Schema *> schemas;
  if (thd->dd_client()->fetch_global_components(&schemas)) return true;

  for (const dd::Schema *schema : schemas) {
    char schema_name_buf[NAME_LEN + 1];
    const char *db = dd::Object_table_definition_impl::fs_name_case(
        schema->name(), schema_name_buf);

    if (load_sp(thd, db, nextval_name)) return true;
    if (load_sp(thd, db, currval_name)) return true;
  }

  return false;
}

/**
  De-init the existence of sequence SP.
*/
void user_seq_sp_exit() {
  if (!user_seq_sp_set) return;
  mysql_rwlock_destroy(&user_seq_sp_set_lock);
}

/**
  Check whether the sp with the given name exist.

  @retval   true   Exist
  @retval   false  Not exist
*/
bool user_seq_sp_exist(THD *thd, const LEX_STRING &f_name) {
  assert(user_seq_sp_set);
  if (!user_seq_sp_set) return false;

  const char *db = nullptr;
  size_t db_len = 0;

  if (thd->lex->sphead) {
    /* In case invoked in a sp */
    db = thd->lex->sphead->m_db.str;
    db_len = thd->lex->sphead->m_db.length;
  } else if (thd->db().str) {
    /* Session has connected to a shema */
    db = thd->db().str;
    db_len = thd->db().length;
  } else {
    /* if no schema connected, treat it as nonexistent */
    return false;
  }

  assert(db);
  assert(is_seq_func_name(f_name.str));

  dd::String_type key;
  key.reserve(db_len + 8);
  key.append(db, db_len);
  if (is_seq_nextval_func_name(f_name.str))
    key.append(nextval_name);
  else
    key.append(currval_name);

  bool exist = false;

  {
    AutoLock lock(&user_seq_sp_set_lock, false);
    exist = user_seq_sp_set->count(key);
  }

  return exist;
}

/**
  Trace changes of procedures, update the existence of sequence SP.

  @param[in]    db          database
  @param[in]    name        sp name, null means drop database
  @param[in]    is_added    true  - newly added
                            false - dropped
*/
void on_user_sp_status_changed(const char *db, const char *name,
                               bool is_added) {
  /* In case upgrade */
  if (!user_seq_sp_set) return;

  assert(db);

  if (name) {
    dd::String_type key;
    key.reserve(NAME_LEN);
    key.append(db);
    if (is_seq_nextval_func_name(name))
      key.append(nextval_name);
    else
      key.append(currval_name);

    {
      AutoLock lock(&user_seq_sp_set_lock, true);
      if (is_added)
        user_seq_sp_set->insert(key);
      else
        user_seq_sp_set->erase(key);
    }
  } else {
    /* In case drop database */
    assert(!is_added);

    dd::String_type key_next;
    key_next.reserve(NAME_LEN);
    key_next.append(db);
    key_next.append(nextval_name);

    dd::String_type key_curr;
    key_curr.reserve(NAME_LEN);
    key_curr.append(db);
    key_curr.append(currval_name);

    {
      AutoLock lock(&user_seq_sp_set_lock, true);
      user_seq_sp_set->erase(key_next);
      user_seq_sp_set->erase(key_curr);
    }
  }
}

bool is_seq_func_name(const char *name) {
  return is_seq_nextval_func_name(name) || is_seq_currval_func_name(name) ||
         is_seq_nextval_skip_func_name(name) ||
         is_seq_nextval_show_func_name(name);
}

bool is_seq_nextval_func_name(const char *name) {
  return !my_strcasecmp(system_charset_info, name, nextval_name.c_str());
}

bool is_seq_nextval_skip_func_name(const char *name) {
  return !my_strcasecmp(system_charset_info, name, nextval_skip_name.c_str());
}

bool is_seq_nextval_show_func_name(const char *name) {
  return !my_strcasecmp(system_charset_info, name, nextval_show_name.c_str());
}

bool is_seq_currval_func_name(const char *name) {
  return !my_strcasecmp(system_charset_info, name, currval_name.c_str());
}

/** Returns the number of microseconds since 1970/1/1 00:00:00.
 Uses the system clock.
 @return us since epoch or 0 if failed to retrieve */
ulonglong time_system_ms(void) {
  const auto now = std::chrono::system_clock::now();
  const auto ret = std::chrono::duration_cast<std::chrono::milliseconds>(
                       now.time_since_epoch())
                       .count();
  return (ret);
}

/// @} (end of group Sequence Engine)
