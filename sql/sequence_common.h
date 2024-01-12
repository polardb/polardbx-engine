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

#ifndef SEQUENCE_COMMON_INCLUDED
#define SEQUENCE_COMMON_INCLUDED

#if 0
#include "binary_log_types.h"  // MYSQL_TYPE_LONGLONG
#endif
#include "map_helpers.h"         // collation_unordered_map
#include "sql/psi_memory_key.h"  // PSI_memory_key
#include "sql/sql_plugin_ref.h"  // plugin_ref

struct handlerton;
class THD;
struct TABLE;
class Alter_info;
struct HA_CREATE_INFO;
class Create_func;

extern PSI_memory_key key_memory_sequence_last_value;

#define SEQUENCE_DEFAULT_BATCH_SIZE (1)
#define SEQUENCE_MAX_BATCH_SIZE (100 * 10000)

/**
  Sequence table field value structure.
*/
struct st_sequence_value {
  ulonglong currval;
  ulonglong nextval;
  ulonglong minvalue;
  ulonglong maxvalue;
  ulonglong start;
  ulonglong increment;
  ulonglong cache;
  ulonglong cycle;
  ulonglong round;
};

/**
  Sequence create information.
*/
class Sequence_info {
 public:
  /* All the sequence fields.*/
  enum Fields {
    FIELD_NUM_CURRVAL = 0,
    FIELD_NUM_NEXTVAL,
    FIELD_NUM_MINVALUE,
    FIELD_NUM_MAXVALUE,
    FIELD_NUM_START,
    FIELD_NUM_INCREMENT,
    FIELD_NUM_CACHE,
    FIELD_NUM_CYCLE,
    FIELD_NUM_ROUND,
    /* This must be last! */
    FIELD_NUM_END
  };

  static constexpr const ulonglong F_CURRVAL = 1L << 0;
  static constexpr const ulonglong F_NEXTVAL = 1L << 1;
  static constexpr const ulonglong F_MINVALUE = 1L << 2;
  static constexpr const ulonglong F_MAXVALUE = 1L << 3;
  static constexpr const ulonglong F_START = 1L << 4;
  static constexpr const ulonglong F_INCREMENT = 1L << 5;
  static constexpr const ulonglong F_CACHE = 1L << 6;
  static constexpr const ulonglong F_CYCLE = 1L << 7;
  static constexpr const ulonglong F_ROUND = 1L << 8;
  static constexpr const ulonglong F_END = 1L << 9;

  static_assert(F_END == (1L << FIELD_NUM_END), "");

  /** Default value for digital sequence cache size */
  static constexpr const ulonglong DEF_CACHE_DIGITAL = 10000;

  /** Default value for timestamp sequence cache size (seconds) */
  static constexpr const ulonglong DEF_CACHE_TIMESTAMP = 10;

  /** Min value for timestamp sequence cache size (seconds) */
  static constexpr const ulonglong MIN_CACHE_TIMESTAMP = 1;

  /** Max value for timestamp sequence cache size (seconds) */
  static constexpr const ulonglong MAX_CACHE_TIMESTAMP = 300;

  /** Sequence type: digital or timestamp */
  enum Types { DIGITAL = 0, TIMESTAMP };

  /**
    Construtor and Destrutor
  */
  Sequence_info();
  virtual ~Sequence_info() {}

  /* Disable the copy and assign function */
  Sequence_info(const Sequence_info &) = delete;
  Sequence_info(const Sequence_info &&) = delete;
  Sequence_info &operator=(const Sequence_info &) = delete;

  /**
    Sequence field setting function

    @param[in]    field_num   Sequence field number
    @param[in]    value       Sequence field value

    @retval       void
  */
  void init_value(const Fields field_num, const ulonglong value);

  void init_type(Types value);

  void fix_timestamp();

  /**
    Sequence field getting function

    @param[in]    field_num   Sequence field number

    @retval       ulonglong   Sequence field value
  */
  ulonglong get_value(const Fields field_num) const;

  /*
    Check whether inited values are valid through
      syntax: 'CREATE SEQUENCE ...'

    @retval       true        Invalid
    @retval       false       valid
  */
  bool check_valid();

  const char *db;
  const char *table_name;
  handlerton *base_db_type; /** Sequence table engine */
 private:
  /**
    Assign initial default values of sequence fields

    @retval   void
  */
  void init_default();

  ulonglong values[Fields::FIELD_NUM_END];

  ulonglong flags;

  enum Types type;
};

typedef Sequence_info::Fields Sequence_field;
typedef Sequence_info::Types Sequence_type;

/**
  Sequence table fields definition.
*/
struct st_sequence_field_info {
  const char *field_name;
  const char *field_length;
  const Sequence_field field_num;
  const enum enum_field_types field_type;
  const LEX_CSTRING comment;
};

/**
  The sequence value structure should be consistent with Sequence field
  definition
*/
static_assert(sizeof(ulonglong) * Sequence_field::FIELD_NUM_END ==
                  sizeof(struct st_sequence_value),
              "");

/**
  Sequence attributes within TABLE_SHARE object, label the table as sequence
  table.
*/
class Sequence_property {
 public:
  Sequence_property() : m_sequence(false), base_db_type(NULL), m_plugin(NULL) {}

  ~Sequence_property();

  /* Disable these copy and assign functions */
  Sequence_property(const Sequence_property &) = delete;
  Sequence_property(const Sequence_property &&) = delete;
  Sequence_property &operator=(const Sequence_property &) = delete;

  /**
    Configure the sequence flags and base db_type when open_table_share.

    @param[in]    plugin      Storage engine plugin
  */
  void configure(plugin_ref plugin);
  bool is_sequence() { return m_sequence; }
  handlerton *db_type() { return base_db_type; }

 private:
  bool m_sequence;
  handlerton *base_db_type;
  plugin_ref m_plugin;
};

/*
 *  Support some operation for sequence
 *  1. nextval_show(sequence)
 */
struct Sequence_operation {
 public:
  enum Operation_type { OPERATION_NONE, OPERATION_SHOW_CACHE };

  Sequence_operation() : m_type(OPERATION_NONE) {}

  Sequence_operation(const Sequence_operation &operation) {
    m_type = operation.m_type;
  }

  Sequence_operation &operator=(const Sequence_operation &operation) {
    m_type = operation.m_type;
    return *this;
  }

  void set_type(Operation_type type) { m_type = type; }

  void reset() { m_type = OPERATION_NONE; }

  bool is_show_cache() { return m_type == OPERATION_SHOW_CACHE; }

 private:
  Operation_type m_type;
};

/** Support nextval_skip(sequence, value) */
struct Sequence_skip {
 public:
  Sequence_skip(bool skip, ulonglong value)
      : m_skip(skip), m_skipped_to(value) {}
  Sequence_skip() : m_skip(false), m_skipped_to(0) {}

  Sequence_skip(const Sequence_skip &skip) {
    m_skip = skip.m_skip;
    m_skipped_to = skip.m_skipped_to;
  }

  Sequence_skip &operator=(const Sequence_skip &skip) {
    m_skip = skip.m_skip;
    m_skipped_to = skip.m_skipped_to;
    return *this;
  }

  void reset() {
    m_skip = false;
    m_skipped_to = 0;
  }

  void init(ulonglong value) {
    m_skip = true;
    m_skipped_to = value;
  }

  bool is_skip() { return m_skip; }
  ulonglong skipped_to() { return m_skipped_to; }

 private:
  bool m_skip;
  ulonglong m_skipped_to;
};

/**
  Sequence scan mode in TABLE object.
*/
class Sequence_scan {
 public:
  /**
    Scan mode example like:

      ORIGINAL_SCAN 'SELECT * FROM s'
      ITERATION_SCAN 'SELECT NEXTVAL(s), CURRVAL(s)'

    Orignal scan only query the base table data.
    Iteration scan will apply the sequence logic.
  */
  enum Scan_mode { ORIGINAL_SCAN = 0, ITERATION_SCAN };

  enum Iter_mode {
    IT_NON,         /* Query the sequence base table */
    IT_NEXTVAL,     /* Query nextval */
    IT_NON_NEXTVAL, /* Query non nextval, maybe currval or others */
  };

  Sequence_scan()
      : m_mode(ORIGINAL_SCAN), m_batch(1), m_skip(), m_operation() {}
  Sequence_scan(const Sequence_scan &seq_scan) : m_mode(seq_scan.m_mode) {}

  void reset() {
    m_mode = ORIGINAL_SCAN;
    m_batch = 1;
    m_skip.reset();
    m_operation.reset();
  }
  void set(Scan_mode mode) { m_mode = mode; }
  Scan_mode get() { return m_mode; }

  void set_batch(ulonglong batch) { m_batch = batch; }
  ulonglong get_batch() { return m_batch; }

  Sequence_skip get_skip() { return m_skip; }
  void set_skip(Sequence_skip skip) { m_skip = skip; }
  void set_skip(ulonglong value) { m_skip.init(value); }

  Sequence_operation get_operation() { return m_operation; }
  void set_operation_show_cache() {
    m_operation.set_type(Sequence_operation::OPERATION_SHOW_CACHE);
  }

  /* Overlap the assignment operator */
  Sequence_scan &operator=(const Sequence_scan &rhs) {
    if (this != &rhs) {
      this->m_mode = rhs.m_mode;
      this->m_batch = rhs.m_batch;
      this->m_skip = rhs.m_skip;
      this->m_operation = rhs.m_operation;
    }
    return *this;
  }

 private:
  Scan_mode m_mode;
  /**
    Used to get a batch of sequence value, currently only used for
    timestamp sequence
 */
  ulonglong m_batch;

  Sequence_skip m_skip;
  Sequence_operation m_operation;
};

typedef Sequence_scan::Scan_mode Sequence_scan_mode;
typedef Sequence_scan::Iter_mode Sequence_iter_mode;
typedef Sequence_operation::Operation_type Sequence_operation_type;

/**
  Sequence currval that was saved in THD object.
*/
class Sequence_last_value {
 public:
  Sequence_last_value() {}
  virtual ~Sequence_last_value() {}

  void set_version(ulonglong version) { m_version = version; }
  ulonglong get_version() { return m_version; }

  ulonglong m_values[Sequence_field::FIELD_NUM_END];

 private:
  ulonglong m_version;
};

typedef collation_unordered_map<std::string, Sequence_last_value *>
    Sequence_last_value_hash;

extern const LEX_CSTRING SEQUENCE_ENGINE_NAME;
extern const LEX_CSTRING SEQUENCE_BASE_ENGINE_NAME;

/**
  Resolve the sequence engine and sequence base engine, it needs to
  unlock_plugin explicitly if thd is null;
*/
extern plugin_ref ha_resolve_sequence(const THD *thd);
extern plugin_ref ha_resolve_sequence_base(const THD *thd);

extern st_sequence_field_info seq_fields[];

extern bool check_sequence_values_valid(Sequence_type type,
                                        const ulonglong *items);

extern Sequence_type convert_sequence_type_by_increment(ulonglong value);

extern bool check_sequence_fields_valid(Alter_info *alter_info);

extern Sequence_iter_mode sequence_iteration_type(TABLE *table);

/**
  Clear or destroy the global Sequence_share_hash and
  session Sequence_last_value_hash.
*/
template <typename K, typename V>
void clear_hash(collation_unordered_map<K, V> *hash) {
  if (hash) {
    for (auto it = hash->cbegin(); it != hash->cend(); ++it) {
      delete it->second;
    }
    hash->clear();
  }
}
template <typename K, typename V>
void destroy_hash(collation_unordered_map<K, V> *hash) {
  if (hash) {
    clear_hash<K, V>(hash);
    delete hash;
  }
}

/**
  Whether it is SEQUENCE object creation;

  @retval   true    CREATE SEQUENCE  or
                    CREATE TABLE ...sequence engine
  @retval   false   Not
*/
bool sequence_create_info(HA_CREATE_INFO *create_info);

/**
  Return the item builder of 'NEXTVAL' and 'CURRVAL'

  @retval   Create_func   Builder
  @retval   NULL          Not surpported function
*/
extern Create_func *find_sequence_function_builder(const LEX_STRING &f_name);

/**
  Check whether the sp with the given name exist.

  @retval   true   Exist
  @retval   false  Not exist
*/
extern bool user_seq_sp_exist(THD *thd, const LEX_STRING &f_name);

/**
  Init/De-init the existence of sequence SP.
*/
extern bool user_seq_sp_init();
extern void user_seq_sp_exit();

/**
  Trace changes of procedures, update the existence of sequence SP.

  @param[in]    db          database
  @param[in]    name        sp name
  @param[in]    is_added    true  - newly added or renamed
                            false - dropped
*/
extern void on_user_sp_status_changed(const char *db, const char *name,
                                      bool is_added);

/* Is the name sequence function */
extern bool is_seq_func_name(const char *name);
extern bool is_seq_nextval_func_name(const char *name);
extern bool is_seq_currval_func_name(const char *name);
extern bool is_seq_nextval_skip_func_name(const char *name);
extern bool is_seq_nextval_show_func_name(const char *name);

extern ulonglong time_system_ms(void);

#endif
