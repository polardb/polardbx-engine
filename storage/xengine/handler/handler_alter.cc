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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

/* The C++ file's header */
#include "sql_class.h"
#include "handler_alter.h"

/* C++ standard header files */
#include <algorithm>
#include <queue>
#include <set>
#include <string>
#include <vector>

/* MySQL includes */
#include "debug_sync.h"
#include "my_bit.h"
#include "my_sys.h"
#include "my_stacktrace.h"
#include "sql_table.h"
#include "m_string.h"
#include "mysql/psi/mysql_stage.h"
#include "mysql/psi/mysql_table.h"
#include "mysql/thread_pool_priv.h"
#include "mysys_err.h"
#include "debug_sync.h"
#include "mysqld.h"
#include "sql_table.h"
#include "sql_thd_internal_api.h"
#include "table.h"
#include "my_loglevel.h"
#include "sql/create_field.h"

/* XENGINE includes */
#include "xengine/compaction_filter.h"
#include "xengine/slice_transform.h"
#include "xengine/utilities/checkpoint.h"
#include "xengine/utilities/convenience.h"
#include "xengine/utilities/memory_util.h"
#include "xengine/perf_level.h"
#include "core/util/memory_stat.h"
#include "core/util/sync_point.h"
#include "core/memory/mod_info.h"
#include "core/port/likely.h"
#include "core/monitoring/query_perf_context.h"
#include "core/util/string_util.h"
#include "xengine/utilities/write_batch_with_index.h"
#include "core/options/options_helper.h"
#include "core/logger/logger.h"
#include "utilities/transactions/transaction_db_impl.h"
#include "db/db_impl.h"
#include "db/column_family.h"
#include "db/version_edit.h"
#include "table/internal_iterator.h"
#include "storage/multi_version_extent_meta_layer.h"

/* MyX includes */
#include "./util/xdb_utils.h"
#include "./event_listener.h"
#include "./ha_xengine_proto.h"
#include "./logger.h"
#include "./xdb_cf_manager.h"
#include "./xdb_cf_options.h"
#include "./xdb_datadic.h"
#include "./xdb_mutex_wrapper.h"
#include "./xdb_threads.h"
#include "./log_ddl.h"

using namespace xengine;
using namespace util;
using namespace common;

namespace myx
{
extern Xdb_dict_manager dict_manager;
extern Xdb_cf_manager cf_manager;
extern Xdb_ddl_manager ddl_manager;
extern Xdb_binlog_manager binlog_manager;
extern Xdb_ddl_log_manager ddl_log_manager;
extern Xdb_drop_index_thread xdb_drop_idx_thread;
extern xengine::util::TransactionDB *xdb;
extern int xengine_inplace_populate_indexes;
extern int32_t xengine_shrink_table_space;
extern bool xengine_enable_bulk_load_api;
extern uint32_t xengine_disable_online_ddl;
extern bool xengine_disable_instant_ddl;
extern ulong xengine_sort_buffer_size;
extern xengine::common::DBOptions xengine_db_options;
extern bool xengine_disable_parallel_ddl;


/** Function to convert the Instant_Type to a comparable int */
uint16_t instant_type_to_int(Instant_Type type)
{
  return (static_cast<typename std::underlying_type<Instant_Type>::type>(type));
}

/** Determine if this is an instant ALTER TABLE.
  This can be checked in *inplace_alter_table() functions, which are called
  after check_if_supported_inplace_alter()*/
bool is_instant(const Alter_inplace_info *ha_alter_info)
{
  return (ha_alter_info->handler_trivial_ctx !=
          instant_type_to_int(Instant_Type::INSTANT_IMPOSSIBLE));
}

/** Determine if one ALTER TABLE can be done instantly on the table
@param[in]	ha_alter_info	The DDL operation
@return Instant_Type accordingly */
Instant_Type ha_xengine::check_if_support_instant_ddl(
    const Alter_inplace_info *ha_alter_info)
{
  if (!(ha_alter_info->handler_flags & ~XENGINE_INPLACE_IGNORE)) {
    return (Instant_Type::INSTANT_NO_CHANGE);
  }

  Alter_inplace_info::HA_ALTER_FLAGS alter_inplace_flags =
      ha_alter_info->handler_flags & ~XENGINE_INPLACE_IGNORE;

  /* If it's an ADD COLUMN without changing existing column orders */
  if (alter_inplace_flags == Alter_inplace_info::ADD_STORED_BASE_COLUMN ||
      alter_inplace_flags ==
          (Alter_inplace_info::ADD_STORED_BASE_COLUMN |
           my_core::Alter_inplace_info::CHANGE_CREATE_OPTION)) {
    // in CHANGE_CREATE_OPTION only support HA_CREATE_USED_COMMENT
    if (alter_inplace_flags &
        my_core::Alter_inplace_info::CHANGE_CREATE_OPTION) {
      // Only support used_fields with HA_CREATE_USED_COMMENT flag
      if (ha_alter_info->create_info->used_fields & ~(HA_CREATE_USED_COMMENT)) {
        return (Instant_Type::INSTANT_IMPOSSIBLE);
      }
    }
    return (Instant_Type::INSTANT_ADD_COLUMN);
  } else {
    return (Instant_Type::INSTANT_IMPOSSIBLE);
  }
}

static int set_duplicate_key_for_print(
    TABLE *new_table, TABLE *old_table, const Xdb_key_def *kd,
    const std::shared_ptr<Xdb_inplace_ddl_dict_info>& dict_info,
    bool is_rebuild);

static MY_ATTRIBUTE((warn_unused_result)) bool xengine_need_rebuild(
    const Alter_inplace_info *ha_alter_info);

/** Get instant ddl information from Data Dictionary
@param[in]	table_def	dd table  */
void ha_xengine::get_instant_ddl_info_if_needed(const dd::Table *table_def)
{
  m_instant_ddl_info.clearup();

  for (const auto col : table_def->columns()) {
    if (col->is_virtual() || col->is_se_hidden()) {
      continue;
    }
    const dd::Properties &se_private_data = col->se_private_data();
    if (!se_private_data.exists(
            dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT_NULL]) &&
        !se_private_data.exists(
            dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT])) {
      continue;
    }
    if (se_private_data.exists(
            dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT_NULL])) {
      m_instant_ddl_info.instantly_added_default_value_null.push_back(true);
      m_instant_ddl_info.instantly_added_default_values.push_back("");
    } else if (se_private_data.exists(
                   dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT])) {
      dd::String_type value;
      se_private_data.get(dd_column_key_strings[DD_INSTANT_COLUMN_DEFAULT],
                          &value);
      size_t len;
      const byte *decoded_default_value;
      DD_instant_col_val_coder coder;
      decoded_default_value = coder.decode(value.c_str(), value.length(), &len);

      std::string default_value(
          reinterpret_cast<const char *>(decoded_default_value), len);
      m_instant_ddl_info.instantly_added_default_value_null.push_back(false);
      m_instant_ddl_info.instantly_added_default_values.push_back(
          default_value);
    }
  }

  if (table_def->se_private_data().exists(
          dd_table_key_strings[DD_TABLE_INSTANT_COLS])) {
    table_def->se_private_data().get(
        dd_table_key_strings[DD_TABLE_INSTANT_COLS],
        &m_instant_ddl_info.m_instant_cols);
    assert(m_instant_ddl_info.instantly_added_default_values.size() ==
           (table->s->fields - m_instant_ddl_info.m_instant_cols));
  } else {
    assert(m_instant_ddl_info.instantly_added_default_values.empty());
  }
  if (table_def->se_private_data().exists(
          dd_table_key_strings[DD_TABLE_NULL_BYTES])) {
    table_def->se_private_data().get(dd_table_key_strings[DD_TABLE_NULL_BYTES],
                                     &m_instant_ddl_info.m_null_bytes);
  }
}

/** Get the error message format string.
 @return the format string or 0 if not found. */
const char *myx_get_err_msg(int error_code) /*!< in: MySQL error code */
{
  return (my_get_err_msg(error_code));
}

/** Checks if inplace alter is supported for a given operation.
 @param[in] altered_table new table
 @param[in] ha_alter_info The DDL operation */
my_core::enum_alter_inplace_result ha_xengine::check_if_supported_inplace_alter(
    TABLE *altered_table, my_core::Alter_inplace_info *const ha_alter_info)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(ha_alter_info != nullptr);

  if (ha_alter_info->handler_flags &
      ~(XENGINE_INPLACE_IGNORE | XENGINE_ALTER_NOREBUILD |
        XENGINE_ALTER_REBUILD)) {
    if (ha_alter_info->handler_flags &
        Alter_inplace_info::ALTER_STORED_COLUMN_TYPE) {
      ha_alter_info->unsupported_reason = myx_get_err_msg(
          ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_COLUMN_TYPE);
    }
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  Instant_Type instant_type = check_if_support_instant_ddl(ha_alter_info);

  ha_alter_info->handler_trivial_ctx =
      instant_type_to_int(Instant_Type::INSTANT_IMPOSSIBLE);

  switch (instant_type) {
    case Instant_Type::INSTANT_IMPOSSIBLE:
      break;
    case Instant_Type::INSTANT_ADD_COLUMN:
      if ((ha_alter_info->alter_info->requested_algorithm ==
              Alter_info::ALTER_TABLE_ALGORITHM_INPLACE) ||
          ha_alter_info->error_if_not_empty) {
        /* When user request using INPLACE or table may be not empty,
         * we have to fall back to INPLACE
         */
        break;
      }
    /* Fall through */
    case Instant_Type::INSTANT_NO_CHANGE:
      ha_alter_info->handler_trivial_ctx = instant_type_to_int(instant_type);
      if (!xengine_disable_instant_ddl) {
        DBUG_RETURN(HA_ALTER_INPLACE_INSTANT);
      } else {
        DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
      }
  }

  if (ha_alter_info->handler_flags & XENGINE_ALTER_NOT_SUPPORTED) {
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  if (ha_alter_info->handler_flags &
      my_core::Alter_inplace_info::CHANGE_CREATE_OPTION) {
    // Only support used_fields with HA_CREATE_USED_COMMENT flag
    if (ha_alter_info->create_info->used_fields & ~(HA_CREATE_USED_COMMENT)) {
      ha_alter_info->unsupported_reason = "XEngineDDL: only supports to change "
                                          "comment of table";
      DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
    }
  }

#if 0
  /* [BUG] Due to a known issue, we disable support of combination other
   * alteration with rename table, so disable it temporarily
   * remove this if branch after the bug is fixed
   */
  if ((ha_alter_info->handler_flags & Alter_inplace_info::ALTER_RENAME) &&
      (ha_alter_info->handler_flags & ~Alter_inplace_info::ALTER_RENAME)) {
    __XHANDLER_LOG(WARN, "XEngineDDL: Combination of renaming table with other "
                         "alteration is disabled due to known issue. table:%s",
                   table->s->table_name.str);
    ha_alter_info->unsupported_reason = "Combination of renaming table with "
                                        "other alteration is disabled due to "
                                        "known issue";
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }
#endif

  /* Only support NULL -> NOT NULL change if strict table sql_mode
  is set. Fall back to COPY for conversion if not strict tables.
  In-Place will fail with an error when trying to convert
  NULL to a NOT NULL value. */
  if ((ha_alter_info->handler_flags &
       Alter_inplace_info::ALTER_COLUMN_NOT_NULLABLE) &&
      !(ha_thd()->is_strict_mode())) {
    ha_alter_info->unsupported_reason =
        myx_get_err_msg(ER_ALTER_OPERATION_NOT_SUPPORTED_REASON_NOT_NULL);
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* DROP PRIMARY KEY is only allowed in combination with ADD
     PRIMARY KEY. */
  if ((ha_alter_info->handler_flags & (Alter_inplace_info::ADD_PK_INDEX |
                                       Alter_inplace_info::DROP_PK_INDEX)) ==
      Alter_inplace_info::DROP_PK_INDEX) {
    __XHANDLER_LOG(WARN, "XEngineDDL: drop primary key is only allowed in "
                         "combination with ADD PRIMARY KEY. table_name: %s",
                   table->s->table_name.str);
    ha_alter_info->unsupported_reason = "With INPLACE DDL, XEngine only allows "
                                        "that DROP PRIMARY KEY is combined with "
                                        "ADD PRIMARY KEY";
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* for a special case, disable inplace DDL
   * When a table has all non-nullable columns defined, and has an unique key,
   * it will be treated as the PRIMARY KEY. When the non-nullable column is
   * changed to nullable, that unique key can't be treated as PRIMARY KEY in
   * new table, so we will has a hidden pk in altered_table.
   * We can't support inplace DROP a PRIMARY KEY.
   */
  if (!has_hidden_pk(table) && has_hidden_pk(altered_table)) {
    __XHANDLER_LOG(WARN, "XEngineDDL: drop primary key is only allowed in "
                         "combination with ADD PRIMARY KEY. table_name: %s",
                   table->s->table_name.str);
    ha_alter_info->unsupported_reason = "With INPLACE DDL, XEngine only allows "
                                        "that DROP PRIMARY KEY is combined with "
                                        "ADD PRIMARY KEY";
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  /* If a column change from NOT NULL to NULL and there's an implicit pk on this
   * column, the table should be rebuild.
   * The alteration should only go through the "Copy" method.
   */

  /* for xengine, if there is no pk in mysql table, xengine will build
   * a hidden pk for table, So It's ok for xengine table.
   */

  List_iterator_fast<Create_field> cf_it(
      ha_alter_info->alter_info->create_list);

  /* Fix the key parts. */
  for (KEY *new_key = ha_alter_info->key_info_buffer;
       new_key < ha_alter_info->key_info_buffer + ha_alter_info->key_count;
       new_key++) {
    for (KEY_PART_INFO *key_part = new_key->key_part;
         key_part < new_key->key_part + new_key->user_defined_key_parts;
         key_part++) {
      const Create_field *new_field;

      DBUG_ASSERT(key_part->fieldnr < altered_table->s->fields);

      cf_it.rewind();
      for (uint fieldnr = 0; (new_field = cf_it++); fieldnr++) {
        if (fieldnr == key_part->fieldnr) {
          break;
        }
      }

      DBUG_ASSERT(new_field);

      key_part->field = altered_table->field[key_part->fieldnr];
      /* In some special cases InnoDB emits "false"
         duplicate key errors with NULL key values. Let
         us play safe and ensure that we can correctly
         print key values even in such cases. */
      key_part->null_offset = key_part->field->null_offset();
      key_part->null_bit = key_part->field->null_bit;

      if (new_field->field) {
        /* This is an existing column. */
        continue;
      }

      /* This is an added column. */
      DBUG_ASSERT(ha_alter_info->handler_flags &
                  Alter_inplace_info::ADD_COLUMN);

      DBUG_ASSERT((key_part->field->auto_flags & Field::NEXT_NUMBER) ==
                  !!(key_part->field->flags & AUTO_INCREMENT_FLAG));

      if (key_part->field->flags & AUTO_INCREMENT_FLAG) {
        /* We cannot assign an AUTO_INCREMENT
           column values during online ALTER. */
        DBUG_ASSERT(key_part->field == altered_table->found_next_number_field);
        __XHANDLER_LOG(WARN, "XEngineDDL: not support assign a auto_increment "
                             "column value. table_name: %s",
                       table->s->table_name.str);

        /* for add autoinc column can't not downgrade lock after phase,
         * so, inplace ddl cost is the same as copy-ddl.
        */
        DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
      }
    }
  }

  /** if xengine_disable_online_ddl is 1, disable online-rebuild ddl,
   *  if xengine_disable_online_ddl is 2, disable online-rebuild/norebuild ddl,
   *  default xengine_disable_online_ddl is 0, enable online-rebuild/norebuild ddl.
  */
  if (xengine_disable_online_ddl == 1 && xengine_need_rebuild(ha_alter_info)) {
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  } else if (xengine_disable_online_ddl == 2) {
    DBUG_RETURN(HA_ALTER_INPLACE_NOT_SUPPORTED);
  }

  DBUG_RETURN(HA_ALTER_INPLACE_NO_LOCK_AFTER_PREPARE);
}

/** Determine if ALTER TABLE needs to rebuild the table.
  @param[in]  ha_alter_info   The DDL operation
  @return whether it is necessary to rebuild the table */
static MY_ATTRIBUTE((warn_unused_result)) bool xengine_need_rebuild(
    const Alter_inplace_info *ha_alter_info)
{
  if (is_instant(ha_alter_info)) {
    return (false);
  }

  Alter_inplace_info::HA_ALTER_FLAGS alter_inplace_flags =
      ha_alter_info->handler_flags & ~(XENGINE_INPLACE_IGNORE);

  if (alter_inplace_flags == Alter_inplace_info::CHANGE_CREATE_OPTION &&
      !(ha_alter_info->create_info->used_fields &
        (HA_CREATE_USED_ROW_FORMAT | HA_CREATE_USED_KEY_BLOCK_SIZE |
         HA_CREATE_USED_TABLESPACE))) {
    /* Any other CHANGE_CREATE_OPTION than changing
       ROW_FORMAT, KEY_BLOCK_SIZE or TABLESPACE can be done
       without rebuilding the table. */
    return (false);
  }

  return (0 != (ha_alter_info->handler_flags & XENGINE_ALTER_REBUILD));
}

#ifndef NDEBUG
void dbug_create_err_inplace_alter()
{
  // This my_printf_error is needed for DEBUG SYNC
  my_printf_error(ER_UNKNOWN_ERROR,
                  "Intentional failure in inplace alter occurred.", MYF(0));
}
#endif


/** check if there is duplicate key during build base data */
bool ha_xengine::check_duplicate_in_base(
    const TABLE *table_arg, const Xdb_key_def &index,
    const xengine::common::Slice& key, unique_key_buf_info *key_buf_info)
{
  bool res = false;
  // used for secondary key
  uint n_null_fields = 0;
  const xengine::util::Comparator *index_comp = index.get_cf()->GetComparator();
  /* Get proper key buffer. */
  uchar *key_buf = key_buf_info->swap_and_get_key_buf();
  uint key_size = 0;
  if (index.is_primary_key()) {
    DBUG_ASSERT(key.size() <= m_max_packed_sk_len);
    memcpy(key_buf, key.data(), key.size());
    key_size = key.size();
  } else {
    /* Get memcmp form of sk without extended pk tail */
    key_size = index.get_memcmp_sk_parts(table_arg, key, key_buf, &n_null_fields);
  }
  key_buf_info->memcmp_key =
      xengine::common::Slice(reinterpret_cast<char *>(key_buf), key_size);
  if (key_buf_info->memcmp_key_old.size() > 0 && n_null_fields == 0 &&
      !index_comp->Compare(key_buf_info->memcmp_key, key_buf_info->memcmp_key_old)) {
    res = true;
  }

  key_buf_info->memcmp_key_old = key_buf_info->memcmp_key;
  return res;
}

/*
   Create key definition needed for storing data in xengine during ADD index
   inplace operations.

   @param in
   table_arg         Table with definition
   tbl_def_arg       New table def structure being populated
   old_tbl_def_arg   Old(current) table def structure
   cfs               Struct array which contains column family information

   @return
   0      - Ok
   other  - error, either given table ddl is not supported by xengine or OOM.
   */
int ha_xengine::create_inplace_key_defs(
    const TABLE *const table_arg, Xdb_tbl_def *const tbl_def_arg,
    const TABLE *const old_table_arg, const Xdb_tbl_def *const old_tbl_def_arg,
    const std::array<key_def_cf_info, MAX_INDEXES + 1> &cfs,
    const std::array<uint32_t, MAX_INDEXES + 1> &index_ids) const
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);
  DBUG_ASSERT(old_tbl_def_arg != nullptr);

  std::shared_ptr<Xdb_key_def> *const old_key_descr =
      old_tbl_def_arg->m_key_descr_arr;
  std::shared_ptr<Xdb_key_def> *const new_key_descr =
      tbl_def_arg->m_key_descr_arr;

  for (uint i = 0; i < tbl_def_arg->m_key_count; i++) {
    const char* new_key_name = get_key_name(i, table_arg, tbl_def_arg);
    auto stat_it = new_key_stats.find(new_key_name);
    DBUG_ASSERT(stat_it != new_key_stats.end());
    const prepare_inplace_key_stat& new_key_stat = stat_it->second;
    DBUG_ASSERT(new_key_stat.key_name == new_key_name);

#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "key stat: %s", new_key_stat.to_string().c_str());
#endif
    if (new_key_stat.key_stat == prepare_inplace_key_stat::COPIED ||
        new_key_stat.key_stat == prepare_inplace_key_stat::RENAMED) {
      DBUG_ASSERT(new_key_stat.mapped_key_index < m_tbl_def->m_key_count);

      /*
         Found matching index in old table definition, so copy it over to the
         new one created.
         */
      const Xdb_key_def &okd = *old_key_descr[new_key_stat.mapped_key_index];
      DBUG_ASSERT(okd.m_name == new_key_stat.mapped_key_name);

#if 0
      uint16 index_dict_version = 0;
      uchar index_type = 0;
      uint16 kv_version = 0;
      const GL_INDEX_ID gl_index_id = okd.get_gl_index_id();
      if (!dict_manager.get_index_info(gl_index_id, &index_dict_version,
                                       &index_type, &kv_version)) {
        // NO_LINT_DEBUG
        __XHANDLER_LOG(ERROR,
                       "XEngineDDL: Could not get index information "
                       "for Index Number (%u,%u), table %s",
                       gl_index_id.cf_id, gl_index_id.index_id,
                       old_tbl_def_arg->full_tablename().c_str());
        DBUG_RETURN(HA_EXIT_FAILURE);
      }
#endif

      /*
         We can't use the copy constructor because we need to update the
         keynr within the pack_info for each field and the keyno of the keydef
         itself.
         Xdb_key_def is already constructed from dictionary, these meta info
         should be up-to-date to use
         */
      new_key_descr[i] = std::make_shared<Xdb_key_def>(
          okd.get_index_number(), i, okd.get_cf(), okd.m_index_dict_version,
          okd.m_index_type, okd.m_kv_format_version, okd.m_is_reverse_cf,
          okd.m_is_auto_cf, new_key_name,
          dict_manager.get_stats(okd.get_gl_index_id()));
    } else if (create_key_def(table_arg, i, tbl_def_arg, &new_key_descr[i],
                              cfs[i], index_ids[i])) {
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    DBUG_ASSERT(new_key_descr[i] != nullptr);
    new_key_descr[i]->setup(table_arg, tbl_def_arg);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** Find the same old index definition in old table.
@param[in] table_arg new_table
@param[in] tbl_def_arg new xengine table definition
@param[in] old_table_arg old_table
@param[in] old_tbl_def_arg old xengine table definition
@return map<key_name, key_no>
*/
std::unordered_map<std::string, uint> ha_xengine::get_old_key_positions(
    const TABLE *const table_arg, const Xdb_tbl_def *const tbl_def_arg,
    const TABLE *const old_table_arg,
    const Xdb_tbl_def *const old_tbl_def_arg) const
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(old_table_arg != nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);
  DBUG_ASSERT(old_tbl_def_arg != nullptr);

  std::shared_ptr<Xdb_key_def> *const old_key_descr =
      old_tbl_def_arg->m_key_descr_arr;
  std::unordered_map<std::string, uint> old_key_pos;
  std::unordered_map<std::string, uint> new_key_pos;
  uint i;

  for (i = 0; i < tbl_def_arg->m_key_count; i++) {
    new_key_pos[get_key_name(i, table_arg, tbl_def_arg)] = i;
  }

  for (i = 0; i < old_tbl_def_arg->m_key_count; i++) {
    if (is_hidden_pk(i, old_table_arg, old_tbl_def_arg)) {
      old_key_pos[old_key_descr[i]->m_name] = i;
      continue;
    }

    /*
       In case of matching key name, need to check key parts of keys as well,
       in case a simultaneous drop + add is performed, where the key name is the
       same but the key parts are different.

       Example:
       CREATE TABLE t1 (a INT, b INT, KEY ka(a)) ENGINE=XENGINE;
       ALTER TABLE t1 DROP INDEX ka, ADD INDEX ka(b), ALGORITHM=INPLACE;
     */
    const KEY *const old_key = &old_table_arg->key_info[i];
    const auto &it = new_key_pos.find(old_key->name);
    if (it == new_key_pos.end()) {
      continue;
    }

    KEY *const new_key = &table_arg->key_info[it->second];

    /* If index algorithms are different we need to rebuild. */
    if (old_key->algorithm != new_key->algorithm) {
      continue;
    }
    /*
       Check that the key is identical between old and new tables.
       If so, we still need to create a new index.
       The exception is if there is an index changed from unique to non-unique,
       in these cases we don't need to rebuild as they are stored the same way
       in
       XENGINE.
       */
    bool unique_to_non_unique =
        (old_key->flags ^ new_key->flags) == HA_NOSAME &&
        (old_key->flags & HA_NOSAME);
    if (!unique_to_non_unique && ((old_key->flags & HA_KEYFLAG_MASK) !=
                                  (new_key->flags & HA_KEYFLAG_MASK))) {
      continue;
    }

    if (compare_key_parts(old_key, new_key)) {
      continue;
    }

    old_key_pos[old_key->name] = i;
  }

  DBUG_RETURN(old_key_pos);
}

/* Check two keys to ensure that key parts within keys match */
int ha_xengine::compare_key_parts(const KEY *const old_key,
                                  const KEY *const new_key) const
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(old_key != nullptr);
  DBUG_ASSERT(new_key != nullptr);

  /* Skip if key parts do not match, as it is a different key */
  if (new_key->user_defined_key_parts != old_key->user_defined_key_parts) {
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  /* Check to see that key parts themselves match */
  for (uint i = 0; i < old_key->user_defined_key_parts; i++) {
    if (strcmp(old_key->key_part[i].field->field_name,
               new_key->key_part[i].field->field_name) != 0) {
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
    /* Check if prefix index key part length has changed */
    if (old_key->key_part[i].length != new_key->key_part[i].length) {
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

static void inplace_alter_table_release(Xdb_tbl_def* &tbl_def)
{
  if (nullptr != tbl_def) {
    if (nullptr != tbl_def->m_key_descr_arr) {
      /* Delete the new key descriptors */
      delete[] tbl_def->m_key_descr_arr;
      /*
       * Explicitly mark as nullptr so we don't accidentally remove entries
       * from data dictionary on cleanup (or cause double delete[]).
       */
      tbl_def->m_key_descr_arr = nullptr;
    }
    delete tbl_def;
    tbl_def = nullptr;
  }
}

/**
 * Online create unique index process, non-unique is simpler
 *
 * t0          t1    t2    t3
 * |-----------|------|----|-----
 *
 * t0: TABLE_CREATE, create table, empty table
 * t1: BUILDING_BASE_INDEX, start to create index
 * t2: CHECK_UNIQUE_CONSTRAINT, finish building index for (t0, t1), start to check unique
 * t3: FINISHED, finish checking unique between (t1, t2) and (t0, t1)
 *
 * t1-t2: new arrivals unique check against data in (t1, then),
 *   the uncommitted 2nd index are updated accordingly from now on.
 *   cf-index is the destination cfh.
 *   At this time, base-index is not done yet, so data in (t1-t2) only do
 *   unique check with its own(mem,L0,L1).
 *
 * t2-t3: we do unique check between(t0,t1),(t1,t2), new arrivals(t2,then)
 *   unique check against data in (t0, t1) & (t1, t2),
 *   (t0, t1) and (t1, then) are in separate parts.(t0,t1) locate at L2 layer of
 *   subtable, and (t1, then) locate at (mem,L0,L1) of subtable.
 *
 * t3: last phase of inplace_ddl, if there were errors happened during the ddl,
 *   we rollback new indexes operations, otherwise, we make new indexes available
 *   for new arrival request.
 */
/**
  Allows the storage engine to update internal structures with concurrent
  writes blocked. If check_if_supported_inplace_alter() returns
  HA_ALTER_INPLACE_NO_LOCK_AFTER_PREPARE or
  HA_ALTER_INPLACE_SHARED_AFTER_PREPARE, this function is called with
  exclusive lock otherwise the same level of locking as for
  inplace_alter_table() will be used.

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function reports error, commit_inplace_alter_table()
  will be called with commit= false.

  @note For partitioning, failing to prepare one partition, means that
  commit_inplace_alter_table() will be called to roll back changes for
  all partitions. This means that commit_inplace_alter_table() might be
  called without prepare_inplace_alter_table() having been called first
  for a given partition.

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
  by ALTER TABLE and holding data used
  during in-place alter.
  @param    old_table_def     dd::Table object describing old version of
  the table.
  @param    new_table_def     dd::Table object for the new version of the
  table. Can be adjusted by this call if SE
  supports atomic DDL. These changes to the
  table definition will be persisted in the
  data-dictionary at statement commit time.

  @retval   true              Error
  @retval   false             Success
  */
bool ha_xengine::prepare_inplace_alter_table(
    TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info,
    const dd::Table *old_table_def, dd::Table *new_table_def)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(altered_table != nullptr);
  DBUG_ASSERT(ha_alter_info != nullptr);
  DBUG_ASSERT(old_table_def != nullptr);
  DBUG_ASSERT(new_table_def != nullptr);

#ifndef NDEBUG
  __XHANDLER_LOG(INFO, "XEngineDDL: prepare alter sql is %s", ha_thd()->query().str);
#endif

  if (is_instant(ha_alter_info)) {
    // Nothing to do if it's instant ddl here
    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  int ret = HA_EXIT_SUCCESS;
  const uint old_n_keys = m_tbl_def->m_key_count;
  uint new_n_keys = altered_table->s->keys;

  if (has_hidden_pk(altered_table)) {
    new_n_keys += 1;
  }

  std::shared_ptr<Xdb_key_def> *const old_key_descr =
      m_tbl_def->m_key_descr_arr;

  auto new_key_descr = new std::shared_ptr<Xdb_key_def>[ new_n_keys ];
  auto new_tdef = new Xdb_tbl_def(m_tbl_def->full_tablename());

  new_tdef->m_key_descr_arr = new_key_descr;
  new_tdef->m_key_count = new_n_keys;
  new_tdef->m_auto_incr_val =
      m_tbl_def->m_auto_incr_val.load(std::memory_order_relaxed);
  new_tdef->m_hidden_pk_val =
      m_tbl_def->m_hidden_pk_val.load(std::memory_order_relaxed);

  bool need_rebuild = xengine_need_rebuild(ha_alter_info);

  if ((ret = prepare_inplace_alter_table_collect_key_stats(
                        altered_table, ha_alter_info))) {
    XHANDLER_LOG(ERROR, "XEngineDDL: failed to collect status of keys for altered table",
                 "table_name", table->s->table_name.str);
  } else if (new_tdef->init_table_id(ddl_manager)) {
    XHANDLER_LOG(ERROR, "XEngineDDL: failed to init table_id for creating table",
                 "table_name", table->s->table_name.str);
    ret = HA_EXIT_FAILURE;
  } else if ((ret = create_key_defs(altered_table, new_tdef, nullptr, table,
                                    m_tbl_def.get(), need_rebuild))) {
    XHANDLER_LOG(ERROR, "XEngineDDL: failed creating new key definitions for altered table",
                 "table_name", table->s->table_name.str);
  } else if (new_tdef->write_dd_table(new_table_def)) {
    XHANDLER_LOG(ERROR, "XEngineDDL: failed to write dd::Table",
                 "table_name", table->s->table_name.str);
    ret = HA_EXIT_FAILURE;
  } else if (need_rebuild) {
    ret = prepare_inplace_alter_table_rebuild(
        altered_table, ha_alter_info, new_tdef, old_key_descr, old_n_keys,
        new_key_descr, new_n_keys);
    if (ret) {
      XHANDLER_LOG(ERROR, "XEngineDDL: prepare for online inplace ddl failed",
                   "table_name", table->s->table_name.str);
    }
  } else {
    ret = prepare_inplace_alter_table_norebuild(
        altered_table, ha_alter_info, new_tdef, old_key_descr, old_n_keys,
        new_key_descr, new_n_keys);
    if (ret) {
      XHANDLER_LOG(ERROR, "XEngineDDL: prepare for online inplace ddl failed",
                   "table_name", table->s->table_name.str);
    }
  }

  if (ret) {
    XHANDLER_LOG(ERROR, "XEngineDDL: prepare for online inplace ddl failed",
                 "table_name", table->s->table_name.str);
    inplace_alter_table_release(new_tdef);
  } else {
#ifndef NDEBUG
    for (auto &it : new_key_stats) {
      DBUG_ASSERT(it.first == it.second.key_name);
      XHANDLER_LOG(INFO, "XEngineDDL: ", "key stat", it.second.to_string().c_str());
    }
#endif
    XHANDLER_LOG(INFO, "XEngineDDL: prepare for online inplace ddl successfully",
                 "table_name", table->s->table_name.str);
  }

  DBUG_RETURN(ret);
}

int ha_xengine::prepare_inplace_alter_table_collect_key_stats(
    TABLE *const altered_table, my_core::Alter_inplace_info *const ha_alter_info)
{
  DBUG_ENTER_FUNC();

  new_key_stats.clear();
  std::map<std::string, uint> all_old_key_names;
  // collect key names from old table
  for (uint i = 0; i < m_tbl_def->m_key_count; ++i) {
    all_old_key_names.emplace(m_tbl_def->m_key_descr_arr[i]->m_name, i);
  }
  // collect (dropped or renamed) key name from Alter_inplace_info
  std::unordered_set<std::string> dropped_key_names;
  for (uint i=0; i < ha_alter_info->index_drop_count; ++i) {
    dropped_key_names.emplace(ha_alter_info->index_drop_buffer[i]->name);
  }
  std::map<std::string, std::string> renamed_key_names;
  if (ha_alter_info->handler_flags & Alter_inplace_info::RENAME_INDEX) {
    for (uint i = 0; i < ha_alter_info->index_rename_count; ++i) {
      renamed_key_names.emplace(
        ha_alter_info->index_rename_buffer[i].new_key->name,
        ha_alter_info->index_rename_buffer[i].old_key->name);
    }
  }

  // iterate over all key info of new table
  for (uint i = 0; i < ha_alter_info->key_count; ++i) {
    prepare_inplace_key_stat new_key_stat;
    new_key_stat.key_name = std::string(ha_alter_info->key_info_buffer[i].name);
    auto old_it1 = all_old_key_names.find(new_key_stat.key_name);
    if (old_it1 != all_old_key_names.end()) {
      if (dropped_key_names.count(new_key_stat.key_name)) {
        // key is dropped from old table and re-created in new table
        // index of key in old table is ignored
        new_key_stat.key_stat = prepare_inplace_key_stat::REDEFINED;
      } else {
        // key is copied from key old table
        new_key_stat.key_stat = prepare_inplace_key_stat::COPIED;
        new_key_stat.mapped_key_name = new_key_stat.key_name;
        new_key_stat.mapped_key_index = old_it1->second;
      }
    } else {
      auto it_re = renamed_key_names.find(new_key_stat.key_name);
      if (it_re != renamed_key_names.end()) {
        auto old_it2 = all_old_key_names.find(it_re->second);
        DBUG_ASSERT(old_it2 != all_old_key_names.end());
        // key is renamed from key in old table
        new_key_stat.key_stat = prepare_inplace_key_stat::RENAMED;
        new_key_stat.mapped_key_name = old_it2->first;
        new_key_stat.mapped_key_index = old_it2->second;
      } else {
        // key is newly added into new table
        new_key_stat.key_stat = prepare_inplace_key_stat::ADDED;
      }
    }
    new_key_stats.emplace(new_key_stat.key_name, new_key_stat);
  }

  // hidden pk
  if (has_hidden_pk(altered_table)) {
    prepare_inplace_key_stat new_key_stat;
    new_key_stat.key_name = HIDDEN_PK_NAME;
    if (has_hidden_pk(table)) {
      // hidden pk is copied from old table
      new_key_stat.key_stat = prepare_inplace_key_stat::COPIED;
      new_key_stat.mapped_key_name = HIDDEN_PK_NAME;
      new_key_stat.mapped_key_index = m_tbl_def->m_key_count - 1;
    } else {
      // primary key is dropped from old table, XEngine will add hidden pk
      new_key_stat.key_stat = prepare_inplace_key_stat::ADDED;
    }
    new_key_stats.emplace(new_key_stat.key_name, new_key_stat);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** Build col_map[new_column_index, old_column_index],
if col_map[new_column_index] == -1, it means new column added into new_table
@param[in] ha_alter_info
@param[in] altered_table
@param[in] old_table
@param[in/out] dictionary used to pack/unpack new record from old record
@return HA_EXIT_SUCCESS/HA_EXIT_FAILURE */
static int setup_col_maps(Alter_inplace_info *ha_alter_info,
                          const TABLE *altered_table, const TABLE *old_table,
                          std::shared_ptr<Xdb_inplace_ddl_dict_info>& dict_info)
{
  int ret = HA_EXIT_SUCCESS;

  uint n_cols = altered_table->s->fields;
  uint old_n_cols = old_table->s->fields;
  dict_info->m_col_map = static_cast<uint *>(
      my_malloc(PSI_NOT_INSTRUMENTED, (n_cols) * sizeof(uint), MYF(0)));
  if (dict_info->m_col_map == nullptr) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: alloc memory failed, table_name: %s", old_table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  dict_info->m_col_map_rev = static_cast<uint *>(
      my_malloc(PSI_NOT_INSTRUMENTED, (old_n_cols) * sizeof(uint), MYF(0)));
  if (dict_info->m_col_map_rev == nullptr) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: alloc memory failed, table_name: %s", old_table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  for (uint i = 0; i < old_n_cols; i++) {
    dict_info->m_col_map_rev[i] = (uint)(-1);
  }

  uint *col_map = dict_info->m_col_map;
  uint *col_map_rev = dict_info->m_col_map_rev;
  List_iterator_fast<Create_field> cf_it(
      ha_alter_info->alter_info->create_list);
  uint new_i = 0;
  while (const Create_field *new_field = cf_it++) {
    uint old_i = 0;
    for (; old_i < old_n_cols; old_i++) {
      const Field *field = old_table->field[old_i];

      if (new_field->field == field) {
        break;
      }
    }

    // found
    if (old_i < old_n_cols) {
      col_map[new_i] = old_i;
      col_map_rev[old_i] = new_i;
      new_i++;
    } else {
      col_map[new_i++] = (uint)(-1);
    }
  }

  DBUG_ASSERT(new_i == n_cols);

  return ret;
}

/* prepare dictionary info for online-ddl/dml transactions
@param[in] ha_alter_info Data used during in-place alter
@param[in] altered_table MySQL table that is being altered
@param[in] old_table MySQL table as it is before the ALTER operation
@param[in] table_name Table name in MySQL
@retval HA_EXIT_SUCCESS/HA_EXIT_FAILURE success/fail */
int ha_xengine::prepare_inplace_alter_table_dict(
    Alter_inplace_info *ha_alter_info, const TABLE *altered_table,
    const TABLE *old_table, Xdb_tbl_def *old_tbl_def, const char *table_name)
{
  int ret = HA_EXIT_SUCCESS;

  old_tbl_def->m_dict_info = std::make_shared<Xdb_inplace_ddl_dict_info>();
  if (old_tbl_def->m_dict_info == nullptr) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: allocate memory failed, table_name: %s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  } else if ((ret = setup_field_converters(
                  altered_table, m_new_pk_descr, old_tbl_def->m_dict_info->m_encoder_arr,
                  old_tbl_def->m_dict_info->m_fields_no_needed_to_decode,
                  old_tbl_def->m_dict_info->m_null_bytes_in_rec,
                  old_tbl_def->m_dict_info->m_maybe_unpack_info))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: setup field converters failed, table_name: %s", table->s->table_name.str);
  } else if ((ret = setup_read_decoders(
                  altered_table, old_tbl_def->m_dict_info->m_encoder_arr,
                  old_tbl_def->m_dict_info->m_decoders_vect, true))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: setup read decoders failed, table_name: %s", table->s->table_name.str);
  } else if ((ret = setup_col_maps(ha_alter_info, altered_table, old_table,
                                   old_tbl_def->m_dict_info))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: setup column maps failed, table_name: %s", table->s->table_name.str);
  } else {
    __XHANDLER_LOG(INFO, "XEngineDDL: prepare alter rebuild table dictionary successfully, table_name: %s", table->s->table_name.str);
  }

  return ret;
}

/** Initialize new xdb_tbl_def and dict info for inplace_rebuild table
@param[in]	altered_table	new TABLE
@param[in]	ha_alter_info	The DDL operation
@param[in]	new_tdef    new xengine tbl def
@param[in]	old_key_descr old table key descr
@param[in]	old_n_keys  number of old table keys
@param[in]	new_key_descr new table key descr
@param[in]	new_n_keys  number of new table keys
@return Success or Failure */
int ha_xengine::prepare_inplace_alter_table_rebuild(
    TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info,
    Xdb_tbl_def *const new_tdef,
    std::shared_ptr<Xdb_key_def> *const old_key_descr, uint old_n_keys,
    std::shared_ptr<Xdb_key_def> *const new_key_descr, uint new_n_keys)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(altered_table);
  DBUG_ASSERT(new_tdef);

  // disable subtable major compaction
  std::vector<ColumnFamilyHandle *> column_family_handles;
  for (uint i = 0; i < new_tdef->m_key_count; i++) {
    std::shared_ptr<Xdb_key_def> kd = new_tdef->m_key_descr_arr[i];
    xengine::db::ColumnFamilyHandle *cf = kd->get_cf();
    column_family_handles.push_back(cf);
  }

  // disable the data compaction from L1 to L2
  xengine::util::TransactionDBImpl *txn_db_impl;
  txn_db_impl = dynamic_cast<xengine::util::TransactionDBImpl *>(xdb);
  xengine::common::Status s;
  s = txn_db_impl->GetDBImpl()->switch_major_compaction(column_family_handles,
                                                        false);
  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: switch_major_compaction fail: %s, table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  // disable the garbage old MVCC version for the unique constraint check.
  s = txn_db_impl->GetDBImpl()->disable_backgroud_merge(column_family_handles);
  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: disable_background_merge fail:%s, table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  // for both update old_tbl and new_tbl
  m_tbl_def->m_inplace_new_tdef = new_tdef;
  for (uint i = 0; i < new_tdef->m_key_count; i++) {
    std::shared_ptr<Xdb_key_def> kd = new_tdef->m_key_descr_arr[i];
    m_tbl_def->create_new_keys_info(kd, altered_table);
  }

  // reallocate memory for pack/unpack buffer,
  // make sure new_tdef have enough space for build new table record
  free_key_buffers();
  int err;
  if ((err = alloc_key_buffers(table, m_tbl_def.get(), true))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: failed allocating key buffers during alter, table_name: %s", table->s->table_name.str);
    DBUG_RETURN(err);
  }

  if ((err = prepare_inplace_alter_table_dict(ha_alter_info, altered_table,
                                              table, m_tbl_def.get(),
                                              table->s->table_name.str))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: failed prepare dictionary for online copy ddl, table_name: %s", table->s->table_name.str);
    DBUG_RETURN(err);
  }

  // check whether keys in altered table can skip unique check
  prepare_inplace_alter_table_skip_unique_check(altered_table, new_tdef);

  ha_alter_info->handler_ctx = new Xdb_inplace_alter_ctx(new_tdef);

#if 0
        /*
           Add uncommitted key definitions to ddl_manager.  We need to do this
           so that the property collector can find this keydef when it needs to
           update stats.  The property collector looks for the keydef in the
           data dictionary, but it won't be there yet since this key definition
           is still in the creation process.
           */
        ddl_manager.add_uncommitted_keydefs(added_indexes);
#endif

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::prepare_inplace_alter_table_skip_unique_check(
    TABLE *const altered_table, Xdb_tbl_def *const new_tbl_def) const
{
  DBUG_ENTER_FUNC();

  // for rebuilt table, check whether index is copied from old table
  for (uint i = 0; i < new_tbl_def->m_key_count; i++) {
    auto &kd = new_tbl_def->m_key_descr_arr[i];
    auto it = new_key_stats.find(kd->m_name);
    DBUG_ASSERT(it != new_key_stats.end());
    kd->m_can_skip_unique_check = false;

    /* only when the key definition isn't changed, it is copied from old table
     * One possible optimization is to skip unique check when columns of the key are re-orderd
     * If any key is affected by modified or dropped column, the key will be
     * dropped and re-created, if old key is also renamed, new key will be
     * created with new name, no more renaming operation.
     */
    if (kd->is_hidden_primary_key() ||
        !(altered_table->key_info[i].flags & HA_NOSAME) ||
        (it->second.key_stat == prepare_inplace_key_stat::COPIED) ||
        (it->second.key_stat == prepare_inplace_key_stat::RENAMED)) {
      kd->m_can_skip_unique_check = true;
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** used convert to new xengine_record from old table_record and new added
columns.
@param[in] table MySQL table as it is before the ALTER operation
@param[in] altered_table MySQL table that is being altered
@param[in] dict_info, dictionary for altered table, read_only
@param[in] pk_packed_slice Packed PK tuple. We need it in order to compute and
store its CRC.
@parama[in] pk_unpack_info unpack_info used to decode PK tuple.
@param[out]  Data slice with record data.
@retval HA_EXIT_SUCCESS/HA_EXIT_FAILURE */
int ha_xengine::convert_new_record_from_old_record(
    const TABLE *table, const TABLE *altered_table,
    const std::shared_ptr<Xdb_inplace_ddl_dict_info>& dict_info,
    const xengine::common::Slice &pk_packed_slice,
    Xdb_string_writer *const pk_unpack_info,
    xengine::common::Slice *const packed_rec,
    String& new_storage_record)
{
  int ret = HA_EXIT_SUCCESS;

  new_storage_record.length(0);
  new_storage_record.fill(XENGINE_RECORD_HEADER_LENGTH, 0);

  String null_bytes_str;
  null_bytes_str.length(0);
  null_bytes_str.fill(dict_info->m_null_bytes_in_rec, 0);

  new_storage_record.append(null_bytes_str);

  // If a primary key may have non-empty unpack_info for certain values,
  // (m_maybe_unpack_info=TRUE), we write the unpack_info block. The block
  // itself was prepared in Xdb_key_def::pack_new_record.
  if (dict_info->m_maybe_unpack_info) {
    new_storage_record.append(reinterpret_cast<char *>(pk_unpack_info->ptr()),
                            pk_unpack_info->get_current_pos());
  }

  Xdb_field_encoder *encoder_arr = dict_info->m_encoder_arr;
  uint *col_map = dict_info->m_col_map;

  uint old_col_index = 0;
  for (uint i = 0; i < altered_table->s->fields; i++) {

    /* field may come from old table or new table.
       if new added field, then it comes form altered_table,
       else field comes from old_table.
    */
    Field *field = nullptr;
    old_col_index = col_map[i];
    if (old_col_index == (uint)(-1)) {
      field = altered_table->field[i];
    } else {
      field = table->field[old_col_index];
    }

    //alter operation make column null->not-null
    if (field->is_null() && !encoder_arr[i].maybe_null()) {
      ret = HA_ERR_INVALID_NULL_ERROR;
    }

    /* Don't pack decodable PK key parts */
    if (encoder_arr[i].m_storage_type != Xdb_field_encoder::STORE_ALL) {
      continue;
    }

    if (encoder_arr[i].maybe_null()) {
      char *const data =
          (char *)new_storage_record.ptr() + XENGINE_RECORD_HEADER_LENGTH;
      if (field->is_null()) {
        data[encoder_arr[i].m_null_offset] |= encoder_arr[i].m_null_mask;
        continue;
      }
    }

    if (encoder_arr[i].m_field_type == MYSQL_TYPE_BLOB ||
        encoder_arr[i].m_field_type == MYSQL_TYPE_JSON) {
      auto *field_blob = (my_core::Field_blob *)field;

      append_blob_to_storage_format(new_storage_record, field_blob);
    } else if (encoder_arr[i].m_field_type == MYSQL_TYPE_VARCHAR) {
      auto *field_var = (Field_varstring *)field;

      append_varchar_to_storage_format(new_storage_record, field_var);
    } else {
      /* Copy the field data */
      const uint len = field->pack_length_in_rec();
      new_storage_record.append(reinterpret_cast<char *>(field->ptr), len);
    }
  }

  if (should_store_row_debug_checksums()) {
    const uint32_t key_crc32 = my_core::crc32(
        0, xdb_slice_to_uchar_ptr(&pk_packed_slice), pk_packed_slice.size());
    const uint32_t val_crc32 =
        my_core::crc32(0, xdb_mysql_str_to_uchar_str(&new_storage_record),
                       new_storage_record.length());
    uchar key_crc_buf[XDB_CHECKSUM_SIZE];
    uchar val_crc_buf[XDB_CHECKSUM_SIZE];
    xdb_netbuf_store_uint32(key_crc_buf, key_crc32);
    xdb_netbuf_store_uint32(val_crc_buf, val_crc32);
    new_storage_record.append((const char *)&XDB_CHECKSUM_DATA_TAG, 1);
    new_storage_record.append((const char *)key_crc_buf, XDB_CHECKSUM_SIZE);
    new_storage_record.append((const char *)val_crc_buf, XDB_CHECKSUM_SIZE);
  }

  *packed_rec = xengine::common::Slice(new_storage_record.ptr(),
                                   new_storage_record.length());

  return ret;
}

/** Prepare new added indexes for online inplace ddl.
@param altered_table, new table
@param ha_alter_info, DDL operation
@param new_tdef, new xengine table definition
@param[in]	old_key_descr old table key descr
@param[in]	old_n_keys  number of old table keys
@param[in]	new_key_descr new table key descr
@param[in]	new_n_keys  number of new table keys
@return Success or Failure */
int ha_xengine::prepare_inplace_alter_table_norebuild(
    TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info,
    Xdb_tbl_def *const new_tdef,
    std::shared_ptr<Xdb_key_def> *const old_key_descr, uint old_n_keys,
    std::shared_ptr<Xdb_key_def> *const new_key_descr, uint new_n_keys)
{
  DBUG_ENTER_FUNC();

  uint i;
  uint j;
  const KEY *key;
  std::unordered_set<std::shared_ptr<Xdb_key_def>> added_indexes;
  std::unordered_set<GL_INDEX_ID> dropped_index_ids;

  /* Determine which(if any) key definition(s) need to be dropped */
  for (i = 0; i < ha_alter_info->index_drop_count; i++) {
    key = ha_alter_info->index_drop_buffer[i];
    for (j = 0; j < old_n_keys; j++) {
      if (!old_key_descr[j]->m_name.compare(key->name)) {
        dropped_index_ids.insert(old_key_descr[j]->get_gl_index_id());
        break;
      }
    }
  }

  /* Determine which(if any) key definitions(s) need to be added */
  int drop_indexes_found = 0;
  for (i = 0; i < ha_alter_info->index_add_count; i++) {
    key = &ha_alter_info->key_info_buffer[ha_alter_info->index_add_buffer[i]];
    for (j = 0; j < new_n_keys; j++) {
      if (!new_key_descr[j]->m_name.compare(key->name)) {
        /*
           Check for cases where an 'identical' index is being dropped and
           re-added in a single ALTER statement.  Turn this into a no-op as the
           index has not changed.
           E.G. Unique index -> non-unique index requires no change
           Note that cases where the index name remains the same but the
           key-parts are changed is already handled in create_inplace_key_defs.
           In these cases the index needs to be rebuilt.
           */
        if (dropped_index_ids.count(new_key_descr[j]->get_gl_index_id())) {
          dropped_index_ids.erase(new_key_descr[j]->get_gl_index_id());
          drop_indexes_found++;
        } else {
          added_indexes.insert(new_key_descr[j]);
        }
        break;
      }
    }
  }

  //const std::unique_ptr<xengine::db::WriteBatch> wb = dict_manager.begin();
  //xengine::db::WriteBatch *const batch = wb.get();

  // disable add-index major compaction
  std::vector<ColumnFamilyHandle *> column_family_handles;
  for (const auto &k : added_indexes) {
    xengine::db::ColumnFamilyHandle *cf = k->get_cf();
    column_family_handles.push_back(cf);
  }

  xengine::util::TransactionDBImpl *txn_db_impl;
  txn_db_impl = dynamic_cast<xengine::util::TransactionDBImpl *>(xdb);
  xengine::common::Status s;
  // disable the data compaction from L1 to L2
  s = txn_db_impl->GetDBImpl()->switch_major_compaction(column_family_handles,
                                                        false);

  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: switch_major_compaction fail: %s, table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }
  // this option is only affect flush data from memtable to L0, this option
  // disable the garbage old MVCC version for the unique constraint check.
  s = txn_db_impl->GetDBImpl()->disable_backgroud_merge(column_family_handles);
  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: disable_background_merge fail:%s, table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  // add new indexes as part of m_tbl_def, then we can update new indexes during
  // online ddl phase
  for (const auto &k : added_indexes) {
    m_tbl_def->create_added_key(k, altered_table);
  }

  /* Update the data dictionary */
  std::unordered_set<GL_INDEX_ID> create_index_ids;
  for (const auto &index : added_indexes) {
    create_index_ids.insert(index->get_gl_index_id());
  }
  //dict_manager.add_create_index(create_index_ids, batch);
  //dict_manager.commit(batch);

  const uint n_dropped_keys =
      ha_alter_info->index_drop_count - drop_indexes_found;
  const uint n_added_keys = ha_alter_info->index_add_count - drop_indexes_found;
  DBUG_ASSERT(dropped_index_ids.size() == n_dropped_keys);
  DBUG_ASSERT(added_indexes.size() == n_added_keys);

  // as new writes are allowed to come immediately after this function returns,
  // we should prepare more here than the offline (no-concurrent-write) method

  if (ha_alter_info->handler_flags &
      (my_core::Alter_inplace_info::ADD_INDEX |
       my_core::Alter_inplace_info::ADD_UNIQUE_INDEX)) {
    /*
       Buffers need to be set up again to account for new, possibly longer
       secondary keys.
       */
    free_key_buffers();

    /*
       If adding unique index, allocate special buffers for duplicate checking.
       */
    int err;
    if ((err = alloc_key_buffers(
             table, m_tbl_def.get(),
             ha_alter_info->handler_flags &
                 my_core::Alter_inplace_info::ADD_UNIQUE_INDEX))) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: failed allocating key buffers during alter, table_name: %s", table->s->table_name.str);
      DBUG_RETURN(err);
    }
  }

  ha_alter_info->handler_ctx = new Xdb_inplace_alter_ctx(
      new_tdef, old_key_descr, new_key_descr, old_n_keys, new_n_keys,
      added_indexes, dropped_index_ids, n_added_keys, n_dropped_keys);

  /*
     Add uncommitted key definitions to ddl_manager.  We need to do this
     so that the property collector can find this keydef when it needs to
     update stats.  The property collector looks for the keydef in the
     data dictionary, but it won't be there yet since this key definition
     is still in the creation process.
     */
  ddl_manager.add_uncommitted_keydefs(added_indexes);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
  Alter the table structure in-place with operations specified using
  HA_ALTER_FLAGS and Alter_inplace_info. The level of concurrency allowed
  during this operation depends on the return value from
  check_if_supported_inplace_alter().

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function reports error, commit_inplace_alter_table()
  will be called with commit= false.

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
  by ALTER TABLE and holding data used
  during in-place alter.
  @param    old_table_def     dd::Table object describing old version of
  the table.
  @param    new_table_def     dd::Table object for the new version of the
  table. Can be adjusted by this call if SE
  supports atomic DDL. These changes to the
  table definition will be persisted in the
  data-dictionary at statement commit time.

  @retval   true              Error
  @retval   false             Success
  */
bool ha_xengine::inplace_alter_table(
    TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info,
    const dd::Table *old_table_def, dd::Table *new_table_def)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(altered_table != nullptr);
  DBUG_ASSERT(ha_alter_info != nullptr);
  if (is_instant(ha_alter_info)) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  int res = HA_EXIT_SUCCESS;
  DBUG_ASSERT(ha_alter_info->handler_ctx != nullptr);
  auto ctx = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);

  if (!ctx->m_rebuild && !ctx->m_added_indexes.empty()) {
    /* Populate all new secondary keys by scanning the primary key. */
    // no rebuild, primary key is not changed
    std::shared_ptr<Xdb_key_def> primary_key;
    res = inplace_populate_indexes(altered_table, ctx->m_added_indexes,
                                   primary_key, false);
  } else if (ctx->m_rebuild) {
    /* Populate new primary key-value and new secondary keys for new table. */
    res = inplace_populate_new_table(altered_table, ctx->m_new_tdef.get());
  }

  if (res == HA_ERR_FOUND_DUPP_KEY) {
    __XHANDLER_LOG(WARN,
                   "XEngineDDL: duplicate error happened during ddl "
                   "ddl_rebuild_type: %d, table_name: %s",
                   (int)ctx->m_rebuild, table->s->table_name.str);
  } else if (res == HA_ERR_INVALID_NULL_ERROR) {
    __XHANDLER_LOG(WARN,
                   "XEngineDDL: convert record error happened during ddl "
                   "ddl_rebuild_type: %d, table_name: %s",
                   (int)ctx->m_rebuild, table->s->table_name.str);
  } else if (res) {
    __XHANDLER_LOG(ERROR,
                   "XEngineDDL: failed populating secondary key during "
                   "alter, errcode=%d, ddl_rebuild_type: %d, table_name: %s",
                   res, (int)ctx->m_rebuild, table->s->table_name.str);

  } else {
    __XHANDLER_LOG(INFO,
                   "XEngineDDL: online inplace ddl succcess, rebuild_type: %d, "
                   "table_name: %s",
                   (int)ctx->m_rebuild, table->s->table_name.str);
  }

  DBUG_EXECUTE_IF("myx_simulate_index_create_rollback", {
    dbug_create_err_inplace_alter();
    DBUG_RETURN(true);
  };);

  DBUG_RETURN(res != HA_EXIT_SUCCESS);
}

/** Build indexes for new table
@param[in] new_table_arg, new altered table
@param[in] new_tbl_def, new xengine table definition
@return SUCCESS/FAILURE */
int ha_xengine::inplace_populate_new_table(TABLE *const new_table_arg,
                                           Xdb_tbl_def *const new_tbl_def)
{
  DBUG_ENTER_FUNC();

#ifndef NDEBUG
  __XHANDLER_LOG(INFO, "XEngineDDL: populate new primary key and secondary keys"
                       " for new table: %s", new_table_arg->s->table_name.str);
#endif
  std::unordered_set<std::shared_ptr<Xdb_key_def>> new_indexes;
  std::shared_ptr<Xdb_key_def> primary_key;

  for (uint i = 0; i < new_tbl_def->m_key_count; i++) {
    if (new_tbl_def->m_key_descr_arr[i]->is_primary_key()) {
      DBUG_ASSERT(nullptr == primary_key);
      primary_key = new_tbl_def->m_key_descr_arr[i];
    } else {
#ifndef NDEBUG
      auto ret = new_indexes.insert(new_tbl_def->m_key_descr_arr[i]);
      DBUG_ASSERT(ret.second);
#else
      (void)new_indexes.insert(new_tbl_def->m_key_descr_arr[i]);
#endif
    }
  }

  int ret = HA_EXIT_SUCCESS;
  ret = inplace_populate_indexes(new_table_arg, new_indexes, primary_key, true);
  // error is handled in inplace_populate_indexes
  DBUG_RETURN(ret);
}

/** Avoid the concurrency of creating index and shrinking extent space.
use xengine_inplace_populate_indexes to indicate how many creating tasks
is running.
@param[in] new_table_arg, new mysql table object
@param[in] indexes, new added index from inplace ddl
@param[in] is_rebuild, indicate rebuild table  or not
@return SUCCESS/FAILURE
*/
int ha_xengine::inplace_populate_indexes(
    TABLE *const new_table_arg,
    const std::unordered_set<std::shared_ptr<Xdb_key_def>> &indexes,
    const std::shared_ptr<Xdb_key_def>& primary_key,
    bool is_rebuild)
{
  DBUG_ENTER_FUNC();

  int ret = HA_EXIT_SUCCESS;
  DBUG_EXECUTE_IF("sleep_before_create_second_index", sleep(2););

  xdb_drop_idx_thread.enter_race_condition();
  if (xengine_shrink_table_space >= 0) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: Shrink extent space is running. table_name: %s", table->s->table_name.str);
    ret = HA_ERR_INTERNAL_ERROR;
    xdb_drop_idx_thread.exit_race_condition();
    DBUG_RETURN(ret);
  }

  xengine_inplace_populate_indexes++;
#ifndef NDEBUG
  __XHANDLER_LOG(INFO, "XEngineDDL: Begin inplace populate indexes "
                       "xengine_inplace_populate_indexes = %d table_name:%s",
                 xengine_inplace_populate_indexes, table->s->table_name.str);
#endif
  xdb_drop_idx_thread.exit_race_condition();

  DBUG_EXECUTE_IF("sleep_in_create_second_index", sleep(5););

  Xdb_transaction *tx = get_or_create_tx(table->in_use);
  /*
     There is one specific scenario where m_sst_info may not be nullptr. This
     happens if the handler we're using happens to be the handler where the PK
     bulk load was done on. The sequence of events that lead to this is as
     follows (T1 is PK bulk load, T2 is SK alter table):

     T1: Execute last INSERT statement
     T1: Return TABLE and handler object back to Table_cache_manager
     T1: Close connection
     T2: Execute ALTER statement
     T2: Take same TABLE/handler from Table_cache_manager
     T2: Call closefrm which will call finalize_bulk_load on every other open
     table/handler *except* the one it's on.
     T2: Acquire stale snapshot of PK
     T1: Call finalize_bulk_load

     This is rare because usually, closefrm will call the destructor (and thus
     finalize_bulk_load) on the handler where PK bulk load is done. However, if
     the thread ids of the bulk load thread and the alter thread differ by a
     multiple of table_cache_instances (8 by default), then they hash to the
     same bucket in Table_cache_manager and the alter thread will not not call
     the destructor on the handler it is holding. Thus, its m_sst_info will not
     be nullptr.

     At this point, it is safe to refresh the snapshot because we know all other
     open handlers have been closed at this point, and the one we're on is the
     only one left.
  */
  if (m_sst_info != nullptr) {
    if ((ret = finalize_bulk_load())) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: finalize_bulk_load failed, errcode=%d, table_name: %s", ret, table->s->table_name.str);
      DBUG_RETURN(ret);
    }
    tx->commit();
  }

  if (nullptr != primary_key) {
#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: populate primary key: %u for new table: %s, firstly",
                   primary_key->get_index_number(), new_table_arg->s->table_name.str);
#endif
    DBUG_ASSERT(primary_key->is_primary_key());
    // build the primary key first, to avoid failure xdb_merge.add() on sk
    ret = inplace_populate_index(new_table_arg, primary_key, is_rebuild);
  }

  if (HA_EXIT_SUCCESS == ret) {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_populate_primary_key_done");
    // build all secondary keys
    for (const auto &index : indexes) {
      DBUG_ASSERT(!index->is_primary_key());
      if ((ret = inplace_populate_index(new_table_arg, index, is_rebuild))) {
        break;
      }
    }
  }

  DEBUG_SYNC(ha_thd(), "xengine.inplace_populate_indexes_done");

  DBUG_EXECUTE_IF("sleep_after_index_base_creation", sleep(5););

  if (ret && ha_thd()->mdl_context.upgrade_shared_lock(table->mdl_ticket,
                MDL_EXCLUSIVE, ha_thd()->variables.lock_wait_timeout)) {
    XHANDLER_LOG(ERROR, "update mdl_exclusive lock fail");
    ret = HA_ERR_INTERNAL_ERROR;
  }

  /*
   * Explicitly tell jemalloc to clean up any unused dirty pages at this point.
   * See https://reviews.facebook.net/D63723 for more details.
   */
  purge_all_jemalloc_arenas();

  DBUG_EXECUTE_IF("crash_during_online_index_creation", DBUG_SUICIDE(););

  DBUG_EXECUTE_IF("after_inplace_populate_indexes finish", sleep(5););

  // clear the bulk load context whenever build index succeed or failed.
  // since the manifest txn don't support span thread, we need ensure the
  // bulk load used in single thread, so clear the bulk_load context as
  // early as we can.
  int ret_bulk_load = HA_EXIT_SUCCESS;
  if (m_sst_info != nullptr) {
    if ((ret_bulk_load = finalize_bulk_load())) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: finalize_bulk_load failed, errcode: %d, table_name: %s",
                     ret_bulk_load, table->s->table_name.str);
    }
    tx->commit();
  }

  ret = (ret != HA_EXIT_SUCCESS) ? ret : ret_bulk_load;

  xdb_drop_idx_thread.enter_race_condition();
  xengine_inplace_populate_indexes--;

  if (ret == HA_ERR_FOUND_DUPP_KEY) {
    __XHANDLER_LOG(WARN, "XEngineDDL: duplicate key happened during ddl, table_name: %s", table->s->table_name.str);
  } else if (ret == HA_ERR_INVALID_NULL_ERROR) {
    __XHANDLER_LOG(WARN, "XEngineDDL: convert new record error during ddl, table_name: %s", table->s->table_name.str);
  } else if (ret) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: failed to finish inplace populate indexes"
                          " xengine_inplace_populate_indexes = %d, errcode: %d, table_name: %s",
                   xengine_inplace_populate_indexes, ret, table->s->table_name.str);
  } else {
    __XHANDLER_LOG(INFO, "XEngineDDL: inplace populate indexes successfully "
                         " xengine_inplace_populate_indexes = %d, table_name: %s",
                   xengine_inplace_populate_indexes, table->s->table_name.str);
  }

  xdb_drop_idx_thread.exit_race_condition();
  DBUG_RETURN(ret);
}

// update Added_key_info.status when duplicate entry is found during inplace DDL
int ha_xengine::inplace_update_added_key_info_status_dup_key(
    TABLE *const new_table_arg, const std::shared_ptr<Xdb_key_def>& index,
    Added_key_info& added_key_info, const common::Slice& dup_key,
    const common::Slice& dup_val, bool is_rebuild, bool use_key/* =  true*/)
{
  int res = HA_EXIT_SUCCESS;
  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];

  uint old_status = HA_EXIT_SUCCESS;
  bool rc = added_key_info.status.compare_exchange_strong(
                old_status, HA_ERR_FOUND_DUPP_KEY);
  if (rc) {
    if (use_key) {
      // using dup_key and dup_val
      if ((res = fill_new_duplicate_record(index.get(), new_table_arg,
                                           dup_key, dup_val, is_rebuild))) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: Fill new duplicate record error for index %d, error %d, table_name: %s",
                       index->get_index_number(), res, table->s->table_name.str);
      } else {
#ifndef NDEBUG
        __XHANDLER_LOG(INFO, "XEngineDDL: Fill new duplicate record for index %d successfully, table_name: %s",
                       index->get_index_number(), table->s->table_name.str);
#endif
      }
    } else {
      // using table->record[0]
      if ((res = set_duplicate_key_for_print(new_table_arg, table, index.get(),
                                             m_tbl_def->m_dict_info, is_rebuild))) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: Generate duplicate key for index %d, error %d, table_name:%s",
                       index->get_index_number(), res, table->s->table_name.str);
      } else {
#ifndef NDEBUG
        __XHANDLER_LOG(INFO, "XEngineDDL: Generate duplicate key for index %d successfully, table_name:%s",
                       index->get_index_number(), table->s->table_name.str);
#endif
      }
    }
    added_key_info.dup_key_saved.store(true);
    print_keydup_error(new_table_arg, key_info, MYF(0));
    res = HA_ERR_FOUND_DUPP_KEY;
  } else if (HA_ERR_FOUND_DUPP_KEY == added_key_info.status.load()) {
    __XHANDLER_LOG(INFO, "XEngineDDL: other one set duplicate entry for index %d, table_name: %s",
                   index->get_index_number(), table->s->table_name.str);
    // the duplicate key is set by other, we wait for the duplicate key to be saved
    while (!added_key_info.dup_key_saved.load()) {}
    print_keydup_error(new_table_arg, key_info, MYF(0));
    res = HA_ERR_FOUND_DUPP_KEY;
  } else {
    res = (int)added_key_info.status.load();
    __XHANDLER_LOG(INFO, "XEngineDDL: error %u for index %d is set by other, table_name: %s",
                   res, index->get_index_number(), table->s->table_name.str);
  }
  return res;
}

void ha_xengine::print_dup_err(int res, std::atomic<int>& dup_ctx_id,
                               const std::shared_ptr<Xdb_key_def>& index,
                               bool is_rebuild, TABLE *const new_table_arg,
                               std::vector<xengine::common::Slice>& dup_key,
                               std::vector<xengine::common::Slice>& dup_val,
                               Added_key_info& added_key_info, KEY* key_info) {
  if (dup_ctx_id >= 0) {
    // ddl dup
    res = inplace_update_added_key_info_status_dup_key(new_table_arg, index,
                                                       added_key_info, dup_key[dup_ctx_id], dup_val[dup_ctx_id], is_rebuild);
  } else if (res == HA_ERR_FOUND_DUPP_KEY) {
    // dml dup
    print_keydup_error(new_table_arg, key_info, MYF(0));
  }
}

void ha_xengine::print_common_err(int res) {
  if (res == HA_ERR_INVALID_NULL_ERROR) {
    my_error(ER_INVALID_USE_OF_NULL, MYF(0));
  } else if (res == HA_ERR_INTERNAL_ERROR) {
    my_error(ER_UNKNOWN_ERROR, MYF(0));
  } else if (res == HA_ERR_XENGINE_OUT_OF_SORTMEMORY) {
    my_printf_error(ER_OUT_OF_SORTMEMORY,
                    "xengine_sort_buffer_size is too small to process merge. "
                    "Please set xengine_sort_buffer_size to a higher value.",MYF(0));
  }
}

int ha_xengine::build_index(xengine::common::Slice& iter_key,
                            xengine::common::Slice& iter_val,
                            ParallelDDLScanCtx& ddl_ctx,
                            xengine::common::Slice& key,
                            xengine::common::Slice& val, TABLE* tbl,
                            bool hidden_pk_exists,
                            const std::shared_ptr<Xdb_key_def>& index,
                            bool is_rebuild, TABLE *const new_table_arg) {

  int res = HA_EXIT_SUCCESS;
  // pk
  ddl_ctx.primary_key.copy(iter_key.data(), iter_key.size(), &my_charset_bin);
  // record
  if ((res = convert_record_from_storage_format(
           &iter_key, &iter_val, tbl->record[0], tbl))) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: Error getting record, table_name: %s", tbl->s->table_name.str);
  }

  longlong hidden_pk_id = 0;
  if (hidden_pk_exists && read_hidden_pk_id_from_rowkey(
                              &hidden_pk_id, &ddl_ctx.primary_key)) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: Error retrieving hidden pk id, table_name: %s", tbl->s->table_name.str);
    return HA_ERR_INTERNAL_ERROR;
  }

  if (!is_rebuild) {
    /* Create new secondary index entry */
    const uint new_packed_size = index->pack_record(
        tbl, ddl_ctx.pack_buffer, tbl->record[0], ddl_ctx.sk_packed_tuple,
        &ddl_ctx.sk_tails, should_store_row_debug_checksums(), hidden_pk_id,
        0, nullptr, new_table_arg);
    DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

    key = xengine::common::Slice(
        reinterpret_cast<const char *>(ddl_ctx.sk_packed_tuple),
        new_packed_size);
    val = xengine::common::Slice(
        reinterpret_cast<const char *>(ddl_ctx.sk_tails.ptr()),
        ddl_ctx.sk_tails.get_current_pos());
  } else if (index->is_primary_key()) {
    /*** rebuild primary key **/
    if (index->is_hidden_primary_key()) {
      DBUG_ASSERT(hidden_pk_exists);
    }

    uint new_packed_size = 0;
    if ((res = index->pack_new_record(
        tbl, ddl_ctx.pack_buffer, tbl->record[0], ddl_ctx.pk_packed_tuple,
        &ddl_ctx.pk_unpack_info, false, hidden_pk_id,
        0, nullptr, new_table_arg,
        m_tbl_def->m_dict_info, new_packed_size))) {
      if (res != HA_ERR_INVALID_NULL_ERROR) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", tbl->s->table_name.str);
      }
      return res;
    }
    DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

    key = xengine::common::Slice(
        reinterpret_cast<const char *>(ddl_ctx.pk_packed_tuple),
        new_packed_size);

    if ((res = convert_new_record_from_old_record(
             tbl, new_table_arg, m_tbl_def->m_dict_info, key,
             &ddl_ctx.pk_unpack_info, &val, ddl_ctx.new_storage_record))) {
      __XHANDLER_LOG(WARN, "XEngineDDL: convert new record error, table_name: %s, code is %d", tbl->s->table_name.str, res);
      return res;
    }
  } else {
    /*** rebuild secondary key **/
    DBUG_ASSERT(index->is_secondary_key());
    uint new_packed_size = 0;

    if (index->pack_new_record(
            tbl, ddl_ctx.pack_buffer, tbl->record[0], ddl_ctx.sk_packed_tuple,
            &ddl_ctx.sk_tails, should_store_row_debug_checksums(),
            hidden_pk_id, 0, nullptr, new_table_arg,
            m_tbl_def->m_dict_info, new_packed_size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", tbl->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
    DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

    key = xengine::common::Slice(
        reinterpret_cast<const char *>(ddl_ctx.sk_packed_tuple),
        new_packed_size);
    val = xengine::common::Slice(
        reinterpret_cast<const char *>(ddl_ctx.sk_tails.ptr()),
        ddl_ctx.sk_tails.get_current_pos());
  }
  return res;
}

int ha_xengine::build_base_global_merge(
    std::vector<std::shared_ptr<ParallelDDLScanCtx>>& ddl_ctx_set,
    const std::shared_ptr<Xdb_key_def>& index, bool is_rebuild,
    TABLE *const new_table_arg, size_t max_threads, bool need_unique_check,
    Added_key_info& added_key_info, size_t part_id,
    xengine::common::Slice& dup_key, xengine::common::Slice& dup_val,
    std::atomic<int>& dup_ctx_id) {

  int res = HA_EXIT_SUCCESS;

  xengine::db::MiniTables tables;
  xengine::storage::ChangeInfo change_info;
  xengine::db::ColumnFamilyHandle *const cf = index->get_cf();
  DBUG_ASSERT(cf != nullptr);
  tables.change_info_ = &change_info;
  tables.level = 2;

  Xdb_sst_info sst_info(
      xdb, m_table_handler->m_table_name, index->get_name(), cf,
      xengine_db_options, false/*THDVAR(ha_thd(), trace_sst_api)*/, &tables);

  const xengine::util::Comparator *index_comp =
      index->get_cf()->GetComparator();
  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];

  std::vector<xengine::common::Slice> merge_keys(max_threads);
  std::vector<xengine::common::Slice> merge_vals(max_threads);

  auto cmp = [&merge_keys, &index_comp](const int& lhs, const int& rhs) {
    return index_comp->Compare(merge_keys[rhs], merge_keys[lhs]) < 0;};
  std::priority_queue<int, std::vector<int>, decltype(cmp)> pq(cmp);

  for (int i = 0; i < static_cast<int>(max_threads); ++i) {
    if ((res = ddl_ctx_set[i]->bg_merge.next(&merge_keys[i], &merge_vals[i],
                                             part_id)) == 0)
      pq.push(i);
    else if (res != HA_ERR_END_OF_FILE)
      return res;
  }

  /* must call my_free to release memory before exiting */
  uchar* dup_sk_packed_tuple = nullptr;
  uchar* dup_sk_packed_tuple_old = nullptr;
  if (need_unique_check &&
      (!(dup_sk_packed_tuple = reinterpret_cast<uchar *>(my_malloc(
             PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
       !(dup_sk_packed_tuple_old = reinterpret_cast<uchar *>(my_malloc(
             PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))))) {
    my_free(dup_sk_packed_tuple);
    my_free(dup_sk_packed_tuple_old);
    __XHANDLER_LOG(ERROR, "XEngineDDL: allocate memory for sk duplication check fail");
    return HA_ERR_INTERNAL_ERROR;
  }

  unique_key_buf_info key_buf(dup_sk_packed_tuple, dup_sk_packed_tuple_old);
  uint64_t merged_count = 0;
  while (pq.size() > 0) {
    int idx = pq.top();
    xengine::common::Slice& merge_key = merge_keys[idx];
    xengine::common::Slice& merge_val = merge_vals[idx];

    /* Perform uniqueness check if needed */
    // xdb_merge.add can only detect duplication in singe (merge) segment
    if (need_unique_check) {
      if (check_duplicate_in_base(new_table_arg, *index, merge_key, &key_buf)) {
        __XHANDLER_LOG(WARN, "XEngineDDL: duplicate entry found during xdb_merge, table_name:%s",
                       table->s->table_name.str);
        dup_key = merge_key;
        dup_val = merge_val;
        dup_ctx_id = part_id;
        res = HA_ERR_FOUND_DUPP_KEY;
        break;
      }
    }

    // Check where duplicateentry is found during DML
    if (merged_count &&
        (0 == merged_count % BUILD_BASE_CHECK_ERROR_FREQUENCY) &&
        (res = inplace_check_dml_error(new_table_arg, key_info,
                                       added_key_info, false))) {
      __XHANDLER_LOG(WARN, "XEngineDDL: error status found during DML, code is %d, table_name:%s",
                     res, table->s->table_name.str);
      break;
    }

    /*
       Insert key and slice to SST via SSTFileWriter API.
       Use mirror index (cfh) to save the index of (t0, t1).
       */
    if ((res = sst_info.put(merge_key, merge_val))) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: Error while bulk loading keys in "
                            "external merge sort, table_name:%s",
                     table->s->table_name.str);
      break;
    }

    ++merged_count;
    pq.pop();
    if ((res = ddl_ctx_set[idx]->bg_merge.next(
             &merge_keys[idx], &merge_vals[idx], part_id)) == 0)
      pq.push(idx);
    else if (res != HA_ERR_END_OF_FILE) // error occur in next2
      break;
    if (table->in_use->killed) {
      res = HA_ERR_QUERY_INTERRUPTED;
      break;
    }
  }
  __XHANDLER_LOG(INFO, "PART: %d, CNT: %d", part_id, merged_count);

  int bulk_load_ret = sst_info.commit();
  if (res == HA_ERR_END_OF_FILE && (res = bulk_load_ret)) {
    // NO_LINT_DEBUG
    __XHANDLER_LOG(ERROR, "XEngineDDL: Error finishing bulk load, table_name:%s",
                   table->s->table_name.str);
  }

  my_free(dup_sk_packed_tuple);
  my_free(dup_sk_packed_tuple_old);

  return res;
}

int ha_xengine::inplace_build_base_phase_parallel(
    TABLE *const new_table_arg, const std::shared_ptr<Xdb_key_def>& index,
    bool need_unique_check, Added_key_info& added_key_info, bool is_rebuild) {
  __XHANDLER_LOG(INFO, "XEngineDDL: inplace build base parallel start");

  DBUG_ENTER_FUNC();

  const bool hidden_pk_exists = has_hidden_pk(table);

  int res = HA_EXIT_SUCCESS;
  Xdb_transaction *tx = get_or_create_tx(table->in_use);

  if (ha_thd()->mdl_context.upgrade_shared_lock(
      table->mdl_ticket, MDL_EXCLUSIVE,
      ha_thd()->variables.lock_wait_timeout)) {
    XHANDLER_LOG(ERROR, "XEngineDDL: update mdl_exclusive lock fail");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  // ensure we get a newest snapshot for the table.
  if (tx->has_snapshot()) {
    tx->release_snapshot();
  }

  /*
  * For rebuilding of secondary key, rebuild population of primary
  * key has been finished at this stage, but duplication may happen
  * on primary key during DML before we get snapshot.
  */
  if (is_rebuild && index->is_secondary_key()) {
    auto iter = std::find_if(
        m_tbl_def->m_inplace_new_keys.cbegin(),
        m_tbl_def->m_inplace_new_keys.cend(),
        [&](const std::pair<const Xdb_key_def*, Added_key_info>& key) {
          return key.first->is_primary_key();
        });
    DBUG_ASSERT(iter != m_tbl_def->m_inplace_new_keys.cend());

    KEY *primary_key = &new_table_arg->key_info[iter->first->get_keyno()];
    if ((res = inplace_check_dml_error(new_table_arg, primary_key,
                                       iter->second))) {
      XHANDLER_LOG(WARN, "XEngineDDL: error happened on primary key "
                         "while building base for subtable",
                   "subtable_id", index->get_index_number(), "code", res);
      DBUG_RETURN(res);
    }
  }

  const uint pk = pk_index(table, m_tbl_def.get());
  ha_index_init(pk, true);

  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];

  inplace_update_added_key_info_step(added_key_info,
                                     Added_key_info::BUILDING_BASE_INDEX);

  table->mdl_ticket->downgrade_lock(MDL_SHARED_UPGRADABLE);

  if (index->is_secondary_key()) {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_create_sk_scan_base_begin");
  } else {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_copy_ddl_scan_base_begin");
  }

  /* Initialize variables for parallelddl */
  size_t max_threads = get_parallel_read_threads();
  std::vector<std::shared_ptr<ParallelDDLScanCtx>> ddl_ctx_set(max_threads);
  const xengine::util::Comparator *index_comp =
      index->get_cf()->GetComparator();
  ulonglong xengine_merge_combine_read_size =
      (xengine_sort_buffer_size * XENGINE_MERGE_COMBINE_READ_SIZE_RATIO);
  const size_t xengine_merge_sample_mem_limit = xengine_sort_buffer_size;
  for (int i = 0; i < static_cast<int>(ddl_ctx_set.size()); i++) {
    ddl_ctx_set[i].reset(new ParallelDDLScanCtx(
        this, std::make_shared<Xdb_index_merge>(
                  thd_xengine_tmpdir(),
                  xengine_sort_buffer_size,
                  xengine_merge_combine_read_size / max_threads, index_comp,
                  max_threads, xengine_merge_sample_mem_limit / max_threads),
        max_threads));
    if (ddl_ctx_set[i]->init())
      DBUG_RETURN(HA_ADMIN_CORRUPT);
  }
  std::atomic<int> dup_ctx_id{-1};
  std::vector<xengine::common::Slice> dup_key(max_threads);
  std::vector<xengine::common::Slice> dup_val(max_threads);

  /* Step1: Scan all records and build new index */
  auto f_scan = [this, &ddl_ctx_set, is_rebuild, new_table_arg, &dup_ctx_id,
                 hidden_pk_exists, &index, &dup_key, &dup_val](
                    const xengine::common::ParallelReader::ExecuteCtx *ctx,
                    db::Iterator *db_iter) {
    int res = HA_EXIT_SUCCESS;

    ParallelDDLScanCtx* ddl_ctx = ddl_ctx_set[ctx->thread_id_].get();
    TABLE& tbl = ddl_ctx->thd_table;
    xengine::common::Slice iter_key = db_iter->key();
    xengine::common::Slice iter_val = db_iter->value();

    xengine::common::Slice key;
    xengine::common::Slice val;
    if ((res = this->build_index(iter_key, iter_val, *ddl_ctx, key, val,
                                 &tbl, hidden_pk_exists, index, is_rebuild,
                                 new_table_arg))) {
//      __XHANDLER_LOG(ERROR, "XEngineDDL: build index error, table_name: %s", tbl.s->table_name.str);
      return res;
    }
    /*
       Add record to offset tree in preparation for writing out to
       disk in sorted chunks.
    */
    bool inserted = false;
    res = ddl_ctx->xdb_merge->add(key, val, inserted);
    if (res) {
      XHANDLER_LOG(ERROR, "Failed add record to sort merge buffer",
                   "value_size", val.size());
      return res;
    } else if (!inserted) {
      dup_ctx_id = ctx->thread_id_;
      dup_key[ctx->thread_id_] = ddl_ctx->xdb_merge->get_dup_key();
      dup_val[ctx->thread_id_] = ddl_ctx->xdb_merge->get_dup_val();
      return HA_ERR_FOUND_DUPP_KEY;
    }
    return 0;
  };

  if ((res = scan_parallel(m_key_descr_arr[pk].get(), tx, std::move(f_scan)))) {
    print_dup_err(res, dup_ctx_id, index, is_rebuild, new_table_arg, dup_key,
                  dup_val, added_key_info, key_info);
    ha_index_end();
    DBUG_RETURN(res);
  }
  ha_index_end();
  __XHANDLER_LOG(INFO, "Scan Finished");

  /* Step2: Sort and choose (partition_num-1) sample sort */
  std::vector<xengine::common::Slice> sample_tmp, sample;
  for (auto& ctx : ddl_ctx_set) {
    ctx->xdb_merge->get_sample(sample_tmp);
  }
  std::sort(sample_tmp.begin(), sample_tmp.end(),
            [index_comp] (const xengine::common::Slice& lhs,
                         const xengine::common::Slice& rhs) {
    return index_comp->Compare(rhs, lhs) > 0;});
  size_t split_interval = std::ceil(static_cast<double>(sample_tmp.size()) /
                                    max_threads);
  for (size_t i = split_interval; i < sample_tmp.size(); i += split_interval) {
    sample.push_back(sample_tmp[i]);
  }
  __XHANDLER_LOG(INFO, "Deal Sample Finished");

  /* Step3: When one thread's scan finished, start background local merge */
  auto f_local_merge = [this, &ddl_ctx_set, &sample](size_t thread_id) {
    if (thread_id >= ddl_ctx_set.size()) {
      DBUG_ASSERT(0);
    }
    return ddl_ctx_set[thread_id]->bg_merge.merge(sample, table->in_use);
  };
  ParallelDDLMergeCtx local_merge_ctx(ddl_ctx_set, f_local_merge);
  bool inject_err = false;
  DBUG_EXECUTE_IF("crash_during_local_merge", inject_err = true;);
  DBUG_EXECUTE_IF("parallel_ddl_local_merge_kill",
                  table->in_use->killed = THD::KILL_QUERY;);
  local_merge_ctx.start(max_threads, inject_err);
  if ((res = local_merge_ctx.finish())) {
    DBUG_EXECUTE_IF("crash_during_local_merge", {
      if (ddl_ctx_set[0]->bg_merge.get_interrupt()) {
      DEBUG_SYNC(ha_thd(), "xengine.local_merge_interrupted");
      };
    });
    DBUG_RETURN(res);
  }
  __XHANDLER_LOG(INFO, "Local Merge Finished");

  /* Step4: Start global merge, each partition perform an n-way merge of
   * n sorted buffers on disk, then writes all results to XENGINE via
   * SSTFileWriter API.
   */
  DEBUG_SYNC(ha_thd(), "xengine.build_base_global_merge_start");
  bool test_stack_err = false;
  DBUG_EXECUTE_IF("xengine.test_pddl_stack_err", test_stack_err = true;);

  dup_ctx_id = -1;
  auto f_global_merge = [this, &ddl_ctx_set, &index, is_rebuild, new_table_arg,
                         max_threads, need_unique_check, &added_key_info,
                         &dup_key, &dup_val, &dup_ctx_id, test_stack_err] (size_t part_id) {
    if (test_stack_err) {
      // wait for function return
      sleep(10);
    }

    return this->build_base_global_merge(ddl_ctx_set, index, is_rebuild,
                                         new_table_arg, max_threads,
                                         need_unique_check, added_key_info,
                                         part_id, dup_key[part_id],
                                         dup_val[part_id], dup_ctx_id);
  };
  ParallelDDLMergeCtx global_merge_ctx(ddl_ctx_set, f_global_merge);
  inject_err = false;
  DBUG_EXECUTE_IF("crash_during_global_merge", inject_err = true;);
  DBUG_EXECUTE_IF("parallel_ddl_global_merge_kill", table->in_use->killed = THD::KILL_QUERY;);
  global_merge_ctx.start(max_threads, inject_err);

  DBUG_EXECUTE_IF("xengine.test_pddl_stack_err", DBUG_RETURN(HA_ERR_INTERNAL_ERROR););

  if ((res = global_merge_ctx.finish())) {
    DBUG_EXECUTE_IF("crash_during_global_merge", {
      if (ddl_ctx_set[0]->bg_merge.get_interrupt()) {
        DEBUG_SYNC(ha_thd(), "xengine.global_merge_interrupted");
      };
    });
    print_dup_err(res, dup_ctx_id, index, is_rebuild, new_table_arg, dup_key,
                  dup_val, added_key_info, key_info);
    DBUG_RETURN(res);
  }

  __XHANDLER_LOG(INFO, "Global Merge Finished");

  __XHANDLER_LOG(INFO, "XEngineDDL: inplace build base parallel end");

  DBUG_RETURN(res);
}

/* Build Base data for new indexes
@param[in] new_table_arg
@param[in] index, xengine index definition
@param[in] need_unique_check, need unique check or not
@param[in] added_key_info: used to call inplace_check_dml_error
@param[in] is_rebuild
@return */
int ha_xengine::inplace_build_base_phase(TABLE *const new_table_arg,
    const std::shared_ptr<Xdb_key_def>& index, bool need_unique_check,
    Added_key_info& added_key_info, bool is_rebuild)
{
  if (!xengine_disable_parallel_ddl && get_parallel_read_threads() > 1)
    return inplace_build_base_phase_parallel(new_table_arg, index,
                                             need_unique_check, added_key_info,
                                             is_rebuild);

  DBUG_ENTER_FUNC();

  const bool hidden_pk_exists = has_hidden_pk(table);

  int res = HA_EXIT_SUCCESS;
  Xdb_transaction *tx = get_or_create_tx(table->in_use);

  const xengine::util::Comparator *index_comp =
      index->get_cf()->GetComparator();
  ulonglong xengine_merge_combine_read_size =
      (xengine_sort_buffer_size * XENGINE_MERGE_COMBINE_READ_SIZE_RATIO);
  Xdb_index_merge xdb_merge(thd_xengine_tmpdir(), xengine_sort_buffer_size,
                            xengine_merge_combine_read_size, index_comp);

  if ((res = xdb_merge.init())) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: xdb_merge init failed, errcode=%d, table_name: %s", res, table->s->table_name.str);
    DBUG_RETURN(res);
  }

  if (ha_thd()->mdl_context.upgrade_shared_lock(
          table->mdl_ticket, MDL_EXCLUSIVE,
          ha_thd()->variables.lock_wait_timeout)) {
    XHANDLER_LOG(ERROR, "XEngineDDL: update mdl_exclusive lock fail");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  // ensure we get a newest snapshot for the table.
  if (tx->has_snapshot()) {
    tx->release_snapshot();
  }

  /* For rebuilding of secondary key, rebuild population of primary
   * key has been finished at this stage, but duplication may happen
   * on primary key during DML before we get snapshot.
   */
  if (is_rebuild && index->is_secondary_key()) {
    auto iter = std::find_if(
        m_tbl_def->m_inplace_new_keys.cbegin(),
        m_tbl_def->m_inplace_new_keys.cend(),
        [&](const std::pair<const Xdb_key_def*, Added_key_info>& key) {
          return key.first->is_primary_key();
        });
    DBUG_ASSERT(iter != m_tbl_def->m_inplace_new_keys.cend());

    KEY *primary_key = &new_table_arg->key_info[iter->first->get_keyno()];
    if ((res = inplace_check_dml_error(new_table_arg, primary_key,
                                       iter->second))) {
      XHANDLER_LOG(WARN, "XEngineDDL: error happened on primary key "
                         "while building base for subtable",
                   "subtable_id", index->get_index_number(), "code", res);
      DBUG_RETURN(res);
    }
  }

  const uint pk = pk_index(table, m_tbl_def.get());
  ha_index_init(pk, true);

  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];
  // get snapshot
  res = index_first(table->record[0]);
  if (res && res != HA_ERR_END_OF_FILE) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: failed to get first record, errcode=%d, table_name: %s", res, table->s->table_name.str);
    ha_index_end();
    table->mdl_ticket->downgrade_lock(MDL_SHARED_UPGRADABLE);
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  inplace_update_added_key_info_step(added_key_info, Added_key_info::BUILDING_BASE_INDEX);

  table->mdl_ticket->downgrade_lock(MDL_SHARED_UPGRADABLE);

  if (index->is_secondary_key()) {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_create_sk_scan_base_begin");
  } else {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_copy_ddl_scan_base_begin");
  }

  bool processed_error = false;
  /* Scan each record in the primary key in order */
  for (; res == 0; res = index_next(table->record[0])) {
    longlong hidden_pk_id = 0;
    if (hidden_pk_exists && read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: Error retrieving hidden pk id, table_name: %s", table->s->table_name.str);
      ha_index_end();
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    xengine::common::Slice key;
    xengine::common::Slice val;
    if (!is_rebuild) {
      /* Create new secondary index entry */
      const uint new_packed_size = index->pack_record(
          table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
          &m_sk_tails, should_store_row_debug_checksums(), hidden_pk_id, 0, nullptr, new_table_arg);
      DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

      key = xengine::common::Slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
      val = xengine::common::Slice(
          reinterpret_cast<const char *>(m_sk_tails.ptr()),
          m_sk_tails.get_current_pos());
    } else if (index->is_primary_key()) {
      /*** rebuild primary key **/
      if (index->is_hidden_primary_key()) {
        DBUG_ASSERT(hidden_pk_exists);
      }

      uint new_packed_size = 0;
      if ((res = index->pack_new_record(
          table, m_pack_buffer, table->record[0], m_pk_packed_tuple,
          &m_pk_unpack_info, false, hidden_pk_id,
          0, nullptr, new_table_arg,
          m_tbl_def->m_dict_info, new_packed_size))) {
        if (res != HA_ERR_INVALID_NULL_ERROR) {
          __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
        }
        ha_index_end();
        DBUG_RETURN(res);
      }
      DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

      key = xengine::common::Slice(
          reinterpret_cast<const char *>(m_pk_packed_tuple), new_packed_size);

      if ((res = convert_new_record_from_old_record(table, new_table_arg,
                                                    m_tbl_def->m_dict_info, key,
                                                    &m_pk_unpack_info, &val,
                                                    m_new_storage_record))) {
         __XHANDLER_LOG(WARN, "XEngineDDL: convert new record error, table_name: %s, code is %d", table->s->table_name.str, res);
         my_error(ER_INVALID_USE_OF_NULL, MYF(0));
         processed_error = true;
         break;
      }
    } else {
      /*** rebuild secondary key **/
      DBUG_ASSERT(index->is_secondary_key());
      uint new_packed_size = 0;

      if (index->pack_new_record(
              table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
              &m_sk_tails, should_store_row_debug_checksums(), hidden_pk_id, 0,
              nullptr, new_table_arg, m_tbl_def->m_dict_info,
              new_packed_size)) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
        ha_index_end();
        DBUG_RETURN(HA_EXIT_FAILURE);
      }
      DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

      key = xengine::common::Slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
      val = xengine::common::Slice(
          reinterpret_cast<const char *>(m_sk_tails.ptr()),
          m_sk_tails.get_current_pos());
    }

    /*
       Add record to offset tree in preparation for writing out to
       disk in sorted chunks.
    */
    bool inserted = false;
    res = xdb_merge.add(key, val, inserted);
    if (res) {
      XHANDLER_LOG(ERROR, "Failed add record to sort merge buffer",
                   "value_size", val.size());
      processed_error = true;
      break;
    } else if (!inserted) {
      // insert failed indicates there is duplicate key in current merge buffer
      // For primary key, if xdb_merge.add with failure(m_offset_tree.emplace
      // returns false), it means the key here is a duplicate key.
      // For secondary key, currently, the key here composites user key
      // columns/parts with primary key, even if there is duplication among user
      // key columns/parts, the key here will not be same.
      if (need_unique_check) {
        // for utf8mb4_0900_ai_ci, we can't unpack record directly, so we get
        // value from old_record and default value.
        // using record[0] to call set_duplicate_key_for_print
        res = inplace_update_added_key_info_status_dup_key(new_table_arg,
                  index, added_key_info, Slice(), Slice(), is_rebuild, false);
      } else {
        XHANDLER_LOG(ERROR, "XEngineDDL: Unexpected error: failed to do "
                            "xdb_merge.add while building base don't check unique",
                     "table_name", table->s->table_name.str);
        DBUG_ASSERT(false);
      }
      ha_index_end();
      DBUG_RETURN(res);
    }

    DBUG_EXECUTE_IF("serial_ddl_scan_phase", table->in_use->killed = THD::KILL_QUERY;);
    if (my_core::thd_killed(current_thd)) {
      ha_index_end();
      DBUG_RETURN(HA_ERR_QUERY_INTERRUPTED);
    }
  }

  if (res != HA_ERR_END_OF_FILE) {
    // NO_LINT_DEBUG
    if (!processed_error) {
      XHANDLER_LOG(ERROR, "XEngineDDL: Error retrieving index entry from primary key",
                   "code", res, "table_name", table->s->table_name.str);
    }
    ha_index_end();
    DBUG_RETURN(res);
  }

  ha_index_end();

  /*
     Perform an n-way merge of n sorted buffers on disk, then writes all
     results to XENGINE via SSTFileWriter API.
     */
  xengine::common::Slice merge_key;
  xengine::common::Slice merge_val;
  unique_key_buf_info key_buf(m_dup_sk_packed_tuple, m_dup_sk_packed_tuple_old);
  uint64_t merged_count = 0;
  while ((res = xdb_merge.next(&merge_key, &merge_val)) == 0) {
    /* Perform uniqueness check if needed */
    // xdb_merge.add can only detect duplication in singe (merge) segment
    if (need_unique_check) {
      if (check_duplicate_in_base(new_table_arg, *index, merge_key, &key_buf)) {
        /*
           Duplicate entry found when trying to create unique secondary key.
           We need to unpack the record into new_table_arg->record[0] as it
           is used inside print_keydup_error so that the error message shows
           the duplicate record.

           for utf8mb4_0900_ai_ci, we can't unpack record directly, so we get
           value from old_record and default value.
           For Unique-PK, we need use pk to search old_table and fill data,
           and then fill new_data record.
           using merge_key/merge_val to call fill_new_duplicate_record
        */
        __XHANDLER_LOG(WARN, "XEngineDDL: duplicate entry found during xdb_merge, table_name:%s",
                       table->s->table_name.str);
        res = inplace_update_added_key_info_status_dup_key(new_table_arg, index,
                  added_key_info, merge_key, merge_val, is_rebuild);
        break;
      }
    }

    // Check where duplicate entry is found during DML
    if (merged_count &&
        (0 == merged_count % BUILD_BASE_CHECK_ERROR_FREQUENCY) &&
        (res = inplace_check_dml_error(new_table_arg, key_info, added_key_info))) {
      __XHANDLER_LOG(WARN, "XEngineDDL: error status found during DML, code is %d, table_name:%s",
                     res, table->s->table_name.str);
      break;
    }

    /*
       Insert key and slice to SST via SSTFileWriter API.
       Use mirror index (cfh) to save the index of (t0, t1).
       */
    if ((res = bulk_load_key(tx, *index, merge_key, merge_val, 2))) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: Error while bulk loading keys in "
                            "external merge sort, table_name:%s",
                     table->s->table_name.str);
      break;
    }
    ++merged_count;

    DBUG_EXECUTE_IF("serial_ddl_merge_phase", table->in_use->killed = THD::KILL_QUERY;);
    if (my_core::thd_killed(current_thd)) {
      // can't return directly, need to finish_bulk_load()
      break;
    }
  }

  // we need to finish bulk load whether there was error happened
  int bulk_load_ret = tx->finish_bulk_load();

  if (my_core::thd_killed(current_thd)) {
    DBUG_RETURN(HA_ERR_QUERY_INTERRUPTED);
  }

  // Here, res == HA_ERR_END_OF_FILE means that we are finished
  if (res == HA_ERR_END_OF_FILE && (res = bulk_load_ret)) {
    // NO_LINT_DEBUG
    __XHANDLER_LOG(ERROR, "XEngineDDL: Error finishing bulk load, table_name:%s",
                   table->s->table_name.str);
  }

  DBUG_RETURN(res);
}

/* Check whether given user key sequences have unique conflict
 * return true which means there is unique conflict when there are two adjacent
 * PUT (after sort by sequence number) in the given user key sequences
 */
bool ha_xengine::check_user_key_sequence(std::vector<UserKeySequence> &user_key_sequences)
{
  if (user_key_sequences.size() > 1) {
    // sort by sequence in descending order
    std::sort(user_key_sequences.begin(), user_key_sequences.end(),
              [](const UserKeySequence &key1, const UserKeySequence &key2) {
                return key1.first > key2.first;
              });
    // check all delta sequences and the latest sequence in base for
    // current user key to make sure there are no two adjacent PUT op
    xengine::db::Iterator::RecordStatus prev_status = xengine::db::Iterator::kError;
    for (auto& key_seq : user_key_sequences) {
      if (xengine::db::Iterator::kExist == prev_status &&
          xengine::db::Iterator::kExist == key_seq.second) {
        return true;
      }
      prev_status = key_seq.second;
    }
  }
  return false;
}

/** check data in (BUILDING_BASE_INDEX, CHECK_UNIQUE_CONSTRAINT)
          against (TABLE_CREATE, BUILDING_BASE_INDEX)
@param new_table_arg
@param index, xengine index definition
@param is_rebuild, rebuild table or not */
/* Uniqueness conflict check with DML for online DDL
 * Iterate over all user keys from DML operations:
 *    1) Using key_seq() and key_status() to get sequence and op
 *    2) Using Next to compare with user key at next position
 *    3) For sequences of same user key, using check_use_key_sequence to do
 *       conflict check
 */
int ha_xengine::inplace_check_unique_phase(TABLE *new_table_arg,
    const std::shared_ptr<Xdb_key_def>& index, Added_key_info& added_key_info,
    bool is_rebuild)
{
  DBUG_ENTER_FUNC();

  if (index->is_secondary_key()) {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_create_sk_check_constraint_begin");
  } else {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_unique_check_constraint_begin");
  }
  int res = HA_EXIT_SUCCESS;

  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];
  if ((res = inplace_check_dml_error(new_table_arg, key_info, added_key_info))) {
    __XHANDLER_LOG(WARN, "error status found during DML, code is %d, table_name: %s", res, table->s->table_name.str);
    DBUG_RETURN(res);
  }

  xengine::common::ReadOptions base_opts;
  base_opts.read_level_ = xengine::common::kOnlyL2;
  base_opts.total_order_seek = true;

  xengine::common::ReadOptions delta_opts;
  delta_opts.read_level_ = xengine::common::kExcludeL2;
  delta_opts.total_order_seek = true;
  // also check DEL/SDEL records from delta
  delta_opts.skip_del_ = false;
  delta_opts.unique_check_ = true;

  // Get Snapshot and Set CheckUniquePhase under protect for MDL-Lock
  if (ha_thd()->mdl_context.upgrade_shared_lock(
    table->mdl_ticket, MDL_EXCLUSIVE,
    ha_thd()->variables.lock_wait_timeout)) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: update mdl_exclusive lock fail, table_name: %s", table->s->table_name.str);
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  inplace_update_added_key_info_step(added_key_info, Added_key_info::CHECK_UNIQUE_CONSTRAINT);

  // (t0, t1) iter
  IteratorUptr base_iter(xdb->NewIterator(base_opts, index->get_cf()));
  // (t1, t2) iter, snapshot
  IteratorUptr delta_iter(xdb->NewIterator(delta_opts, index->get_cf()));

  table->mdl_ticket->downgrade_lock(MDL_SHARED_UPGRADABLE);

  uint user_defined_key_parts = key_info->user_defined_key_parts;
  const bool all_parts_used = (user_defined_key_parts == index->get_key_parts());

  Arena arena; // used to do Slice::deep_copy
  std::vector<UserKeySequence> user_key_sequences;
  bool memcmp_key_inited = false, has_dup = false;
  uint memcmp_key_size = 0;
  xengine::common::Slice memcmp_key, saved_iter_key, saved_iter_val;
  for (delta_iter->SeekToFirst(); delta_iter->Valid(); delta_iter->Next()) {
    uint iter_key_memcmp_size = delta_iter->key().size();
    if (index->is_secondary_key()) {
      uint n_null_fields = 0;
      iter_key_memcmp_size = index->get_memcmp_sk_size(
              new_table_arg, delta_iter->key(), &n_null_fields);
      if (n_null_fields > 0) {
        continue;
      }
    }

    if (memcmp_key_inited) {
      // check whether this iterator key has same user key
      if (memcmp_key_size != iter_key_memcmp_size || (0 != memcmp(
              memcmp_key.data(), delta_iter->key().data(), memcmp_key_size))) {
        // user key is changed, check whether the old one exists in base or not
        base_iter->Seek(memcmp_key);
        if (HA_EXIT_SUCCESS ==
            read_key_exact(*index, base_iter.get(), all_parts_used, memcmp_key)) {
#ifndef NDEBUG
          __XHANDLER_LOG(INFO, "XEngineDDL: find match record %s in L2 on index:%d during DDL",
                         base_iter->key().ToString(true).c_str(),
                         index->get_index_number());
#endif
          // Use 0 as the sequence number of the record in base tier
          user_key_sequences.emplace_back(0, xengine::db::Iterator::kExist);
        }
        // check all sequences with status for current user key
        has_dup = check_user_key_sequence(user_key_sequences);
        user_key_sequences.clear();
        if (has_dup) {
          break;
        }
        memcmp_key_inited = false;
      }
    }

    if (!memcmp_key_inited) {
      if (index->is_secondary_key()) {
        uint n_null_fields = 0;
        uint sk_memcmp_size = index->get_memcmp_sk_parts(
            new_table_arg, delta_iter->key(), m_dup_sk_packed_tuple,
            &n_null_fields);
        DBUG_ASSERT(n_null_fields == 0);
        DBUG_ASSERT(sk_memcmp_size == iter_key_memcmp_size);
        memcmp_key = xengine::common::Slice(
          reinterpret_cast<char *>(m_dup_sk_packed_tuple), sk_memcmp_size);
      } else {
        memcmp_key = delta_iter->key().deep_copy(arena);
      }
      memcmp_key_size = iter_key_memcmp_size;
      memcmp_key_inited = true;
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: unique check for %s(size=%u) on index:%d during DDL",
                     memcmp_key.ToString(true).c_str(), memcmp_key_size,
                     index->get_index_number());
#endif
    }

    // collect status and sequences number for the same user key
    auto delta_sequence = delta_iter->key_seq();
    auto status = delta_iter->key_status();
    DBUG_ASSERT(xengine::db::Iterator::kNonExist != status);
    user_key_sequences.emplace_back(delta_sequence, status);
    // To print duplicate entry we may need value information to unpack the record
    // save the first PUT record for later usage if duplicate entry is found
    if (xengine::db::Iterator::kExist == status && saved_iter_val.empty()) {
      saved_iter_key = delta_iter->key().deep_copy(arena);
      saved_iter_val = delta_iter->value().deep_copy(arena);
    }
#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: find match record %s(%u, %s) in L0&L1 on index:%d during DDL",
                   delta_iter->key().ToString(true).c_str(), delta_sequence,
                   (xengine::db::Iterator::kExist == status) ? "PUT":"DEL",
                   index->get_index_number());
#endif
  }

  if (!has_dup && memcmp_key_inited && !user_key_sequences.empty()) {
    /* check whether the user key exists in base or not for last user key when
     * delta_iter is not valid
     */
    base_iter->Seek(memcmp_key);
    if (HA_EXIT_SUCCESS ==
        read_key_exact(*index, base_iter.get(), all_parts_used, memcmp_key)) {
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: find match record %s in L2 on index:%d during DDL",
                     base_iter->key().ToString(true).c_str(),
                     index->get_index_number());
#endif
      // Use 0 as the sequence number of the record in base tier
      user_key_sequences.emplace_back(0, xengine::db::Iterator::kExist);
    }
    // check all sequences with status for current user key
    has_dup = check_user_key_sequence(user_key_sequences);
    user_key_sequences.clear();
  }

  if (has_dup) {
    DBUG_ASSERT(!saved_iter_key.empty());
    __XHANDLER_LOG(WARN, "XEngineDDL: duplicate entry is found %s during DDL on index:%d",
                   saved_iter_key.ToString(true).c_str(),
                   index->get_index_number());
    // Current user key is duplicate entry
    // using saved_iter_key/saved_iter_val to call fill_new_duplicate_record
    res = inplace_update_added_key_info_status_dup_key(new_table_arg, index,
              added_key_info, saved_iter_key, saved_iter_val, is_rebuild);
  }

  if (index->is_secondary_key()) {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_create_sk_check_constraint_done");
  } else {
    DEBUG_SYNC(ha_thd(), "xengine.inplace_unique_check_constraint_done");
  }

  /**
   * at this point, cf-index has all the data (t0, now)
   *
   * new data is still allowed to come before commit, it goes to cf-index
   * as usual since the lock is released, and still check unique against
   * cf-index, cf-mirror has already removed and not checked anymore.
   *
   * upper layer will call commit_inplace_alter_table(..., commit=true)
   * even duplicates happen here, but that function notices and returns
   * error, then another commit_inplace_alter_table(..., commit=false) will
   * be called.
   */

  DBUG_RETURN(res);
}

/**
 * Update status of index during inplace_populate_index
 */
int ha_xengine::inplace_update_added_key_info_step(
    Added_key_info& added_key_info, Added_key_info::ADD_KEY_STEP step) const
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(step >= Added_key_info::TABLE_CREATE &&
              step <= Added_key_info::FINISHED);

  added_key_info.step = step;

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
 * Check where duplicate status is set by DML during inplace populate index
 *
 * @param table_arg: used to call print_keydup_error
 * @param key: used to call print_keydup_error
 * @param added_key_info: used to check status
 * @param print_err: used to identify whether print err
 */
int ha_xengine::inplace_check_dml_error(TABLE *const table_arg, KEY *key,
                                      const Added_key_info& added_key_info, bool print_err)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(nullptr != table_arg);
  if (HA_ERR_FOUND_DUPP_KEY == added_key_info.status.load()) {
    // wait dup_key dup_val saved.
    while (!added_key_info.dup_key_saved.load()) {
    }

    // new table duplicate record is set by dml session
    if (print_err) {
      // can't print err in child thread
      print_keydup_error(table_arg, key, MYF(0));
    }
    DBUG_RETURN(HA_ERR_FOUND_DUPP_KEY);
  } else if (HA_ERR_INVALID_NULL_ERROR == added_key_info.status.load()) {
    if (print_err) {
      my_error(ER_INVALID_USE_OF_NULL, MYF(0));
    }
    DBUG_RETURN(HA_ERR_INVALID_NULL_ERROR);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** Scan the Primary Key index entries and populate the new add index keys.
for new table, we build new storage record from old_table_record and default values.
@param new_table_arg
@param index, new added index
@param is_rebuild, rebuild table or not
@return SUCCESS/FAILURE */
int ha_xengine::inplace_populate_index(TABLE *const new_table_arg,
                                       const std::shared_ptr<Xdb_key_def>& index,
                                       bool is_rebuild)
{
  DBUG_ENTER_FUNC();

  int res = HA_EXIT_SUCCESS;
  bool need_unique_check = false;
  KEY* key_info = &new_table_arg->key_info[index->get_keyno()];
  if (!index->is_hidden_primary_key()) {
    need_unique_check = is_rebuild ? !index->m_can_skip_unique_check :
        key_info->flags & HA_NOSAME;
  }

  std::unordered_map<const Xdb_key_def *, Added_key_info>::iterator iter;
  if (!is_rebuild) {
    iter = m_tbl_def->m_added_key.find(index.get());
    DBUG_ASSERT(iter != m_tbl_def->m_added_key.end());
  } else {
    iter = m_tbl_def->m_inplace_new_keys.find(index.get());
    DBUG_ASSERT(iter != m_tbl_def->m_inplace_new_keys.end());
  }

  if ((res = inplace_build_base_phase(
      new_table_arg, index, need_unique_check, iter->second, is_rebuild))) {
    print_common_err(res);
    if (res != HA_ERR_FOUND_DUPP_KEY && res != HA_ERR_INVALID_NULL_ERROR) {
      __XHANDLER_LOG(ERROR,
                     "XEngineDDL: build base error, code: %d, table_name: %s",
                     res, table->s->table_name.str);
    }
  } else if (need_unique_check &&
             (res = inplace_check_unique_phase(new_table_arg, index, iter->second, is_rebuild))) {
    if (res != HA_ERR_FOUND_DUPP_KEY) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: check unique phase error, code: %d, table_name: %s",
                     res, table->s->table_name.str);
    }
  } else if ((res = inplace_update_added_key_info_step(
                        iter->second, Added_key_info::FINISHED))) {
    __XHANDLER_LOG(ERROR, "Failed to update state of Added_key_info::step after unique check");
  } else {
#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: build index %d for table %s successfully",
                   index->get_index_number(), table->s->table_name.str);
#endif
  }

  if (res == HA_ERR_FOUND_DUPP_KEY) {
    __XHANDLER_LOG(WARN, "XEngineDDL: duplicated error happened during build index %d for table %s",
                   index->get_index_number(), table->s->table_name.str);
  }

  DBUG_RETURN(res);
}

/** Update metadata in commit phase for instant ADD COLUMN.
  Basically, it should remember number of instant columns,
  and the default value of newly added columns.
  Note this function should only update the metadata
  which would not result in failure
  @param[in]  new_table   New InnoDB table object
  @param[in]  old_table   MySQL table as it is before the ALTER operation
  @param[in]  altered_table   MySQL table that is being altered
  @param[in]  old_dd_tab  Old dd::Table
  @param[in,out]  new_dd_tab  New dd::Table */
void ha_xengine::dd_commit_instant_table(const TABLE *old_table,
                                         const TABLE *altered_table,
                                         const dd::Table *old_dd_tab,
                                         dd::Table *new_dd_tab)
{
  assert(old_dd_tab->columns().size() <= new_dd_tab->columns()->size());

  // pass DD_TABLE_INSTANT_COLS from old to new DD
  if (!new_dd_tab->se_private_data().exists(
          dd_table_key_strings[DD_TABLE_INSTANT_COLS])) {
    uint32_t instant_cols = old_table->s->fields;

    if (dd_table_has_instant_cols(*old_dd_tab)) {
      old_dd_tab->se_private_data().get(
          dd_table_key_strings[DD_TABLE_INSTANT_COLS], &instant_cols);
    }

    new_dd_tab->se_private_data().set(
        dd_table_key_strings[DD_TABLE_INSTANT_COLS], instant_cols);
  }

  // pass DD_TABLE_NULL_BYTES from old to new DD
  if (!new_dd_tab->se_private_data().exists(
          dd_table_key_strings[DD_TABLE_NULL_BYTES])) {
    uint32_t null_bytes = m_null_bytes_in_rec;

    if (dd_table_has_instant_cols(*old_dd_tab)) {
      old_dd_tab->se_private_data().get(
          dd_table_key_strings[DD_TABLE_NULL_BYTES], &null_bytes);
    }

    new_dd_tab->se_private_data().set(dd_table_key_strings[DD_TABLE_NULL_BYTES],
                                      null_bytes);
  }

  /* To remember old default values if exist */
  dd_copy_table_columns(*new_dd_tab, *old_dd_tab);

  /* Then add all new default values */
  dd_add_instant_columns(old_table, altered_table, new_dd_tab);

  // assert(dd_table_has_instant_cols(*new_dd_tab));
}

void ha_xengine::dd_commit_inplace_no_change(const dd::Table *old_dd_tab,
                                             dd::Table *new_dd_tab)
{
  dd_copy_private(*new_dd_tab, *old_dd_tab);

  /* To remember old default values if exist */
  dd_copy_table_columns(*new_dd_tab, *old_dd_tab);
  /*if (!dd_table_is_partitioned(new_dd_tab->table()) ||
    dd_part_is_first(reinterpret_cast<dd::Partition *>(new_dd_tab))) {
    dd_copy_table(new_dd_tab->table(), old_dd_tab->table());
    }*/
}

/** Update table level instant metadata in commit phase,
    may be instant column name is modified, or default value is modifed
    after ddl operation.
  1. if add columns mixed with other ddl operation, it will be inplace type
  2. if data type changed or not-null/null attribute changed, it will be copy/rebuild
  so instant columns and instant nulls should be same to old_table
  @param[in]  old_dd_tab  old dd::Table
  @param[in]  new_dd_tab  new dd::Table */
void dd_commit_inplace_update_instant_meta(
    my_core::Alter_inplace_info *const ha_alter_info,
    const dd::Table *old_dd_tab, dd::Table *new_dd_tab, TABLE *table,
    TABLE *altered_table)
{
  if (!dd_table_has_instant_cols(*old_dd_tab)) {
    return;
  }

  assert(old_dd_tab->se_private_data().exists(
      dd_table_key_strings[DD_TABLE_INSTANT_COLS]));
  assert(old_dd_tab->se_private_data().exists(
      dd_table_key_strings[DD_TABLE_NULL_BYTES]));

  uint32_t instant_cols = 0;
  old_dd_tab->se_private_data().get(dd_table_key_strings[DD_TABLE_INSTANT_COLS],
                                    &instant_cols);

  new_dd_tab->se_private_data().set(dd_table_key_strings[DD_TABLE_INSTANT_COLS],
                                    instant_cols);

  uint32_t null_bytes = 0;
  old_dd_tab->se_private_data().get(dd_table_key_strings[DD_TABLE_NULL_BYTES],
                                    &null_bytes);

  new_dd_tab->se_private_data().set(dd_table_key_strings[DD_TABLE_NULL_BYTES],
                                    null_bytes);
  //update dd-column meta if necessary
  assert(table->s->fields == altered_table->s->fields);
  if (!(ha_alter_info->handler_flags & Alter_inplace_info::ALTER_COLUMN_NAME)) {
    dd_copy_table_columns(*new_dd_tab, *old_dd_tab);
    return;
  }

  //some field name changed during inplae-ddl
  Field *old_field, *new_field;
  for (uint i=0; i < table->s->fields; i++) {
    old_field = table->field[i];
    new_field = altered_table->field[i];

    dd::Column *old_col = const_cast<dd::Column *>(dd_find_column(old_dd_tab, old_field->field_name));

    dd::Column *new_col = const_cast<dd::Column *>(
        dd_find_column(new_dd_tab, new_field->field_name));

#ifndef NDEBUG
    if (new_field->flags & FIELD_IS_RENAMED) {
        assert(my_strcasecmp(system_charset_info, old_field->field_name,
                             new_field->field_name));
    }
#endif

    if (!old_col->se_private_data().empty()) {
      if (!new_col->se_private_data().empty())
        new_col->se_private_data().clear();
      new_col->set_se_private_data(old_col->se_private_data());
    }
  }

  return;
}


void ha_xengine::dd_commit_inplace_instant(Alter_inplace_info *ha_alter_info,
                                           const TABLE *old_table,
                                           const TABLE *altered_table,
                                           const dd::Table *old_dd_tab,
                                           dd::Table *new_dd_tab)
{
  assert(is_instant(ha_alter_info));

  auto type = static_cast<Instant_Type>(ha_alter_info->handler_trivial_ctx);

  switch (type) {
    case Instant_Type::INSTANT_NO_CHANGE:
      dd_commit_inplace_no_change(old_dd_tab, new_dd_tab);
      break;
    case Instant_Type::INSTANT_ADD_COLUMN:
      dd_copy_private(*new_dd_tab, *old_dd_tab);
      dd_commit_instant_table(old_table, altered_table, &old_dd_tab->table(),
                              &new_dd_tab->table());
      break;
    case Instant_Type::INSTANT_IMPOSSIBLE:
    default:
      assert(0);
  }
}

/** If ddl failed, we need to rollback new added indexes.
@param added_indexes, new add indexes in ddl
@ret, SUCCESS/FAILURE. */
int ha_xengine::rollback_added_index(
    const std::unordered_set<std::shared_ptr<Xdb_key_def>>& added_indexes)
{
  int ret = HA_EXIT_SUCCESS;

  /* Remove uncommitted key definitions from ddl_manager */
  ddl_manager.remove_uncommitted_keydefs(added_indexes);

  /* Rollback created indexes */
  m_tbl_def->clear_keys_for_ddl();

  /* fix #22611997, in rollback case, we need renew members of handlers
     Buffers need to be set up again to account for new, possibly longer
     secondary keys. */
  free_key_buffers();

  if ((ret = alloc_key_buffers(table, m_tbl_def.get()))) {
    __XHANDLER_LOG(ERROR, "XEngine failed allocating key buffers during alter. table_name: %s", table->s->table_name.str);
    return ret;
  }

  return ret;
}

/**
  Commit or rollback the changes made during prepare_inplace_alter_table()
  and inplace_alter_table() inside the storage engine.
  Note that in case of rollback the allowed level of concurrency during
  this operation will be the same as for inplace_alter_table() and thus
  might be higher than during prepare_inplace_alter_table(). (For example,
  concurrent writes were blocked during prepare, but might not be during
  rollback).

  @note Storage engines are responsible for reporting any errors by
  calling my_error()/print_error()

  @note If this function with commit= true reports error, it will be called
  again with commit= false.

  @note In case of partitioning, this function might be called for rollback
  without prepare_inplace_alter_table() having been called first.
  Also partitioned tables sets ha_alter_info->group_commit_ctx to a NULL
  terminated array of the partitions handlers and if all of them are
  committed as one, then group_commit_ctx should be set to NULL to indicate
  to the partitioning handler that all partitions handlers are committed.
  @see prepare_inplace_alter_table().

  @param    altered_table     TABLE object for new version of table.
  @param    ha_alter_info     Structure describing changes to be done
  by ALTER TABLE and holding data used
  during in-place alter.
  @param    commit            True => Commit, False => Rollback.
  @param    old_table_def     dd::Table object describing old version of
  the table.
  @param    new_table_def     dd::Table object for the new version of the
  table. Can be adjusted by this call if SE
  supports atomic DDL. These changes to the
  table definition will be persisted in the
  data-dictionary at statement commit time.

  @retval   true              Error
  @retval   false             Success
  */

bool ha_xengine::commit_inplace_alter_table(
    my_core::TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info, bool commit,
    const dd::Table *old_dd_tab, dd::Table *new_dd_tab)
{
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(nullptr != ha_alter_info);

  auto ctx = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);
  if (is_instant(ha_alter_info)) {
    dd_commit_inplace_instant(ha_alter_info, table, altered_table, old_dd_tab,
                              new_dd_tab);
#ifndef NDEBUG
  __XHANDLER_LOG(INFO, "XEngineDDL: commit instant alter sql is %s, commit is %d", ha_thd()->query().str, (int)commit);
#endif
    DBUG_RETURN(HA_EXIT_SUCCESS);
  } else if (nullptr == ctx) {
    /* If ctx has not been created yet, nothing to do here */
    __XHANDLER_LOG(INFO, "XEngineDDL: ctx is released or not initialized");
    DBUG_RETURN(HA_EXIT_SUCCESS);
  } else {
    int ret = HA_EXIT_SUCCESS;
    bool is_rebuild = ctx->m_rebuild;
    if (ctx->m_rebuild) {
      ret = commit_inplace_alter_table_rebuild(altered_table, ha_alter_info,
                                               commit, old_dd_tab, new_dd_tab);
    } else {
      ret = commit_inplace_alter_table_norebuild(
          altered_table, ha_alter_info, commit, old_dd_tab, new_dd_tab);
    }

    if (!ret) {
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: inplace ddl %s successfully, table: %s",
                     commit ? "commit" : "rollback", table->s->table_name.str);
#endif
    } else {
      __XHANDLER_LOG(WARN, "XEngineDDL: failed to commit inplace ddl, table: %s, errcode is %d",
                     table->s->table_name.str, ret);
    }

#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: commit alter sql is %s, inplace_rebuild: %d, commit is %d, code is %d", ha_thd()->query().str, (int)is_rebuild, (int)commit, ret);
#endif

    DBUG_RETURN(ret);
  }
}


/** Maybe autoincrement value changed by Alter Statement,
Get the auto-increment value of the table on commit.
@param[in] ha_alter_info Data used during in-place alter
@param[in] altered_table MySQL table that is being altered
@param[in] old_table MySQL table as it is before the ALTER operation
@retval HA_EXIT_SUCCESS Success, others FAILED */
int ha_xengine::commit_inplace_alter_get_autoinc(Alter_inplace_info *ha_alter_info,
                                         const TABLE *altered_table,
                                         const TABLE *old_table)
{
  DBUG_ENTER("alter_commit_get_autoinc");
  DBUG_ASSERT(nullptr != ha_alter_info);

  auto ctx = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);
  DBUG_ASSERT(ctx);

  if (altered_table->found_next_number_field &&
      ((ha_alter_info->handler_flags &
       Alter_inplace_info::CHANGE_CREATE_OPTION) &&
      (ha_alter_info->create_info->used_fields & HA_CREATE_USED_AUTO))) {
    /* An AUTO_INCREMENT value was supplied, but the table was not
    rebuilt. Get the user-supplied value or the last value from the
    sequence. */

    ulonglong set_autoinc_value =
        ha_alter_info->create_info->auto_increment_value;

    if (old_table->found_next_number_field) {
      load_auto_incr_value();
    }

    ulonglong max_value_table = m_tbl_def->m_auto_incr_val;
    if (set_autoinc_value > max_value_table) {
      ctx->m_new_tdef->m_auto_incr_val = set_autoinc_value;
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** Both inplace alter ddl(rebuild/no_rebuild) should do these things at commit phase
  1. update autoinc if necessary
  2. update xengine dictionary
  3. calulate statistics
  4. enable major compaction and inner merge operation
@param[in] altered_table, new table
@param[in] ha_alter_info, DDL operation
@param[in] commit, true indicate commit, and false indicate rollback
@param[in] new_added_indexes
@return SUCCESS/FAILURE */
int ha_xengine::commit_inplace_alter_table_common(
    my_core::TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info, bool commit,
    const std::unordered_set<std::shared_ptr<Xdb_key_def>> &new_added_indexes)
{
  DBUG_ENTER_FUNC();

  //update autoinc if necessary
  if (commit_inplace_alter_get_autoinc(ha_alter_info, altered_table, table)) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: get autoinc value failed. table_name: %s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  DBUG_ASSERT(nullptr != ha_alter_info);
  auto ctx0 = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);
  DBUG_ASSERT(ctx0);

  m_tbl_def->clear_keys_for_ddl();

  // current Xdb_tbl_def represented by m_tbl_def will be unreferenced
  // new Xdb_tbl_def represented by ctx0->m_new_tdef will be referenced
  m_tbl_def = ctx0->m_new_tdef;
  m_key_descr_arr = m_tbl_def->m_key_descr_arr;
  m_pk_descr = m_key_descr_arr[pk_index(altered_table, m_tbl_def.get())];

  /*
     For partitioned tables, we need to commit all changes to all tables at
     once, unlike in the other inplace alter API methods.
   */
  inplace_alter_handler_ctx **ctx_array;
  inplace_alter_handler_ctx *ctx_single[2];
  if (ha_alter_info->group_commit_ctx) {
    DBUG_EXECUTE_IF("crash_during_index_creation_partition", DBUG_SUICIDE(););
    ctx_array = ha_alter_info->group_commit_ctx;
  } else {
    ctx_single[0] = ctx0;
    ctx_single[1] = nullptr;
    ctx_array = ctx_single;
  }

  DBUG_ASSERT(ctx0 == ctx_array[0]);
  ha_alter_info->group_commit_ctx = nullptr;

  THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
  Xdb_transaction *const tx = get_or_create_tx(thd);
  xengine_register_tx(ht, thd, tx);
  auto batch = dynamic_cast<xengine::db::WriteBatch *>(tx->get_blind_write_batch());
  std::unordered_set<GL_INDEX_ID> create_index_ids;

  dict_manager.lock();
  for (inplace_alter_handler_ctx **pctx = ctx_array; *pctx; pctx++) {
    auto ctx = dynamic_cast<Xdb_inplace_alter_ctx *>(*pctx);

    for (uint i = 0; i < ctx->m_new_tdef->m_key_count; i++) {
      create_index_ids.insert(ctx->m_new_tdef->m_key_descr_arr[i]->get_gl_index_id());
    }

    if (ddl_manager.put_and_write(ctx->m_new_tdef, batch, &ddl_log_manager,
                                  thd_thread_id(thd), false)) {
      DBUG_ASSERT(0);
    }

    /*
       Remove uncommitted key definitions from ddl_manager, as they are now
       committed into the data dictionary.
    */
    ddl_manager.remove_uncommitted_keydefs(new_added_indexes);
  }

  dict_manager.unlock();

  /** if crash here, during crash-recovery, drop_thread will collect
    create-ongoing-indexs then drop subtables;
    during post_recover phase, we collect ddl_logs and find subtables, if
    exists, then drop it; if not, do nothing. */
  DBUG_EXECUTE_IF("ddl_log_crash_before_remove_index_ongoing", DBUG_SUICIDE(););

  /* Mark ongoing create indexes as finished/remove from data dictionary */
  //dict_manager.finish_indexes_operation(create_index_ids,
  //                                      Xdb_key_def::DDL_CREATE_INDEX_ONGOING);

  /** if crash here, during post_recover phase, we drop it. */
  DBUG_EXECUTE_IF("ddl_log_crash_after_remove_index_ongoing", DBUG_SUICIDE(););

  /*
     We need to recalculate the index stats here manually.  The reason is that
     the secondary index does not exist inside
     m_index_num_to_keydef until it is committed to the data dictionary, which
     prevents us from updating the stats normally as the ddl_manager cannot
     find the proper gl_index_ids yet during adjust_stats calls.
     */
  if (calculate_stats(altered_table, nullptr, nullptr)) {
    /* Failed to update index statistics, should never happen */
    DBUG_ASSERT(0);
  }

  // enable add-index major compaction
  std::vector<ColumnFamilyHandle *> column_family_handles;
  for (auto& index : new_added_indexes) {
    xengine::db::ColumnFamilyHandle *cf = index->get_cf();
    column_family_handles.push_back(cf);
  }

  auto txn_db_impl = dynamic_cast<xengine::util::TransactionDBImpl *>(xdb);
  xengine::common::Status s;
  txn_db_impl->GetDBImpl()->switch_major_compaction(column_family_handles,
                                                    true);
  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: switch_major_compaction fail: %s. table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  s = txn_db_impl->GetDBImpl()->enable_backgroud_merge(column_family_handles);
  if (!s.ok()) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: enable_background_merge fail: %s. table_name: %s",
                   s.ToString().c_str(), table->s->table_name.str);
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** commit phase for inplace_rebuild ddl
@param[in] altered_table  new table
@param[in] ha_alter_info  DDL operation
@param[in] commit, true is commit and false is rollback
@param[in] old_dd_tab
@param[in] new_dd_tab
@return true is ERROR and false is Success */
bool ha_xengine::commit_inplace_alter_table_rebuild(
    my_core::TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info, bool commit,
    const dd::Table *old_dd_tab, dd::Table *new_dd_tab)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(altered_table != nullptr);
  DBUG_ASSERT(ha_alter_info != nullptr);
  auto ctx0 = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);
  Xdb_tbl_def *new_tdef = m_tbl_def->m_inplace_new_tdef;
  DBUG_ASSERT(new_tdef != nullptr);

  int ret = HA_EXIT_SUCCESS;

  std::unordered_set<std::shared_ptr<Xdb_key_def>> new_added_indexes;
  for (uint i=0; i < new_tdef->m_key_count; i++) {
    new_added_indexes.insert(new_tdef->m_key_descr_arr[i]);
  }

  if (commit) {
    // if there were dups between inplace_alter_table's return and now,
    // capture it here
    for (auto &k : m_tbl_def->m_inplace_new_keys) {
      if ((ret = inplace_check_dml_error(altered_table,
                                &altered_table->key_info[k.first->get_keyno()],
                                k.second))) {
        __XHANDLER_LOG(WARN, "XEngineDDL: found error when rebuild table, code: %d", ret);

        rollback_added_index(new_added_indexes);
        ret = HA_EXIT_FAILURE;
        goto rebuild_fun_end;
      }
    }
  } else {
    /* rollback created indexes **/
    ret = rollback_added_index(new_added_indexes);
    goto rebuild_fun_end;
  }

  /*
     IMPORTANT: When rollback is requested, mysql will abort with
     an assertion failure. That means every failed commit during inplace alter
     table will result in a fatal error on the server. Indexes ongoing creation
     will be detected when the server restarts, and dropped.
  */
  DBUG_EXECUTE_IF("ddl_log_crash_before_inplace_ddl_commit", DBUG_SUICIDE(););

  ret = commit_inplace_alter_table_common(altered_table, ha_alter_info, commit,
                                          new_added_indexes);

rebuild_fun_end:
  // any way, whether succeed or fail, we need to reload new_tdef for atomic ddl
  delete ctx0;
  ha_alter_info->handler_ctx = nullptr;

  DBUG_RETURN(ret);
}

/** commit phase for inplace_norebuild ddl
@param[in] altered_table  new table
@param[in] ha_alter_info  DDL operation
@param[in] commit, true is commit and false is rollback
@param[in] old_dd_tab
@param[in] new_dd_tab
@return true is ERROR and false is Success */
bool ha_xengine::commit_inplace_alter_table_norebuild(
    my_core::TABLE *const altered_table,
    my_core::Alter_inplace_info *const ha_alter_info, bool commit,
    const dd::Table *old_dd_tab, dd::Table *new_dd_tab)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(altered_table != nullptr);
  DBUG_ASSERT(ha_alter_info != nullptr);

  auto ctx0 = dynamic_cast<Xdb_inplace_alter_ctx *>(ha_alter_info->handler_ctx);

  int ret = HA_EXIT_SUCCESS;
  if (commit) {
    // if there were dups between inplace_alter_table's return and now,
    // capture it here
    for (auto &k : m_tbl_def->m_added_key) {
      if ((ret = inplace_check_dml_error(altered_table,
                                &altered_table->key_info[k.first->get_keyno()],
                                k.second))) {
        __XHANDLER_LOG(WARN, "XEngineDDL: found error when build unique index, code is %d", ret);

        DBUG_ASSERT(ctx0 != nullptr);
        rollback_added_index(ctx0->m_added_indexes);
        ret = HA_EXIT_FAILURE;
        goto fun_end;
      }
    }
  }

  /*
   * IMPORTANT: When rollback is requested, mysql will abort with
   * an assertion failure. That means every failed commit during inplace alter
   * table will result in a fatal error on the server. Indexes ongoing creation
   * will be detected when the server restarts, and dropped.
   *
   * For partitioned tables, a rollback call to this function (commit == false)
   * is done for each partition.  A successful commit call only executes once
   * for all partitions.
   */
  if (!commit) {
    // be consistent with ha_xengine::open,
    // only when add indexes case, we will enlarge our buffer
    if (ha_alter_info->handler_flags &
        (my_core::Alter_inplace_info::ADD_INDEX |
         my_core::Alter_inplace_info::ADD_UNIQUE_INDEX)) {
      ret = rollback_added_index(ctx0->m_added_indexes);
      goto fun_end;
    } else {
      ret = HA_EXIT_SUCCESS;
      goto fun_end;
    }
  }

  DBUG_EXECUTE_IF("ddl_log_crash_before_inplace_ddl_commit", DBUG_SUICIDE(););

  dd_commit_inplace_update_instant_meta(ha_alter_info, old_dd_tab, new_dd_tab, table, altered_table);

  /*
     For partitioned tables, we need to commit all changes to all tables at
     once, unlike in the other inplace alter API methods.
     */
#ifndef NDEBUG
  inplace_alter_handler_ctx **ctx_array;
  inplace_alter_handler_ctx *ctx_single[2];
#endif

  DBUG_ASSERT(ctx0);
  if (ha_alter_info->group_commit_ctx) {
    DBUG_EXECUTE_IF("crash_during_index_creation_partition", DBUG_SUICIDE(););
#ifndef NDEBUG
    ctx_array = ha_alter_info->group_commit_ctx;
  } else {
    ctx_single[0] = ctx0;
    ctx_single[1] = nullptr;
    ctx_array = ctx_single;
#endif
  }

  DBUG_ASSERT(ctx0 == ctx_array[0]);
  ha_alter_info->group_commit_ctx = nullptr;

  ret = commit_inplace_alter_table_common(altered_table, ha_alter_info, commit,
                                          ctx0->m_added_indexes);

fun_end:
  // any way, whether succeed or fail, we need to reload new_tdef for atomic ddl
  delete ctx0;
  ha_alter_info->handler_ctx = nullptr;

  DBUG_RETURN(ret);
}

/** Unique check for ongoing DML with new index building concurrently.
  used both for lock_and_check_new_index_pk and
  lock_and_check_new_index_sk.

@param[in] kd, xengine key definition
@param[in/out] ki, altered_table and dup key information
@param[in] row_info, Xdb_transaction, new_pk_slice
@param[in] key, key slice to check
@param[in] is_rebuild
@param[out] duplicate key found or not
@return SUCCESS/FAILURE  */
void ha_xengine::check_new_index_unique_inner(
      const Xdb_key_def &kd, Added_key_info &ki,
      const struct update_row_info &row_info, const xengine::common::Slice& key,
      bool is_rebuild, bool *const found) const
{
#ifndef NDEBUG
  __XHANDLER_LOG(INFO, "XEngineDDL: unique check for %s(size=%u) on index:%d during DML",
                 key.ToString(true).c_str(), key.size(), kd.get_index_number());
#endif
  DBUG_ASSERT(ki.altered_table);

  KEY *key_info = &ki.altered_table->key_info[kd.get_keyno()];
  DBUG_ASSERT(key_info);

  uint user_defined_key_parts = key_info->user_defined_key_parts;
  const bool all_parts_used = (user_defined_key_parts == kd.get_key_parts());
  const bool total_order_seek =
    !can_use_bloom_filter(ha_thd(), kd, key, all_parts_used,
                          is_ascending(kd, HA_READ_KEY_EXACT));
  const bool fill_cache = true;

  // check against (t1, -)
  IteratorUptr iter(row_info.tx->get_iterator(kd.get_cf(), total_order_seek,
    fill_cache, true /* read current data */, false /* acquire snapshot */,
    true /*exclude_l2*/, true/* for unique check */));
  /*
   * Need to scan the transaction to see if there is a duplicate key.
   * Also need to scan XENGINE and verify the key has not been deleted
   * in the transaction.
   */
  // Stage 1 check write batch only
  auto wb_status = xengine::db::Iterator::kNonExist;
  auto uk_iter = dynamic_cast<UniqueCheckBaseDeltaIterator*>(iter.get());
  DBUG_ASSERT(nullptr != uk_iter);
  for (uk_iter->Seek(key); !uk_iter->is_at_base() && uk_iter->Valid(); uk_iter->Next()) {
    uint cmp_size = uk_iter->key().size();
    if (kd.is_secondary_key()) {
      uint n_null_fields = 0;
      cmp_size = kd.get_memcmp_sk_size(ki.altered_table, uk_iter->key(),
                                       &n_null_fields);
      if (n_null_fields > 0) {
#ifndef NDEBUG
        __XHANDLER_LOG(INFO, "XEngineDDL: null field exists: %u", n_null_fields);
#endif
        continue;
      }
    }

    if (cmp_size != key.size() ||
        memcmp(key.data(), uk_iter->key().data(), cmp_size)) {
      // current key doesn't match exactly, no further check in WriteBatch
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: record in WriteBatch doesn't match:(key:%s, size:%u) for indxe:%d",
                     uk_iter->key().ToString(true).c_str(), cmp_size,
                     kd.get_index_number());
#endif
      break;
    }

    wb_status = uk_iter->key_status();
    if (xengine::db::Iterator::kExist == wb_status) {
      // find conflict user key, no further check in WriteBatch and base
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: find duplicate record %s in WriteBatch for indxe:%d",
                     key.ToString(true).c_str(), kd.get_index_number());
#endif
      break;
    }
    DBUG_ASSERT(xengine::db::Iterator::kDeleted == wb_status);
    // found deleted record, check more in WriteBatch
#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: find deleted record %s in WriteBatch for indxe:%d",
                   uk_iter->key().ToString(true).c_str(), kd.get_index_number());
#endif
  }

  if (xengine::db::Iterator::kExist == wb_status) {
    // duplicate record is found in WriteBatch
    *found = true;
  } else if (xengine::db::Iterator::kDeleted == wb_status) {
    // duplicate record is deleted in WriteBatch
    *found = false;
  } else if (ki.step >= Added_key_info::CHECK_UNIQUE_CONSTRAINT) {
    // Stage 2 check L0, L1 and L2 if not match in WriteBatch, if needed
    /* no matching record is found in WriteBatch
     * further check all user keys from base iterator of delta (L0, L1) and L2.
     * We only do this unique check procedure after DDL has finished building
     * baseline, because we only can get static L2 data at this stage.
     * If we do this unique check procedure only with data in L0&L1, it is just
     * waste of time due to incomplete data set
     */
    std::vector<UserKeySequence> user_key_sequences;
    for (uk_iter->change_to_base(); uk_iter->Valid(); uk_iter->Next()) {
      uint cmp_size = uk_iter->key().size();
      if (kd.is_secondary_key()) {
        uint n_null_fields = 0;
        cmp_size = kd.get_memcmp_sk_size(ki.altered_table, uk_iter->key(),
                                         &n_null_fields);
        if (n_null_fields > 0) {
#ifndef NDEBUG
          __XHANDLER_LOG(INFO, "XEngineDDL: null field exist: %u", n_null_fields);
#endif
          continue;
        }
      }
      if (cmp_size != key.size() ||
          memcmp(key.data(), uk_iter->key().data(), cmp_size)) {
        // no more matching record in L0 and L1
#ifndef NDEBUG
        __XHANDLER_LOG(INFO, "XEngineDDL: record in L0&L1 doesn't match: (key:%s, size:%u) for index:%d",
                       uk_iter->key().ToString().c_str(), cmp_size,
                       kd.get_index_number());
#endif
        break;
      }
      auto sequence = uk_iter->key_seq();
      auto status = uk_iter->key_status();
      DBUG_ASSERT(xengine::db::Iterator::kExist == status ||
                  xengine::db::Iterator::kDeleted == status);
      // match in (L0, L1), collect status for later usage
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: match record %s(%u, %s) in L0&L1 for index:%d",
                     uk_iter->key().ToString(true).c_str(), sequence,
                     (xengine::db::Iterator::kExist == status) ? "PUT" : "DEL",
                     kd.get_index_number());
#endif
      user_key_sequences.emplace_back(sequence, status);
    }
    xengine::common::ReadOptions opts;
    opts.total_order_seek = true;
    opts.read_level_ = xengine::common::kOnlyL2;
    iter.reset(xdb->NewIterator(opts, kd.get_cf()));
    iter->Seek(key);
    if (HA_EXIT_SUCCESS ==
        read_key_exact(kd, iter.get(), all_parts_used, key)) {
#ifndef NDEBUG
      __XHANDLER_LOG(INFO, "XEngineDDL: find match record %s in L2 for index:%d",
                     iter->key().ToString(true).c_str(), kd.get_index_number());
#endif
      user_key_sequences.emplace_back(0, xengine::db::Iterator::kExist);
    }
    *found = (!user_key_sequences.empty() &&
              (check_user_key_sequence(user_key_sequences) ||
              xengine::db::Iterator::kExist == user_key_sequences.begin()->second));
  }

  if (*found) {
    __XHANDLER_LOG(WARN, "XEngineDDL: duplicate entry is found during DML for %s on index:%d",
                   key.ToString(true).c_str(), kd.get_index_number());
    // save the duplicated key
    uint err = HA_EXIT_SUCCESS;
    bool rc = ki.status.compare_exchange_strong(err, HA_ERR_FOUND_DUPP_KEY);
    // err only set from 0 -> DUP_KEY, not otherwise. false alert is possible,
    // e.g. the dup record is deleted later, but not detect that right now.
    if (rc) {
      if (set_duplicate_key_for_print(ki.altered_table, table, &kd,
                                      m_tbl_def->m_dict_info, is_rebuild)) {
        __XHANDLER_LOG(WARN, "XEngineDDL: Generate duplicate key error, table_name:%s",
                       table->s->table_name.str);
      } else {
#ifndef NDEBUG
        __XHANDLER_LOG(INFO, "XEngineDDL: Generate duplicate key successfully, table_name:%s",
                       table->s->table_name.str);
#endif
      }

      ki.dup_key_saved.store(true);
    }
  }
}

/** Only used for rebuild table, if primary key is modified,
    all secondary indexes will also modified.
@param[in] kd, xengine key definition
@param[in/out] ki, altered_table and dup key information
@param[in] row_info, old_data and new_data
@param[out] duplicate key found or not
@return SUCCESS/FAILURE  */
int ha_xengine::lock_and_check_new_index_pk(
    const Xdb_key_def &kd, Added_key_info &ki,
    const struct update_row_info &row_info, bool *const pk_changed,
    bool *const found) const
{
  DEBUG_SYNC(ha_thd(), "xengine.check_and_lock_pk_inplace");
  DBUG_ASSERT(found != nullptr);
  DBUG_ASSERT(!has_hidden_pk(ki.altered_table));
  DBUG_ASSERT(row_info.new_data != nullptr);

  *pk_changed = false;
  // pack user data with new table
  uint new_pk_size = 0;
  if (m_new_pk_descr->pack_new_record(table, m_pack_buffer, row_info.new_data,
                                      m_pk_packed_tuple, nullptr,
                                      false, 0, 0, nullptr, ki.altered_table,
                                      m_tbl_def->m_dict_info, new_pk_size)) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: pack new data for new table error. table_name: %s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }

  if (row_info.old_data != nullptr) {
    // for update
    uint old_pk_size = 0;
    if (m_new_pk_descr->pack_new_record(table, m_pack_buffer, row_info.old_data,
                                        m_pk_packed_tuple_old, nullptr,
                                        false, 0, 0, nullptr, ki.altered_table,
                                        m_tbl_def->m_dict_info, old_pk_size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack old data for new table error. table_name: %s", table->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
    DBUG_ASSERT(old_pk_size > 0);
    // If the keys are the same, then no lock is needed
    if ((new_pk_size == old_pk_size) &&
        !memcmp(m_pk_packed_tuple, m_pk_packed_tuple_old, new_pk_size)) {
      *found = false;
      return HA_EXIT_SUCCESS;
    }
    *pk_changed = true;
  }

  /* For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
   * always require locking.
   */
  xengine::common::Slice new_pk_slice((const char *)m_pk_packed_tuple, new_pk_size);
  std::string tmp_value;
  const xengine::common::Status s =
    get_for_update(row_info.tx, kd.get_cf(), new_pk_slice, &tmp_value);
  if (!s.ok() && !s.IsNotFound()) {
    __XHANDLER_LOG(WARN, "XEngineDDL: get_for_update for key(%s) on pk(%u) failed with error:%s, table_name: %s",
                   new_pk_slice.ToString(true).c_str(),
                   kd.get_index_number(), s.ToString().c_str(),
                   table->s->table_name.str);
    return row_info.tx->set_status_error(ki.altered_table->in_use, s,
                                         *m_new_pk_descr, m_tbl_def->m_inplace_new_tdef);
  }

  if (!kd.m_can_skip_unique_check) {
    check_new_index_unique_inner(kd, ki, row_info, new_pk_slice, true, found);
  }

  return HA_EXIT_SUCCESS;
}

/** During online-ddl running, new arrival inserts/updates need to do unique-check
    for new-index base from ddl-session and increments itself.
@param[in] kd, xengine key definition
@param[in/out] ki, altered_table and dup key information
@param[in] row_info, old_data and new_data
@param[out] duplicate key found or not
@param[in] true is rebuild-ddl, and we should pack new record.
@return SUCCESS/FAILURE  */
int ha_xengine::lock_and_check_new_index_sk(
    const Xdb_key_def &kd, Added_key_info &ki,
    const struct update_row_info &row_info, bool *const found,
    bool is_rebuild/* = false */) const
{
  DBUG_ASSERT(found != nullptr);
  *found = false;

  /* Can skip checking this key if none of the key fields have changed. */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(kd.get_keyno())) {
    return HA_EXIT_SUCCESS;
  }

  TABLE *altered_table = ki.altered_table;
  KEY *key_info = nullptr;
  uint n_null_fields = 0;
  uint user_defined_key_parts = 1;

  key_info = &altered_table->key_info[kd.get_keyno()];
  user_defined_key_parts = key_info->user_defined_key_parts;

  /** If there are no uniqueness requirements, there's no need to obtain a
     lock for this key. */
  if (!(key_info->flags & HA_NOSAME)) {
    return HA_EXIT_SUCCESS;
  }

  /* Calculate the new key for obtaining the lock
     For unique secondary indexes, the key used for locking does not
     include the extended fields.*/
  uint size = 0;
  if (!is_rebuild) {
    size = kd.pack_record(table, m_pack_buffer, row_info.new_data,
                          m_sk_packed_tuple, nullptr, false, 0,
                          user_defined_key_parts, &n_null_fields, altered_table);
  } else {
    if (kd.pack_new_record(table, m_pack_buffer, row_info.new_data,
                           m_sk_packed_tuple, nullptr, false, 0,
                           user_defined_key_parts, &n_null_fields,
                           altered_table, m_tbl_def->m_dict_info, size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record errror, table_name: %s", table->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
  }

  if (n_null_fields > 0) {
#ifndef NDEBUG
    __XHANDLER_LOG(INFO, "XEngineDDL: null field exist: %u", n_null_fields);
#endif
    /** If any fields are marked as NULL this will never match another row as
       to NULL never matches anything else including another NULL. */
    return HA_EXIT_SUCCESS;
  }

  const xengine::common::Slice new_slice =
      xengine::common::Slice((const char *)m_sk_packed_tuple, size);

  /** For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
     always require locking. */
  if (row_info.old_data != nullptr) {
    // don't include the hidden_pk_id for sk comparision
    if (!is_rebuild) {
      size = kd.pack_record(table, m_pack_buffer, row_info.old_data,
                            m_sk_packed_tuple_old, nullptr, false,
                            0, user_defined_key_parts, nullptr, altered_table);
    } else {
      if (kd.pack_new_record(table, m_pack_buffer, row_info.old_data,
                             m_sk_packed_tuple_old, nullptr, false,
                             0, user_defined_key_parts, nullptr, altered_table,
                             m_tbl_def->m_dict_info, size)) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record errror, table_name: %s", table->s->table_name.str);
        return HA_EXIT_FAILURE;
      }
    }
    assert(size <= m_max_packed_sk_len);
    const xengine::common::Slice old_slice =
        xengine::common::Slice((const char *)m_sk_packed_tuple_old, size);

    /** For updates, if the keys are the same, then no lock is needed
        Also check to see if the key has any fields set to NULL. If it does, then
        this key is unique since NULL is not equal to each other, so no lock is
        needed. */
    if (!Xdb_pk_comparator::bytewise_compare(new_slice, old_slice)) {
      return HA_EXIT_SUCCESS;
    }
  }

  /** Perform a read to determine if a duplicate entry exists - since this is
      a secondary indexes a range scan is needed.

      note: we intentionally don't set options.snapshot here. We want to read
      the latest committed data. */

  const bool all_parts_used = (user_defined_key_parts == kd.get_key_parts());

  /**This iterator seems expensive since we need to allocate and free
     memory for each unique index.

     If this needs to be optimized, for keys without NULL fields, the
     extended primary key fields can be migrated to the value portion of the
     key. This enables using Get() instead of Seek() as in the primary key
     case.

     The bloom filter may need to be disabled for this lookup. */
  const bool total_order_seek =
      !can_use_bloom_filter(ha_thd(), kd, new_slice, all_parts_used,
                            is_ascending(kd, HA_READ_KEY_EXACT));
  const bool fill_cache = true;  // !THDVAR(ha_thd(), skip_fill_cache);

  // psergey-todo: we just need to take lock, lookups not needed:

  DBUG_ASSERT(!kd.m_is_reverse_cf);
  const xengine::common::Status s = lock_unique_key(
      row_info.tx, kd.get_cf(), new_slice, total_order_seek, fill_cache);
  if (!s.ok() && !s.IsNotFound()) {
    __XHANDLER_LOG(WARN, "XEngineDDL: lock_unique_key for key(%s) on index:%d failed with error:%s, table_name: %s",
                   new_slice.ToString(true).c_str(),
                   kd.get_index_number(), s.ToString().c_str(),
                   table->s->table_name.str);
    auto tbl = is_rebuild ? m_tbl_def->m_inplace_new_tdef : m_tbl_def.get();
    return row_info.tx->set_status_error(ki.altered_table->in_use, s, kd, tbl);
  }

  if (!kd.m_can_skip_unique_check) {
    check_new_index_unique_inner(kd, ki, row_info, new_slice, is_rebuild, found);
  }

  return HA_EXIT_SUCCESS;
}

/* unique check and get lock of key for DDL altered table during DML
 *
 *@param[in] row_info: row record to insert or update
 *@param[out] if true it means primary key of new table is changed (either DML
 *            updates the value of pk, or definition of pk is changed by DDL)
 */
int ha_xengine::check_uniqueness_and_lock_rebuild(
  const struct update_row_info &row_info, bool *const pk_changed)
{
  int rc = HA_EXIT_SUCCESS;
  /*
    check new table index if necessary
    we need do unique-check for new-pk or new-uk, if failed,
    make sure dml transaction go ahead, and record duplicate-error,
    then make ddl transaction failed.
  */
  for (auto &k : m_tbl_def->m_inplace_new_keys) {
    const Xdb_key_def &kd = *k.first;
    TABLE *atab = k.second.altered_table;
    bool found = false;
    if (kd.is_hidden_primary_key()) {
      *pk_changed = false;
      // No need to check with copied hidden pk
      continue;
    } else if (kd.is_primary_key()) {
      *pk_changed = false;
      // check whether the DML will update primary key
      rc = lock_and_check_new_index_pk(kd, k.second, row_info,
                                       pk_changed, &found);

      // this primary key is newly added/re-defined
      // we need convert UPDATE to DEL+PUT
      *pk_changed = *pk_changed || !kd.m_can_skip_unique_check;
    } else {
      rc = lock_and_check_new_index_sk(kd, k.second, row_info, &found, true);
    }

    if (rc != HA_EXIT_SUCCESS){
      __XHANDLER_LOG(WARN, "check duplicated key failed for new key of inplace DDL, errcode is %d", rc);
      break;
    }
  }
  /* update new table for inplace rebuild online ddl if necessary */
  if (row_info.old_data == nullptr) {
    // this is for INSERT
    *pk_changed = false;
  }
  return rc;
}

/** update rebuild new table for inplace_rebuild_ddl
@param[in]	row_info	The DDL operation
@param[in]	pk_changed	flag indicate whether we should convert update to (del,put)
@return Success or Failure */
int ha_xengine::update_new_table(struct update_row_info &row_info, bool pk_changed) {
  int ret = HA_EXIT_SUCCESS;

  for (auto &k : m_tbl_def->m_inplace_new_keys) {
    const Xdb_key_def &kd = *k.first;
    const TABLE *atab = k.second.altered_table;

    if (k.second.step < Added_key_info::BUILDING_BASE_INDEX) {
      continue;
    }
    DBUG_ASSERT(!row_info.new_pk_slice.empty());

    if (kd.is_primary_key()) {
      ret = update_pk_for_new_table(kd, k.second, row_info, pk_changed, atab);
    } else {
      DBUG_ASSERT(kd.is_secondary_key());
      ret = update_sk_for_new_table(kd, k.second, row_info, atab);
    }

    if (ret > 0) {
      __XHANDLER_LOG(WARN, "XEngineDDL: update indexes error, code is %d, table_name: %s", ret, table->s->table_name.str);
      return ret;
    }
  }

  return ret;
}

xengine::common::Status ha_xengine::delete_or_singledelete_new_table(
    const TABLE *altered_table, Xdb_tbl_def *new_tbl_def,
    uint index, Xdb_transaction *const tx,
    xengine::db::ColumnFamilyHandle *const column_family,
    const xengine::common::Slice &key) {

    bool can_use_single_del_opt = false;

    /*
      - Secondary Indexes can always use SingleDelete.
      - If the index is PRIMARY KEY, and if all of the columns of the table
      are covered by the PRIMARY KEY, SingleDelete can be used.
    */
    if (index != pk_index(altered_table, new_tbl_def)) {
      can_use_single_del_opt = true;
    } else if (!has_hidden_pk(altered_table) &&
               altered_table->key_info[index].actual_key_parts == altered_table->s->fields) {
     can_use_single_del_opt = true;
   }

  if (can_use_single_del_opt)
    return tx->single_delete(column_family, key);

  return tx->delete_key(column_family, key);
}


/** update primary key for new table in inplace_rebuild_ddl
@param[in] kd, pk_def
@param[in] row_info, struct store old-data and new-data
@param[in] pk_changed, flag indicate whether we should convert update to (del,put)
@param[in] altered_table
@return Success or Failure */
int ha_xengine::update_pk_for_new_table(const Xdb_key_def &kd,
                          Added_key_info &ki,
                          const struct update_row_info &row_info,
                          const bool pk_changed,
                          const TABLE *altered_table) {
  int rc = HA_EXIT_SUCCESS;

  DBUG_ASSERT(altered_table != nullptr);
  DBUG_ASSERT(m_tbl_def->m_inplace_new_tdef);

  const uint key_id = kd.get_keyno();
  const bool hidden_pk = is_hidden_pk(key_id, altered_table, m_tbl_def->m_inplace_new_tdef);

  if (!hidden_pk && pk_changed) {
    /*
     * generate old_pk for new-table and delete it
    */
    DBUG_ASSERT(row_info.old_data != nullptr);
    DBUG_ASSERT(m_pk_packed_tuple_old);

    uint old_packed_size = 0;
    if (kd.pack_new_record(
            table, m_pack_buffer, row_info.old_data, m_pk_packed_tuple_old,
            nullptr, false, 0, 0, nullptr,
            altered_table, m_tbl_def->m_dict_info, old_packed_size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack old pk for new table error, table_name: %s", table->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
    assert(old_packed_size <= m_max_packed_sk_len);

    xengine::common::Slice old_pk_slice =
        xengine::common::Slice(reinterpret_cast<char*>(m_pk_packed_tuple_old), old_packed_size);
    const xengine::common::Status s =
        delete_or_singledelete_new_table(altered_table, m_tbl_def->m_inplace_new_tdef, key_id, row_info.tx, kd.get_cf(), old_pk_slice);
    if (!s.ok()) {
      __XHANDLER_LOG(WARN, "XEngineDDL: failed to delete old record(%s) for updating pk(%u), code is %d, table_name: %s",
                     old_pk_slice.ToString(true).c_str(),
                     kd.get_index_number(), s.code(), table->s->table_name.str);
      return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def.get());
    }
  }

// auto_increment already increased in old table, we just use newest value for new_table
/*
  if (table->next_number_field) {
    update_auto_incr_val();
  }
*/

  xengine::common::Slice value_slice;
  if ((rc = convert_new_record_from_old_record(
           table, altered_table, m_tbl_def->m_dict_info, row_info.new_pk_slice,
           row_info.new_pk_unpack_info, &value_slice, m_new_storage_record))) {
    if (rc == HA_ERR_INVALID_NULL_ERROR) {
      __XHANDLER_LOG(WARN, "XEngineDDL: convert null to not null failed, code is %d, table_name: %s", rc, table->s->table_name.str);

      //if errcode is set, set HA_ERR_INVALID_NULL_ERROR is failed, that'ok
      uint err = HA_EXIT_SUCCESS;
      ki.status.compare_exchange_strong(err, HA_ERR_INVALID_NULL_ERROR);

      return HA_EXIT_SUCCESS;
    } else {
      __XHANDLER_LOG(ERROR, "XEngineDDL: convert new record from old record error, code is %d, table_name: %s", rc, table->s->table_name.str);

      return rc;
    }
  }

  const auto cf = kd.get_cf();
  if (xengine_enable_bulk_load_api /*&& THDVAR(table->in_use, bulk_load)*/ &&
      !hidden_pk) {
    /*
      Write the primary key directly to an SST file using an SstFileWriter
     */
    rc = bulk_load_key(row_info.tx, kd, row_info.new_pk_slice, value_slice, 0);
  } else if (row_info.skip_unique_check) {
    /*
      It is responsibility of the user to make sure that the data being
      inserted doesn't violate any unique keys.
    */
    row_info.tx->get_blind_write_batch()->Put(cf, row_info.new_pk_slice,
                                              value_slice);
  } else if (row_info.tx->m_ddl_transaction) {
    /*
      DDL statement must check for unique key conflicts. For example:
      ALTER TABLE tbl DROP PRIMARY KEY, ADD PRIMARY KEY(non_unique_column)
    */
    row_info.tx->get_indexed_write_batch()->Put(cf, row_info.new_pk_slice,
                                                value_slice);
  } else {
    const auto s = row_info.tx->put(cf, row_info.new_pk_slice, value_slice);
    if (!s.ok()) {
      if (s.IsBusy()) {
        __XHANDLER_LOG(WARN, "XEngineDDL: duplicate entry is found during DML for key(%s) on pk:%d, table_name:%s",
                       row_info.new_pk_slice.ToString(true).c_str(),
                       kd.get_index_number(), table->s->table_name.str);
        errkey = table->s->primary_key;
        m_dupp_errkey = errkey;
        rc = HA_ERR_FOUND_DUPP_KEY;
        // ToDo confirm this is due to duplicate entry which should be handled by DDL
      } else {
        __XHANDLER_LOG(WARN, "XEngineDDL: failed to put record(%s:%s) for updating pk(%u), code is %d, table_name: %s",
                       row_info.new_pk_slice.ToString(true).c_str(),
                       value_slice.ToString(true).c_str(),
                       kd.get_index_number(), rc, table->s->table_name.str);
        rc = row_info.tx->set_status_error(table->in_use, s, *m_pk_descr,
                                           m_tbl_def.get());
      }
    }
  }

  return rc;
}


/** update secondary keys for new table in inplace_rebuild_ddl
@param[in] kd, xengine key definition
@param[in] row_info, struct store old-data and new-data
@param[in] altered_table MySQL new table
@return Success or Failure */
int ha_xengine::update_sk_for_new_table(const Xdb_key_def &kd,
                                        Added_key_info &ki,
                                        const struct update_row_info &row_info,
                                        const TABLE *const altered_table)
{
  uint new_packed_size;
  uint old_packed_size;

  xengine::common::Slice new_key_slice;
  xengine::common::Slice new_value_slice;
  xengine::common::Slice old_key_slice;

  const uint key_id = kd.get_keyno();
  /*
    Can skip updating this key if none of the key fields have changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  const bool store_row_debug_checksums = should_store_row_debug_checksums();

  if (kd.pack_new_record(table, m_pack_buffer, row_info.new_data,
                         m_sk_packed_tuple, &m_sk_tails,
                         store_row_debug_checksums,
                         row_info.hidden_pk_id, 0, nullptr, altered_table,
                         m_tbl_def->m_dict_info, new_packed_size)) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
    return HA_EXIT_FAILURE;
  }
  DBUG_ASSERT(new_packed_size <= m_max_packed_sk_len);

  if (row_info.old_data != nullptr) {
    // The old value
    if (kd.pack_new_record(table, m_pack_buffer, row_info.old_data,
                           m_sk_packed_tuple_old, &m_sk_tails_old,
                           store_row_debug_checksums, row_info.hidden_pk_id, 0,
                           nullptr, altered_table, m_tbl_def->m_dict_info,
                           old_packed_size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
    assert(old_packed_size <= m_max_packed_sk_len);

    /*
      Check if we are going to write the same value. This can happen when
      one does
        UPDATE tbl SET col='foo'
      and we are looking at the row that already has col='foo'.

      We also need to compare the unpack info. Suppose, the collation is
      case-insensitive, and unpack info contains information about whether
      the letters were uppercase and lowercase.  Then, both 'foo' and 'FOO'
      will have the same key value, but different data in unpack_info.

      (note: anyone changing bytewise_compare should take this code into
      account)
    */
    if (old_packed_size == new_packed_size &&
        m_sk_tails_old.get_current_pos() == m_sk_tails.get_current_pos() &&
        memcmp(m_sk_packed_tuple_old, m_sk_packed_tuple, old_packed_size) ==
            0 &&
        memcmp(m_sk_tails_old.ptr(), m_sk_tails.ptr(),
               m_sk_tails.get_current_pos()) == 0) {
      return HA_EXIT_SUCCESS;
    }

    /*
      Deleting entries from secondary index should skip locking, but
      be visible to the transaction.
      (also note that DDL statements do not delete rows, so this is not a DDL
       statement)
    */
    old_key_slice = xengine::common::Slice(
        reinterpret_cast<const char *>(m_sk_packed_tuple_old), old_packed_size);

    row_info.tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                         old_key_slice);
  }

  xengine::db::WriteBatchBase *write_batch;
  write_batch = row_info.tx->get_indexed_write_batch();

  new_key_slice = xengine::common::Slice(
      reinterpret_cast<const char *>(m_sk_packed_tuple), new_packed_size);
  new_value_slice =
      xengine::common::Slice(reinterpret_cast<const char *>(m_sk_tails.ptr()),
                     m_sk_tails.get_current_pos());

  write_batch->Put(kd.get_cf(), new_key_slice, new_value_slice);

  return HA_EXIT_SUCCESS;
}


/** delete for new table in inplace_rebuild_ddl
@param[in] row_info, struct store old-data and new-data
@return Success or Failure */
int ha_xengine::delete_row_new_table(struct update_row_info &row_info) {
  DBUG_ENTER_FUNC();

  int ret = HA_EXIT_SUCCESS;
  bool get_new_pk_flag = false;

  for (auto &k : m_tbl_def->m_inplace_new_keys) {
    const Xdb_key_def &kd = *k.first;
    const TABLE *atab = k.second.altered_table;

    if (k.second.step < Added_key_info::BUILDING_BASE_INDEX) {
      continue;
    }

    /** generate new primary key **/
    if (!get_new_pk_flag) {
      if ((ret = get_new_pk_for_delete(&row_info, atab))) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: get new primary pk for rebuild ddl error, table_name: %s", table->s->table_name.str);
        DBUG_RETURN(ret);
      }
      get_new_pk_flag = true;
    }

    Xdb_transaction *const tx = row_info.tx;
    DBUG_ASSERT(tx != nullptr);

    if (kd.is_primary_key()) {
      const uint index = pk_index(atab, m_tbl_def->m_inplace_new_tdef);
      xengine::common::Status s = delete_or_singledelete_new_table(
          atab, m_tbl_def->m_inplace_new_tdef, index, row_info.tx, kd.get_cf(),
          row_info.new_pk_slice);
      if (!s.ok()) {
        __XHANDLER_LOG(WARN, "XEngineDDL: failed to delete record(%s) on pk (%u) "
                             "of new table, code is %d, table_name: %s",
                       row_info.new_pk_slice.ToString(true).c_str(),
                       kd.get_index_number(), s.code(), table->s->table_name.str);
        DBUG_RETURN(
            tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
      }
    } else {
      uint packed_size;
      if ((ret = kd.pack_new_record(table, m_pack_buffer, row_info.old_data,
                                       m_sk_packed_tuple, &m_sk_tails,
                                       false, row_info.hidden_pk_id, 0, nullptr,
                                       atab, m_tbl_def->m_dict_info, packed_size))) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: pack new reocrd error, table_name: %s", table->s->table_name.str);
        DBUG_RETURN(HA_EXIT_FAILURE);
      }
      DBUG_ASSERT(packed_size <= m_max_packed_sk_len);

      xengine::common::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      /* Deleting on secondary key doesn't need any locks: */
      tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                  secondary_key_slice);

    }

    if (ret != HA_EXIT_SUCCESS) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: delete indexes error, code is %d, table_name: %s", ret, table->s->table_name.str);
      DBUG_RETURN(ret);
    }
  }

  DBUG_RETURN(ret);
}

/** Build new primary key for delete-stmt in storage format
@param[in] row_info, MySQL new/old record
@param[in] altered_table MySQL new table
@return SUCCESS/FAILURE */
int ha_xengine::get_new_pk_for_delete(struct update_row_info *const row_info,
                                      const TABLE *altered_table)
{
  DBUG_ENTER_FUNC();
  uint size = 0;

  DBUG_ASSERT(m_new_pk_descr != nullptr);

  if (!has_hidden_pk(altered_table)) {
    row_info->new_pk_unpack_info = &m_pk_unpack_info;

    if (m_new_pk_descr->pack_new_record(
            table, m_pack_buffer, row_info->old_data, m_pk_packed_tuple,
            row_info->new_pk_unpack_info, false, 0, 0, nullptr, altered_table,
            m_tbl_def->m_dict_info, size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    DBUG_ASSERT(size <= m_max_packed_sk_len);

  } else {
    //we don't support drop primary key online-ddl
    DBUG_ASSERT(has_hidden_pk(altered_table) && has_hidden_pk(table));
    if (read_hidden_pk_id_from_rowkey(&row_info->hidden_pk_id, &m_last_rowkey)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: read hidden_pk error, table_name: %s", table->s->table_name.str);
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    //for new subtable_id changed, we can't use m_last_rowkey directly.
    size =
        m_new_pk_descr->pack_hidden_pk(row_info->hidden_pk_id, m_pk_packed_tuple);
  }

  row_info->new_pk_slice =
      xengine::common::Slice((const char *)m_pk_packed_tuple, size);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}


/** Build new primary key for update-stmt in storage format
@param[in] row_info, MySQL new/old record
@param[in] altered_table MySQL new table
@return SUCCESS/FAILURE */
int ha_xengine::get_new_pk_for_update(struct update_row_info *const row_info, const TABLE *altered_table) {
  uint size = 0;

  /*
    Get new row key for any insert, and any update where the pk is not hidden.
    Row key for updates with hidden pk is handled below.
  */
  if (!has_hidden_pk(altered_table)) {
    row_info->hidden_pk_id = 0;

    row_info->new_pk_unpack_info = &m_pk_unpack_info;

    if (m_new_pk_descr->pack_new_record(
        table, m_pack_buffer, row_info->new_data, m_pk_packed_tuple,
        row_info->new_pk_unpack_info, false, 0, 0, nullptr, altered_table,
        m_tbl_def->m_dict_info, size)) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: pack new record error, table_name: %s", table->s->table_name.str);
      return HA_EXIT_FAILURE;
    }
    DBUG_ASSERT(size <= m_max_packed_sk_len);
  } else {
    //if new table has hidden_pk, there should be same with old table,
    //hidden_pk already set in get_pk_for_update
    DBUG_ASSERT(row_info->hidden_pk_id > 0);
    size =
        m_new_pk_descr->pack_hidden_pk(row_info->hidden_pk_id, m_pk_packed_tuple);
  }

  row_info->new_pk_slice =
      xengine::common::Slice((const char *)m_pk_packed_tuple, size);

  return HA_EXIT_SUCCESS;
}


/** fill duplicate value into new table record
TODO if pk is changed between old_table and new_table, it's hard to get
     old_table_record from new_pk_slice.

     1. if pk_changed, we use unpack_record get value from memcmp_key
     2. we try to get old_record, and then fill new duplicate record
     3. if step 2 failed(duplicate key deleted), we retry use unpack_record
@param[in] index  duplicate xengine key definition
@param[in] new_table_arg MySQL Table Object
@param[in] dup_key duplicate key
@param[in] dup_val duplicate value
@param[in] rebuild ddl or not */
int ha_xengine::fill_new_duplicate_record(const Xdb_key_def *index,
                                          TABLE *new_table_arg,
                                          const xengine::common::Slice &dup_key,
                                          const xengine::common::Slice &dup_val,
                                          bool is_rebuild)
{
  DBUG_ENTER_FUNC();

  bool pk_def_changed = false;
  for (auto &k : m_tbl_def->m_inplace_new_keys) {
    if (k.first->is_primary_key()) {
      pk_def_changed = !k.first->m_can_skip_unique_check;
      break;
    }
  }

  int res;
  if (pk_def_changed && is_rebuild) {
    if (index->is_primary_key()) {
      DBUG_ASSERT(!index->is_hidden_primary_key());
      if (index->get_support_icp_flag()) {
        res = index->unpack_record_pk(new_table_arg, new_table_arg->record[0],
                                      &dup_key, &dup_val, m_tbl_def->m_dict_info);
      } else {
        res = convert_record_from_storage_format(
            &dup_key, &dup_val, new_table_arg->record[0], new_table_arg,
            m_tbl_def->m_dict_info->m_null_bytes_in_rec,
            m_tbl_def->m_dict_info->m_maybe_unpack_info, index,
            m_tbl_def->m_dict_info->m_decoders_vect,
            m_tbl_def->m_dict_info->m_instant_ddl_info,
            m_tbl_def->m_dict_info->m_verify_row_debug_checksums,
            m_tbl_def->m_dict_info->m_row_checksums_checked);
      }
    } else {
      res = index->unpack_record(new_table_arg, new_table_arg->record[0],
                                 &dup_key, &dup_val, false);
    }

    if (res) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: unable to unpack record from value");
    }
  } else {
    //for utf8mb4_0900, try to use old-table record
    if ((res = fill_old_table_record(new_table_arg, table, dup_key, index))) {
      if (res != HA_ERR_KEY_NOT_FOUND) {
        __XHANDLER_LOG(ERROR, "XEngineDDL: Fill old table record failed, table_name:%s",
                     table->s->table_name.str);
        DBUG_RETURN(res);
      }
    }

    if (res == HA_ERR_KEY_NOT_FOUND) {
      DBUG_ASSERT(!index->is_primary_key());
      if (index->unpack_record(new_table_arg, new_table_arg->record[0],
                               &dup_key, &dup_val,
                               false)) {
        /* Should never reach here */
        __XHANDLER_LOG(ERROR, "XEngineDDL: Error retrieving index entry, table_name:%s",
                       table->s->table_name.str);
        DBUG_ASSERT(0);
      }
      __XHANDLER_LOG(INFO, "XEngineDDL: duplicated entry for index:%d is "
                           "deledted in old table:%s during online DML",
                     index->get_index_number(), table->s->table_name.str);
    } else if ((res = set_duplicate_key_for_print(new_table_arg, table, index,
                                                  m_tbl_def->m_dict_info, is_rebuild))) {
      __XHANDLER_LOG(ERROR,
                     "XEngineDDL: Set duplicate record for new table failed, "
                     "table_name:%s",
                     table->s->table_name.str);
      DBUG_RETURN(res);
    } else {
      __XHANDLER_LOG(INFO,
                     "XEngineDDL: Set duplicate record for new table successsfully, "
                     "table_name:%s",
                     table->s->table_name.str);
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/** Get lastest row by rowid, used find duplicate-key record.
    There is a case may duplicate-key record is deleted, then we need use
    unpack_record to get field-value from memcmp-key.But for utf8mb4_0900_ai_ci,
    we can't get varchar/char value.

    Only used by inplace ddl session.
*/
int ha_xengine::get_latest_row_by_rowid(uchar *const buf, const char *const rowid,
                                        const uint rowid_size) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(rowid != nullptr);
  DBUG_ASSERT(table != nullptr);

  int ret = HA_EXIT_SUCCESS;

  xengine::common::Slice key_slice(rowid, rowid_size);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);

  bool found;
  xengine::common::Status s;

  s = tx->get_latest(m_pk_descr->get_cf(), key_slice, &m_retrieved_record);
  if (!s.IsNotFound() && !s.ok()) {
    __XHANDLER_LOG(WARN, "XEngienDDL: lock failed for key(%s) on index %u, code is %d, table_name is: %s",
                   key_slice.ToString(true).c_str(), m_pk_descr->get_index_number(),
                   s.code(), table->s->table_name.str);
    DBUG_RETURN(tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
  }
  found = !s.IsNotFound();

  if (found) {
    m_last_rowkey.copy((const char *)rowid, rowid_size, &my_charset_bin);
    ret = convert_record_from_storage_format(&key_slice, m_retrieved_record,
                                             buf, table);
  } else {
    /*
      Note: we don't need to unlock the row. It is intentional that we keep
      locks on rows that don't exist.
    */
    ret = HA_ERR_KEY_NOT_FOUND;
  }

  DBUG_RETURN(ret);
}

/** We need fill old record before print duplicate key information.
@param[in] old_table
@param[in] dup_key, duplicate_key found during buiding index
@param[in] dup_kd,  duplicate xengine key definition
@return SUCCESS/FAILURE */
int ha_xengine::fill_old_table_record(TABLE *new_table,
                                      TABLE *old_table,
                                      const xengine::common::Slice &dup_key,
                                      const Xdb_key_def *dup_kd)
{
  int ret = HA_EXIT_SUCCESS;
  uint pk_size = 0;

  if (dup_kd->is_primary_key()) {
    //use old_pk index_number
    memcpy(m_pk_packed_tuple, dup_key.data(), dup_key.size());
    xdb_netbuf_store_index(m_pk_packed_tuple, m_pk_descr->get_index_number());
    pk_size = dup_key.size();
  } else {
    pk_size =
        dup_kd->get_primary_key_tuple(new_table, *m_pk_descr, &dup_key, m_pk_packed_tuple);
    if (pk_size == XDB_INVALID_KEY_LEN) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: Get primary key failed, table_name: %s", table->s->table_name.str);
      ret = HA_ERR_INTERNAL_ERROR;
    }
  }

  if (ret != HA_EXIT_SUCCESS) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: Fill table record failed, table_name: %s", table->s->table_name.str);
    return ret;
  } else if ((ret = get_latest_row_by_rowid(old_table->record[0], reinterpret_cast<const char *>(m_pk_packed_tuple), pk_size))) {
    if (ret == HA_ERR_KEY_NOT_FOUND) {
      __XHANDLER_LOG(WARN, "XEngineDDL: duplicate record maybe deleted, table_name: %s", table->s->table_name.str);
    } else {
      __XHANDLER_LOG(ERROR, "XEngineDDL: Find duplicate record error, table_name: %s", table->s->table_name.str);
    }

    return ret;
  }

  return HA_EXIT_SUCCESS;
}

static int fill_duplicate_blob_val(Field_blob *const old_lob,
                                   Field_blob *const new_lob)
{
  const uint length_bytes = old_lob->pack_length() - portable_sizeof_char_ptr;
  char *data_ptr;
  memcpy(&data_ptr, old_lob->ptr + length_bytes, sizeof(uchar **));

  if (new_lob->store(data_ptr, old_lob->get_length(), old_lob->charset())) {
    return HA_EXIT_FAILURE;
  }

  return HA_EXIT_SUCCESS;
}

/** Set duplicate field value for new table, the value comes for old_reocrd.
  If field is added in the ddl, we use default value to fill.
@param[in] new_table Altered_table
@param[in] old_table table before DDL operation
@param[in] kd, duplicate key found during ddl
@param[in] dict_info, dictionary used for ddl, such as col_map
@param[in] is_rebuild, if ddl is rebuild ddl
@return SUCCESS/FAILURE */
static int set_duplicate_key_for_print(
    TABLE *new_table, TABLE *old_table, const Xdb_key_def *kd,
    const std::shared_ptr<Xdb_inplace_ddl_dict_info>& dict_info, bool is_rebuild)
{
  const bool is_secondary_key = kd->is_secondary_key();
  const bool hidden_pk_exists = (new_table->s->primary_key == MAX_INDEXES);

  const bool is_hidden_pk = kd->is_hidden_primary_key();
  DBUG_ASSERT(!is_hidden_pk);

  uint *col_map = nullptr;
  if (is_rebuild) {
    DBUG_ASSERT(dict_info != nullptr);
    col_map = dict_info->m_col_map;
  } else {
    DBUG_ASSERT(dict_info == nullptr);
  }

  uint old_col_index = 0;
  for (uint i = 0; i < kd->get_key_parts(); i++) {
    /*
      Hidden pk field is packed at the end of the secondary keys, but the SQL
      layer does not know about it. Skip retrieving field if hidden pk.
    */
    if ((is_secondary_key && hidden_pk_exists && i + 1 == kd->get_key_parts())) {
      continue;
    }

    Field *field = kd->get_table_field_for_part_no(new_table, i);
    Field *old_field = nullptr;

    bool use_old_field = false;
    if (is_rebuild) {
      old_col_index = col_map[field->field_index];
      if (old_col_index != (uint)(-1)) {
        old_field = old_table->field[old_col_index];
        use_old_field = true;
      }
    } else {
      old_field = old_table->field[field->field_index];
      use_old_field = true;
    }

    if (use_old_field) {
      //If lob type is part of key, we need store blob value for new field
      //Bug #24173631
      if (old_field->real_type() == MYSQL_TYPE_BLOB ||
          old_field->real_type() == MYSQL_TYPE_JSON) {
        DBUG_ASSERT(field->real_type() == old_field->real_type());
        if (fill_duplicate_blob_val((Field_blob *)old_field,
                                    (Field_blob *)field)) {
          __XHANDLER_LOG(ERROR,
                         "store duplicated blob field failed, index_id: %d",
                         kd->get_index_number());
          return HA_EXIT_FAILURE;
        }
      } else {
        // for non-duplicate sk, may be duplicated because of new primary-key.
        // DBUG_ASSERT(!old_field->is_real_null());
        memcpy(field->ptr, old_field->ptr, old_field->pack_length());
      }
    } else {
      DBUG_ASSERT(is_rebuild);
      // blob can not have default value.
      DBUG_ASSERT(field->real_type() != MYSQL_TYPE_BLOB &&
                  field->real_type() != MYSQL_TYPE_JSON);
      uint field_offset = field->ptr - new_table->record[0];
      memcpy(field->ptr, new_table->s->default_values + field_offset,
             field->pack_length());
    }

    field->set_notnull();
  }

  return HA_EXIT_SUCCESS;
}

} // namespace myx
