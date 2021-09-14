/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012, Monty Program Ab

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/* C++ standard header files */
#include <map>
#include <string>
#include <vector>

/* MySQL header files */
#include "table.h"
#include "mysql/plugin.h"
#include <sql_show.h>
#include "sql/sql_class.h"
#include "sql/dd/dd_schema.h"
#include "storage/perfschema/pfs_visitor.h"

/* X-Engine header files */
#include "./core/compact/compaction.h"
#include "./core/db/version_set.h"
#include "./core/compact/task_type.h"
#include "./core/storage/extent_space_manager.h"
#include "xengine/compaction_filter.h"
#include "xengine/convenience.h"
#include "xengine/filter_policy.h"
#include "xengine/memtablerep.h"
#include "xengine/merge_operator.h"
#include "xengine/slice_transform.h"
#include "xengine/utilities/transaction_db.h"
#include "./core/db/db_impl.h"
#include "./core/db/debug_info.h"
#include "./core/utilities/transactions/transaction_db_impl.h"
#include "./core/db/internal_stats.h"
#include "xengine/thread_status.h"
#include "./core/storage/multi_version_extent_meta_layer.h"

/* MyX header files */
#include "./ha_xengine.h"
#include "./ha_xengine_proto.h"
#include "./xdb_cf_manager.h"
#include "./xdb_datadic.h"
#include "./xdb_utils.h"
#include "./dd_operations.h"
#include "./xdb_i_s.h"

extern SHOW_VAR status_vars[];
extern struct System_status_var global_status_var;
extern mysql_mutex_t LOCK_status;

namespace myx {
bool Xdb_dd_helper::traverse_all_xengine_tables(
    THD *thd, bool exit_on_failure, ulong lock_timeout,
    std::function<bool(const dd::Schema *, const dd::Table *)> &&visitor) {
  auto client = thd->dd_client();
  std::vector<dd::String_type> schema_names;
  if (client->fetch_global_component_names<dd::Schema>(&schema_names)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to fetch schema names!");
    return true;
  }

  for (auto &schema_name : schema_names) {
    dd::Schema_MDL_locker schema_mdl_locker(thd);
    dd::cache::Dictionary_client::Auto_releaser schema_releaser(client);
    const dd::Schema *dd_schema = nullptr;
    std::vector<dd::String_type> table_names;
    if (schema_mdl_locker.ensure_locked(schema_name.c_str()) ||
        client->acquire(schema_name, &dd_schema)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire dd::Schema object",
                   "schema_name", schema_name.c_str());
      if (!exit_on_failure) continue;
      return true;
    } else if (nullptr == dd_schema) {
      XHANDLER_LOG(INFO, "XEngine: dd::Schema was dropped", "schema_name",
                   schema_name.c_str());
      continue;
    } else if (client->fetch_schema_table_names_by_engine(
                   dd_schema, xengine_hton_name, &table_names)) {
      XHANDLER_LOG(ERROR,
                   "XEngine: failed to fetch table names from dd::Schema",
                   "schema_name", schema_name.c_str());
      if (!exit_on_failure) continue;
      return true;
    } else {
      for (auto &table_name : table_names) {
        dd::cache::Dictionary_client::Auto_releaser tbl_releaser(client);
        MDL_ticket *tbl_ticket = nullptr;
        const dd::Table *dd_table = nullptr;
        if (!acquire_xengine_table(thd, lock_timeout, schema_name.c_str(),
                                   table_name.c_str(), dd_table, tbl_ticket) &&
            (nullptr != dd_table)) {
          bool error = visitor(dd_schema, dd_table);
          if (tbl_ticket) dd::release_mdl(thd, tbl_ticket);
          if (error && exit_on_failure) return true;
        }
      }
    }
  }
  return false;
}

bool Xdb_dd_helper::get_schema_id_map(
    dd::cache::Dictionary_client* client,
    std::vector<const dd::Schema*>& all_schemas,
    std::map<dd::Object_id, const dd::Schema*>& schema_id_map)
{
  all_schemas.clear();
  schema_id_map.clear();
  if (!client->fetch_global_components(&all_schemas)) {
    for (auto& sch : all_schemas) {
      schema_id_map.insert({sch->id(), sch});
    }
    return false;
  }
  return true;
}

bool Xdb_dd_helper::get_xengine_subtable_map(THD* thd,
    std::map<uint32_t, std::pair<std::string, std::string>>& subtable_map)
{
  subtable_map.clear();
  bool error = Xdb_dd_helper::traverse_all_xengine_tables(
      thd, true, 0,
      [&subtable_map](const dd::Schema *dd_schema,
                      const dd::Table *dd_table) -> bool {
        std::ostringstream oss;
        oss << dd_schema->name() << '.' << dd_table->name();

        uint32_t subtable_id = 0;
        for (auto &dd_index : dd_table->indexes()) {
          const auto &p = dd_index->se_private_data();
          if (!p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
              p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id)) {
            XHANDLER_LOG(ERROR, "XEngine: failed to get subtable_id",
                         "table_name", dd_table->name().c_str(), "index_name",
                         dd_index->name().c_str());
            return true;
          } else {
            subtable_map.emplace(
                subtable_id,
                std::make_pair(oss.str(), dd_index->name().c_str()));
          }
        }

        const auto &p = dd_table->se_private_data();
        if (p.exists(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID])) {
          if (p.get(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID],
                    &subtable_id)) {
            XHANDLER_LOG(ERROR,
                         "XEngine: failed to get subtable_id for hidden pk",
                         "table_name", dd_table->name().c_str());
            return true;
          } else {
            subtable_map.emplace(subtable_id,
                                 std::make_pair(oss.str(), HIDDEN_PK_NAME));
          }
        }

        return false;
      });

  return error;
}

bool Xdb_dd_helper::acquire_xengine_table(THD* thd, ulong lock_timeout,
    const char* schema_name, const char* table_name,
    const dd::Table*& dd_table, MDL_ticket*& mdl_ticket)
{
  bool error = false;
  dd_table = nullptr;
  // acquire the shared MDL lock if don't own
  if (!dd::has_shared_table_mdl(thd, schema_name, table_name)) {
    if (!lock_timeout) lock_timeout = thd->variables.lock_wait_timeout;
    MDL_request mdl_request;
    MDL_REQUEST_INIT(&mdl_request, MDL_key::TABLE, schema_name, table_name,
                     MDL_SHARED, MDL_EXPLICIT);
    if (thd->mdl_context.acquire_lock(&mdl_request, lock_timeout)) {
      XHANDLER_LOG(ERROR, "XEngine: failed to acquire shared lock on dd::Table",
                   K(schema_name), K(table_name));
      return true;
    } else {
      mdl_ticket = mdl_request.ticket;
    }
  }

  // acquire the dd::Table and verify if it is XEngine table
  if (thd->dd_client()->acquire(schema_name, table_name, &dd_table)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to acquire dd::Table",
                 K(schema_name), K(table_name));
    error = true;
  } else if (!dd_table|| dd_table->engine() != xengine_hton_name) {
    XHANDLER_LOG(WARN, "XEngine: no such xengine table",
                 K(schema_name), K(table_name));
    dd_table = nullptr;
  }

  if (!dd_table && mdl_ticket) {
    dd::release_mdl(thd, mdl_ticket);
    mdl_ticket = nullptr;
  }

  return error;
}

bool Xdb_dd_helper::get_xengine_subtable_ids(THD* thd, ulong lock_timeout,
                                             std::set<uint32_t> &subtable_ids)
{
  subtable_ids.clear();
  bool error = traverse_all_xengine_tables(
      thd, true, lock_timeout,
      [&subtable_ids](const dd::Schema *, const dd::Table *dd_table) -> bool {
        uint32_t subtable_id = 0;
        for (auto &dd_index : dd_table->indexes()) {
          const auto &p = dd_index->se_private_data();
          if (!p.exists(dd_index_key_strings[DD_INDEX_SUBTABLE_ID]) ||
              p.get(dd_index_key_strings[DD_INDEX_SUBTABLE_ID], &subtable_id)) {
            XHANDLER_LOG(ERROR, "XEngine: failed to get subtable_id",
                         "table_name", dd_table->name().c_str(), "index_name",
                         dd_index->name().c_str());
            return true;
          }
          subtable_ids.insert(subtable_id);
        }

        const auto &p = dd_table->se_private_data();
        if (p.exists(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID])) {
          if (p.get(dd_table_key_strings[DD_TABLE_HIDDEN_PK_ID],
                    &subtable_id)) {
            XHANDLER_LOG(ERROR,
                         "XEngine: failed to get subtable_id for hidden pk",
                         "table_name", dd_table->name().c_str());
            return true;
          } else {
            subtable_ids.insert(subtable_id);
          }
        }
        return false;
      });

  if (error) {
    XHANDLER_LOG(ERROR,
                 "XEngine: failed to collect subtable ids for all XEngine "
                 "table from data dictionary");
  }

  return error;
}

/**
  Define the INFORMATION_SCHEMA (I_S) structures needed by MyX storage
  engine.
*/

#define XENGINE_FIELD_INFO(_name_, _len_, _type_, _flag_)                      \
  { _name_, _len_, _type_, 0, _flag_, nullptr, 0 }

#define XENGINE_FIELD_INFO_END                                                 \
  XENGINE_FIELD_INFO(nullptr, 0, MYSQL_TYPE_NULL, 0)

/*
  Support for INFORMATION_SCHEMA.XENGINE_CFSTATS dynamic table
 */
namespace XDB_CFSTATS_FIELD {
enum { SUBTABLE_ID = 0, STAT_TYPE, VALUE };
} // namespace XDB_CFSTATS_FIELD

static ST_FIELD_INFO xdb_i_s_cfstats_fields_info[] = {
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("STAT_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_cfstats_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  bool ret;
  uint64_t val;

  const std::vector<std::pair<const std::string, std::string>> cf_properties = {
      {xengine::db::DB::Properties::kNumImmutableMemTable,
       "NUM_IMMUTABLE_MEM_TABLE"},
      {xengine::db::DB::Properties::kMemTableFlushPending,
       "MEM_TABLE_FLUSH_PENDING"},
      {xengine::db::DB::Properties::kCompactionPending, "COMPACTION_PENDING"},
      {xengine::db::DB::Properties::kCurSizeActiveMemTable,
       "CUR_SIZE_ACTIVE_MEM_TABLE"},
      {xengine::db::DB::Properties::kCurSizeAllMemTables,
       "CUR_SIZE_ALL_MEM_TABLES"},
      {xengine::db::DB::Properties::kNumEntriesActiveMemTable,
       "NUM_ENTRIES_ACTIVE_MEM_TABLE"},
      {xengine::db::DB::Properties::kNumEntriesImmMemTables,
       "NUM_ENTRIES_IMM_MEM_TABLES"},
      {xengine::db::DB::Properties::kEstimateTableReadersMem,
       "NON_BLOCK_CACHE_SST_MEM_USAGE"},
      {xengine::db::DB::Properties::kNumLiveVersions, "NUM_LIVE_VERSIONS"}};

  xengine::db::DB *const xdb = xdb_get_xengine_db();
  const Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  DBUG_ASSERT(xdb != nullptr);

  for (const auto &subtable_id : cf_manager.get_subtable_ids()) {
    xengine::db::ColumnFamilyHandle *cfh;

    cfh = cf_manager.get_cf(subtable_id);
    if (cfh == nullptr)
      continue;

    for (const auto &property : cf_properties) {
      if (!xdb->GetIntProperty(cfh, property.first, &val))
        continue;

      DBUG_ASSERT(tables != nullptr);

      tables->table->field[XDB_CFSTATS_FIELD::SUBTABLE_ID]->store(
          subtable_id, true);
      tables->table->field[XDB_CFSTATS_FIELD::STAT_TYPE]->store(
          property.second.c_str(), property.second.size(), system_charset_info);
      tables->table->field[XDB_CFSTATS_FIELD::VALUE]->store(val, true);

      ret = my_core::schema_table_store_record(thd, tables->table);

      if (ret)
        DBUG_RETURN(ret);
    }
  }
  DBUG_RETURN(0);
}

static int xdb_i_s_cfstats_init(void *p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_cfstats_fields_info;
    schema->fill_table = xdb_i_s_cfstats_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_DBSTATS dynamic table
 */
namespace XDB_DBSTATS_FIELD {
enum { STAT_TYPE = 0, VALUE };
} // namespace XDB_DBSTATS_FIELD
static const int64_t PC_MAX_IDX = 0;

static ST_FIELD_INFO xdb_i_s_dbstats_fields_info[] = {
    XENGINE_FIELD_INFO("STAT_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_dbstats_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  bool ret;
  uint64_t val;

  const std::vector<std::pair<std::string, std::string>> db_properties = {
      {xengine::db::DB::Properties::kBackgroundErrors, "DB_BACKGROUND_ERRORS"},
      {xengine::db::DB::Properties::kNumSnapshots, "DB_NUM_SNAPSHOTS"},
      {xengine::db::DB::Properties::kOldestSnapshotTime,
       "DB_OLDEST_SNAPSHOT_TIME"}};

  xengine::db::DB *const xdb = xdb_get_xengine_db();
  const xengine::table::BlockBasedTableOptions &table_options =
      xdb_get_table_options();

  for (const auto &property : db_properties) {
    if (!xdb->GetIntProperty(property.first, &val))
      continue;

    DBUG_ASSERT(tables != nullptr);

    tables->table->field[XDB_DBSTATS_FIELD::STAT_TYPE]->store(
        property.second.c_str(), property.second.size(), system_charset_info);
    tables->table->field[XDB_DBSTATS_FIELD::VALUE]->store(val, true);

    ret = my_core::schema_table_store_record(thd, tables->table);

    if (ret)
      DBUG_RETURN(ret);
  }

  /*
    Currently, this can only show the usage of a block cache allocated
    directly by the handlerton. If the column family config specifies a block
    cache (i.e. the column family option has a parameter such as
    block_based_table_factory={block_cache=1G}), then the block cache is
    allocated within the xengine::common::GetColumnFamilyOptionsFromString().

    There is no interface to retrieve this block cache, nor fetch the usage
    information from the column family.
   */
  val = (table_options.block_cache ? table_options.block_cache->GetUsage() : 0);
  tables->table->field[XDB_DBSTATS_FIELD::STAT_TYPE]->store(
      STRING_WITH_LEN("DB_BLOCK_CACHE_USAGE"), system_charset_info);
  tables->table->field[XDB_DBSTATS_FIELD::VALUE]->store(val, true);

  ret = my_core::schema_table_store_record(thd, tables->table);

  DBUG_RETURN(ret);
}

static int xdb_i_s_dbstats_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_dbstats_fields_info;
    schema->fill_table = xdb_i_s_dbstats_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_PERF_CONTEXT dynamic table
 */
namespace XDB_PERF_CONTEXT_FIELD {
enum { TABLE_SCHEMA = 0, TABLE_NAME, PARTITION_NAME, STAT_TYPE, VALUE };
} // namespace XDB_PERF_CONTEXT_FIELD

static ST_FIELD_INFO xdb_i_s_perf_context_fields_info[] = {
    XENGINE_FIELD_INFO("TABLE_SCHEMA", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("TABLE_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("PARTITION_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING,
                       MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("STAT_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_perf_context_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);

  int ret = 0;
  Field **field = tables->table->field;

  const std::vector<std::string> tablenames = xdb_get_open_table_names();
  for (const auto &it : tablenames) {
    std::string str, dbname, tablename, partname;

    if (xdb_normalize_tablename(it, &str)) {
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }

    if (xdb_split_normalized_tablename(str, &dbname, &tablename, &partname)) {
      continue;
    }

    DBUG_ASSERT(field != nullptr);

    // name string in Xdb_tbl_def is from filename which uses my_system_filename
    field[XDB_PERF_CONTEXT_FIELD::TABLE_SCHEMA]->store(
        dbname.c_str(), dbname.size(), &my_charset_filename);
    field[XDB_PERF_CONTEXT_FIELD::TABLE_NAME]->store(
        tablename.c_str(), tablename.size(), &my_charset_filename);
    if (partname.empty()) {
      field[XDB_PERF_CONTEXT_FIELD::PARTITION_NAME]->set_null();
    } else {
      field[XDB_PERF_CONTEXT_FIELD::PARTITION_NAME]->set_notnull();
      field[XDB_PERF_CONTEXT_FIELD::PARTITION_NAME]->store(
          partname.c_str(), partname.size(), system_charset_info);
    }
  }

  DBUG_RETURN(0);
}

static int xdb_i_s_perf_context_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_perf_context_fields_info;
    schema->fill_table = xdb_i_s_perf_context_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_PERF_CONTEXT_GLOBAL dynamic table
 */
namespace XDB_PERF_CONTEXT_GLOBAL_FIELD {
enum { STAT_TYPE = 0, VALUE };
} // namespace XDB_PERF_CONTEXT_GLOBAL_FIELD

static ST_FIELD_INFO xdb_i_s_perf_context_global_fields_info[] = {
    XENGINE_FIELD_INFO("STAT_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_perf_context_global_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);
  DBUG_RETURN(0);
}

static int xdb_i_s_perf_context_global_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);
  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_perf_context_global_fields_info;
    schema->fill_table = xdb_i_s_perf_context_global_fill_table;
  }

  DBUG_RETURN(ret);
}

#if 0
/*
  Support for INFORMATION_SCHEMA.XENGINE_CFOPTIONS dynamic table
 */
namespace XDB_CFOPTIONS_FIELD {
enum { CF_NAME = 0, OPTION_TYPE, VALUE };
} // namespace XDB_CFOPTIONS_FIELD

static ST_FIELD_INFO xdb_i_s_cfoptions_fields_info[] = {
    XENGINE_FIELD_INFO("CF_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("OPTION_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_cfoptions_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);

  bool ret;

  Xdb_cf_manager &cf_manager = xdb_get_cf_manager();

  std::string val;
  xengine::common::ColumnFamilyOptions opts;
  std::string dummy_cf_name("default");
  cf_manager.get_cf_options(dummy_cf_name, &opts);

  std::vector<std::pair<std::string, std::string>> cf_option_types = {
      {"COMPARATOR", opts.comparator == nullptr
                          ? "NULL"
                          : std::string(opts.comparator->Name())},
      {"MERGE_OPERATOR", opts.merge_operator == nullptr
                              ? "NULL"
                              : std::string(opts.merge_operator->Name())},
      {"COMPACTION_FILTER",
        opts.compaction_filter == nullptr
            ? "NULL"
            : std::string(opts.compaction_filter->Name())},
      {"COMPACTION_FILTER_FACTORY",
        opts.compaction_filter_factory == nullptr
        ? "NULL"
        : std::string(opts.compaction_filter_factory->Name())},
      {"WRITE_BUFFER_SIZE", std::to_string(opts.write_buffer_size)},
      {"FLUSH_DELETE_PERCENT", std::to_string(opts.flush_delete_percent)},
      {"COMPACTION_DELETE_PERCENT", std::to_string(opts.compaction_delete_percent)},
      {"FLUSH_DELETE_PERCENT_TRIGGER", std::to_string(opts.flush_delete_percent_trigger)},
      {"FLUSH_DELETE_RECORD_TRIGGER", std::to_string(opts.flush_delete_record_trigger)},
      {"MAX_WRITE_BUFFER_NUMBER",
        std::to_string(opts.max_write_buffer_number)},
      {"MIN_WRITE_BUFFER_NUMBER_TO_MERGE",
        std::to_string(opts.min_write_buffer_number_to_merge)},
      {"NUM_LEVELS", std::to_string(opts.num_levels)},
      {"LEVEL0_FILE_NUM_COMPACTION_TRIGGER",
        std::to_string(opts.level0_file_num_compaction_trigger)},
      {"LEVEL0_LAYER_NUM_COMPACTION_TRIGGER",
        std::to_string(opts.level0_layer_num_compaction_trigger)},
      {"MINOR_WINDOW_SIZE", std::to_string(opts.minor_window_size)},
      {"LEVEL1_EXTENTS_MAJOR_COMPACTION_TRIGGER",
        std::to_string(opts.level1_extents_major_compaction_trigger)},
      {"LEVEL0_SLOWDOWN_WRITES_TRIGGER",
        std::to_string(opts.level0_slowdown_writes_trigger)},
      {"LEVEL0_STOP_WRITES_TRIGGER",
        std::to_string(opts.level0_stop_writes_trigger)},
      {"MAX_MEM_COMPACTION_LEVEL",
        std::to_string(opts.max_mem_compaction_level)},
      {"TARGET_FILE_SIZE_BASE", std::to_string(opts.target_file_size_base)},
      {"TARGET_FILE_SIZE_MULTIPLIER",
        std::to_string(opts.target_file_size_multiplier)},
      {"MAX_BYTES_FOR_LEVEL_BASE",
        std::to_string(opts.max_bytes_for_level_base)},
      {"LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES",
        opts.level_compaction_dynamic_level_bytes ? "ON" : "OFF"},
      {"MAX_BYTES_FOR_LEVEL_MULTIPLIER",
        std::to_string(opts.max_bytes_for_level_multiplier)},
      {"SOFT_RATE_LIMIT", std::to_string(opts.soft_rate_limit)},
      {"HARD_RATE_LIMIT", std::to_string(opts.hard_rate_limit)},
      {"RATE_LIMIT_DELAY_MAX_MILLISECONDS",
        std::to_string(opts.rate_limit_delay_max_milliseconds)},
      {"ARENA_BLOCK_SIZE", std::to_string(opts.arena_block_size)},
      {"DISABLE_AUTO_COMPACTIONS",
        opts.disable_auto_compactions ? "ON" : "OFF"},
      {"PURGE_REDUNDANT_KVS_WHILE_FLUSH",
        opts.purge_redundant_kvs_while_flush ? "ON" : "OFF"},
      {"MAX_SEQUENTIAL_SKIP_IN_ITERATIONS",
        std::to_string(opts.max_sequential_skip_in_iterations)},
      {"MEMTABLE_FACTORY", opts.memtable_factory == nullptr
                                ? "NULL"
                                : opts.memtable_factory->Name()},
      {"INPLACE_UPDATE_SUPPORT", opts.inplace_update_support ? "ON" : "OFF"},
      {"INPLACE_UPDATE_NUM_LOCKS",
        opts.inplace_update_num_locks ? "ON" : "OFF"},
      {"MEMTABLE_PREFIX_BLOOM_BITS_RATIO",
        std::to_string(opts.memtable_prefix_bloom_size_ratio)},
      {"MEMTABLE_PREFIX_BLOOM_HUGE_PAGE_TLB_SIZE",
        std::to_string(opts.memtable_huge_page_size)},
      {"BLOOM_LOCALITY", std::to_string(opts.bloom_locality)},
      {"MAX_SUCCESSIVE_MERGES", std::to_string(opts.max_successive_merges)},
      {"OPTIMIZE_FILTERS_FOR_HITS",
        (opts.optimize_filters_for_hits ? "ON" : "OFF")},
  };

  // get MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL option value
  val = opts.max_bytes_for_level_multiplier_additional.empty() ? "NULL" : "";
  for (const auto &level : opts.max_bytes_for_level_multiplier_additional) {
    val.append(std::to_string(level) + ":");
  }
  val.pop_back();
  cf_option_types.push_back(
      {"MAX_BYTES_FOR_LEVEL_MULTIPLIER_ADDITIONAL", val});

  // get COMPRESSION_TYPE option value
  GetStringFromCompressionType(&val, opts.compression);
  if (val.empty()) {
    val = "NULL";
  }
  cf_option_types.push_back({"COMPRESSION_TYPE", val});

  // get COMPRESSION_PER_LEVEL option value
  val = opts.compression_per_level.empty() ? "NULL" : "";
  for (const auto &compression_type : opts.compression_per_level) {
    std::string res;
    GetStringFromCompressionType(&res, compression_type);
    if (!res.empty()) {
      val.append(res + ":");
    }
  }
  val.pop_back();
  cf_option_types.push_back({"COMPRESSION_PER_LEVEL", val});

  // get compression_opts value
  val = std::to_string(opts.compression_opts.window_bits) + ":";
  val.append(std::to_string(opts.compression_opts.level) + ":");
  val.append(std::to_string(opts.compression_opts.strategy));
  cf_option_types.push_back({"COMPRESSION_OPTS", val});

  // bottommost_compression
  if (opts.bottommost_compression) {
    std::string res;
    GetStringFromCompressionType(&res, opts.bottommost_compression);
    if (!res.empty()) {
      cf_option_types.push_back({"BOTTOMMOST_COMPRESSION", res});
    }
  }

  // get PREFIX_EXTRACTOR option
  cf_option_types.push_back(
      {"PREFIX_EXTRACTOR", opts.prefix_extractor == nullptr
                                ? "NULL"
                                : std::string(opts.prefix_extractor->Name())});

  // get COMPACTION_STYLE option
  switch (opts.compaction_style) {
    case xengine::common::kCompactionStyleLevel:
      val = "kCompactionStyleLevel";
      break;
    case xengine::common::kCompactionStyleUniversal:
      val = "kCompactionStyleUniversal";
      break;
    case xengine::common::kCompactionStyleFIFO:
      val = "kCompactionStyleFIFO";
      break;
    case xengine::common::kCompactionStyleNone:
      val = "kCompactionStyleNone";
      break;
    default:
      val = "NULL";
    }
    cf_option_types.push_back({"COMPACTION_STYLE", val});

    // get COMPACTION_OPTIONS_UNIVERSAL related options
    const xengine::common::CompactionOptionsUniversal compac_opts =
        opts.compaction_options_universal;
    val = "{SIZE_RATIO=";
    val.append(std::to_string(compac_opts.size_ratio));
    val.append("; MIN_MERGE_WIDTH=");
    val.append(std::to_string(compac_opts.min_merge_width));
    val.append("; MAX_MERGE_WIDTH=");
    val.append(std::to_string(compac_opts.max_merge_width));
    val.append("; MAX_SIZE_AMPLIFICATION_PERCENT=");
    val.append(std::to_string(compac_opts.max_size_amplification_percent));
    val.append("; COMPRESSION_SIZE_PERCENT=");
    val.append(std::to_string(compac_opts.compression_size_percent));
    val.append("; STOP_STYLE=");
    switch (compac_opts.stop_style) {
    case xengine::common::kCompactionStopStyleSimilarSize:
      val.append("kCompactionStopStyleSimilarSize}");
      break;
    case xengine::common::kCompactionStopStyleTotalSize:
      val.append("kCompactionStopStyleTotalSize}");
      break;
    default:
      val.append("}");
  }
  cf_option_types.push_back({"COMPACTION_OPTIONS_UNIVERSAL", val});

  // get COMPACTION_OPTION_FIFO option
  cf_option_types.push_back(
      {"COMPACTION_OPTION_FIFO::MAX_TABLE_FILES_SIZE",
        std::to_string(opts.compaction_options_fifo.max_table_files_size)});

  // get block-based table related options
  const xengine::table::BlockBasedTableOptions &table_options =
      xdb_get_table_options();

  // get BLOCK_BASED_TABLE_FACTORY::CACHE_INDEX_AND_FILTER_BLOCKS option
  cf_option_types.push_back(
      {"BLOCK_BASED_TABLE_FACTORY::CACHE_INDEX_AND_FILTER_BLOCKS",
        table_options.cache_index_and_filter_blocks ? "1" : "0"});

  // get BLOCK_BASED_TABLE_FACTORY::INDEX_TYPE option value
  switch (table_options.index_type) {
    case xengine::table::BlockBasedTableOptions::kBinarySearch:
      val = "kBinarySearch";
      break;
    case xengine::table::BlockBasedTableOptions::kHashSearch:
      val = "kHashSearch";
      break;
    default:
      val = "NULL";
    }
    cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::INDEX_TYPE", val});

    // get BLOCK_BASED_TABLE_FACTORY::HASH_INDEX_ALLOW_COLLISION option value
    cf_option_types.push_back(
        {"BLOCK_BASED_TABLE_FACTORY::HASH_INDEX_ALLOW_COLLISION",
         table_options.hash_index_allow_collision ? "ON" : "OFF"});

    // get BLOCK_BASED_TABLE_FACTORY::CHECKSUM option value
    switch (table_options.checksum) {
    case xengine::table::kNoChecksum:
      val = "kNoChecksum";
      break;
    case xengine::table::kCRC32c:
      val = "kCRC32c";
      break;
    case xengine::table::kxxHash:
      val = "kxxHash";
      break;
    default:
      val = "NULL";
  }
  cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::CHECKSUM", val});

  // get BLOCK_BASED_TABLE_FACTORY::NO_BLOCK_CACHE option value
  cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::NO_BLOCK_CACHE",
                              table_options.no_block_cache ? "ON" : "OFF"});

  // get BLOCK_BASED_TABLE_FACTORY::FILTER_POLICY option
  cf_option_types.push_back(
      {"BLOCK_BASED_TABLE_FACTORY::FILTER_POLICY",
        table_options.filter_policy == nullptr
            ? "NULL"
            : std::string(table_options.filter_policy->Name())});

  // get BLOCK_BASED_TABLE_FACTORY::WHOLE_KEY_FILTERING option
  cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::WHOLE_KEY_FILTERING",
                              table_options.whole_key_filtering ? "1" : "0"});

  // get BLOCK_BASED_TABLE_FACTORY::BLOCK_CACHE option
  cf_option_types.push_back(
        {"BLOCK_BASED_TABLE_FACTORY::BLOCK_CACHE",
         table_options.block_cache == nullptr
             ? "NULL"
             : std::to_string(table_options.block_cache->GetUsage())});

  // get BLOCK_BASED_TABLE_FACTORY::BLOCK_CACHE_COMPRESSED option
  cf_option_types.push_back(
        {"BLOCK_BASED_TABLE_FACTORY::BLOCK_CACHE_COMPRESSED",
         table_options.block_cache_compressed == nullptr
             ? "NULL"
             : std::to_string(
                   table_options.block_cache_compressed->GetUsage())});

  // get BLOCK_BASED_TABLE_FACTORY::BLOCK_SIZE option
  cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::BLOCK_SIZE",
                               std::to_string(table_options.block_size)});

  // get BLOCK_BASED_TABLE_FACTORY::BLOCK_SIZE_DEVIATION option
  cf_option_types.push_back(
        {"BLOCK_BASED_TABLE_FACTORY::BLOCK_SIZE_DEVIATION",
         std::to_string(table_options.block_size_deviation)});

  // get BLOCK_BASED_TABLE_FACTORY::BLOCK_RESTART_INTERVAL option
  cf_option_types.push_back(
        {"BLOCK_BASED_TABLE_FACTORY::BLOCK_RESTART_INTERVAL",
         std::to_string(table_options.block_restart_interval)});

  // get BLOCK_BASED_TABLE_FACTORY::FORMAT_VERSION option
  cf_option_types.push_back({"BLOCK_BASED_TABLE_FACTORY::FORMAT_VERSION",
                               std::to_string(table_options.format_version)});

  for (const auto &cf_option_type : cf_option_types) {
    DBUG_ASSERT(tables->table != nullptr);
    DBUG_ASSERT(tables->table->field != nullptr);

    tables->table->field[XDB_CFOPTIONS_FIELD::CF_NAME]->store(
        dummy_cf_name.c_str(), dummy_cf_name.size(), system_charset_info);
    tables->table->field[XDB_CFOPTIONS_FIELD::OPTION_TYPE]->store(
        cf_option_type.first.c_str(), cf_option_type.first.size(),
        system_charset_info);
    tables->table->field[XDB_CFOPTIONS_FIELD::VALUE]->store(
        cf_option_type.second.c_str(), cf_option_type.second.size(),
        system_charset_info);

    ret = my_core::schema_table_store_record(thd, tables->table);

    if (ret)
      DBUG_RETURN(ret);
  }

  DBUG_RETURN(0);
}
#endif

/*
  Support for INFORMATION_SCHEMA.XENGINE_GLOBAL_INFO dynamic table
 */
namespace XDB_GLOBAL_INFO_FIELD {
enum { TYPE = 0, NAME, VALUE };
}

static ST_FIELD_INFO xdb_i_s_global_info_fields_info[] = {
    XENGINE_FIELD_INFO("TYPE", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("NAME", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

/*
 * helper function for xdb_i_s_global_info_fill_table
 * to insert (TYPE, KEY, VALUE) rows into
 * information_schema.xengine_global_info
 */
static int xdb_global_info_fill_row(my_core::THD *const thd,
                                    my_core::TABLE_LIST *const tables,
                                    const char *const type,
                                    const char *const name,
                                    const char *const value) {
  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);
  DBUG_ASSERT(tables->table != nullptr);
  DBUG_ASSERT(type != nullptr);
  DBUG_ASSERT(name != nullptr);
  DBUG_ASSERT(value != nullptr);

  Field **field = tables->table->field;
  DBUG_ASSERT(field != nullptr);

  field[XDB_GLOBAL_INFO_FIELD::TYPE]->store(type, strlen(type),
                                            system_charset_info);
  field[XDB_GLOBAL_INFO_FIELD::NAME]->store(name, strlen(name),
                                            system_charset_info);
  field[XDB_GLOBAL_INFO_FIELD::VALUE]->store(value, strlen(value),
                                             system_charset_info);

  return my_core::schema_table_store_record(thd, tables->table);
}

static int xdb_i_s_global_info_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);

  static const uint32_t INT_BUF_LEN = 21;
  static const uint32_t GTID_BUF_LEN = 60;
  static const uint32_t CF_ID_INDEX_BUF_LEN = 60;

  int ret = 0;

  /* binlog info */
  Xdb_binlog_manager *const blm = xdb_get_binlog_manager();
  DBUG_ASSERT(blm != nullptr);

  char file_buf[FN_REFLEN + 1] = {0};
  my_off_t pos = 0;
  char pos_buf[INT_BUF_LEN] = {0};
  char gtid_buf[GTID_BUF_LEN] = {0};

  if (blm->read(file_buf, &pos, gtid_buf)) {
    snprintf(pos_buf, INT_BUF_LEN, "%lu", (uint64_t)pos);
    ret |= xdb_global_info_fill_row(thd, tables, "BINLOG", "FILE", file_buf);
    ret |= xdb_global_info_fill_row(thd, tables, "BINLOG", "POS", pos_buf);
    ret |= xdb_global_info_fill_row(thd, tables, "BINLOG", "GTID", gtid_buf);
  }

  /* max index info */
  const Xdb_dict_manager *const dict_manager = xdb_get_dict_manager();
  DBUG_ASSERT(dict_manager != nullptr);

  uint32_t max_index_id;
  char max_index_id_buf[INT_BUF_LEN] = {0};

  if (dict_manager->get_max_index_id(&max_index_id)) {
    snprintf(max_index_id_buf, INT_BUF_LEN, "%u", max_index_id);
    ret |= xdb_global_info_fill_row(thd, tables, "MAX_INDEX_ID", "MAX_INDEX_ID",
                                    max_index_id_buf);
  }

  uint64_t max_table_id;
  if (dict_manager->get_max_table_id(&max_table_id)) {
    snprintf(max_index_id_buf, INT_BUF_LEN, "%lu", max_table_id);
    ret |= xdb_global_info_fill_row(thd, tables, "MAX_TABLE_ID", "MAX_TABLE_ID",
                                    max_index_id_buf);
  }

#if 0
  /* cf_id -> cf_flags */
  char cf_id_buf[INT_BUF_LEN] = {0};
  char cf_value_buf[FN_REFLEN + 1] = {0};
  const Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  std::unique_ptr<xengine::db::ColumnFamilyHandle> cf_ptr;
  for (const auto &cf_handle : cf_manager.get_all_cf()) {
    cf_ptr.reset(cf_handle);
    uint flags;
    dict_manager->get_cf_flags(cf_handle->GetID(), &flags);
    snprintf(cf_id_buf, INT_BUF_LEN, "%u", cf_handle->GetID());
    snprintf(cf_value_buf, FN_REFLEN, "%s [%u]", cf_handle->GetName().c_str(),
             flags);
    ret |= xdb_global_info_fill_row(thd, tables, "CF_FLAGS", cf_id_buf,
                                    cf_value_buf);

    if (ret)
      break;
  }

  /* DDL_DROP_INDEX_ONGOING */
  std::unordered_set<GL_INDEX_ID> gl_index_ids;
  dict_manager->get_ongoing_index_operation(
      &gl_index_ids, Xdb_key_def::DDL_DROP_INDEX_ONGOING);
  char cf_id_index_buf[CF_ID_INDEX_BUF_LEN] = {0};
  for (auto gl_index_id : gl_index_ids) {
    snprintf(cf_id_index_buf, CF_ID_INDEX_BUF_LEN, "cf_id:%u,index_id:%u",
             gl_index_id.cf_id, gl_index_id.index_id);
    ret |= xdb_global_info_fill_row(thd, tables, "DDL_DROP_INDEX_ONGOING",
                                    cf_id_index_buf, "");

    if (ret)
      break;
  }
#endif

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_COMPACTION_STATS dynamic table
 */
static int xdb_i_s_compact_stats_fill_table(
    my_core::THD *thd, my_core::TABLE_LIST *tables,
    my_core::Item *cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);

  DBUG_ENTER_FUNC();

  int ret = 0;

  xengine::db::DB *xdb = xdb_get_xengine_db();
  Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  DBUG_ASSERT(xdb != nullptr);

  for (auto subtable_id : cf_manager.get_subtable_ids()) {
    xengine::db::ColumnFamilyHandle *cfh;
    /*
       Only the cf name is important. Whether it was generated automatically
       does not matter, so is_automatic is ignored.
    */
    cfh = cf_manager.get_cf(subtable_id);
    if (cfh == nullptr) {
      continue;
    }
    std::map<std::string, double> props;
    bool bool_ret MY_ATTRIBUTE((__unused__));
    bool_ret = xdb->GetMapProperty(cfh, "xengine.cfstats", &props);
    DBUG_ASSERT(bool_ret);

    for (auto const &prop_ent : props) {
      std::string prop_name = prop_ent.first;
      double value = prop_ent.second;
      std::size_t del_pos = prop_name.find('.');
      DBUG_ASSERT(del_pos != std::string::npos);
      std::string level_str = prop_name.substr(0, del_pos);
      std::string type_str = prop_name.substr(del_pos + 1);

      Field **field = tables->table->field;
      DBUG_ASSERT(field != nullptr);
      field[0]->store(subtable_id, true);
      field[1]->store(level_str.c_str(), level_str.size(), system_charset_info);
      field[2]->store(type_str.c_str(), type_str.size(), system_charset_info);
      field[3]->store(value, true);

      ret |= my_core::schema_table_store_record(thd, tables->table);
      if (ret != 0) {
        DBUG_RETURN(ret);
      }
    }
  }

  DBUG_RETURN(ret);
}

static ST_FIELD_INFO xdb_i_s_compact_stats_fields_info[] = {
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LEVEL", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("TYPE", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("VALUE", sizeof(double), MYSQL_TYPE_DOUBLE, 0),
    XENGINE_FIELD_INFO_END};

namespace // anonymous namespace = not visible outside this source file
{
struct Xdb_ddl_scanner : public Xdb_tables_scanner {
  my_core::THD *m_thd;
  my_core::TABLE *m_table;

  int add_table(Xdb_tbl_def *tdef) override;
};
} // anonymous namespace

/*
  Support for INFORMATION_SCHEMA.XENGINE_COMPACTION_TASK dynamic table
 */
namespace XDB_COMPACTION_TASK_FIELD {
enum {
  SUBTABLE_ID = 0,
  TYPE,
  COMPACTION_TYPE,
  STAGE,
  MEM_DATA,
  LEVEL0_DATA,
  LEVEL1_DATA,
  LEVEL2_DATA
};
} // namespace XDB_MEMTABLE_FIELD


static ST_FIELD_INFO xdb_i_s_xengine_compaction_task_fields_info[] = {
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("COMPACTION_TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("STAGE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("MEM_DATA", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LEVEL0_DATA", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LEVEL1_DATA", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LEVEL2_DATA", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_xengine_compaction_task_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  int ret = xengine::common::Status::kOk;
  xengine::db::DB *xdb = xdb_get_xengine_db();
  xengine::util::Env *env = xdb->GetEnv();
  std::vector<xengine::util::ThreadStatus> thread_list;
  // FIXME no mutex protect
  xengine::common::Status s = env->GetThreadList(&thread_list);
  for (auto thread : thread_list) {
    tables->table->field[XDB_COMPACTION_TASK_FIELD::SUBTABLE_ID]->store(
          thread.subtable_id_, true);
    if (thread.operation_type == xengine::util::ThreadStatus::OP_FLUSH) {

      tables->table->field[XDB_COMPACTION_TASK_FIELD::TYPE]->store(
          STRING_WITH_LEN("FLUSH"), system_charset_info);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::COMPACTION_TYPE]->store(
          STRING_WITH_LEN("FLUSH_MEMTABLE"), system_charset_info);
      std::string stage_name =
        xengine::util::ThreadStatus::GetOperationStageName(thread.operation_stage);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::STAGE]->store(
          stage_name.c_str(), stage_name.size(), system_charset_info);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::MEM_DATA]->store(
          thread.op_properties
              [xengine::util::ThreadStatus::FLUSH_BYTES_MEMTABLES],
          true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL0_DATA]->store(0,
                                                                        true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL1_DATA]->store(0,
                                                                        true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL2_DATA]->store(0,
                                                                        true);
      ret = my_core::schema_table_store_record(thd, tables->table);

    } else if (thread.operation_type ==
               xengine::util::ThreadStatus::OP_COMPACTION) {
      tables->table->field[XDB_COMPACTION_TASK_FIELD::TYPE]->store(
          STRING_WITH_LEN("COMPACTION"), system_charset_info);
      std::string compaction_type = xengine::db::get_task_type_name(
          thread.compaction_type_);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::COMPACTION_TYPE]->store(
          compaction_type.c_str(), compaction_type.size(), system_charset_info);
      std::string stage_name =
        xengine::util::ThreadStatus::GetOperationStageName(thread.operation_stage);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::STAGE]->store(
          stage_name.c_str(), stage_name.size(), system_charset_info);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::MEM_DATA]->store(0, true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL0_DATA]->store(
          xengine::storage::MAX_EXTENT_SIZE *
          thread.op_properties[xengine::util::ThreadStatus::COMPACTION_INPUT_EXTENT_LEVEL0], true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL1_DATA]->store(
          xengine::storage::MAX_EXTENT_SIZE *
          thread.op_properties[xengine::util::ThreadStatus::COMPACTION_INPUT_EXTENT_LEVEL1], true);
      tables->table->field[XDB_COMPACTION_TASK_FIELD::LEVEL2_DATA]->store(
          xengine::storage::MAX_EXTENT_SIZE *
          thread.op_properties[xengine::util::ThreadStatus::COMPACTION_INPUT_EXTENT_LEVEL2], true);
      ret = my_core::schema_table_store_record(thd, tables->table);
    }
  }

  DBUG_RETURN(ret);
}
static int xdb_i_s_xengine_compaction_task_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_xengine_compaction_task_fields_info;
    schema->fill_table = xdb_i_s_xengine_compaction_task_fill_table;
  }

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_memtable virtual table */
namespace XDB_COMPACTION_HISTORY_FIELD {
enum {
  SUBTABLE_ID = 0,
  SEQUENCE,
  TYPE,
  COST_TIME,
  START_TIME,
  END_TIME,
  INPUT_EXTENTS,
  REUSE_EXTENTS,
  INPUT_BLOCKS,
  REUSE_BLOCKS,
  INPUT_RECORDS,
  REUSE_RECORDS,
  OUTPUT_EXTENTS,
  OUTPUT_BLOCKS,
  OUTPUT_RECORDS,
  MERGE_RECORDS,
  DELETE_RECORDS,
  READ_SPEED,
  WRITE_SPEED,
  WRITE_AMP
};
} //namespace XDB_COMPACTION_HISTORY_FIELD

static ST_FIELD_INFO xdb_i_s_xengine_compaction_history_fields_info[] = {
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("SEQUENCE", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("TYPE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("COST_TIME", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("START_TIME", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("END_TIME", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("INPUT_EXTENTS", sizeof(int32_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("REUSE_EXTENTS", sizeof(int32_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("INPUT_BLOCKS", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("REUSE_BLOCKS", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("INPUT_RECORDS", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("REUSE_RECORDS", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("OUTPUT_EXTENTS", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("OUTPUT_BLOCKS", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("OUTPUT_RECORDS", sizeof(int32_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("MERGE_RECORDS", sizeof(int32_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("DELETE_RECORDS", sizeof(int32_t), MYSQL_TYPE_LONGLONG,
                       0),
    XENGINE_FIELD_INFO("READ_SPEED", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("WRITE_SPEED", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("WRITE_AMP", sizeof(double), MYSQL_TYPE_DOUBLE, 0),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_xengine_compaction_history_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  int ret = xengine::common::Status::kOk;
  xengine::db::DB *xdb = xdb_get_xengine_db();
  std::mutex *compaction_history_mutex = nullptr;
  xengine::storage::CompactionJobStatsInfo *sum = nullptr;
  std::list<xengine::storage::CompactionJobStatsInfo*> &compaction_history =
      xdb->get_compaction_history(&compaction_history_mutex, &sum);
  // protect the compaction history list
  std::lock_guard<std::mutex> guard(*compaction_history_mutex);
  // put the sum at first
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::SUBTABLE_ID]->store(
        sum->subtable_id_, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::SEQUENCE]->store(
        sum->sequence_, false);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::TYPE]->store(
        STRING_WITH_LEN("SUM"), system_charset_info);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::COST_TIME]->store(
        sum->stats_.record_stats_.micros, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::START_TIME]->store(
        sum->stats_.record_stats_.start_micros, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::END_TIME]->store(
        sum->stats_.record_stats_.end_micros, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_EXTENTS]->store(
        sum->stats_.record_stats_.total_input_extents, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_EXTENTS]->store(
        sum->stats_.record_stats_.reuse_extents, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_BLOCKS]->store(
        0, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_BLOCKS]->store(
        sum->stats_.record_stats_.reuse_datablocks, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_RECORDS]->store(
        sum->stats_.record_stats_.merge_input_records, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_RECORDS]->store(
        0, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_EXTENTS]->store(
        sum->stats_.record_stats_.merge_output_extents, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_BLOCKS]->store(
        sum->stats_.record_stats_.merge_datablocks, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_RECORDS]->store(
        sum->stats_.record_stats_.merge_output_records, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::MERGE_RECORDS]->store(
        0, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::DELETE_RECORDS]->store(
        sum->stats_.record_stats_.merge_delete_records, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::READ_SPEED]->store(0,
                                                                        true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::WRITE_SPEED]->store(
        0, true);
  tables->table->field[XDB_COMPACTION_HISTORY_FIELD::WRITE_AMP]->store(
        sum->stats_.record_stats_.write_amp, true);
  ret = my_core::schema_table_store_record(thd, tables->table);
  for (auto jobinfo : compaction_history) {
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::SUBTABLE_ID]->store(
        jobinfo->subtable_id_, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::SEQUENCE]->store(
        jobinfo->sequence_, false);

    std::string compaction_type = xengine::db::get_task_type_name(jobinfo->type_);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::TYPE]->store(
        compaction_type.c_str(), compaction_type.size(), system_charset_info);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::COST_TIME]->store(
        jobinfo->stats_.record_stats_.micros, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::START_TIME]->store(
        jobinfo->stats_.record_stats_.start_micros, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::END_TIME]->store(
        jobinfo->stats_.record_stats_.end_micros, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_EXTENTS]->store(
        jobinfo->stats_.record_stats_.total_input_extents, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_EXTENTS]->store(
        jobinfo->stats_.record_stats_.reuse_extents, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_BLOCKS]->store(
        0, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_BLOCKS]->store(
        jobinfo->stats_.record_stats_.reuse_datablocks, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::INPUT_RECORDS]->store(
        jobinfo->stats_.record_stats_.merge_input_records, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::REUSE_RECORDS]->store(
        0, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_EXTENTS]->store(
        jobinfo->stats_.record_stats_.merge_output_extents, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_BLOCKS]->store(
        jobinfo->stats_.record_stats_.merge_datablocks, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::OUTPUT_RECORDS]->store(
        jobinfo->stats_.record_stats_.merge_output_records, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::MERGE_RECORDS]->store(
        0, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::DELETE_RECORDS]->store(
        jobinfo->stats_.record_stats_.merge_delete_records, true);

    // Omit the speed when task duration is less than 1 second
    int64_t read_speed = jobinfo->stats_.record_stats_.micros < 1000000 ? 0
      : (1000000 * 2 * jobinfo->stats_.record_stats_.total_input_extents /
          jobinfo->stats_.record_stats_.micros);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::READ_SPEED]->store(
        read_speed, true);

    int64_t merge_speed = jobinfo->stats_.record_stats_.micros < 1000000 ? 0
      : (1000000 * 2 * (jobinfo->stats_.record_stats_.total_input_extents -
            jobinfo->stats_.record_stats_.reuse_extents) /
          jobinfo->stats_.record_stats_.micros);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::WRITE_SPEED]->store(
        merge_speed, true);
    tables->table->field[XDB_COMPACTION_HISTORY_FIELD::WRITE_AMP]->store(
        jobinfo->stats_.record_stats_.write_amp, true);
    ret = my_core::schema_table_store_record(thd, tables->table);
  }
  DBUG_RETURN(ret);
}

static int xdb_i_s_xengine_compaction_history_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_xengine_compaction_history_fields_info;
    schema->fill_table = xdb_i_s_xengine_compaction_history_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_MEM_ALLOC dynamic table
 */
namespace XDB_MEM_ALLOC_FIELD {
enum { MODULE = 0, MALLOC, ALLOC, USED, AVG_ALLOC, ALLOC_CNT, TOTAL_ALLOC_CNT };
} // namespace XDB_MEM_ALLOC_FIELD

static ST_FIELD_INFO xdb_i_s_xengine_mem_alloc_fields_info[] = {
    XENGINE_FIELD_INFO("MODULE", NAME_LEN + 1, MYSQL_TYPE_STRING, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("MALLOC", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ALLOC", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("USED", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("AVG_ALLOC", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ALLOC_CNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("TOTAL_ALLOC_CNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_xengine_mem_alloc_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  int ret = xengine::common::Status::kOk;
  xengine::memory::MemItemDump items[xengine::memory::ModMemSet::kModMaxCnt];
  std::string mod_names[xengine::memory::ModMemSet::kModMaxCnt];
  xengine::memory::AllocMgr::get_instance()->get_memory_usage(items, mod_names);
  for (size_t i = 0; i < xengine::memory::ModMemSet::kModMaxCnt; i++) {
    tables->table->field[XDB_MEM_ALLOC_FIELD::MODULE]->store(
        mod_names[i].c_str(), mod_names[i].size(), system_charset_info);
    tables->table->field[XDB_MEM_ALLOC_FIELD::MALLOC]->store(
        items[i].malloc_size_, true);
    tables->table->field[XDB_MEM_ALLOC_FIELD::ALLOC]->store(items[i].alloc_size_,
                                                            true);
    tables->table->field[XDB_MEM_ALLOC_FIELD::USED]->store(
        items[i].hold_size_, true);
    tables->table->field[XDB_MEM_ALLOC_FIELD::AVG_ALLOC]->store(
        items[i].avg_alloc_, true);
    tables->table->field[XDB_MEM_ALLOC_FIELD::ALLOC_CNT]->store(
        items[i].alloc_cnt_, true);
    tables->table->field[XDB_MEM_ALLOC_FIELD::TOTAL_ALLOC_CNT]->store(
        items[i].total_alloc_cnt_, true);
    /* Tell MySQL about this row in the virtual table */
    ret = my_core::schema_table_store_record(thd, tables->table);
    if (ret != 0) {
      break;
    }
  }
  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_mem_alloc virtual table */
static int xdb_i_s_xengine_mem_alloc_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_xengine_mem_alloc_fields_info;
    schema->fill_table = xdb_i_s_xengine_mem_alloc_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_DDL dynamic table
 */
namespace XDB_DDL_FIELD {
enum {
  TABLE_SCHEMA = 0,
  TABLE_NAME,
  PARTITION_NAME,
  INDEX_NAME,
  SUBTABLE_ID,
  INDEX_NUMBER,
  INDEX_TYPE,
  KV_FORMAT_VERSION,
  //CF
};
} // namespace XDB_DDL_FIELD

static ST_FIELD_INFO xdb_i_s_ddl_fields_info[] = {
    XENGINE_FIELD_INFO("TABLE_SCHEMA", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("TABLE_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("PARTITION_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING,
                       MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("INDEX_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("INDEX_NUMBER", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("INDEX_TYPE", sizeof(uint16_t), MYSQL_TYPE_SHORT, 0),
    XENGINE_FIELD_INFO("KV_FORMAT_VERSION", sizeof(uint16_t), MYSQL_TYPE_SHORT,
                       0),
    //XENGINE_FIELD_INFO("CF", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

int Xdb_ddl_scanner::add_table(Xdb_tbl_def *tdef) {
  DBUG_ASSERT(tdef != nullptr);

  int ret = 0;

  DBUG_ASSERT(m_table != nullptr);
  Field **field = m_table->field;
  DBUG_ASSERT(field != nullptr);

  // name string in Xdb_tbl_def is from filename which uses my_system_filename
  const std::string &dbname = tdef->base_dbname();
  field[XDB_DDL_FIELD::TABLE_SCHEMA]->store(dbname.c_str(), dbname.size(),
                                            &my_charset_filename);

  const std::string &tablename = tdef->base_tablename();
  field[XDB_DDL_FIELD::TABLE_NAME]->store(tablename.c_str(), tablename.size(),
      strstr(tablename.c_str(), tmp_file_prefix) != NULL ? system_charset_info
                                                         : &my_charset_filename);

  const std::string &partname = tdef->base_partition();
  if (partname.empty()) {
    field[XDB_DDL_FIELD::PARTITION_NAME]->set_null();
  } else {
    field[XDB_DDL_FIELD::PARTITION_NAME]->set_notnull();
    field[XDB_DDL_FIELD::PARTITION_NAME]->store(
        partname.c_str(), partname.size(), &my_charset_filename);

  }

  for (uint i = 0; i < tdef->m_key_count; i++) {
    const Xdb_key_def &kd = *tdef->m_key_descr_arr[i];

    field[XDB_DDL_FIELD::INDEX_NAME]->store(kd.m_name.c_str(), kd.m_name.size(),
                                            system_charset_info);

    GL_INDEX_ID gl_index_id = kd.get_gl_index_id();
    field[XDB_DDL_FIELD::SUBTABLE_ID]->store(gl_index_id.cf_id, true);
    field[XDB_DDL_FIELD::INDEX_NUMBER]->store(gl_index_id.index_id, true);
    field[XDB_DDL_FIELD::INDEX_TYPE]->store(kd.m_index_type, true);
    field[XDB_DDL_FIELD::KV_FORMAT_VERSION]->store(kd.m_kv_format_version,
                                                   true);

    //std::string cf_name = kd.get_cf()->GetName();
    //field[XDB_DDL_FIELD::CF]->store("", 0,
    //                                system_charset_info);

    ret = my_core::schema_table_store_record(m_thd, m_table);
    if (ret)
      return ret;
  }
  return HA_EXIT_SUCCESS;
}

static int xdb_i_s_ddl_fill_table(my_core::THD *const thd,
                                  my_core::TABLE_LIST *const tables,
                                  my_core::Item *const cond) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);

  Xdb_ddl_scanner ddl_arg;
  ddl_arg.m_thd = thd;
  ddl_arg.m_table = tables->table;

  Xdb_ddl_manager *ddl_manager = xdb_get_ddl_manager();
  DBUG_ASSERT(ddl_manager != nullptr);
  int ret = ddl_manager->scan_for_tables(&ddl_arg);

  DBUG_RETURN(ret);
}

static int xdb_i_s_ddl_init(void *const p) {
  DBUG_ENTER_FUNC();

  my_core::ST_SCHEMA_TABLE *schema;

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_ddl_fields_info;
    schema->fill_table = xdb_i_s_ddl_fill_table;
  }

  DBUG_RETURN(ret);
}

#if 0
static int xdb_i_s_cfoptions_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_cfoptions_fields_info;
    schema->fill_table = xdb_i_s_cfoptions_fill_table;
  }

  DBUG_RETURN(ret);
}
#endif

static int xdb_i_s_global_info_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);
  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = reinterpret_cast<my_core::ST_SCHEMA_TABLE *>(p);

    schema->fields_info = xdb_i_s_global_info_fields_info;
    schema->fill_table = xdb_i_s_global_info_fill_table;
  }

  DBUG_RETURN(ret);
}

static int xdb_i_s_compact_stats_init(void *p) {
  my_core::ST_SCHEMA_TABLE *schema;

  DBUG_ENTER_FUNC();
  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    schema = reinterpret_cast<my_core::ST_SCHEMA_TABLE *>(p);

    schema->fields_info = xdb_i_s_compact_stats_fields_info;
    schema->fill_table = xdb_i_s_compact_stats_fill_table;
  }

  DBUG_RETURN(ret);
}

/* Given a path to a file return just the filename portion. */
static std::string xdb_filename_without_path(const std::string &path) {
  /* Find last slash in path */
  const size_t pos = path.rfind('/');

  /* None found?  Just return the original string */
  if (pos == std::string::npos) {
    return std::string(path);
  }

  /* Return everything after the slash (or backslash) */
  return path.substr(pos + 1);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_INDEX_FILE_MAP dynamic table
 */
namespace XDB_INDEX_FILE_MAP_FIELD {
enum {
  SUBTABLE_ID = 0,
  INDEX_NUMBER,
  SST_NAME,
  NUM_ROWS,
  DATA_SIZE,
  ENTRY_DELETES,
  ENTRY_SINGLEDELETES,
  ENTRY_MERGES,
  ENTRY_OTHERS,
  DISTINCT_KEYS_PREFIX
};
} // namespace XDB_INDEX_FILE_MAP_FIELD

static ST_FIELD_INFO xdb_i_s_index_file_map_fields_info[] = {
    /* The information_schema.xengine_index_file_map virtual table has four
     * fields:
     *   SUBTABLE_ID => the index's column family contained in the SST file
     *   INDEX_NUMBER => the index id contained in the SST file
     *   SST_NAME => the name of the SST file containing some indexes
     *   NUM_ROWS => the number of entries of this index id in this SST file
     *   DATA_SIZE => the data size stored in this SST file for this index id */
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("INDEX_NUMBER", sizeof(uint32_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("SST_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("NUM_ROWS", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("DATA_SIZE", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ENTRY_DELETES", sizeof(int64_t), MYSQL_TYPE_LONGLONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ENTRY_SINGLEDELETES", sizeof(int64_t),
                       MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("ENTRY_MERGES", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ENTRY_OTHERS", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("DISTINCT_KEYS_PREFIX", MAX_REF_PARTS * 25,
                       MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

/* Fill the information_schema.xengine_index_file_map virtual table */
static int xdb_i_s_index_file_map_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);
  DBUG_ASSERT(tables->table != nullptr);

  int ret = 0;
  Field **field = tables->table->field;
  DBUG_ASSERT(field != nullptr);

  /* Iterate over all the column families */
  xengine::db::DB *const xdb = xdb_get_xengine_db();
  DBUG_ASSERT(xdb != nullptr);

  const Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  std::unique_ptr<xengine::db::ColumnFamilyHandle> cf_ptr;
  for (const auto &cf_handle : cf_manager.get_all_cf()) {
    cf_ptr.reset(cf_handle);
    /* Grab the the properties of all the tables in the column family */
    xengine::common::TablePropertiesCollection table_props_collection;
    const xengine::common::Status s =
        xdb->GetPropertiesOfAllTables(cf_handle, &table_props_collection);
    if (!s.ok()) {
      continue;
    }

    /* Iterate over all the items in the collection, each of which contains a
     * name and the actual properties */
    for (const auto &props : table_props_collection) {
      /* Add the SST name into the output */
      const std::string sst_name = xdb_filename_without_path(props.first);
      field[XDB_INDEX_FILE_MAP_FIELD::SST_NAME]->store(
          sst_name.data(), sst_name.size(), system_charset_info);

      /* Get the __indexstats__ data out of the table property */
      std::vector<Xdb_index_stats> stats;
      Xdb_tbl_prop_coll::read_stats_from_tbl_props(props.second, &stats);
      if (stats.empty()) {
        /*
        field[XDB_INDEX_FILE_MAP_FIELD::SUBTABLE_ID]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::INDEX_NUMBER]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::NUM_ROWS]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::DATA_SIZE]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_DELETES]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_SINGLEDELETES]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_MERGES]->store(-1, true);
        field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_OTHERS]->store(-1, true);
        */
        sql_print_information("XEngine got empty stats from table properties");
      } else {
        for (auto it : stats) {
          /* Add the index number, the number of rows, and data size to the
           * output */
          field[XDB_INDEX_FILE_MAP_FIELD::SUBTABLE_ID]->store(
              it.m_gl_index_id.cf_id, true);
          field[XDB_INDEX_FILE_MAP_FIELD::INDEX_NUMBER]->store(
              it.m_gl_index_id.index_id, true);
          field[XDB_INDEX_FILE_MAP_FIELD::NUM_ROWS]->store(it.m_rows, true);
          field[XDB_INDEX_FILE_MAP_FIELD::DATA_SIZE]->store(it.m_data_size,
                                                            true);
          field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_DELETES]->store(
              it.m_entry_deletes, true);
          field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_SINGLEDELETES]->store(
              it.m_entry_single_deletes, true);
          field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_MERGES]->store(
              it.m_entry_merges, true);
          field[XDB_INDEX_FILE_MAP_FIELD::ENTRY_OTHERS]->store(
              it.m_entry_others, true);
          std::string distinct_keys_prefix;

          for (size_t i = 0; i < it.m_distinct_keys_per_prefix.size(); i++) {
            if (i > 0) {
              distinct_keys_prefix += ",";
            }
            distinct_keys_prefix +=
                std::to_string(it.m_distinct_keys_per_prefix[i]);
          }

          field[XDB_INDEX_FILE_MAP_FIELD::DISTINCT_KEYS_PREFIX]->store(
              distinct_keys_prefix.data(), distinct_keys_prefix.size(),
              system_charset_info);

          /* Tell MySQL about this row in the virtual table */
          ret = my_core::schema_table_store_record(thd, tables->table);
          if (ret != 0) {
            break;
          }
        }
      }
    }
  }

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_index_file_map virtual table */
static int xdb_i_s_index_file_map_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);
  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_index_file_map_fields_info;
    schema->fill_table = xdb_i_s_index_file_map_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_LOCKS dynamic table
 */
namespace XDB_LOCKS_FIELD {
enum { SUBTABLE_ID = 0, TRANSACTION_ID, KEY, MODE };
} // namespace XDB_LOCKS_FIELD

static ST_FIELD_INFO xdb_i_s_lock_info_fields_info[] = {
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("TRANSACTION_ID", sizeof(uint32_t), MYSQL_TYPE_LONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("KEY", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("MODE", 32, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

/* Fill the information_schema.xengine_locks virtual table */
static int xdb_i_s_lock_info_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);
  DBUG_ASSERT(tables->table != nullptr);

  int ret = 0;

  xengine::util::TransactionDB *const xdb = xdb_get_xengine_db();
  DBUG_ASSERT(xdb != nullptr);

  /* cf id -> xengine::util::KeyLockInfo */
  std::unordered_multimap<uint32_t, xengine::util::KeyLockInfo> lock_info =
      xdb->GetLockStatusData();

  for (const auto &lock : lock_info) {
    const uint32_t cf_id = lock.first;
    const auto &key_lock_info = lock.second;
    const auto key_hexstr = xdb_hexdump(key_lock_info.key.c_str(),
                                        key_lock_info.key.length(), FN_REFLEN);

    for (const auto &id : key_lock_info.ids) {
      tables->table->field[XDB_LOCKS_FIELD::SUBTABLE_ID]->store(cf_id, true);
      tables->table->field[XDB_LOCKS_FIELD::TRANSACTION_ID]->store(id, true);

      tables->table->field[XDB_LOCKS_FIELD::KEY]->store(
          key_hexstr.c_str(), key_hexstr.size(), system_charset_info);
      tables->table->field[XDB_LOCKS_FIELD::MODE]->store(
          key_lock_info.exclusive ? "X" : "S", 1, system_charset_info);

      /* Tell MySQL about this row in the virtual table */
      ret = my_core::schema_table_store_record(thd, tables->table);
      if (ret != 0) {
        break;
      }
    }
  }
  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_lock_info virtual table */
static int xdb_i_s_lock_info_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_lock_info_fields_info;
    schema->fill_table = xdb_i_s_lock_info_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_TRX dynamic table
 */
namespace XDB_TRX_FIELD {
enum {
  TRANSACTION_ID = 0,
  STATE,
  NAME,
  WRITE_COUNT,
  LOCK_COUNT,
  TIMEOUT_SEC,
  WAITING_KEY,
  WAITING_SUBTABLE_ID,
  IS_REPLICATION,
  SKIP_TRX_API,
  READ_ONLY,
  HAS_DEADLOCK_DETECTION,
  NUM_ONGOING_BULKLOAD,
  THREAD_ID,
  QUERY
};
} // namespace XDB_TRX_FIELD

static ST_FIELD_INFO xdb_i_s_trx_info_fields_info[] = {
    XENGINE_FIELD_INFO("TRANSACTION_ID", sizeof(ulonglong), MYSQL_TYPE_LONGLONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("STATE", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("WRITE_COUNT", sizeof(ulonglong), MYSQL_TYPE_LONGLONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LOCK_COUNT", sizeof(ulonglong), MYSQL_TYPE_LONGLONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("TIMEOUT_SEC", sizeof(uint32_t), MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("WAITING_KEY", FN_REFLEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("WAITING_SUBTABLE_ID", sizeof(uint32_t), MYSQL_TYPE_LONG,
                       MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("IS_REPLICATION", sizeof(uint32_t), MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("SKIP_TRX_API", sizeof(uint32_t), MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("READ_ONLY", sizeof(uint32_t), MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("HAS_DEADLOCK_DETECTION", sizeof(uint32_t),
                       MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("NUM_ONGOING_BULKLOAD", sizeof(uint32_t),
                       MYSQL_TYPE_LONG, 0),
    XENGINE_FIELD_INFO("THREAD_ID", sizeof(ulong), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("QUERY", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO_END};

/* Fill the information_schema.xengine_trx virtual table */
static int xdb_i_s_trx_info_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(tables != nullptr);
  DBUG_ASSERT(tables->table != nullptr);

  int ret = 0;

  const std::vector<Xdb_trx_info> &all_trx_info = xdb_get_all_trx_info();

  for (const auto &info : all_trx_info) {
    auto name_hexstr =
        xdb_hexdump(info.name.c_str(), info.name.length(), NAME_LEN);
    auto key_hexstr = xdb_hexdump(info.waiting_key.c_str(),
                                  info.waiting_key.length(), FN_REFLEN);
    tables->table->field[XDB_TRX_FIELD::TRANSACTION_ID]->store(info.trx_id,
                                                               true);
    tables->table->field[XDB_TRX_FIELD::STATE]->store(
        info.state.c_str(), info.state.length(), system_charset_info);
    tables->table->field[XDB_TRX_FIELD::NAME]->store(
        name_hexstr.c_str(), name_hexstr.length(), system_charset_info);
    tables->table->field[XDB_TRX_FIELD::WRITE_COUNT]->store(info.write_count,
                                                            true);
    tables->table->field[XDB_TRX_FIELD::LOCK_COUNT]->store(info.lock_count,
                                                           true);
    tables->table->field[XDB_TRX_FIELD::TIMEOUT_SEC]->store(info.timeout_sec,
                                                            false);
    tables->table->field[XDB_TRX_FIELD::WAITING_KEY]->store(
        key_hexstr.c_str(), key_hexstr.length(), system_charset_info);
    tables->table->field[XDB_TRX_FIELD::WAITING_SUBTABLE_ID]->store(
        info.waiting_cf_id, true);
    tables->table->field[XDB_TRX_FIELD::IS_REPLICATION]->store(
        info.is_replication, false);
    tables->table->field[XDB_TRX_FIELD::SKIP_TRX_API]->store(info.skip_trx_api,
                                                             false);
    tables->table->field[XDB_TRX_FIELD::READ_ONLY]->store(info.read_only,
                                                          false);
    tables->table->field[XDB_TRX_FIELD::HAS_DEADLOCK_DETECTION]->store(
        info.deadlock_detect, false);
    tables->table->field[XDB_TRX_FIELD::NUM_ONGOING_BULKLOAD]->store(
        info.num_ongoing_bulk_load, false);
    tables->table->field[XDB_TRX_FIELD::THREAD_ID]->store(info.thread_id, true);
    tables->table->field[XDB_TRX_FIELD::QUERY]->store(
        info.query_str.c_str(), info.query_str.length(), system_charset_info);

    /* Tell MySQL about this row in the virtual table */
    ret = my_core::schema_table_store_record(thd, tables->table);
    if (ret != 0) {
      break;
    }
  }

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_trx_info virtual table */
static int xdb_i_s_trx_info_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;

    schema = (my_core::ST_SCHEMA_TABLE *)p;

    schema->fields_info = xdb_i_s_trx_info_fields_info;
    schema->fill_table = xdb_i_s_trx_info_fill_table;
  }

  DBUG_RETURN(ret);
}

static int xdb_i_s_deinit(void *p MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  DBUG_RETURN(0);
}

namespace XENGINE_TABLES_FIELD {
enum {
  SCHEMA_NAME = 0,
  NAME,
  N_COLS,
  INSTANT_COLS
};
}  // namespace XENGINE_TABLES_FIELD

static ST_FIELD_INFO xengine_tables_fields_info[] = {
    XENGINE_FIELD_INFO("SCHEMA_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("N_COLS", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("INSTANT_COLS", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO_END};

static int i_s_xengine_tables_fill_table(THD *thd, TABLE_LIST *tables, Item *) {
  DBUG_ENTER_FUNC();
  TABLE *table = tables->table;
  bool error = Xdb_dd_helper::traverse_all_xengine_tables(
      thd, false, 0,
      [&thd, &table](const dd::Schema *dd_schema,
                     const dd::Table *dd_table) -> bool {
        DBUG_ASSERT(dd_schema && dd_table);
        table->field[XENGINE_TABLES_FIELD::SCHEMA_NAME]->store(
            dd_schema->name().c_str(), dd_schema->name().length(),
            system_charset_info);
        table->field[XENGINE_TABLES_FIELD::NAME]->store(
            dd_table->name().c_str(), dd_table->name().length(),
            system_charset_info);
        table->field[XENGINE_TABLES_FIELD::N_COLS]->store(
            dd_table->columns().size(), true);
        uint32_t instant_cols = 0;
        if (dd_table->se_private_data().exists(
                myx::dd_table_key_strings[myx::DD_TABLE_INSTANT_COLS])) {
          dd_table->se_private_data().get(
              myx::dd_table_key_strings[myx::DD_TABLE_INSTANT_COLS],
              &instant_cols);
        }
        table->field[XENGINE_TABLES_FIELD::INSTANT_COLS]->store(instant_cols,
                                                                true);
        return my_core::schema_table_store_record(thd, table);
      });

  if (error) {
    XHANDLER_LOG(
        ERROR,
        "XEngine: failed to scan all XENGINE tables from data dictionary!");
  }

  DBUG_RETURN(0);
}

/** Bind the dynamic table INFORMATION_SCHEMA.xengine_tables
@param[in,out]	p	table schema object
@return 0 on success */
static int xengine_tables_init(void *p) {
  ST_SCHEMA_TABLE *schema;

  DBUG_ENTER("xengine_tables_init");

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    schema = (ST_SCHEMA_TABLE *)p;

    schema->fields_info = xengine_tables_fields_info;
    schema->fill_table = i_s_xengine_tables_fill_table;
  }

  DBUG_RETURN(ret);
}

/**  XENGINE_COLUMNS  **************************************************/
/* Fields of the dynamic table INFORMATION_SCHEMA.XENGINE_COLUMNS
Every time any column gets changed, added or removed, please remember
to change i_s_innodb_plugin_version_postfix accordingly, so that
the change can be propagated to server */

namespace XENGINE_COLUMNS_FIELD {
enum {
  NAME = 0,
  SCHEMA_NAME,
  TABLE_NAME,
  IS_INSTANTLY_ADDED,
  HAS_ORIGINAL_DEFAULT,
  ORIGIN_DEFAULT_VALUE
};
} // namespace XENGINE_TABLES_FIELD 

static ST_FIELD_INFO xengine_columns_fields_info[] = {
    XENGINE_FIELD_INFO("NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("SCHEMA_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("TABLE_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("IS_INSTANTLY_ADDED", sizeof(int8_t), MYSQL_TYPE_TINY, 0),
    XENGINE_FIELD_INFO("HAS_ORIGINAL_DEFAULT", sizeof(int8_t), MYSQL_TYPE_TINY,0),
    XENGINE_FIELD_INFO("ORIGIN_DEFAULT_VALUE", 65536 * 4, MYSQL_TYPE_BLOB, MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO_END};

/** Function to fill information_schema.xengine_columns.
@param[in]	thd		thread
@param[in,out]	tables		tables to fill
@return 0 on success */
static int i_s_xengine_columns_fill_table(THD *thd, TABLE_LIST *tables, Item *) {
  DBUG_ENTER_FUNC();
  TABLE *table = tables->table;
  bool error = Xdb_dd_helper::traverse_all_xengine_tables(
      thd, false, 0,
      [&thd, &table](const dd::Schema *dd_schema,
                     const dd::Table *dd_table) -> bool {
        bool res = false;
        for (const auto col : dd_table->columns()) {
          table->field[XENGINE_COLUMNS_FIELD::NAME]->store(
              col->name().c_str(), col->name().length(), system_charset_info);
          table->field[XENGINE_COLUMNS_FIELD::SCHEMA_NAME]->store(
              dd_schema->name().c_str(), dd_schema->name().length(),
              system_charset_info);
          table->field[XENGINE_COLUMNS_FIELD::TABLE_NAME]->store(
              dd_table->name().c_str(), dd_table->name().length(),
              system_charset_info);

          if (col->se_private_data().exists(
                  myx::dd_column_key_strings
                      [myx::DD_INSTANT_COLUMN_DEFAULT_NULL])) {
            table->field[XENGINE_COLUMNS_FIELD::IS_INSTANTLY_ADDED]->store(
                true, true);
            table->field[XENGINE_COLUMNS_FIELD::HAS_ORIGINAL_DEFAULT]->store(
                false, true);
            table->field[XENGINE_COLUMNS_FIELD::ORIGIN_DEFAULT_VALUE]
                ->set_null();
          } else if (col->se_private_data().exists(
                         myx::dd_column_key_strings
                             [myx::DD_INSTANT_COLUMN_DEFAULT])) {
            dd::String_type value;
            col->se_private_data().get(
                myx::dd_column_key_strings[myx::DD_INSTANT_COLUMN_DEFAULT],
                &value);
            table->field[XENGINE_COLUMNS_FIELD::IS_INSTANTLY_ADDED]->store(
                true, true);
            table->field[XENGINE_COLUMNS_FIELD::HAS_ORIGINAL_DEFAULT]->store(
                true, true);
            table->field[XENGINE_COLUMNS_FIELD::ORIGIN_DEFAULT_VALUE]
                ->set_notnull();
            table->field[XENGINE_COLUMNS_FIELD::ORIGIN_DEFAULT_VALUE]->store(
                value.c_str(), value.length(), system_charset_info);
          } else {
            table->field[XENGINE_COLUMNS_FIELD::IS_INSTANTLY_ADDED]->store(
                false, true);
            table->field[XENGINE_COLUMNS_FIELD::HAS_ORIGINAL_DEFAULT]->store(
                false, true);
            table->field[XENGINE_COLUMNS_FIELD::ORIGIN_DEFAULT_VALUE]
                ->set_null();
          }
          res |= my_core::schema_table_store_record(thd, table);
        }
        return res;
      });

  if (error) {
    XHANDLER_LOG(
        ERROR,
        "XEngine: failed to scan all XENGINE tables from data dictionary!");
  }

  DBUG_RETURN(0);
}

/** Bind the dynamic table INFORMATION_SCHEMA.xengine_columns
@param[in,out]	p	table schema object
@return 0 on success */
static int xengine_columns_init(void *p) {
  ST_SCHEMA_TABLE *schema;

  DBUG_ENTER("xengine_columns_init");

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    schema = (ST_SCHEMA_TABLE *)p;

    schema->fields_info = xengine_columns_fields_info;
    schema->fill_table = i_s_xengine_columns_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_SUBTABLE dynamic table
 */
namespace XDB_SUBTABLE_FIELD {
enum {
  TABLE_NAME = 0,
  SUBTABLE_NAME,
  SUBTABLE_ID,
  TABLE_SPACE_ID,
  LEVEL,
  LAYER,
  EXTENTS,
  DATA,
  INDEX,
  ACCESS_COUNT,
  //CACHED_SIZE,
  READ,
  WRITE
};
} // namespace XDB_SUBTABLE_FIELD

static ST_FIELD_INFO xdb_i_s_xengine_subtable_fields_info[] = {
    // format as schema_name.table_name
    XENGINE_FIELD_INFO("TABLE_NAME", 2 * (NAME_LEN + 1), MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("SUBTABLE_NAME", NAME_LEN + 1, MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("SUBTABLE_ID", sizeof(int64_t), MYSQL_TYPE_LONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("TABLE_SPACE_ID", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LEVEL", sizeof(int32_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("LAYER", sizeof(int32_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("EXTENTS", sizeof(int32_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("DATA", sizeof(int32_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("INDEX", sizeof(int32_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("ACCESS_COUNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    //XENGINE_FIELD_INFO("CACHED_SIZE", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
    XENGINE_FIELD_INFO("READ", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("WRITE", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_xengine_subtable_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  int ret = xengine::common::Status::kOk;
  xengine::db::DB *const xdb = xdb_get_xengine_db();
  std::vector<xengine::db::ColumnFamilyHandle*> subtables;
  xdb->get_all_subtable(subtables);

  // collect subtables from dd
  std::map<uint32_t, std::pair<std::string, std::string>> subtable_id_map;
  if (Xdb_dd_helper::get_xengine_subtable_map(thd, subtable_id_map)) {
    XHANDLER_LOG(ERROR, "XEngine: failed to collection subtables from dd");
  }
  subtable_id_map.emplace(0, std::make_pair("<internal>", DEFAULT_CF_NAME));
  subtable_id_map.emplace(DEFAULT_SYSTEM_SUBTABLE_ID,
                          std::make_pair("<internal>", DEFAULT_SYSTEM_SUBTABLE_NAME));
  static std::string unknown_subtable("<Unknown>");

  // free the handle and release the cfd reference
  xengine::db::SuperVersion *sv = nullptr;
  xengine::storage::StorageManager *storage_manager = nullptr;
  uint64_t extents_number = 0;
  uint64_t data_size = 0;
  uint64_t index_size = 0;
  uint64_t layers = 0;
  int64_t pos = 0;
  xengine::util::Arena arena;
  std::unique_ptr<xengine::table::InternalIterator,
                  xengine::memory::ptr_destruct<xengine::table::InternalIterator>>
      iter;
  xengine::common::ReadOptions read_options;
  xengine::db::InternalStats *internal_stats = nullptr;
  xengine::db::SubTable *st = nullptr;

  for (auto subtable : subtables) {
    if (nullptr == subtable) {
      continue;
    }
    st = static_cast<xengine::db::ColumnFamilyHandleImpl*>(subtable)->cfd();
    sv = xdb->GetAndRefSuperVersion(st);
    if (nullptr == sv) {
      continue;
    }
    std::string table_name, index_name;
    uint32_t subtable_id = sv->cfd_.load()->GetID();
    auto name_it = subtable_id_map.find(subtable_id);
    if (name_it != subtable_id_map.end()) {
      table_name = name_it->second.first;
      index_name = name_it->second.second;
    } else {
      table_name = index_name = unknown_subtable;
    }
    storage_manager = sv->cfd_.load()->get_storage_manager();
    // FIXME don't iterate the meta to get the extent info
    for (int32_t level = 0; level < xengine::storage::MAX_TIER_COUNT; level++) {
      iter.reset(storage_manager->get_single_level_iterator(
          read_options, &arena, sv->current_meta_, level));
      if (iter != nullptr) {
        extents_number = 0;
        data_size = 0;
        index_size = 0;
        iter->SeekToFirst();
        // iterate one level to get all the extents info
        while (xengine::common::Status::kOk == ret && iter->Valid()) {
          xengine::common::Slice extent_meta_buf = iter->key();
          extents_number++;
          data_size += ((storage::ExtentMeta*)extent_meta_buf.data())->data_size_;
          index_size += ((storage::ExtentMeta*)extent_meta_buf.data())->index_size_;
          iter->Next();
        }
        tables->table->field[XDB_SUBTABLE_FIELD::TABLE_NAME]->store(
            table_name.c_str(), table_name.size(), system_charset_info);
        tables->table->field[XDB_SUBTABLE_FIELD::SUBTABLE_NAME]->store(
            index_name.c_str(), index_name.size(), system_charset_info);
        tables->table->field[XDB_SUBTABLE_FIELD::SUBTABLE_ID]->store(
            subtable_id, true);
        tables->table->field[XDB_SUBTABLE_FIELD::TABLE_SPACE_ID]->store(
            sv->cfd_.load()->get_table_space_id(), true);
        tables->table->field[XDB_SUBTABLE_FIELD::LEVEL]->store(level, true);
        if (0 == level) {
          layers = (sv->current_meta_->get_extent_layer_version(0) != nullptr)
                       ? sv->current_meta_->get_extent_layer_version(0)->get_extent_layer_size()
                       : 0;
        } else {
          layers = 1;
        }
        tables->table->field[XDB_SUBTABLE_FIELD::LAYER]->store(layers, true);
        tables->table->field[XDB_SUBTABLE_FIELD::EXTENTS]->store(extents_number,
                                                                 true);
        tables->table->field[XDB_SUBTABLE_FIELD::DATA]->store(data_size, true);
        tables->table->field[XDB_SUBTABLE_FIELD::INDEX]->store(index_size,
                                                               true);
        internal_stats = st->internal_stats();
        if (internal_stats != nullptr) {
          tables->table->field[XDB_SUBTABLE_FIELD::ACCESS_COUNT]->store(
          internal_stats->get_cf_stats_count(xengine::db::InternalStats::ACCESS_CNT), true);
          tables->table->field[XDB_SUBTABLE_FIELD::READ]->store(
          internal_stats->get_cf_stats_value(xengine::db::InternalStats::BYTES_READ), true);
          tables->table->field[XDB_SUBTABLE_FIELD::WRITE]->store(
          internal_stats->get_cf_stats_value(xengine::db::InternalStats::BYTES_WRITE), true);
        } else {
          tables->table->field[XDB_SUBTABLE_FIELD::ACCESS_COUNT]->store(0, true);
          tables->table->field[XDB_SUBTABLE_FIELD::READ]->store(0, true);
          tables->table->field[XDB_SUBTABLE_FIELD::WRITE]->store(0, true);
        }

        ret = my_core::schema_table_store_record(thd, tables->table);
      }
    }
    xdb->ReturnAndCleanupSuperVersion(sv->cfd_, sv);
  }
  // release the reference and space
  xdb->return_all_subtable(subtables);

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_subtable virtual table */
static int xdb_i_s_xengine_subtable_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_xengine_subtable_fields_info;
    schema->fill_table = xdb_i_s_xengine_subtable_fill_table;
  }

  DBUG_RETURN(ret);
}

/*
  Support for INFORMATION_SCHEMA.XENGINE_QUERY_TRACE dynamic table
 */
namespace XDB_QUERY_TRACE_FIELD {
enum {
  NAME = 0,
  TYPE,
  COST,
  COST_US,
  PERCENT,
  COUNT,
  COST_US_PER_QUERY,
  COUNT_PER_QUERY,
};
const int64_t MAX_NAME_SIZE = 256;
const int64_t MAX_TYPE_SIZE = 10;
} // namespace XDB_QUERY_TRACE_FIELD

static ST_FIELD_INFO xdb_i_s_query_trace_fields_info[] = {
    XENGINE_FIELD_INFO("NAME", XDB_QUERY_TRACE_FIELD::MAX_NAME_SIZE,
                       MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("TYPE", XDB_QUERY_TRACE_FIELD::MAX_TYPE_SIZE,
                       MYSQL_TYPE_STRING, 0),
    XENGINE_FIELD_INFO("COST", sizeof(double), MYSQL_TYPE_DOUBLE, MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("COST_US", sizeof(double), MYSQL_TYPE_DOUBLE, MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("PERCENT", sizeof(double), MYSQL_TYPE_DOUBLE, MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("COUNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, MY_I_S_UNSIGNED),
    XENGINE_FIELD_INFO("COST_US_PER_QUERY", sizeof(double), MYSQL_TYPE_DOUBLE, MY_I_S_MAYBE_NULL),
    XENGINE_FIELD_INFO("COUNT_PER_QUERY", sizeof(double), MYSQL_TYPE_DOUBLE, 0),
    XENGINE_FIELD_INFO_END};

static int xdb_i_s_query_trace_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  int ret = xengine::common::Status::kOk;
  constexpr int32_t SQL_STATS_COUNT = 4;
  constexpr int32_t MAX_TRACE_POINT =
      static_cast<int32_t>(xengine::monitor::TracePoint::QUERY_TIME_MAX_VALUE);
  constexpr int32_t MAX_ENGINE_COUNT =
      static_cast<int32_t>(xengine::monitor::CountPoint::QUERY_COUNT_MAX_VALUE);
  constexpr int32_t MAX_COUNT_POINT = MAX_ENGINE_COUNT + SQL_STATS_COUNT;

  // get stats from QueryPerfContext
  uint64_t trace_time_i[MAX_TRACE_POINT];
  int64_t trace_count_i[MAX_TRACE_POINT];
  int64_t counter_i[MAX_COUNT_POINT];
  memset(trace_time_i, 0x00, sizeof(trace_time_i[0]) * MAX_TRACE_POINT);
  memset(trace_count_i, 0x00, sizeof(trace_count_i[0]) * MAX_TRACE_POINT);
  memset(counter_i, 0x00, sizeof(counter_i[0]) * MAX_COUNT_POINT);
  auto trace_ctx = xengine::monitor::get_tls_query_perf_context();
  trace_ctx->get_global_trace_info(trace_time_i, trace_count_i);
  for (int32_t i = 0 ; i < MAX_ENGINE_COUNT; ++i) {
    auto count_point = static_cast<xengine::monitor::CountPoint>(i);
    counter_i[i] = trace_ctx->get_global_count(count_point);
  }

  System_status_var status_totals;

  if (thd->fill_status_recursion_level++ == 0) {
    mysql_mutex_lock(&LOCK_status);
  }
  PFS_connection_status_visitor visitor(&status_totals);
  PFS_connection_iterator::visit_global(false, /* hosts */
                                        false, /* users */
                                        false, /* accounts */
                                        false, /* threads */
                                        true,  /* THDs */
                                        &visitor);

  int64_t sql_cmd_type[] = {SQLCOM_SELECT, SQLCOM_INSERT, SQLCOM_UPDATE,
                          SQLCOM_DELETE};
  const char *sql_cmd_name[] = {"SQLCOM_SELECT", "SQLCOM_INSERT", "SQLCOM_UPDATE",
                              "SQLCOM_DELETE"};

  for (int i = MAX_ENGINE_COUNT; i < MAX_COUNT_POINT; ++i) {
    int64_t cmd = sql_cmd_type[i - MAX_ENGINE_COUNT];
    counter_i[i] = status_totals.com_stat[cmd];
  }

  if (thd->fill_status_recursion_level-- == 1) {
    mysql_mutex_unlock(&LOCK_status);
  }

  // We need to calculate average and convert unit to second, so use double
  // instead of int.

  double trace_time[MAX_TRACE_POINT];
  double trace_count[MAX_TRACE_POINT];
  double counter[MAX_COUNT_POINT];
  double trace_time_sum = 0.0;

  for (int32_t i = 0; i < MAX_TRACE_POINT; ++i) {
    trace_time[i] = static_cast<double>(trace_time_i[i]);
    trace_time_sum += trace_time[i];
  }

  for (int32_t i = 0; i < MAX_TRACE_POINT; ++i) {
    trace_count[i] = static_cast<double>(trace_count_i[i]);
  }

  // The first trace point is SERVER_OPERATION which hits once per query.
  double query_count = trace_count[0];

  for (int32_t i = 0; i < MAX_COUNT_POINT; ++i) {
    counter[i] = static_cast<double>(counter_i[i]);
  }


  static double cycles_per_second = static_cast<double>(
        xengine::monitor::get_trace_unit(1000 /* 1000 milli second */));
  static double cycles_per_us = cycles_per_second * 1e-6;

  if (0 == cycles_per_second) {
    sql_print_error("query trace get_trace_unit returns 0");
  } else if (0 == query_count) {
    sql_print_warning("query count is 0, can not fill XENGINE_QUERY_TRACE table");
  } else if (0 == trace_time_sum) {
    sql_print_warning("trace time sum is 0, can not fill XENGINE_QUERY_TRACE table");
  } else {
    // output stats to IS table.
    Field **field_array = tables->table->field;
    const char **trace_point_name = xengine::monitor::get_trace_point_name();
    const char **count_point_name = xengine::monitor::get_count_point_name();
    const char *type_name = "TRACE";
    int64_t type_name_len = strlen(type_name);
    for (int32_t i = 0; i < MAX_TRACE_POINT; ++i) {
      double trace_time_in_sec = trace_time[i] / cycles_per_second;
      double trace_time_in_us = trace_time[i] / cycles_per_us;

      field_array[XDB_QUERY_TRACE_FIELD::NAME]->store(trace_point_name[i],
                                                  strlen(trace_point_name[i]),
                                                  system_charset_info);
      field_array[XDB_QUERY_TRACE_FIELD::TYPE]->store(type_name,
                                                      type_name_len,
                                                      system_charset_info);
      field_array[XDB_QUERY_TRACE_FIELD::COST]->set_notnull();
      field_array[XDB_QUERY_TRACE_FIELD::COST]->store(trace_time_in_sec);
      field_array[XDB_QUERY_TRACE_FIELD::COST_US]->set_notnull();
      field_array[XDB_QUERY_TRACE_FIELD::COST_US]->store(trace_time_in_us);
      field_array[XDB_QUERY_TRACE_FIELD::PERCENT]->set_notnull();
      field_array[XDB_QUERY_TRACE_FIELD::PERCENT]->store(
                                        trace_time[i] / trace_time_sum * 100.0);
      field_array[XDB_QUERY_TRACE_FIELD::COUNT]->store(trace_count_i[i], true);
      field_array[XDB_QUERY_TRACE_FIELD::COST_US_PER_QUERY]->set_notnull();
      field_array[XDB_QUERY_TRACE_FIELD::COST_US_PER_QUERY]->store(
                                        trace_time_in_us / query_count);
      field_array[XDB_QUERY_TRACE_FIELD::COUNT_PER_QUERY]->store(
                                        trace_count[i] / query_count);
      my_core::schema_table_store_record(thd, tables->table);
    }

    type_name = "COUNTER";
    type_name_len = strlen(type_name);
    for (int32_t i = 0; i < MAX_COUNT_POINT; ++i) {
      const char *counter_name = nullptr;
      if (i < MAX_ENGINE_COUNT) {
        counter_name = count_point_name[i];
      } else {
        counter_name = sql_cmd_name[i - MAX_ENGINE_COUNT];
      }
      field_array[XDB_QUERY_TRACE_FIELD::NAME]->store(counter_name,
                                                  strlen(counter_name),
                                                  system_charset_info);
      field_array[XDB_QUERY_TRACE_FIELD::TYPE]->store(type_name,
                                                      type_name_len,
                                                      system_charset_info);
      field_array[XDB_QUERY_TRACE_FIELD::COST]->set_null();
      field_array[XDB_QUERY_TRACE_FIELD::COST_US]->set_null();
      field_array[XDB_QUERY_TRACE_FIELD::PERCENT]->set_null();
      field_array[XDB_QUERY_TRACE_FIELD::COUNT]->store(counter_i[i], true);
      field_array[XDB_QUERY_TRACE_FIELD::COST_US_PER_QUERY]->set_null();
      field_array[XDB_QUERY_TRACE_FIELD::COUNT_PER_QUERY]->store(
                                            counter[i] / query_count);
      my_core::schema_table_store_record(thd, tables->table);
    }
  }

  DBUG_RETURN(ret);
}

/* Initialize the information_schema.xengine_subtable virtual table */
static int xdb_i_s_query_trace_init(void *const p) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(p != nullptr);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_query_trace_fields_info;
    schema->fill_table = xdb_i_s_query_trace_fill_table;
  }

  DBUG_RETURN(ret);
}

namespace XDB_TABLE_SPACE_FIELD {
enum {
  TABLE_SPACE_ID = 0,
  EXTENT_SPACE_TYPE,
  FILE_NUMBER,
  TOTAL_EXTENT_COUNT,
  USED_EXTENT_COUNT,
  FREE_EXTENT_COUNT
};
} // namespace XDB_TABLE_SPACE_FIELD

static int xdb_i_s_xengine_table_space_fill_one_row(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    const xengine::storage::DataFileStatistics &data_file_statistics) {

  tables->table->field[XDB_TABLE_SPACE_FIELD::TABLE_SPACE_ID]->store(data_file_statistics.table_space_id_, true);
  tables->table->field[XDB_TABLE_SPACE_FIELD::EXTENT_SPACE_TYPE]->store(data_file_statistics.extent_space_type_, true);
  tables->table->field[XDB_TABLE_SPACE_FIELD::FILE_NUMBER]->store(data_file_statistics.file_number_, true);
  tables->table->field[XDB_TABLE_SPACE_FIELD::TOTAL_EXTENT_COUNT]->store(data_file_statistics.total_extent_count_, true);
  tables->table->field[XDB_TABLE_SPACE_FIELD::USED_EXTENT_COUNT]->store(data_file_statistics.used_extent_count_, true);
  tables->table->field[XDB_TABLE_SPACE_FIELD::FREE_EXTENT_COUNT]->store(data_file_statistics.free_extent_count_, true);

  return my_core::schema_table_store_record(thd, tables->table);
}
static int xdb_i_s_xengine_table_space_fill_table(
    my_core::THD *const thd, my_core::TABLE_LIST *const tables,
    my_core::Item *const cond MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();
  int ret = xengine::common::Status::kOk;
  std::vector<xengine::storage::DataFileStatistics> data_file_stats;
  xengine::db::DB *const xdb = xdb_get_xengine_db();

  if (xengine::common::Status::kOk == (ret = xdb->get_data_file_stats(data_file_stats))) {
    for (uint32_t i = 0; xengine::common::Status::kOk == ret && i < data_file_stats.size(); ++i) {
      ret = xdb_i_s_xengine_table_space_fill_one_row(thd, tables, data_file_stats.at(i));
    }
  }
  DBUG_RETURN(ret);
}

static ST_FIELD_INFO xdb_i_s_xengine_table_space_fields_info[] = {
  XENGINE_FIELD_INFO("TABLE_SPACE_ID", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("EXTENT_SPACE_TYPE", sizeof(int32_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("FILE_NUMBER", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("TOTAL_EXTENT_COUNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("USED_EXTENT_COUNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("FREE_EXTENT_COUNT", sizeof(int64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO_END};
/* Initialize the information_schema.xengine_table_space virtual table */
static int xdb_i_s_xengine_table_space_init(void *const p)
{
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(nullptr != p);

  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_xengine_table_space_fields_info;
    schema->fill_table = xdb_i_s_xengine_table_space_fill_table;
  }

  DBUG_RETURN(ret);
}


static struct st_mysql_information_schema xdb_i_s_info = {
    MYSQL_INFORMATION_SCHEMA_INTERFACE_VERSION};

struct st_mysql_plugin xdb_i_s_cfstats = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_CFSTATS",
    "Alibaba",
    "XEngine sub table stats",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_cfstats_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_dbstats = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_DBSTATS",
    "Alibaba",
    "XEngine database stats",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_dbstats_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_perf_context = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_PERF_CONTEXT",
    "Alibaba",
    "XEngine perf context stats",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_perf_context_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_perf_context_global = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_PERF_CONTEXT_GLOBAL",
    "Alibaba",
    "XEngine perf context stats in global",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_perf_context_global_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

#if 0
struct st_mysql_plugin xdb_i_s_cfoptions = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_CF_OPTIONS",
    "Alibaba",
    "XEngine sub table options",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_cfoptions_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};
#endif

struct st_mysql_plugin xdb_i_s_global_info = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_GLOBAL_INFO",
    "Alibaba",
    "XEngine global information",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_global_info_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_compact_stats = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_COMPACTION_STATS",
    "Alibaba",
    "XEngine compaction stats",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_compact_stats_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_ddl = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_DDL",
    "Alibaba",
    "XEngine Data Dictionary",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_ddl_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_index_file_map = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_INDEX_FILE_MAP",
    "Alibaba",
    "XEngine index file map",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_index_file_map_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_lock_info = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_LOCKS",
    "Alibaba",
    "XEngine lock information",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_lock_info_init,
    nullptr,
    nullptr,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_trx_info = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_TRX",
    "Alibaba",
    "XEngine transaction information",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_trx_info_init,
    nullptr,
    nullptr,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_xengine_compaction_task = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_COMPACTION_TASK",
    "Alibaba",
    "XEngine all running compaction tasks",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_xengine_compaction_task_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_xengine_compaction_history = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_COMPACTION_HISTORY",
    "Alibaba",
    "XEngine all historical compaction tasks",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_xengine_compaction_history_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_xengine_mem_alloc = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_MEM_ALLOC",
    "Alibaba",
    "XEngine memory allocate stats",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_xengine_mem_alloc_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin i_s_xengine_tables = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
     MYSQL_INFORMATION_SCHEMA_PLUGIN,

    /* pointer to type-specific plugin descriptor */
    /* void* */
    &xdb_i_s_info,

    /* plugin name */
    /* const char* */
    "XENGINE_TABLES",

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    "Alibaba",

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    "XEngine tables",

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    PLUGIN_LICENSE_GPL,

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    xengine_tables_init,

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    nullptr,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    xdb_i_s_deinit,

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    0x0001,

    /* SHOW_VAR* */
    nullptr,

    /* SYS_VAR** */
    nullptr,

    /* reserved for dependency checking */
    /* void* */
    nullptr,

    /* Plugin flags */
    /* unsigned long */
    0,
};

struct st_mysql_plugin i_s_xengine_columns = {
    /* the plugin type (a MYSQL_XXX_PLUGIN value) */
    /* int */
    MYSQL_INFORMATION_SCHEMA_PLUGIN,

    /* pointer to type-specific plugin descriptor */
    /* void* */
    &xdb_i_s_info,

    /* plugin name */
    /* const char* */
    "XENGINE_COLUMNS",

    /* plugin author (for SHOW PLUGINS) */
    /* const char* */
    "Alibaba",

    /* general descriptive text (for SHOW PLUGINS) */
    /* const char* */
    "XEngine table columns",

    /* the plugin license (PLUGIN_LICENSE_XXX) */
    /* int */
    PLUGIN_LICENSE_GPL,

    /* the function to invoke when plugin is loaded */
    /* int (*)(void*); */
    xengine_columns_init,

    /* the function to invoke when plugin is un installed */
    /* int (*)(void*); */
    nullptr,

    /* the function to invoke when plugin is unloaded */
    /* int (*)(void*); */
    xdb_i_s_deinit,

    /* plugin version (for SHOW PLUGINS) */
    /* unsigned int */
    0x0001,

    /* SHOW_VAR* */
    nullptr,

    /* SYS_VAR** */
    nullptr,

    /* reserved for dependency checking */
    /* void* */
    nullptr,

    /* Plugin flags */
    /* unsigned long */
    0,
};

struct st_mysql_plugin xdb_i_s_xengine_subtable = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_SUBTABLE",
    "Alibaba",
    "XDB subtable information",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_xengine_subtable_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

struct st_mysql_plugin xdb_i_s_query_trace = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_QUERY_TRACE",
    "Alibaba",
    "XDB internal performance trace info",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_query_trace_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

namespace XENGINE_DEBUG_INFO {
enum {
  DEBUG_KEY = 0,
  KEY_DESC,
  ITEM_ID,
  TIMESTAMP,
  INFO_1,
  INFO_2,
  INFO_3,
  INFO_4,
  INFO_5,
  INFO_6,
};
const int64_t VALUE_VARCHAR_LEN = 1024;
const int64_t DESC_VARCHAR_LEN = 256;
}

static ST_FIELD_INFO xdb_i_s_debug_info_fields_info[] = {
  XENGINE_FIELD_INFO("DEBUG_KEY", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("KEY_DESC", XENGINE_DEBUG_INFO::DESC_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("ITEM_ID", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("TIMESTAMP", sizeof(uint64_t), MYSQL_TYPE_LONGLONG, 0),
  XENGINE_FIELD_INFO("INFO_1", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("INFO_2", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("INFO_3", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("INFO_4", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("INFO_5", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO("INFO_6", XENGINE_DEBUG_INFO::VALUE_VARCHAR_LEN, MYSQL_TYPE_STRING, 0),
  XENGINE_FIELD_INFO_END
};

class XengineDebugInfoFiller {
public:
  XengineDebugInfoFiller(THD *t, TABLE_LIST *tl) : thd_(t), tables_(tl) {}
  ~XengineDebugInfoFiller() {}
  int operator()(const std::string &key, const xengine::common::DebugInfoEntry &entry) {
    Field **field = tables_->table->field;
    field[XENGINE_DEBUG_INFO::DEBUG_KEY]->store(key.c_str(), key.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::KEY_DESC]->store(entry.key_desc_.c_str(), entry.key_desc_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::ITEM_ID]->store(entry.item_id_, true);
    field[XENGINE_DEBUG_INFO::TIMESTAMP]->store(entry.timestamp_, true);
    field[XENGINE_DEBUG_INFO::INFO_1]->store(entry.debug_info_1_.c_str(), entry.debug_info_1_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::INFO_2]->store(entry.debug_info_2_.c_str(), entry.debug_info_2_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::INFO_3]->store(entry.debug_info_3_.c_str(), entry.debug_info_3_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::INFO_4]->store(entry.debug_info_4_.c_str(), entry.debug_info_4_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::INFO_5]->store(entry.debug_info_5_.c_str(), entry.debug_info_5_.size(), system_charset_info);
    field[XENGINE_DEBUG_INFO::INFO_6]->store(entry.debug_info_6_.c_str(), entry.debug_info_6_.size(), system_charset_info);
    return my_core::schema_table_store_record(thd_, tables_->table);
  }
private:
  THD *thd_;
  TABLE_LIST* tables_;
};

static int xdb_i_s_debug_info_fill_table(THD *thd, TABLE_LIST *tables, Item *) {
  DBUG_ENTER_FUNC();

  XengineDebugInfoFiller filler(thd, tables);
  xengine::common::DebugInfoStation::get_instance()->foreach(filler);

  DBUG_RETURN(0);
}

static int xdb_i_s_xengine_debug_info_init(void *const p) {
  DBUG_ENTER_FUNC();
  DBUG_ASSERT(p != nullptr);
  int ret = 0;
  if (!xdb_is_initialized()) {
    ret = 1;
  } else {
    my_core::ST_SCHEMA_TABLE *schema;
    schema = (my_core::ST_SCHEMA_TABLE *)p;
    schema->fields_info = xdb_i_s_debug_info_fields_info;
    schema->fill_table = xdb_i_s_debug_info_fill_table;
  }
  DBUG_RETURN(ret);
}

struct st_mysql_plugin xdb_i_s_xengine_debug_info = {
  MYSQL_INFORMATION_SCHEMA_PLUGIN,
  &xdb_i_s_info,
  "XENGINE_DEBUG_INFO",
  "Alibaba",
  "schemaless xengine debug info",
  PLUGIN_LICENSE_GPL,
  xdb_i_s_xengine_debug_info_init,
  nullptr,   /* check_uninstall */
  xdb_i_s_deinit,
  0x0001,   /* version number  */
  nullptr,  /* status variables */
  nullptr,  /* system variables */
  nullptr,  /* config options */
  0,        /* flags */
};

struct st_mysql_plugin xdb_i_s_xengine_table_space = {
    MYSQL_INFORMATION_SCHEMA_PLUGIN,
    &xdb_i_s_info,
    "XENGINE_TABLE_SPACE",
    "Alibaba",
    "XEngine table space information",
    PLUGIN_LICENSE_GPL,
    xdb_i_s_xengine_table_space_init,
    nullptr,
    xdb_i_s_deinit,
    0x0001,  /* version number (0.1) */
    nullptr, /* status variables */
    nullptr, /* system variables */
    nullptr, /* config options */
    0,       /* flags */
};

} // namespace myx
