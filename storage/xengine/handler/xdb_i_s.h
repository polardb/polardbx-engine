/*
   Portions Copyright (c) 2020, Alibaba Group Holding Limited
   Copyright (c) 2012,2013 Monty Program Ab

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
#pragma once

namespace myx {

struct Xdb_dd_helper {
  // traverse over data dictionary of all xengine tables
  static bool traverse_all_xengine_tables(
      THD *thd, bool exit_on_failure, ulong lock_timeout,
      std::function<bool(const dd::Schema *, const dd::Table *)> &&visitor);

  // get all schemas from data dictionary and build map from schema_id to schema
  static bool get_schema_id_map(
      dd::cache::Dictionary_client* client,
      std::vector<const dd::Schema*>& all_schemas,
      std::map<dd::Object_id, const dd::Schema*>& schema_id_map);

  // get all XEngine subtables from data dictionary and build map from id to name
  static bool get_xengine_subtable_map(THD* thd,
      std::map<uint32_t, std::pair<std::string, std::string>>& subtable_map);

  // lock and acquire dd::Table object with engine() == XENIGNE
  // if ouput dd_table isn't null, caller needs to release the mdl lock
  static bool acquire_xengine_table(THD* thd, ulong lock_timeout,
      const char* schema_name, const char* table_name,
      const dd::Table*& dd_table, MDL_ticket*& mdl_ticket);

  // get id of all XEngine subtables from data dictionary
  static bool get_xengine_subtable_ids(THD* thd, ulong lock_timeout,
      std::set<uint32_t>& subtable_ids);
};

/*
  Declare INFORMATION_SCHEMA (I_S) plugins needed by MyX storage engine.
*/

extern struct st_mysql_plugin xdb_i_s_cfstats;
extern struct st_mysql_plugin xdb_i_s_dbstats;
extern struct st_mysql_plugin xdb_i_s_perf_context;
extern struct st_mysql_plugin xdb_i_s_perf_context_global;
//extern struct st_mysql_plugin xdb_i_s_cfoptions;
extern struct st_mysql_plugin xdb_i_s_compact_stats;
extern struct st_mysql_plugin xdb_i_s_global_info;
extern struct st_mysql_plugin xdb_i_s_ddl;
extern struct st_mysql_plugin xdb_i_s_index_file_map;
extern struct st_mysql_plugin xdb_i_s_lock_info;
extern struct st_mysql_plugin xdb_i_s_trx_info;
extern struct st_mysql_plugin i_s_xengine_tables;
extern struct st_mysql_plugin i_s_xengine_columns;
extern struct st_mysql_plugin xdb_i_s_xengine_compaction_task;
extern struct st_mysql_plugin xdb_i_s_xengine_compaction_history;
extern struct st_mysql_plugin xdb_i_s_xengine_mem_alloc;
extern struct st_mysql_plugin xdb_i_s_xengine_subtable;
extern struct st_mysql_plugin xdb_i_s_query_trace;
extern struct st_mysql_plugin xdb_i_s_xengine_debug_info;
extern struct st_mysql_plugin xdb_i_s_xengine_table_space;
// extern struct st_mysql_plugin xdb_i_s_xengine_compaction_history;
} // namespace myx
