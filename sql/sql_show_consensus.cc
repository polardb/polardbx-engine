/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "sql/sql_show.h"

#include "my_config.h"

#include <errno.h>
#include <math.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <time.h>
#include <algorithm>
#include <atomic>
#include <functional>
#include <memory>
#include <new>
#include <string>

#include "keycache.h"                                // dflt_key_cache
#include "mutex_lock.h"                              // MUTEX_LOCK
#include "mysql/components/services/log_builtins.h"  // LogErr
#include "mysql/plugin.h"                            // st_mysql_plugin
#include "scope_guard.h"                             // Scope_guard
#include "sql/auth/auth_acls.h"                      // DB_ACLS
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_schema.h"                // dd::Schema_MDL_locker
#include "sql/dd/dd_table.h"                 // is_encrypted
#include "sql/dd/types/column.h"             // dd::Column
#include "sql/dd/types/table.h"              // dd::Table
#include "sql/debug_sync.h"                  // DEBUG_SYNC
#include "sql/derror.h"                      // ER_THD
#include "sql/error_handler.h"               // Internal_error_handler
#include "sql/field.h"                       // Field
#include "sql/filesort.h"                    // filesort_free_buffers
#include "sql/item.h"                        // Item_empty_string
#include "sql/log.h"                         // query_logger
#include "sql/mysqld.h"                      // lower_case_table_names
#include "sql/mysqld_thd_manager.h"          // Global_THD_manager
#include "sql/opt_trace.h"                   // fill_optimizer_trace_info
#include "sql/partition_info.h"              // partition_info
#include "sql/protocol.h"                    // Protocol
#include "sql/sp_head.h"                     // sp_head
#include "sql/sql_base.h"                    // close_thread_tables
#include "sql/sql_class.h"                   // THD
#include "sql/sql_db.h"                      // get_default_db_collation
#include "sql/sql_executor.h"                // QEP_TAB
#include "sql/sql_lex.h"                     // LEX
#include "sql/sql_optimizer.h"               // JOIN
#include "sql/sql_parse.h"                   // command_name
#include "sql/sql_partition.h"               // HA_USE_AUTO_PARTITION
#include "sql/sql_plugin.h"                  // PLUGIN_IS_DELETED, LOCK_plugin
#include "sql/sql_profile.h"                 // query_profile_statistics_info
#include "sql/sql_table.h"                   // primary_key_name
#include "sql/sql_tmp_table.h"               // create_ondisk_from_heap
#include "sql/sql_trigger.h"                 // acquire_shared_mdl_for_trigger
#include "sql/strfunc.h"                     // lex_string_strmake
#include "sql/sys_vars_consensus.h"
#include "sql/table_trigger_dispatcher.h"    // Table_trigger_dispatcher
#include "sql/temp_table_param.h"            // Temp_table_param
#include "sql/trigger.h"                     // Trigger
#include "sql/tztime.h"                      // my_tz_SYSTEM

//#include "sql/ppi/ppi_table_iostat.h"
//#include "sql/sql_implicit_common.h"
//#include "sql/sql_statistics_common.h"
//#include "sql/proxy/proxy_current_connection.h"

#include "komodo0consensus0xlog.h"
#include "komodo0xlog.h"

extern char *opt_cluster_info;

int fill_consensus_commit_pos(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_consensus_commit_pos");
  CHARSET_INFO *cs = system_charset_info;
  TABLE *table = tables->table;
  char fname[FN_REFLEN];
  uint64_t pos, index;
  komodo::xlog_engine_get_log_manager().get_commit_pos(fname, &pos, &index);
  table->field[0]->store(fname, strlen(fname), cs);
  table->field[1]->store((ulonglong)pos);
  table->field[2]->store((ulonglong)index);
  if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  DBUG_RETURN(0);
}

int fill_alisql_cluster_global(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_global");
  int res = komodo::xlog_fill_cluster_info(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_alisql_cluster_local(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_local");
  int res = komodo::xlog_fill_local_info(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_alisql_cluster_health(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_health");
  int res = komodo::xlog_fill_cluster_health_info(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_alisql_cluster_learner_source(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_learner_source");
  int res = komodo::xlog_fill_learner_source(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_alisql_cluster_prefetch_channel(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_prefetch_channel");
  int res = 0;
  TABLE *table = tables->table;

  // get from consensus alg layer
  komodo::Consensus_prefetch_manager *prefetch_mgr = komodo::xlog_engine_get_prefetch_manager();
  if (!prefetch_mgr) DBUG_RETURN(0);

  prefetch_mgr->lock_prefetch_channels_hash(true);
  for (auto iter = prefetch_mgr->get_channels_hash()->begin();
       iter != prefetch_mgr->get_channels_hash()->end(); ++iter) {
    std::string stop_flag_str =
        iter->second->get_stop_preftch_request() ? "YES" : "NO";
    int field_num = 0;
    table->field[field_num++]->store((longlong)iter->second->get_channel_id(),
                                     true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_first_index_in_cache(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_last_index_in_cache(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_prefetch_cache_size(), true);
    table->field[field_num++]->store(
        (longlong)iter->second->get_current_request(), true);
    table->field[field_num++]->store(
        stop_flag_str.c_str(), stop_flag_str.length(), system_charset_info);

    if (schema_table_store_record(thd, table)) DBUG_RETURN(1);
  }

  prefetch_mgr->unlock_prefetch_channels_hash();
  DBUG_RETURN(res);
}

int fill_alisql_cluster_consensus_status(THD *thd, Table_ref *tables, Item *) {
  DBUG_ENTER("fill_alisql_cluster_consensus_status");
  int res = komodo::xlog_fill_cluster_status(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_alisql_cluster_consensus_membership_change(THD *thd, Table_ref *tables,
                                                    Item *) {
  DBUG_ENTER("fill_alisql_cluster_consensus_membership_change");
  int res = komodo::xlog_fill_cluster_membership_changes(thd, tables->table);
  DBUG_RETURN(res);
}

int fill_consensus_ha_history(THD *thd, Table_ref *tables,
                                                    Item *) {
  DBUG_ENTER("fill_consensus_ha_history");
  int res = komodo::xlog_fill_cluster_ha_history(thd, tables->table);
  DBUG_RETURN(res);
}

ST_FIELD_INFO consensus_commit_pos_info[] = {
    {"LOGNAME", FN_REFLEN, MYSQL_TYPE_STRING, 0, 0, "", 0},
    {"POSITION", 21, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {"INDEX", 21, MYSQL_TYPE_LONGLONG, 0, MY_I_S_UNSIGNED, "", 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, "", 0}};

ST_FIELD_INFO alisql_cluster_global_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"MATCH_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"NEXT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"HAS_VOTED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"FORCE_SYNC", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"ELECTION_WEIGHT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"LEARNER_SOURCE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"APPLIED_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"PIPELINING", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SEND_APPLIED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"INSTANCE_TYPE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"DISABLE_ELECTION", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SERVER_IP", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SERVER_PORT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_local_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"CURRENT_TERM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CURRENT_LEADER", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COMMIT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_LOG_TERM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_LOG_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"VOTED_FOR", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"LAST_APPLY_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"SERVER_READY_FOR_RW", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"INSTANCE_TYPE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"DISABLE_ELECTION", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"APPLY_RUNNING", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"LEADER_IP", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"LEADER_PORT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_health_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"ROLE", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"CONNECTED", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"LOG_DELAY_NUM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"APPLY_DELAY_NUM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"APPLY_RUNNING", 3, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"APPLY_DELAY_SECONDS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_learner_source_fields_info[] = {
    {"LEARNER_SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"LEARNER_IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SOURCE_SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"SOURCE_IP_PORT", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SOURCE_LAST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"SOURCE_COMMIT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_MATCH_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_NEXT_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"LEARNER_APPLIED_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_prefetch_channel_info[] = {
    {"CHANNEL_ID", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"FIRST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"LAST_INDEX", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CACHE_SIZE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"CURRENT_REQUEST", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"STOP_FLAG", 10, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_consensus_status_fields_info[] = {
    {"SERVER_ID", 10, MYSQL_TYPE_LONG, 0, 0, 0, 0},
    {"COUNT_MSG_APPEND_LOG", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {"COUNT_MSG_REQUEST_VOTE", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG,
     0, 0, 0, 0},
    {"COUNT_HEARTBEAT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"COUNT_ON_MSG_APPEND_LOG", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_ON_MSG_REQUEST_VOTE", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_ON_HEARTBEAT", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"COUNT_REPLICATE_LOG", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"COUNT_LOG_META_GET_IN_CACHE", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {"COUNT_LOG_META_GET_TOTAL", MY_INT64_NUM_DECIMAL_DIGITS,
     MYSQL_TYPE_LONGLONG, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO alisql_cluster_consensus_membership_change_fields_info[] = {
    {"TIME", 32, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"COMMAND", 512, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};

ST_FIELD_INFO consensus_ha_history_fields_info[] = {
    {"FINISH_TIMESTAMP", 36, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"CURRENT_TERM", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"TYPE", 15, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"SOURCE_ADDRESS", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"TARGET_ADDRESS", NAME_CHAR_LEN, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {"CONSENSUS_TIME_MS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0,
     0, 0, 0},
    {"SERVER_TIME_MS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0,
     0, 0},
    {"TOTAL_TIME_MS", MY_INT64_NUM_DECIMAL_DIGITS, MYSQL_TYPE_LONGLONG, 0, 0, 0,
     0},
    {"HA_SUCCESS", 6, MYSQL_TYPE_STRING, 0, 0, 0, 0},
    {0, 0, MYSQL_TYPE_STRING, 0, 0, 0, 0}};
