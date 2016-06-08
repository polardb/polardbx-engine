/* Copyright (c) 2015, 2016, Oracle and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation; version 2 of the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "psi_memory_key.h"

#include "my_global.h"
#include "my_thread.h"                          // Needed by mysql_memory.h
#include "mysql/psi/psi_base.h"                 // PSI_FLAG_GLOBAL
#include "mysql/psi/mysql_memory.h"

/*
  MAINTAINER: Please keep this list in order, to limit merge collisions.
*/

extern "C" {

PSI_memory_key key_memory_DATE_TIME_FORMAT;
PSI_memory_key key_memory_DDL_LOG_MEMORY_ENTRY;
PSI_memory_key key_memory_Event_queue_element_for_exec_names;
PSI_memory_key key_memory_Event_scheduler_scheduler_param;
PSI_memory_key key_memory_File_query_log_name;
PSI_memory_key key_memory_Filesort_info_merge;
PSI_memory_key key_memory_Filesort_info_record_pointers;
PSI_memory_key key_memory_Gcalc_dyn_list_block;
PSI_memory_key key_memory_Geometry_objects_data;
PSI_memory_key key_memory_Gis_read_stream_err_msg;
PSI_memory_key key_memory_Gtid_state_to_string;
PSI_memory_key key_memory_HASH_ROW_ENTRY;
PSI_memory_key key_memory_JOIN_CACHE;
PSI_memory_key key_memory_JSON;
PSI_memory_key key_memory_LOG_POS_COORD;
PSI_memory_key key_memory_LOG_name;
PSI_memory_key key_memory_MPVIO_EXT_auth_info;
PSI_memory_key key_memory_MYSQL_BIN_LOG_basename;
PSI_memory_key key_memory_MYSQL_BIN_LOG_index;
PSI_memory_key key_memory_MYSQL_LOCK;
PSI_memory_key key_memory_MYSQL_LOG_name;
PSI_memory_key key_memory_MYSQL_RELAY_LOG_basename;
PSI_memory_key key_memory_MYSQL_RELAY_LOG_index;
PSI_memory_key key_memory_Mutex_cond_array_Mutex_cond;
PSI_memory_key key_memory_NAMED_ILINK_name;
PSI_memory_key key_memory_NET_buff;
PSI_memory_key key_memory_NET_compress_packet;
PSI_memory_key key_memory_Owned_gtids_sidno_to_hash;
PSI_memory_key key_memory_Owned_gtids_to_string;
PSI_memory_key key_memory_PROFILE;
PSI_memory_key key_memory_QUICK_RANGE_SELECT_mrr_buf_desc;
PSI_memory_key key_memory_Query_cache;
PSI_memory_key key_memory_Quick_ranges;
PSI_memory_key key_memory_READ_INFO;
PSI_memory_key key_memory_READ_RECORD_cache;
PSI_memory_key key_memory_Relay_log_info_group_relay_log_name;
PSI_memory_key key_memory_Row_data_memory_memory;
PSI_memory_key key_memory_Rpl_info_file_buffer;
PSI_memory_key key_memory_Rpl_info_table;
PSI_memory_key key_memory_SLAVE_INFO;
PSI_memory_key key_memory_ST_SCHEMA_TABLE;
PSI_memory_key key_memory_Security_context;
PSI_memory_key key_memory_Slave_job_group_group_relay_log_name;
PSI_memory_key key_memory_Sort_param_tmp_buffer;
PSI_memory_key key_memory_Sys_var_charptr_value;
PSI_memory_key key_memory_TABLE;
PSI_memory_key key_memory_TABLE_RULE_ENT;
PSI_memory_key key_memory_TABLE_sort_io_cache;
PSI_memory_key key_memory_TC_LOG_MMAP_pages;
PSI_memory_key key_memory_THD_Session_sysvar_resource_manager;
PSI_memory_key key_memory_THD_Session_tracker;
PSI_memory_key key_memory_THD_db;
PSI_memory_key key_memory_THD_handler_tables_hash;
PSI_memory_key key_memory_THD_variables;
PSI_memory_key key_memory_Table_trigger_dispatcher;
PSI_memory_key key_memory_Unique_merge_buffer;
PSI_memory_key key_memory_Unique_sort_buffer;
PSI_memory_key key_memory_User_level_lock;
PSI_memory_key key_memory_XID;
PSI_memory_key key_memory_XID_STATE;
PSI_memory_key key_memory_acl_mem;
PSI_memory_key key_memory_acl_memex;
PSI_memory_key key_memory_acl_cache;
PSI_memory_key key_memory_binlog_cache_mngr;
PSI_memory_key key_memory_binlog_pos;
PSI_memory_key key_memory_binlog_recover_exec;
PSI_memory_key key_memory_binlog_statement_buffer;
PSI_memory_key key_memory_binlog_ver_1_event;
PSI_memory_key key_memory_bison_stack;
PSI_memory_key key_memory_blob_mem_storage;
PSI_memory_key key_memory_db_worker_hash_entry;
PSI_memory_key key_memory_delegate;
PSI_memory_key key_memory_errmsgs;
PSI_memory_key key_memory_fill_schema_schemata;
PSI_memory_key key_memory_native_functions;
PSI_memory_key key_memory_frm;
PSI_memory_key key_memory_frm_extra_segment_buff;
PSI_memory_key key_memory_frm_form_pos;
PSI_memory_key key_memory_frm_string;
PSI_memory_key key_memory_gdl;
PSI_memory_key key_memory_get_all_tables;
PSI_memory_key key_memory_global_system_variables;
PSI_memory_key key_memory_handler_errmsgs;
PSI_memory_key key_memory_handlerton;
PSI_memory_key key_memory_hash_index_key_buffer;
PSI_memory_key key_memory_help;
PSI_memory_key key_memory_host_cache_hostname;
PSI_memory_key key_memory_ignored_db;
PSI_memory_key key_memory_locked_table_list;
PSI_memory_key key_memory_locked_thread_list;
PSI_memory_key key_memory_my_bitmap_map;
PSI_memory_key key_memory_my_str_malloc;
PSI_memory_key key_memory_new_frm_mem;
PSI_memory_key key_memory_opt_bin_logname;
PSI_memory_key key_memory_partition_syntax_buffer;
PSI_memory_key key_memory_prepared_statement_map;
PSI_memory_key key_memory_prepared_statement_main_mem_root;
PSI_memory_key key_memory_protocol_rset_root;
PSI_memory_key key_memory_prune_partitions_exec;
PSI_memory_key key_memory_queue_item;
PSI_memory_key key_memory_quick_group_min_max_select_root;
PSI_memory_key key_memory_quick_index_merge_root;
PSI_memory_key key_memory_quick_range_select_root;
PSI_memory_key key_memory_quick_ror_intersect_select_root;
PSI_memory_key key_memory_quick_ror_union_select_root;
PSI_memory_key key_memory_rpl_filter;
PSI_memory_key key_memory_rpl_slave_check_temp_dir;
PSI_memory_key key_memory_rpl_slave_command_buffer;
PSI_memory_key key_memory_servers;
PSI_memory_key key_memory_shared_memory_name;
PSI_memory_key key_memory_show_slave_status_io_gtid_set;
PSI_memory_key key_memory_sp_head_call_root;
PSI_memory_key key_memory_sp_head_execute_root;
PSI_memory_key key_memory_sp_head_main_root;
PSI_memory_key key_memory_table_mapping_root;
PSI_memory_key key_memory_table_share;
PSI_memory_key key_memory_table_triggers_list;
PSI_memory_key key_memory_test_quick_select_exec;
PSI_memory_key key_memory_thd_main_mem_root;
PSI_memory_key key_memory_thd_timer;
PSI_memory_key key_memory_thd_transactions;
PSI_memory_key key_memory_user_conn;
PSI_memory_key key_memory_user_var_entry;
PSI_memory_key key_memory_user_var_entry_value;
PSI_memory_key key_memory_warning_info_warn_root;
PSI_memory_key key_memory_sp_cache;
PSI_memory_key key_memory_write_set_extraction;

}

#ifdef HAVE_PSI_INTERFACE

static PSI_memory_info all_server_memory[]=
{
  { &key_memory_locked_table_list, "Locked_tables_list::m_locked_tables_root", 0},
  { &key_memory_locked_thread_list, "display_table_locks", PSI_FLAG_THREAD},
  { &key_memory_thd_transactions, "THD::transactions::mem_root", PSI_FLAG_THREAD},
  { &key_memory_delegate, "Delegate::memroot", 0},
  { &key_memory_acl_mem, "sql_acl_mem", PSI_FLAG_GLOBAL},
  { &key_memory_acl_memex, "sql_acl_memex", PSI_FLAG_GLOBAL},
  { &key_memory_acl_cache, "acl_cache", PSI_FLAG_GLOBAL},
  { &key_memory_thd_main_mem_root, "thd::main_mem_root", PSI_FLAG_THREAD},
  { &key_memory_help, "help", 0},
  { &key_memory_new_frm_mem, "new_frm_mem", 0},
  { &key_memory_table_share, "TABLE_SHARE::mem_root", PSI_FLAG_GLOBAL},
  { &key_memory_gdl, "gdl", 0},
  { &key_memory_table_triggers_list, "Table_triggers_list", 0},
  { &key_memory_servers, "servers", 0},
  { &key_memory_prepared_statement_map, "Prepared_statement_map", PSI_FLAG_THREAD},
  { &key_memory_prepared_statement_main_mem_root, "Prepared_statement::main_mem_root", PSI_FLAG_THREAD},
  { &key_memory_protocol_rset_root, "Protocol_local::m_rset_root", PSI_FLAG_THREAD},
  { &key_memory_warning_info_warn_root, "Warning_info::m_warn_root", PSI_FLAG_THREAD},
  { &key_memory_sp_cache, "THD::sp_cache", 0},
  { &key_memory_sp_head_main_root, "sp_head::main_mem_root", 0},
  { &key_memory_sp_head_execute_root, "sp_head::execute_mem_root", PSI_FLAG_THREAD},
  { &key_memory_sp_head_call_root, "sp_head::call_mem_root", PSI_FLAG_THREAD},
  { &key_memory_table_mapping_root, "table_mapping::m_mem_root", 0},
  { &key_memory_quick_range_select_root, "QUICK_RANGE_SELECT::alloc", PSI_FLAG_THREAD},
  { &key_memory_quick_index_merge_root, "QUICK_INDEX_MERGE_SELECT::alloc", PSI_FLAG_THREAD},
  { &key_memory_quick_ror_intersect_select_root, "QUICK_ROR_INTERSECT_SELECT::alloc", PSI_FLAG_THREAD},
  { &key_memory_quick_ror_union_select_root, "QUICK_ROR_UNION_SELECT::alloc", PSI_FLAG_THREAD},
  { &key_memory_quick_group_min_max_select_root, "QUICK_GROUP_MIN_MAX_SELECT::alloc", PSI_FLAG_THREAD},
  { &key_memory_test_quick_select_exec, "test_quick_select", PSI_FLAG_THREAD},
  { &key_memory_prune_partitions_exec, "prune_partitions::exec", 0},
  { &key_memory_binlog_recover_exec, "MYSQL_BIN_LOG::recover", 0},
  { &key_memory_blob_mem_storage, "Blob_mem_storage::storage", 0},

  { &key_memory_NAMED_ILINK_name, "NAMED_ILINK::name", 0},
  { &key_memory_String_value, "String::value", 0},
  { &key_memory_Sys_var_charptr_value, "Sys_var_charptr::value", 0},
  { &key_memory_queue_item, "Queue::queue_item", 0},
  { &key_memory_THD_db, "THD::db", 0},
  { &key_memory_user_var_entry, "user_var_entry", 0},
  { &key_memory_Slave_job_group_group_relay_log_name, "Slave_job_group::group_relay_log_name", 0},
  { &key_memory_Relay_log_info_group_relay_log_name, "Relay_log_info::group_relay_log_name", 0},
  { &key_memory_binlog_cache_mngr, "binlog_cache_mngr", 0},
  { &key_memory_Row_data_memory_memory, "Row_data_memory::memory", 0},

  { &key_memory_Gtid_set_to_string, "Gtid_set::to_string", 0},
  { &key_memory_Gtid_state_to_string, "Gtid_state::to_string", 0},
  { &key_memory_Owned_gtids_to_string, "Owned_gtids::to_string", 0},
  { &key_memory_log_event, "Log_event", 0},
  { &key_memory_Incident_log_event_message, "Incident_log_event::message", 0},
  { &key_memory_Rows_query_log_event_rows_query, "Rows_query_log_event::rows_query", 0},

  { &key_memory_Sort_param_tmp_buffer, "Sort_param::tmp_buffer", 0},
  { &key_memory_Filesort_info_merge, "Filesort_info::merge", 0},
  { &key_memory_Filesort_info_record_pointers, "Filesort_info::record_pointers", 0},
  { &key_memory_Filesort_buffer_sort_keys, "Filesort_buffer::sort_keys", 0},
  { &key_memory_handler_errmsgs, "handler::errmsgs", 0},
  { &key_memory_handlerton, "handlerton", 0},
  { &key_memory_XID, "XID", 0},
  { &key_memory_host_cache_hostname, "host_cache::hostname", 0},
  { &key_memory_user_var_entry_value, "user_var_entry::value", 0},
  { &key_memory_User_level_lock, "User_level_lock", 0},
  { &key_memory_MYSQL_LOG_name, "MYSQL_LOG::name", 0},
  { &key_memory_TC_LOG_MMAP_pages, "TC_LOG_MMAP::pages", 0},
  { &key_memory_my_bitmap_map, "my_bitmap_map", 0},
  { &key_memory_QUICK_RANGE_SELECT_mrr_buf_desc, "QUICK_RANGE_SELECT::mrr_buf_desc", 0},
  { &key_memory_Event_queue_element_for_exec_names, "Event_queue_element_for_exec::names", 0},
  { &key_memory_my_str_malloc, "my_str_malloc", 0},
  { &key_memory_MYSQL_BIN_LOG_basename, "MYSQL_BIN_LOG::basename", 0},
  { &key_memory_MYSQL_BIN_LOG_index, "MYSQL_BIN_LOG::index", 0},
  { &key_memory_MYSQL_RELAY_LOG_basename, "MYSQL_RELAY_LOG::basename", 0},
  { &key_memory_MYSQL_RELAY_LOG_index, "MYSQL_RELAY_LOG::index", 0},
  { &key_memory_rpl_filter, "rpl_filter memory", 0},
  { &key_memory_errmsgs, "errmsgs", 0},
  { &key_memory_Gcalc_dyn_list_block, "Gcalc_dyn_list::block", 0},
  { &key_memory_Gis_read_stream_err_msg, "Gis_read_stream::err_msg", 0},
  { &key_memory_Geometry_objects_data, "Geometry::ptr_and_wkb_data", 0},
  { &key_memory_MYSQL_LOCK, "MYSQL_LOCK", 0},
  { &key_memory_NET_buff, "NET::buff", 0},
  { &key_memory_NET_compress_packet, "NET::compress_packet", 0},
  { &key_memory_Event_scheduler_scheduler_param, "Event_scheduler::scheduler_param", 0},
  { &key_memory_Gtid_set_Interval_chunk, "Gtid_set::Interval_chunk", 0},
  { &key_memory_Owned_gtids_sidno_to_hash, "Owned_gtids::sidno_to_hash", 0},
  { &key_memory_Sid_map_Node, "Sid_map::Node", 0},
  { &key_memory_Mutex_cond_array_Mutex_cond, "Mutex_cond_array::Mutex_cond", 0},
  { &key_memory_TABLE_RULE_ENT, "TABLE_RULE_ENT", 0},

  { &key_memory_Rpl_info_table, "Rpl_info_table", 0},
  { &key_memory_Rpl_info_file_buffer, "Rpl_info_file::buffer", 0},
  { &key_memory_db_worker_hash_entry, "db_worker_hash_entry", 0},
  { &key_memory_rpl_slave_check_temp_dir, "rpl_slave::check_temp_dir", 0},
  { &key_memory_rpl_slave_command_buffer, "rpl_slave::command_buffer", 0},
  { &key_memory_binlog_ver_1_event, "binlog_ver_1_event", 0},
  { &key_memory_SLAVE_INFO, "SLAVE_INFO", 0},
  { &key_memory_binlog_pos, "binlog_pos", 0},
  { &key_memory_HASH_ROW_ENTRY, "HASH_ROW_ENTRY", 0},
  { &key_memory_binlog_statement_buffer, "binlog_statement_buffer", 0},
  { &key_memory_partition_syntax_buffer, "partition_syntax_buffer", 0},
  { &key_memory_READ_INFO, "READ_INFO", 0},
  { &key_memory_JOIN_CACHE, "JOIN_CACHE", 0},
  { &key_memory_TABLE_sort_io_cache, "TABLE::sort_io_cache", 0},
  { &key_memory_frm, "frm", 0},
  { &key_memory_Unique_sort_buffer, "Unique::sort_buffer", 0},
  { &key_memory_Unique_merge_buffer, "Unique::merge_buffer", 0},
  { &key_memory_TABLE, "TABLE", PSI_FLAG_GLOBAL},
  { &key_memory_frm_extra_segment_buff, "frm::extra_segment_buff", 0},
  { &key_memory_frm_form_pos, "frm::form_pos", 0},
  { &key_memory_frm_string, "frm::string", 0},
  { &key_memory_LOG_name, "LOG_name", 0},
  { &key_memory_DATE_TIME_FORMAT, "DATE_TIME_FORMAT", 0},
  { &key_memory_DDL_LOG_MEMORY_ENTRY, "DDL_LOG_MEMORY_ENTRY", 0},
  { &key_memory_ST_SCHEMA_TABLE, "ST_SCHEMA_TABLE", 0},
  { &key_memory_ignored_db, "ignored_db", 0},
  { &key_memory_PROFILE, "PROFILE", 0},
  { &key_memory_global_system_variables, "global_system_variables", 0},
  { &key_memory_THD_variables, "THD::variables", 0},
  { &key_memory_Security_context, "Security_context", 0},
  { &key_memory_shared_memory_name, "Shared_memory_name", 0},
  { &key_memory_bison_stack, "bison_stack", 0},
  { &key_memory_THD_handler_tables_hash, "THD::handler_tables_hash", 0},
  { &key_memory_hash_index_key_buffer, "hash_index_key_buffer", 0},
  { &key_memory_user_conn, "user_conn", 0},
  { &key_memory_LOG_POS_COORD, "LOG_POS_COORD", 0},
  { &key_memory_XID_STATE, "XID_STATE", 0},
  { &key_memory_MPVIO_EXT_auth_info, "MPVIO_EXT::auth_info", 0},
  { &key_memory_opt_bin_logname, "opt_bin_logname", 0},
  { &key_memory_Query_cache, "Query_cache", PSI_FLAG_GLOBAL},
  { &key_memory_READ_RECORD_cache, "READ_RECORD_cache", 0},
  { &key_memory_Quick_ranges, "Quick_ranges", 0},
  { &key_memory_File_query_log_name, "File_query_log::name", 0},
  { &key_memory_Table_trigger_dispatcher, "Table_trigger_dispatcher::m_mem_root", 0},
  { &key_memory_thd_timer, "thd_timer", 0},
  { &key_memory_THD_Session_tracker, "THD::Session_tracker", 0},
  { &key_memory_THD_Session_sysvar_resource_manager, "THD::Session_sysvar_resource_manager", 0},
  { &key_memory_show_slave_status_io_gtid_set, "show_slave_status_io_gtid_set", 0},
  { &key_memory_write_set_extraction, "write_set_extraction", 0},
  { &key_memory_get_all_tables, "get_all_tables", 0},
  { &key_memory_fill_schema_schemata, "fill_schema_schemata", 0},
  { &key_memory_native_functions, "native_functions", PSI_FLAG_GLOBAL}
};

void register_server_memory_keys()
{
  const char* category= "sql";
  int count= array_elements(all_server_memory);
  mysql_memory_register(category, all_server_memory, count);
}

#endif // HAVE_PSI_INTERFACE
