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

#ifdef USE_PRAGMA_IMPLEMENTATION
#pragma implementation // gcc: Class implementation
#endif

#define MYSQL_SERVER 1

/* The C++ file's header */
#include "sql_class.h"
#include "./ha_xengine.h"
#include "./handler_alter.h"

/* C++ standard header files */
#include <algorithm>
#include <queue>
#include <set>
#include <string>
#include <vector>
#include <atomic>

/* MySQL includes */
#include "debug_sync.h"
#include "my_bit.h"
#include "my_sys.h"
#include "my_stacktrace.h"
#include "sql_audit.h"
#include "sql_table.h"
#include "m_string.h"
#include "mysql/psi/mysql_stage.h"
#include "mysql/psi/mysql_table.h"
#include "mysql/thread_pool_priv.h"
#include "mysys_err.h"
#include "debug_sync.h"
#include "json_dom.h"
#include "mysqld.h"
#include "sql_audit.h"
#include "sql_lex.h"
#include "sql_partition.h"
#include "sql_table.h"
#include "sql_thd_internal_api.h"
#include "table.h"
#include "my_loglevel.h"
#include "sql/mysqld_thd_manager.h"    // Global_THD_manager
#include "sql/sql_base.h"

//#include "async_commit_manager.h"

/* XENGINE includes */
#include "xengine/compaction_filter.h"
#include "xengine/persistent_cache.h"
#include "xengine/rate_limiter.h"
#include "xengine/slice_transform.h"
#include "xengine/utilities/checkpoint.h"
#include "xengine/utilities/convenience.h"
#include "xengine/utilities/memory_util.h"
#include "xengine/async_callback.h"
#include "xengine/perf_level.h"
#include "core/cache/row_cache.h"
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
#include "xengine/options.h"
#include "table/internal_iterator.h"
#include "storage/multi_version_extent_meta_layer.h"
#include "xengine/utilities/hotbackup.h"

/* XEngine handler includes */
#include "./event_listener.h"
#include "./ha_xengine_proto.h"
#include "./logger.h"
#include "./xdb_cf_manager.h"
#include "./xdb_cf_options.h"
#include "./xdb_datadic.h"
#include "./xdb_i_s.h"
#include "./xdb_index_merge.h"
#include "./xdb_mutex_wrapper.h"
#include "./xdb_threads.h"
#include "./xengine_api.h"
#include "./log_ddl.h"

#ifdef HAVE_ASAN
#ifndef HAVE_purify
#define HAVE_purify
#endif
#endif

// Internal MySQL APIs not exposed in any header.
extern "C" {
/**
  Mark transaction to rollback and mark error as fatal to a sub-statement.
  @param  thd   Thread handle
  @param  all   TRUE <=> rollback main transaction.
*/
//void thd_mark_transaction_to_rollback(MYSQL_THD thd, bool all);

/**
 *   Get the user thread's binary logging format
 *   @param thd  user thread
 *   @return Value to be used as index into the binlog_format_names array
*/
//int thd_binlog_format(const MYSQL_THD thd);

/**
 *   Check if binary logging is filtered for thread's current db.
 *   @param  thd   Thread handle
 *   @retval 1 the query is not filtered, 0 otherwise.
*/
//bool thd_binlog_filter_ok(const MYSQL_THD thd);

xengine_cb_t xengine_api_cb[] = {
    (xengine_cb_t)xengine_open_table,
    (xengine_cb_t)xengine_read_row,
    (xengine_cb_t)xengine_insert_row,
    (xengine_cb_t)xengine_delete_row,
    (xengine_cb_t)xengine_update_row,
    (xengine_cb_t)xengine_iter_seek,
    (xengine_cb_t)xengine_iter_first,
    (xengine_cb_t)xengine_iter_next,
    (xengine_cb_t)xengine_set_match_mode,
    (xengine_cb_t)xengine_key_tuple_create,
    (xengine_cb_t)xengine_value_tuple_create,
    (xengine_cb_t)xengine_clust_iter_create,
    (xengine_cb_t)xengine_sec_iter_create,
    (xengine_cb_t)xengine_delete_iter,
    (xengine_cb_t)xengine_tuple_read_u8,
    (xengine_cb_t)xengine_tuple_read_u16,
    (xengine_cb_t)xengine_tuple_read_u24,
    (xengine_cb_t)xengine_tuple_read_u32,
    (xengine_cb_t)xengine_tuple_read_u64,
    (xengine_cb_t)xengine_tuple_read_i8,
    (xengine_cb_t)xengine_tuple_read_i16,
    (xengine_cb_t)xengine_tuple_read_i24,
    (xengine_cb_t)xengine_tuple_read_i32,
    (xengine_cb_t)xengine_tuple_read_i64,
    (xengine_cb_t)xengine_tuple_read_float,
    (xengine_cb_t)xengine_tuple_read_double,
    (xengine_cb_t)xengine_iter_get_n_cols,
    (xengine_cb_t)xengine_col_set_value,
    (xengine_cb_t)xengine_col_get_value,
    (xengine_cb_t)xengine_col_get_meta,
    (xengine_cb_t)xengine_trx_begin,
    (xengine_cb_t)xengine_trx_commit,
    (xengine_cb_t)xengine_trx_rollback,
    (xengine_cb_t)xengine_trx_start,
    (xengine_cb_t)xengine_trx_release,
    (xengine_cb_t)xengine_lock,
    (xengine_cb_t)xengine_close_table,
    (xengine_cb_t)xengine_new_trx,
    (xengine_cb_t)xengine_delete_tuple,
    (xengine_cb_t)xengine_col_get_name,
    (xengine_cb_t)xengine_table_truncate,
    (xengine_cb_t)xengine_open_index_using_name,
    (xengine_cb_t)xengine_cfg_get_cfg,
    (xengine_cb_t)xengine_set_memcached_sync,
    (xengine_cb_t)xengine_set_cluster_access,
    (xengine_cb_t)xengine_commit_trx,
    (xengine_cb_t)xengine_cfg_trx_level,
    (xengine_cb_t)xengine_iter_get_n_user_cols,
    (xengine_cb_t)xengine_set_lock_mode,
    (xengine_cb_t)xengine_get_idx_field_name,
    (xengine_cb_t)xengine_trx_get_start_time,
    (xengine_cb_t)xengine_cfg_bk_commit_interval,
    (xengine_cb_t)xengine_ut_strerr,
    (xengine_cb_t)xengine_stmt_begin,
    (xengine_cb_t)xengine_trx_read_only,
    (xengine_cb_t)xengine_is_virtual_table,
    (xengine_cb_t)xengine_set_tuple_key,
    (xengine_cb_t)xengine_tuple_set_n_fields_cmp,
    (xengine_cb_t)xengine_pk_search};
}

using namespace xengine;
using namespace util;
using namespace common;
class BaseDeltaIterator;

namespace myx {

static st_global_stats global_stats;
static st_export_stats export_stats;

/**
  Updates row counters based on the table type and operation type.
*/
void ha_xengine::update_row_stats(const operation_type &type) {
  DBUG_ASSERT(type < ROWS_MAX);
  // Find if we are modifying system databases.
  if (table->s && m_tbl_def->m_is_mysql_system_table)
    global_stats.system_rows[type].inc();
  else
    global_stats.rows[type].inc();
}

void dbug_dump_database(xengine::db::DB *db);
static handler *xengine_create_handler(my_core::handlerton *hton,
                                       my_core::TABLE_SHARE *table_arg,
                                       bool partitioned,
                                       my_core::MEM_ROOT *mem_root);

bool can_use_bloom_filter(THD *thd, const Xdb_key_def &kd,
                          const xengine::common::Slice &eq_cond,
                          const bool use_all_keys, bool is_ascending);

///////////////////////////////////////////////////////////
// Parameters and settings
///////////////////////////////////////////////////////////
static char *xengine_override_cf_options = nullptr;
// for #21180311
//static char *xengine_filter_policy = nullptr;
//static char *xengine_prefix_extractor = nullptr;
static xengine::common::ColumnFamilyOptions xengine_default_cf_options;
static char *xengine_cf_compression_per_level = nullptr;
static char *xengine_cf_compression_opts = nullptr;
static char *xengine_cf_memtable_options = nullptr;
Xdb_cf_options xengine_cf_options_map;

///////////////////////////////////////////////////////////
// Globals
///////////////////////////////////////////////////////////
handlerton *xengine_hton;

bool xdb_initialized = false;
xengine::util::TransactionDB *xdb = nullptr;
xengine::util::BackupSnapshot *backup_instance = nullptr;

static std::shared_ptr<xengine::monitor::Statistics> xengine_stats;
static std::unique_ptr<xengine::util::Env> flashcache_aware_env;
static std::shared_ptr<Xdb_tbl_prop_coll_factory> properties_collector_factory;

Xdb_dict_manager dict_manager;
Xdb_cf_manager cf_manager;
Xdb_ddl_manager ddl_manager;
const char *m_mysql_gtid;
Xdb_binlog_manager binlog_manager;
Xdb_ddl_log_manager ddl_log_manager;
Xdb_drop_index_thread xdb_drop_idx_thread;

/**
  XEngine background thread control
  N.B. This is besides XENGINE's own background threads
       (@see xengine::common::CancelAllBackgroundWork())
*/

static Xdb_background_thread xdb_bg_thread;

// List of table names (using regex) that are exceptions to the strict
// collation check requirement.
Regex_list_handler *xdb_collation_exceptions;

static const char *const ERRSTR_ROLLBACK_ONLY =
    "This transaction was rolled back and cannot be "
    "committed. Only supported operation is to roll it back, "
    "so all pending changes will be discarded. "
    "Please restart another transaction.";

static void xengine_flush_all_memtables() {
  const Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  std::unique_ptr<xengine::db::ColumnFamilyHandle> cf_ptr;
  for (const auto &cf_handle : cf_manager.get_all_cf()) {
    cf_ptr.reset(cf_handle);
    xdb->Flush(xengine::common::FlushOptions(), cf_handle);
  }
}


///////////////////////////////////////////////////////////
// Hash map: table name => open table handler
///////////////////////////////////////////////////////////

namespace // anonymous namespace = not visible outside this source file
{

const ulong TABLE_HASH_SIZE = 32;

struct Xdb_open_tables_map {
  /* Hash table used to track the handlers of open tables */
  //my_core::HASH m_hash;
  std::unordered_map<std::string, Xdb_table_handler *> m_hash;
  /* The mutex used to protect the hash table */
  mutable mysql_mutex_t m_mutex;

// comment by beilou 5.7->8.0 we use std::map instead
//  void init_hash(void) {
//    (void)my_hash_init(&m_hash, my_core::system_charset_info, TABLE_HASH_SIZE,
//                       0, 0, (my_hash_get_key)Xdb_open_tables_map::get_hash_key,
//                       0, 0, PSI_NOT_INSTRUMENTED);
//  }
//
  void free_hash(void) { m_hash.clear(); }

  static uchar *get_hash_key(Xdb_table_handler *const table_handler,
                             size_t *const length,
                             bool not_used MY_ATTRIBUTE((__unused__)));

  Xdb_table_handler *get_table_handler(const char *const table_name);
  void release_table_handler(Xdb_table_handler *const table_handler);

  std::vector<std::string> get_table_names(void) const;
};

} // anonymous namespace

static Xdb_open_tables_map xdb_open_tables;

#if 0 //unused function
static std::string xdb_normalize_dir(std::string dir) {
  while (dir.size() > 0 && dir.back() == '/') {
    dir.resize(dir.size() - 1);
  }
  return dir;
}
#endif

#if 0 // DEL-SYSVAR
static int32_t xengine_max_backup_snapshot_time = (2 * 60 * 60); // tow hours

// create a file to indicate the snapshot hold by server
static const char *SNAPSHOT_HOLD_FILE = "SNAPSHOT_HOLD";

static int create_snapshot_hold_file(std::string path) {
  int fd = -1;
  int ret = xengine::common::Status::kOk;

  if ((fd = my_open(path.c_str(), O_CREAT, MYF(MY_WME))) < 0) {
    //my_printf_error(ER_UNKNOWN_ERROR, "create the snapshot hold file failed errno %lld", errno);
    my_printf_error(ER_UNKNOWN_ERROR, "create the snapshot hold file failed errno %s", MYF(0), strerror(errno));
    ret = xengine::common::Status::kIOError;
  } else if (my_close(fd, MYF(MY_WME))) {
    my_printf_error(ER_UNKNOWN_ERROR, "close the snapshot hold file failed errno %s", MYF(0), strerror(errno));
    ret = xengine::common::Status::kIOError;
  }

  return ret;
}

static int xengine_create_checkpoint(
    THD *const thd MY_ATTRIBUTE((__unused__)),
    struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const save MY_ATTRIBUTE((__unused__)),
    struct st_mysql_value *const value) {
  char buf[FN_REFLEN];
  int len = sizeof(buf);
  const char *const checkpoint_dir_raw = value->val_str(value, buf, &len);
  if (checkpoint_dir_raw) {
    if (xdb != nullptr) {
      std::string checkpoint_dir = std::string(checkpoint_dir_raw);
      size_t i = checkpoint_dir.rfind('/', checkpoint_dir.length());
      // checkpoint path must be xxx/x
      if (i == std::string::npos || i == (checkpoint_dir.length() - 1)) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "XEngine: the format of path to create checkpoint is wrong\n", MYF(0));
        return HA_ERR_INTERNAL_ERROR;
      }
      std::string phase_string = checkpoint_dir.substr(i + 1, checkpoint_dir.length() - i);
      if (phase_string.size() != 1 || phase_string.c_str()[0] < '1' || phase_string.c_str()[0] > '5') {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "XEngine: only four phases to do checkpoint\n", MYF(0));
        return HA_ERR_INTERNAL_ERROR;
      }
      int phase = std::stol(checkpoint_dir.substr(i + 1, checkpoint_dir.length() - i));
      assert(phase >= 1 && phase <= 5);
      // NO_LINT_DEBUG
      sql_print_information("XEngine: creating checkpoint in directory : %s\n",
                            checkpoint_dir.c_str());
      xengine::util::Checkpoint *checkpoint;
      auto status = xengine::util::Checkpoint::Create(xdb, &checkpoint);
      if (status.ok()) {
        //std::function<int(const char*, int, int64_t, int, xb_wstream_t *)> stream_function = xstream_extent;
        status = checkpoint->manual_checkpoint(checkpoint_dir.c_str(), phase, -1);
                                                //&stream_function, -1);
        if (status.ok()) {
          sql_print_information(
              "XEngine: created checkpoint in directory : %s\n",
              checkpoint_dir.c_str());
          if (4 == phase) { // the last phase start the timer
            xdb_bg_thread.set_snapshot_counter(xengine_max_backup_snapshot_time);
            std::string backup_snapshot_hold_file = checkpoint_dir + "/" + SNAPSHOT_HOLD_FILE;
            if (create_snapshot_hold_file(backup_snapshot_hold_file)) {
              my_printf_error(ER_UNKNOWN_ERROR, "XEngine: create the backup snapshot hold file failed\n", MYF(0));
              status = xengine::common::Status::IOError();
            }
            // for delete the file
            xdb_bg_thread.set_backup_snapshot_hold_file(backup_snapshot_hold_file);
          } else if (5 == phase) {
            // close the timer release the backup snapshot normally
            xdb_bg_thread.set_snapshot_counter(0);
          }
        } else {
          my_printf_error(
              ER_UNKNOWN_ERROR,
              "XEngine: Failed to create checkpoint directory. status %d %s",
              MYF(0), status.code(), status.ToString().c_str());
        }
        delete checkpoint;
      } else {
        const std::string err_text(status.ToString());
        my_printf_error(
            ER_UNKNOWN_ERROR,
            "XEngine: failed to initialize checkpoint. status %d %s\n", MYF(0),
            status.code(), err_text.c_str());
      }
      return status.code();
    }
  }
  return HA_ERR_INTERNAL_ERROR;
}

/* This method is needed to indicate that the
   XENGINE_CREATE_CHECKPOINT command is not read-only */
static void xengine_create_checkpoint_stub(THD *const thd,
                                           struct SYS_VAR *const var,
                                           void *const var_ptr,
                                           const void *const save) {}
#endif

static int xengine_hotbackup(THD *const thd MY_ATTRIBUTE((__unused__)),
                             struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
                             void *const save MY_ATTRIBUTE((__unused__)),
                             struct st_mysql_value *const value);

static void xengine_hotbackup_stub(THD *const thd,
                                   struct SYS_VAR *const var,
                                   void *const var_ptr,
                                   const void *const save)
{}


static void xengine_force_flush_memtable_now_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static int xengine_force_flush_memtable_now(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  sql_print_information("XEngine: Manual memtable flush\n");
  DBUG_EXECUTE_IF("before_sleep_in_flush_collect_stats",
  SyncPoint::GetInstance()->SetCallBack("StorageManager::apply::before_stats_collect", [&](void* arg) {
                                        sleep(20);
                                        fprintf(stdout, "sleep 20s before storage manager stats collect\n");
                                        });
  SyncPoint::GetInstance()->EnableProcessing();
  );
  xengine_flush_all_memtables();
  DBUG_EXECUTE_IF("after_sleep_in_flush_collect_stats",
  SyncPoint::GetInstance()->DisableProcessing();
  );
  return HA_EXIT_SUCCESS;
}

#if 0 // DEL-SYSVAR
static void xengine_drop_index_wakeup_thread(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr MY_ATTRIBUTE((__unused__)), const void *const save);
#endif

static bool xengine_pause_background_work = 0;
static mysql_mutex_t xdb_sysvars_mutex;

static void xengine_set_pause_background_work(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr MY_ATTRIBUTE((__unused__)), const void *const save) {
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  const bool pause_requested = *static_cast<const bool *>(save);
  if (xengine_pause_background_work != pause_requested) {
    if (pause_requested) {
      xdb->PauseBackgroundWork();
    } else {
      xdb->ContinueBackgroundWork();
    }
    xengine_pause_background_work = pause_requested;
  }
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_compaction_options(THD *thd,
                                           struct SYS_VAR *var,
                                           void *var_ptr, const void *save);
#if 0 // DEL-SYSVAR
static void xengine_set_query_trace_print_slow(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_query_trace_print_stats(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_query_trace_print_all(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_query_trace_slow_threshold_us(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
#endif
static void xengine_set_query_trace_sum(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *value);
static void xengine_set_query_trace_print_slow(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_mutex_backtrace_threshold_ns(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_block_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_row_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_db_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_db_total_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
#if 0 // DEL-SYSVAR
static void xengine_set_arena_block_size(
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
#endif
static void xengine_set_level0_file_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_level0_layer_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_level1_extents_major_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_disable_auto_compactions(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_flush_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_compaction_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_flush_delete_percent_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_flush_delete_record_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_compaction_task_extents_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_level2_usage_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_scan_add_blocks_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_bottommost_level(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
#if 0 // DEL-SYSVAR
static void xengine_set_max_bytes_for_level_base(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_max_bytes_for_level_multiplier(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_level0_slowdown_writes_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_level0_stop_writes_trigger(
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_target_file_size_base(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);
static void xengine_set_target_file_size_multiplier(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save);

static void xengine_set_table_stats_sampling_pct(THD *thd,
                                                 struct SYS_VAR *var,
                                                 void *var_ptr,
                                                 const void *save);

static void xengine_set_delayed_write_rate(THD *thd,
                                           struct SYS_VAR *var,
                                           void *var_ptr, const void *save);
#endif

static void xdb_set_collation_exception_list(const char *exception_list);
static void xengine_set_collation_exception_list(THD *thd,
                                                 struct SYS_VAR *var,
                                                 void *var_ptr,
                                                 const void *save);

#if 0 // DEL-SYSVAR
static void
xengine_set_bulk_load(THD *thd,
                      struct SYS_VAR *var MY_ATTRIBUTE((__unused__)),
                      void *var_ptr, const void *save);
#endif

static void xengine_set_stats_dump_period_sec(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_dump_malloc_stats(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_dump_memtable_limit_size(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_auto_shrink_enabled(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_max_free_extent_percent(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_shrink_allocate_interval(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_total_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_auto_shrink_schedule_interval(THD *thd,
                                                      struct SYS_VAR *const var,
                                                      void *const var_ptr,
                                                      const void *const save);
static void xengine_set_estimate_cost_depth(THD *thd, struct SYS_VAR *const var,
                                            void *const var_ptr,
                                            const void *const save);

static void xengine_set_idle_tasks_schedule_time(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);
static void xengine_set_rate_limiter_bytes_per_sec(THD *thd,
                                                   struct SYS_VAR *const var,
                                                   void *const var_ptr,
                                                   const void *const save);


#if 0 // DEL-SYSVAR
static void xengine_set_max_background_compactions(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static void xengine_set_background_flushes(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);
#endif
#if 0 //unused function
static void xengine_set_sync_mode(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);
#endif

static void xengine_shrink_table_space_sub_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save);

static int xengine_shrink_table_space_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value);


//////////////////////////////////////////////////////////////////////////////
// Options definitions
//////////////////////////////////////////////////////////////////////////////
static long long xengine_block_cache_size;
static long long xengine_row_cache_size;
/* Use unsigned long long instead of uint64_t because of MySQL compatibility */
static unsigned long xengine_rate_limiter_bytes_per_sec;
//static unsigned long long xengine_delayed_write_rate;
//static unsigned long xengine_persistent_cache_size;
static uint32_t xengine_flush_log_at_trx_commit;
static char *xengine_wal_dir;
//static char *xengine_persistent_cache_path;
//static uint64_t xengine_index_type;
static uint32_t xengine_debug_optimizer_n_rows = 0;
static bool xengine_force_compute_memtable_stats = true;
//static bool xengine_debug_optimizer_no_zero_cardinality;
static uint32_t xengine_wal_recovery_mode;
static bool xengine_parallel_wal_recovery = 0;
static uint32_t xengine_parallel_recovery_thread_num = 0;
//static uint32_t xengine_access_hint_on_compaction_start;
//static uint64_t xengine_manual_compact_type;
//static char *xengine_compact_cf_name;
static long long xengine_compact_cf_id;
static long long xengine_pending_shrink_subtable_id = 0;
//static char *xengine_checkpoint_name;
//static bool xengine_signal_drop_index_thread;
static char *xengine_hotbackup_name;
static bool xengine_strict_collation_check = 1;
static bool xengine_enable_2pc = 1; //2pc is not configurable in the near future, for atomic ddl.
static char *xengine_strict_collation_exceptions;
static bool xengine_collect_sst_properties = 1;
static bool xengine_force_flush_memtable_now_var = 0;
//static uint64_t xengine_number_stat_computes = 0;
//static uint32_t xengine_seconds_between_stat_computes = 3600;
static long long xengine_compaction_sequential_deletes = 0l;
static long long xengine_compaction_sequential_deletes_window = 0l;
static long long xengine_compaction_sequential_deletes_file_size = 0l;
static uint32_t xengine_validate_tables = 1;
static char *xengine_datadir;
static uint32_t xengine_table_stats_sampling_pct = XDB_DEFAULT_TBL_STATS_SAMPLE_PCT;
static bool rpl_skip_tx_api_var = false;
//static bool xengine_disable_auto_index_per_cf = 0;
//static bool xengine_print_snapshot_conflict_queries = 0;
static bool xengine_skip_unique_key_check_in_boost_insert = 0;

int32_t xengine_shrink_table_space = -1;
bool xengine_enable_bulk_load_api = false;

//TODO yxian atomic
// how many inplace populate indexes
int xengine_inplace_populate_indexes = 0;

static uint64_t xengine_query_trace_sum = 0;
static bool xengine_query_trace_print_slow = 1;

bool xengine_enable_print_ddl_log = true;

uint32_t xengine_disable_online_ddl = 0;
bool xengine_disable_instant_ddl = false;

ulong xengine_sort_buffer_size = 0;
ulong purge_acquire_lock_timeout = 30; // seconds
int purge_schedule_interval = 1800; // 30 minutues
bool opt_purge_invalid_subtable_bg = true;
bool xengine_disable_parallel_ddl = false;

std::atomic<uint64_t> xengine_snapshot_conflict_errors(0);
std::atomic<uint64_t> xengine_wal_group_syncs(0);

static xengine::common::DBOptions xdb_init_xengine_db_options(void) {
  xengine::common::DBOptions o;

  o.create_if_missing = true;
  o.listeners.push_back(std::make_shared<Xdb_event_listener>(&ddl_manager));
  o.info_log_level = xengine::util::InfoLogLevel::INFO_LEVEL;
  o.max_subcompactions = DEFAULT_SUBCOMPACTIONS;

  return o;
}

xengine::common::DBOptions xengine_db_options = xdb_init_xengine_db_options();
static xengine::table::BlockBasedTableOptions xengine_tbl_options;

static std::shared_ptr<xengine::util::RateLimiter> xengine_rate_limiter;

static xengine::logger::InfoLogLevel get_xengine_log_level(ulong l) {
  switch (l) {
    case 0 /* SYSTEM_LEVEL */:
      return xengine::logger::InfoLogLevel::FATAL_LEVEL;
    case 1 /* ERROR_LEVEL */:
      return xengine::logger::InfoLogLevel::ERROR_LEVEL;
    case 2 /* WARNING_LEVEL */:
      return xengine::logger::InfoLogLevel::WARN_LEVEL;
    case 3 /* INFORMATION_LEVEL */:
      return xengine::logger::InfoLogLevel::INFO_LEVEL;
    default:
      return xengine::logger::InfoLogLevel::DEBUG_LEVEL;
  }
  return xengine::logger::InfoLogLevel::INFO_LEVEL;
}

#if 0 // DEL-SYSVAR
static void xengine_set_xengine_manual_compact_type(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xengine_manual_compact_type = *static_cast<const uint64_t *>(save);
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
#endif

static void xengine_compact_column_family_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static int xengine_compact_column_family(THD *const thd,
                                         struct SYS_VAR *const var,
                                         void *const var_ptr,
                                         struct st_mysql_value *const value) {
  long long mvalue = 0;
  DBUG_ASSERT(value != nullptr);

  if (value->val_int(value, &mvalue)) {
    /* The value is NULL. That is invalid. */
    __XHANDLER_LOG(ERROR, "XEngine: Can't parse value for compact_cf\n");
    return HA_EXIT_FAILURE;
  }

  // get cfd_id and compaction type
  uint32_t cf_id = mvalue & 0xffff;
  int32_t compact_type = mvalue >> 32;
  if (0 == cf_id) {
    if (nullptr != xdb) {
      __XHANDLER_LOG(INFO, "XEngine: Manual compaction of all sub tables: %u", cf_id);
      xdb->CompactRange(xengine::common::CompactRangeOptions(), nullptr, nullptr, compact_type);
    }
  } else {
    auto cfh = cf_manager.get_cf(cf_id);
    if (cfh != nullptr  && xdb != nullptr) {
      __XHANDLER_LOG(INFO, "XEngine: Manual compaction of sub table: %u\n", cf_id);
      xdb->CompactRange(xengine::common::CompactRangeOptions(), cfh, nullptr, nullptr, compact_type);
    }
  }
  return HA_EXIT_SUCCESS;
}

static void xengine_reset_subtable_pending_shrink_stub(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static int xengine_reset_subtable_pending_shrink(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  long long mvalue = 0;
  uint64_t subtable_id = 0;
  DBUG_ASSERT(value != nullptr);

  if (value->val_int(value, &mvalue)) {
    /**The value is NULL. That is invalid.*/
    XHANDLER_LOG(ERROR, "XEngine: Can't parse value for reset_pending_shrink");
    return HA_EXIT_FAILURE;
  } else {
    subtable_id = static_cast<uint64_t>(mvalue);
    xdb->reset_pending_shrink(subtable_id);
    XHANDLER_LOG(INFO, "XEngine: success to reset pending shrink", "index_id",
                 subtable_id);
  }

  return HA_EXIT_SUCCESS;
}

#if 0 // DEL-SYSVAR
static const char *index_type_names[] = {"kBinarySearch", "kHashSearch", NullS};

static TYPELIB index_type_typelib = {array_elements(index_type_names) - 1,
                                     "index_type_typelib", index_type_names,
                                     nullptr};
#endif

const ulong XDB_MAX_LOCK_WAIT_SECONDS = 1024 * 1024 * 1024;
const ulong XDB_MAX_ROW_LOCKS = 1024 * 1024 * 1024;
const ulong XDB_DEFAULT_BULK_LOAD_SIZE = 10000;
const ulong XDB_MAX_BULK_LOAD_SIZE = 1024 * 1024 * 1024;
const int64 XDB_DEFAULT_BLOCK_CACHE_SIZE = 512 * 1024 * 1024;
const int64 XDB_DEFAULT_ROW_CACHE_SIZE = 0;
const int64 XDB_MIN_BLOCK_CACHE_SIZE = 1024;
const int64 XDB_MIN_ROW_CACHE_SIZE = 0;
const int XDB_MAX_CHECKSUMS_PCT = 100;
const size_t XDB_MAX_ROW_SIZE = 32 * 1024 * 1024;
const size_t DEFAULT_XENGINE_MAX_TOTAL_WAL_SIZE = 100L << 30; // 100GB
const int DEFAULT_XENGINE_TABLE_CACHE_NUMSHARDBITS = 7;
const size_t DEFAULT_XENGINE_DB_WRITE_BUFFER_SIZE = 100L << 30; // 100GB
const size_t DEFAULT_XENGINE_DB_TOTAL_WRITE_BUFFER_SIZE = 100L << 30; // 100GB
const size_t DEFAULT_XENGINE_BLOCK_SIZE = 16384;
const uint64_t DEFAULT_XENGINE_BATCH_GROUP_SLOT_ARRAY_SIZE = 8;
const uint64_t DEFAULT_XENGINE_BATCH_GROUP_MAX_GROUP_SIZE = 8;
const uint64_t DEFAULT_XENGINE_BATCH_GROUP_MAX_LEADER_WAIT_TIME_US = 50;
const uint64_t DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_NUMBER = 4;
const uint64_t DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_SINGLE_BUFFER_SIZE = 64 * 1024;
const uint64_t DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_SWITCH_LIMIT = 32 * 1024;
// default value for ColumnFamilyOptions
static const size_t DEFAULT_XENGINE_WRITE_BUFFER_SIZE = 256 << 20; // 256 MB
static const size_t DEFAULT_XENGINE_ARENA_BLOCK_SIZE = 32768;
static const int DEFAULT_XENGINE_MAX_WRITE_BUFFER_NUMBER = 1000;
static const int DEFAULT_XENGINE_MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN = 1000;
static const int DEFAULT_XENGINE_MIN_WRITE_BUFFER_NUMBER_TO_MERGE = 2;
static const int DEFAULT_XENGINE_LEVEL0_FILE_NUM_COMPACTION_TRIGGER = 64;
static const int DEFAULT_XENGINE_LEVEL0_LAYER_NUM_COMPACTION_TRIGGER = 8;
static const int DEFAULT_XENGINE_LEVEL1_EXTENTS_MAJOR_COMPACTION_TRIGGER = 1000;
static const bool DEFAULT_XENGINE_DISABLE_AUTO_COMPACTIONS = false;
static const long long DEFAULT_XENGINE_LEVEL2_USAGE_PERCENT = 70;
static const int DEFAULT_XENGINE_FLUSH_DELETE_PERCENT = 70;
static const int DEFAULT_XENGINE_COMPACTION_DELETE_PERCENT = 70;
static const int DEFAULT_XENGINE_FLUSH_DELETE_PERCENT_TRIGGER = 700000;
static const int DEFAULT_XENGINE_FLUSH_DELETE_RECORD_TRIGGER = 700000;
static const char *DEFAULT_XENGINE_COMPRESSION_ALG = "kNoCompression";
static const char *DEFAULT_XENGINE_COMPRESSION_OPTS = "-14:1:0";
static const bool DEFAULT_XENGINE_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES = true;
static const bool DEFAULT_XENGINE_OPTIMIZE_FILTERS_FOR_HITS = true;
static const ulong DEFAULT_XENGINE_SORT_BUF_SIZE = 4 * 1024 * 1024;
static const ulong XENGINE_MIN_SORT_BUF_SIZE = 64 * 1024;
static const ulonglong XENGINE_MAX_SORT_BUF_SIZE = (((ulonglong)16) << 30); //16G
//static const size_t XDB_DEFAULT_MERGE_COMBINE_READ_SIZE = 8 * 1024 * 1024;
//static const size_t XDB_MIN_MERGE_COMBINE_READ_SIZE = 100;

// TODO: 0 means don't wait at all, and we don't support it yet?
static MYSQL_THDVAR_ULONG(lock_wait_timeout, PLUGIN_VAR_RQCMDARG,
                          "Number of seconds to wait for lock", nullptr,
                          nullptr, /*default*/ 1, /*min*/ 1,
                          /*max*/ XDB_MAX_LOCK_WAIT_SECONDS, 0);

static MYSQL_THDVAR_BOOL(deadlock_detect, PLUGIN_VAR_RQCMDARG,
                         "Enables deadlock detection", nullptr, nullptr, FALSE);

#if 0 // DEL-SYSVAR
static MYSQL_THDVAR_BOOL(
    trace_sst_api, PLUGIN_VAR_RQCMDARG,
    "Generate trace output in the log for each call to the SstFileWriter",
    nullptr, nullptr, FALSE);

//static MYSQL_THDVAR_BOOL(
//    bulk_load, PLUGIN_VAR_RQCMDARG,
//    "Use bulk-load mode for inserts. This disables "
//    "unique_checks and enables xengine_commit_in_the_middle.",
//    nullptr, xengine_set_bulk_load, FALSE);

//static MYSQL_SYSVAR_BOOL(enable_bulk_load_api, xengine_enable_bulk_load_api,
//                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
//                         "Enables using SstFileWriter for bulk loading",
//                         nullptr, nullptr, xengine_enable_bulk_load_api);

static MYSQL_SYSVAR_BOOL(disable_auto_index_per_cf, xengine_disable_auto_index_per_cf,
                         PLUGIN_VAR_RQCMDARG,
                         "Disable auto using columnfamily for every index",
                         nullptr, nullptr, xengine_disable_auto_index_per_cf);

static MYSQL_SYSVAR_BOOL(enable_print_ddl_log, xengine_enable_print_ddl_log,
                         PLUGIN_VAR_RQCMDARG,
                         "Enable print ddl log into error log",
                         nullptr, nullptr, xengine_enable_print_ddl_log);

static MYSQL_THDVAR_STR(tmpdir, PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_MEMALLOC,
                        "Directory for temporary files during DDL operations.",
                        nullptr, nullptr, "");

static MYSQL_THDVAR_STR(
    skip_unique_check_tables, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
    "Skip unique constraint checking for the specified tables", nullptr,
    nullptr, ".*");

static MYSQL_THDVAR_BOOL(
    commit_in_the_middle, PLUGIN_VAR_RQCMDARG,
    "Commit rows implicitly every xengine_bulk_load_size, on bulk load/insert, "
    "update and delete",
    nullptr, nullptr, FALSE);

static MYSQL_THDVAR_BOOL(
    blind_delete_primary_key, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Deleting rows by primary key lookup, without reading rows (Blind Deletes)."
    " Blind delete is disabled if the table has secondary key",
    nullptr, nullptr, FALSE);

static MYSQL_THDVAR_STR(
    read_free_rpl_tables,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC | PLUGIN_VAR_READONLY,
    "List of tables that will use read-free replication on the slave "
    "(i.e. not lookup a row during replication)",
    nullptr, nullptr, "");

static MYSQL_SYSVAR_BOOL(
    rpl_skip_tx_api, rpl_skip_tx_api_var, PLUGIN_VAR_RQCMDARG,
    "Use write batches for replication thread instead of tx api", nullptr,
    nullptr, false);

static MYSQL_THDVAR_BOOL(skip_bloom_filter_on_read, PLUGIN_VAR_RQCMDARG,
                         "Skip using bloom filter for reads", nullptr, nullptr,
                         FALSE);
#endif

static MYSQL_THDVAR_ULONG(max_row_locks, PLUGIN_VAR_RQCMDARG,
                          "Maximum number of locks a transaction can have",
                          nullptr, nullptr,
                          /*default*/ XDB_MAX_ROW_LOCKS,
                          /*min*/ 1,
                          /*max*/ XDB_MAX_ROW_LOCKS, 0);

static MYSQL_THDVAR_BOOL(
    lock_scanned_rows, PLUGIN_VAR_RQCMDARG,
    "Take and hold locks on rows that are scanned but not updated", nullptr,
    nullptr, FALSE);

#if 0 // DEL-SYSVAR
static MYSQL_THDVAR_ULONG(bulk_load_size, PLUGIN_VAR_RQCMDARG,
                          "Max #records in a batch for bulk-load mode", nullptr,
                          nullptr,
                          /*default*/ XDB_DEFAULT_BULK_LOAD_SIZE,
                          /*min*/ 1,
                          /*max*/ XDB_MAX_BULK_LOAD_SIZE, 0);

static MYSQL_THDVAR_ULONGLONG(
    merge_buf_size, PLUGIN_VAR_RQCMDARG,
    "Size to allocate for merge sort buffers written out to disk "
    "during inplace index creation.",
    nullptr, nullptr,
    /* default (64MB) */ XDB_DEFAULT_MERGE_BUF_SIZE,
    /* min (100B) */ XDB_MIN_MERGE_BUF_SIZE,
    /* max */ SIZE_T_MAX, 1);

static MYSQL_THDVAR_ULONGLONG(
    merge_combine_read_size, PLUGIN_VAR_RQCMDARG,
    "Size that we have to work with during combine (reading from disk) phase "
    "of "
    "external sort during fast index creation.",
    nullptr, nullptr,
    /* default (1GB) */ XDB_DEFAULT_MERGE_COMBINE_READ_SIZE,
    /* min (100B) */ XDB_MIN_MERGE_COMBINE_READ_SIZE,
    /* max */ SIZE_T_MAX, 1);

static MYSQL_SYSVAR_BOOL(
    create_if_missing, xengine_db_options.create_if_missing,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::create_if_missing for XEngine", nullptr, nullptr,
    xengine_db_options.create_if_missing);

static MYSQL_SYSVAR_BOOL(
    create_missing_column_families,
    xengine_db_options.create_missing_column_families,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::create_missing_column_families for XEngine", nullptr, nullptr,
    xengine_db_options.create_missing_column_families);

static MYSQL_SYSVAR_BOOL(
    error_if_exists, xengine_db_options.error_if_exists,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::error_if_exists for XEngine", nullptr, nullptr,
    xengine_db_options.error_if_exists);

static MYSQL_SYSVAR_BOOL(
    paranoid_checks, xengine_db_options.paranoid_checks,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::paranoid_checks for XEngine", nullptr, nullptr,
    xengine_db_options.paranoid_checks);

static MYSQL_SYSVAR_ULONGLONG(delayed_write_rate, xengine_delayed_write_rate,
                              PLUGIN_VAR_RQCMDARG,
                              "DBOptions::delayed_write_rate", nullptr,
                              xengine_set_delayed_write_rate,
                              xengine_db_options.delayed_write_rate, 0,
                              UINT64_MAX, 0);

static MYSQL_THDVAR_INT(
    perf_context_level, PLUGIN_VAR_RQCMDARG,
    "Perf Context Level for xengine internal timer stat collection", nullptr,
    nullptr,
    /* default */ xengine::monitor::PerfLevel::kUninitialized,
    /* min */ xengine::monitor::PerfLevel::kUninitialized,
    /* max */ xengine::monitor::PerfLevel::kOutOfBounds - 1, 0);
#endif

static MYSQL_SYSVAR_UINT(
    wal_recovery_mode, xengine_wal_recovery_mode,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::wal_recovery_mode for XEngine. Default is kAbsoluteConsistency",
    nullptr, nullptr,
    /* default */ (uint)xengine::common::WALRecoveryMode::kAbsoluteConsistency,
    /* min */ (uint)xengine::common::WALRecoveryMode::kTolerateCorruptedTailRecords,
    /* max */ (uint)xengine::common::WALRecoveryMode::kSkipAnyCorruptedRecords, 0);

static MYSQL_SYSVAR_BOOL(
    parallel_wal_recovery, xengine_parallel_wal_recovery,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::parallel_wal_recovery for XEngine. Default is FALSE",
    nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_UINT(
    parallel_recovery_thread_num, xengine_parallel_recovery_thread_num,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::parallel_recovery_thread_num for XEngine. Default is 0",
    nullptr, nullptr, 0,
    0, 1024, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_ULONG(compaction_readahead_size,
                          xengine_db_options.compaction_readahead_size,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::compaction_readahead_size for XEngine",
                          nullptr, nullptr,
                          xengine_db_options.compaction_readahead_size,
                          /* min */ 0L, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    new_table_reader_for_compaction_inputs,
    xengine_db_options.new_table_reader_for_compaction_inputs,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::new_table_reader_for_compaction_inputs for XEngine", nullptr,
    nullptr, xengine_db_options.new_table_reader_for_compaction_inputs);

static MYSQL_SYSVAR_UINT(
    access_hint_on_compaction_start, xengine_access_hint_on_compaction_start,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::access_hint_on_compaction_start for XEngine", nullptr, nullptr,
    /* default */ (uint)xengine::common::Options::AccessHint::NORMAL,
    /* min */ (uint)xengine::common::Options::AccessHint::NONE,
    /* max */ (uint)xengine::common::Options::AccessHint::WILLNEED, 0);

static MYSQL_SYSVAR_BOOL(
    allow_concurrent_memtable_write,
    xengine_db_options.allow_concurrent_memtable_write,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::allow_concurrent_memtable_write for XEngine", nullptr, nullptr,
    true);

static MYSQL_SYSVAR_BOOL(
    enable_write_thread_adaptive_yield,
    xengine_db_options.enable_write_thread_adaptive_yield,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::enable_write_thread_adaptive_yield for XEngine", nullptr,
    nullptr, false);

static MYSQL_SYSVAR_BOOL(
    query_trace_enable_count, xengine_db_options.query_trace_enable_count,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::query_trace_enable_count for XEngine", nullptr,
    xengine_set_query_trace_print_slow,
    xengine_db_options.query_trace_enable_count);

static MYSQL_SYSVAR_BOOL(
    query_trace_print_stats, xengine_db_options.query_trace_print_stats,
    PLUGIN_VAR_RQCMDARG,
    "DBOptions::query_trace_print_stats for XEngine", nullptr,
    xengine_set_query_trace_print_stats,
    xengine_db_options.query_trace_print_stats);

#endif

static MYSQL_SYSVAR_ULONG(mutex_backtrace_threshold_ns,
                          xengine_db_options.mutex_backtrace_threshold_ns,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::mutex_backtrace_threshold_ns for XEngine", nullptr,
                          xengine_set_mutex_backtrace_threshold_ns, 100000000UL,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_INT(max_open_files, xengine_db_options.max_open_files,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::max_open_files for XEngine", nullptr,
                        nullptr, xengine_db_options.max_open_files,
                        /* min */ -1, /* max */ INT_MAX, 0);
#endif

static MYSQL_SYSVAR_ULONG(max_total_wal_size,
                          xengine_db_options.max_total_wal_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::max_total_wal_size for XEngine", nullptr,
                          nullptr, DEFAULT_XENGINE_MAX_TOTAL_WAL_SIZE,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(
    use_fsync, xengine_db_options.use_fsync,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::use_fsync for XEngine", nullptr, nullptr,
    xengine_db_options.use_fsync);
#endif

static MYSQL_SYSVAR_STR(wal_dir, xengine_wal_dir,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::wal_dir for XEngine", nullptr, nullptr,
                        xengine_db_options.wal_dir.c_str());
#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_STR(
    persistent_cache_path, xengine_persistent_cache_path,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Path for BlockBasedTableOptions::persistent_cache for XEngine", nullptr,
    nullptr, "");

static MYSQL_SYSVAR_ULONG(
    persistent_cache_size, xengine_persistent_cache_size,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Size of cache for BlockBasedTableOptions::persistent_cache for XEngine",
    nullptr, nullptr, xengine_persistent_cache_size,
    /* min */ 0L, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(
    delete_obsolete_files_period_micros,
    xengine_db_options.delete_obsolete_files_period_micros,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::delete_obsolete_files_period_micros for XEngine", nullptr,
    nullptr, xengine_db_options.delete_obsolete_files_period_micros,
    /* min */ 0L, /* max */ LONG_MAX, 0);
#endif

static MYSQL_SYSVAR_INT(base_background_compactions,
                        xengine_db_options.base_background_compactions,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::base_background_compactions for XEngine",
                        nullptr, nullptr,
                        xengine_db_options.base_background_compactions,
                        /* min */ -1, /* max */ MAX_BACKGROUND_COMPACTIONS, 0);

static MYSQL_SYSVAR_INT(max_background_compactions,
                        xengine_db_options.max_background_compactions,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::max_background_compactions for XEngine",
                        nullptr, nullptr,
                        xengine_db_options.max_background_compactions,
                        /* min */ 1, /* max */ MAX_BACKGROUND_COMPACTIONS, 0);

static MYSQL_SYSVAR_INT(max_background_dumps,
                        xengine_db_options.max_background_dumps,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::max_background_dumps for XEngine",
                        nullptr, nullptr,
                        xengine_db_options.max_background_dumps,
                        /* min */ 1, /* max */ MAX_BACKGROUND_DUMPS, 0);

static MYSQL_SYSVAR_ULONG(dump_memtable_limit_size,
                          xengine_db_options.dump_memtable_limit_size,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::dump_memtable_limit_size for XEngine",
                          nullptr, xengine_set_dump_memtable_limit_size,
                          xengine_db_options.dump_memtable_limit_size,
                          /* min */ 0, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(auto_shrink_enabled,
                         xengine_db_options.auto_shrink_enabled,
                         PLUGIN_VAR_RQCMDARG,
                         "DBOptions::auto_shrink_enabled for XEngine",
                         nullptr, xengine_set_auto_shrink_enabled,
                         xengine_db_options.auto_shrink_enabled);

static MYSQL_SYSVAR_ULONG(max_free_extent_percent,
                          xengine_db_options.max_free_extent_percent,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::max_free_extent_percent for XEngine",
                          nullptr, xengine_set_max_free_extent_percent,
                          xengine_db_options.max_free_extent_percent,
                          /* min */ 0, /* max */ 100, 0);

static MYSQL_SYSVAR_ULONG(shrink_allocate_interval,
                          xengine_db_options.shrink_allocate_interval,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::shrink_allocate_interval for XEngine",
                          nullptr, xengine_set_shrink_allocate_interval,
                          xengine_db_options.shrink_allocate_interval,
                          /* min */ 0, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(max_shrink_extent_count,
                          xengine_db_options.max_shrink_extent_count,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::max_shrink_extent_count for XEngine",
                          nullptr, xengine_set_max_shrink_extent_count,
                          xengine_db_options.max_shrink_extent_count,
                          /* min */ 0, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(total_max_shrink_extent_count,
                          xengine_db_options.total_max_shrink_extent_count,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::total_max_shrink_extent_count for XEngine",
                          nullptr, xengine_set_total_max_shrink_extent_count,
                          xengine_db_options.total_max_shrink_extent_count,
                          /* min */ 0, /* max */ ULONG_MAX, 0);
static MYSQL_SYSVAR_ULONG(auto_shrink_schedule_interval,
                          xengine_db_options.auto_shrink_schedule_interval,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::auto_shrink_schedule_interval for XEngine",
                          nullptr, xengine_set_auto_shrink_schedule_interval,
                          xengine_db_options.auto_shrink_schedule_interval,
                          /* min */ 0, /* max */ ULONG_MAX, 0);
static MYSQL_SYSVAR_ULONG(estimate_cost_depth,
                          xengine_db_options.estimate_cost_depth,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::estimate_cost_depth for XEngine", nullptr,
                          xengine_set_estimate_cost_depth,
                          xengine_db_options.estimate_cost_depth,
                          /* min */ 0, /* max*/ ULONG_MAX, 0);
static MYSQL_SYSVAR_ULONG(idle_tasks_schedule_time,
                          xengine_db_options.idle_tasks_schedule_time,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::idle_tasks_schedule_time for XEngine",
                          nullptr, xengine_set_idle_tasks_schedule_time,
                          xengine_db_options.idle_tasks_schedule_time,
                          /* min */ 0, /* max */ ULONG_MAX, 0);
static MYSQL_SYSVAR_ULONG(rate_limiter_bytes_per_sec,
                          xengine_rate_limiter_bytes_per_sec,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::rate_limiter bytes_per_sec for XEngine",
                          nullptr, xengine_set_rate_limiter_bytes_per_sec,
                          /* default */ 0L,
                          /* min */ 0L, /* max */ MAX_RATE_LIMITER_BYTES_PER_SEC, 0);

static MYSQL_SYSVAR_INT(max_background_flushes,
                        xengine_db_options.max_background_flushes,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::max_background_flushes for XEngine",
                        nullptr, nullptr,
                        xengine_db_options.max_background_flushes,
                        /* min */ 1, /* max */ MAX_BACKGROUND_FLUSHES, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_UINT(max_subcompactions,
                         xengine_db_options.max_subcompactions,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::max_subcompactions for XEngine", nullptr,
                         nullptr, xengine_db_options.max_subcompactions,
                         /* min */ 1, /* max */ MAX_SUBCOMPACTIONS, 0);

static MYSQL_SYSVAR_ULONG(max_log_file_size,
                          xengine_db_options.max_log_file_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::max_log_file_size for XEngine", nullptr,
                          nullptr, xengine_db_options.max_log_file_size,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(log_file_time_to_roll,
                          xengine_db_options.log_file_time_to_roll,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::log_file_time_to_roll for XEngine",
                          nullptr, nullptr,
                          xengine_db_options.log_file_time_to_roll,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(keep_log_file_num,
                          xengine_db_options.keep_log_file_num,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::keep_log_file_num for XEngine", nullptr,
                          nullptr, xengine_db_options.keep_log_file_num,
                          /* min */ 0L, /* max */ LONG_MAX, 0);
#endif

static MYSQL_SYSVAR_ULONG(max_manifest_file_size,
                          xengine_db_options.max_manifest_file_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::max_manifest_file_size for XEngine",
                          nullptr, nullptr,
                          xengine_db_options.max_manifest_file_size,
                          /* min */ 0L, /* max */ ULONG_MAX, 0);

static MYSQL_SYSVAR_INT(table_cache_numshardbits,
                        xengine_db_options.table_cache_numshardbits,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "DBOptions::table_cache_numshardbits for XEngine",
                        nullptr, nullptr,
                        xengine_db_options.table_cache_numshardbits,
                        /* min */ 1, /* max */ 15, 0);

#if 0  // DEL-SYSVAR
static MYSQL_SYSVAR_ULONG(wal_ttl_seconds, xengine_db_options.WAL_ttl_seconds,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::WAL_ttl_seconds for XEngine", nullptr,
                          nullptr, xengine_db_options.WAL_ttl_seconds,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(wal_size_limit_mb,
                          xengine_db_options.WAL_size_limit_MB,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::WAL_size_limit_MB for XEngine", nullptr,
                          nullptr, xengine_db_options.WAL_size_limit_MB,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(manifest_preallocation_size,
                          xengine_db_options.manifest_preallocation_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::manifest_preallocation_size for XEngine",
                          nullptr, nullptr,
                          xengine_db_options.manifest_preallocation_size,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    use_direct_reads, xengine_db_options.use_direct_reads,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::use_direct_reads for XEngine", nullptr, nullptr,
    xengine_db_options.use_direct_reads);

static MYSQL_SYSVAR_BOOL(
    use_direct_io_for_flush_and_compaction,
    xengine_db_options.use_direct_io_for_flush_and_compaction,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::use_direct_io_for_flush_and_compaction for XEngine", nullptr, nullptr,
    xengine_db_options.use_direct_io_for_flush_and_compaction);

static MYSQL_SYSVAR_BOOL(
    allow_mmap_reads, xengine_db_options.allow_mmap_reads,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::allow_mmap_reads for XEngine", nullptr, nullptr,
    xengine_db_options.allow_mmap_reads);

static MYSQL_SYSVAR_BOOL(
    allow_mmap_writes, xengine_db_options.allow_mmap_writes,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::allow_mmap_writes for XEngine", nullptr, nullptr,
    xengine_db_options.allow_mmap_writes);

static MYSQL_SYSVAR_BOOL(
    is_fd_close_on_exec, xengine_db_options.is_fd_close_on_exec,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::is_fd_close_on_exec for XEngine", nullptr, nullptr,
    xengine_db_options.is_fd_close_on_exec);

static MYSQL_SYSVAR_BOOL(use_direct_write_for_wal,
                         xengine_db_options.use_direct_write_for_wal,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::use_direct_write_for_wal for XEngine",
                         nullptr, nullptr,
                         xengine_db_options.use_direct_write_for_wal);
#endif

static MYSQL_SYSVAR_ULONG(batch_group_max_group_size,
                         xengine_db_options.batch_group_max_group_size,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::batch_group_max_group_size for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_BATCH_GROUP_MAX_GROUP_SIZE, /* 8 */
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(batch_group_slot_array_size,
                         xengine_db_options.batch_group_slot_array_size,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::batch_group_slot_array_size for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_BATCH_GROUP_SLOT_ARRAY_SIZE, /* 8 */
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(batch_group_max_leader_wait_time_us,
                         xengine_db_options.batch_group_max_leader_wait_time_us,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::batch_group_max_leader_wait_time_us for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_BATCH_GROUP_MAX_LEADER_WAIT_TIME_US, /* 50 */
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(concurrent_writable_file_buffer_num,
                         xengine_db_options.concurrent_writable_file_buffer_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::concurrent_writable_file_buffer_num for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_NUMBER,
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(concurrent_writable_file_single_buffer_size,
                         xengine_db_options.concurrent_writable_file_single_buffer_size,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::concurrent_writable_file_single_buffer_size for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_SINGLE_BUFFER_SIZE,
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(concurrent_writable_file_buffer_switch_limit,
                         xengine_db_options.concurrent_writable_file_buffer_switch_limit,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::concurrent_writable_file_buffer_switch_limit for XEngine",
                         nullptr, nullptr,
                         DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_SWITCH_LIMIT,
                         /* min */ 1, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(compaction_type,
                         xengine_db_options.compaction_type,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::compaction_type for XEngine",
                         nullptr, nullptr,
                         xengine_db_options.compaction_type,
                         /* min */ 0, /* max */ 1, 0);

static MYSQL_SYSVAR_ULONG(compaction_mode,
                         xengine_db_options.compaction_mode,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::compaction_mode for XEngine",
                         nullptr, nullptr,
                         xengine_db_options.compaction_mode,
                         /* min */ 0, /* max */ 2, 0);

static MYSQL_SYSVAR_ULONG(cpu_compaction_thread_num,
                         xengine_db_options.cpu_compaction_thread_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::cpu_compaction_thread_num for XEngine",
                         nullptr, nullptr,
                         xengine_db_options.cpu_compaction_thread_num,
                         /* min */ 1, /* max */ 128, 1);

static MYSQL_SYSVAR_ULONG(fpga_compaction_thread_num,
                         xengine_db_options.fpga_compaction_thread_num,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "DBOptions::fpga_compaction_thread_num for XEngine",
                         nullptr, nullptr,
                         xengine_db_options.fpga_compaction_thread_num,
                         /* min */ 1, /* max */ 128, 1);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(skip_unique_key_check_in_boost_insert,
                         xengine_skip_unique_key_check_in_boost_insert,
                         PLUGIN_VAR_RQCMDARG,
                         "Disable unique key check in autocommit insert, whick boost insert perf",
                         nullptr,
                         nullptr,
                         FALSE);
#endif

static MYSQL_SYSVAR_UINT(stats_dump_period_sec,
                         xengine_db_options.stats_dump_period_sec,
                         PLUGIN_VAR_RQCMDARG,
                         "DBOptions::stats_dump_period_sec for XEngine",
                         nullptr, xengine_set_stats_dump_period_sec,
                         xengine_db_options.stats_dump_period_sec,
                         /* min */ 0, /* max */ INT_MAX, 0);
static MYSQL_SYSVAR_BOOL(dump_malloc_stats,
                         xengine_db_options.dump_malloc_stats,
                         PLUGIN_VAR_RQCMDARG,
                         "DBOptions::dump_malloc_stats for XENGINE",
                         nullptr, xengine_set_dump_malloc_stats,
                         xengine_db_options.dump_malloc_stats);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(
    advise_random_on_open, xengine_db_options.advise_random_on_open,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::advise_random_on_open for XEngine", nullptr, nullptr,
    xengine_db_options.advise_random_on_open);
#endif

static MYSQL_SYSVAR_ULONG(db_write_buffer_size,
                          xengine_db_options.db_write_buffer_size,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::db_write_buffer_size for XEngine",
                          nullptr, xengine_set_db_write_buffer_size,
                          DEFAULT_XENGINE_DB_WRITE_BUFFER_SIZE,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(db_total_write_buffer_size,
                          xengine_db_options.db_total_write_buffer_size,
                          PLUGIN_VAR_RQCMDARG,
                          "DBOptions::db_total_write_buffer_size for XEngine",
                          nullptr, xengine_set_db_total_write_buffer_size,
                          DEFAULT_XENGINE_DB_TOTAL_WRITE_BUFFER_SIZE,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(
    use_adaptive_mutex, xengine_db_options.use_adaptive_mutex,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::use_adaptive_mutex for XEngine", nullptr, nullptr,
    xengine_db_options.use_adaptive_mutex);

static MYSQL_SYSVAR_BOOL(
    avoid_flush_during_recovery, xengine_db_options.avoid_flush_during_recovery,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::avoid_flush_during_recovery for XEngine", nullptr, nullptr,
    xengine_db_options.avoid_flush_during_recovery);

static MYSQL_SYSVAR_ULONG(bytes_per_sync, xengine_db_options.bytes_per_sync,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::bytes_per_sync for XEngine", nullptr,
                          nullptr, xengine_db_options.bytes_per_sync,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_ULONG(wal_bytes_per_sync,
                          xengine_db_options.wal_bytes_per_sync,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::wal_bytes_per_sync for XEngine", nullptr,
                          nullptr, xengine_db_options.wal_bytes_per_sync,
                          /* min */ 0L, /* max */ LONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    enable_thread_tracking, xengine_db_options.enable_thread_tracking,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "DBOptions::enable_thread_tracking for XEngine", nullptr, nullptr,
    xengine_db_options.enable_thread_tracking);
#endif

static MYSQL_SYSVAR_LONGLONG(block_cache_size, xengine_block_cache_size,
                             PLUGIN_VAR_RQCMDARG,
                             "block_cache size for XEngine",
                             nullptr, xengine_set_block_cache_size,
                             /* default */ XDB_DEFAULT_BLOCK_CACHE_SIZE,
                             /* min */ XDB_MIN_BLOCK_CACHE_SIZE,
                             /* max */ LLONG_MAX,
                             /* Block size */ XDB_MIN_BLOCK_CACHE_SIZE);

static MYSQL_SYSVAR_LONGLONG(row_cache_size, xengine_row_cache_size,
                             PLUGIN_VAR_RQCMDARG,
                             "row_cache size for XEngine",
                             nullptr, xengine_set_row_cache_size,
                             /* default */ XDB_DEFAULT_ROW_CACHE_SIZE,
                             /* min */ XDB_MIN_ROW_CACHE_SIZE,
                             /* max */ LLONG_MAX,
                             /* Block size */ XDB_MIN_ROW_CACHE_SIZE);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(
    cache_index_and_filter_blocks,
    xengine_tbl_options.cache_index_and_filter_blocks,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::cache_index_and_filter_blocks for XEngine",
    nullptr, nullptr, true);

// When pin_l0_filter_and_index_blocks_in_cache is true, XEngine will  use the
// LRU cache, but will always keep the filter & index block's handle checked
// out (=won't call ShardedLRUCache::Release), plus the parsed out objects
// the LRU cache will never push flush them out, hence they're pinned.
//
// This fixes the mutex contention between :ShardedLRUCache::Lookup and
// ShardedLRUCache::Release which reduced the QPS ratio (QPS using secondary
// index / QPS using PK).
static MYSQL_SYSVAR_BOOL(
    pin_l0_filter_and_index_blocks_in_cache,
    xengine_tbl_options.pin_l0_filter_and_index_blocks_in_cache,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "pin_l0_filter_and_index_blocks_in_cache for XEngine", nullptr, nullptr,
    true);

static MYSQL_SYSVAR_ENUM(index_type, xengine_index_type,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "BlockBasedTableOptions::index_type for XEngine",
                         nullptr, nullptr,
                         (uint64_t)xengine_tbl_options.index_type,
                         &index_type_typelib);

static MYSQL_SYSVAR_BOOL(
    hash_index_allow_collision,
    xengine_tbl_options.hash_index_allow_collision,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::hash_index_allow_collision for XEngine", nullptr,
    nullptr, xengine_tbl_options.hash_index_allow_collision);

static MYSQL_SYSVAR_BOOL(
    no_block_cache, xengine_tbl_options.no_block_cache,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::no_block_cache for XEngine", nullptr, nullptr,
    xengine_tbl_options.no_block_cache);
#endif

static MYSQL_SYSVAR_ULONG(block_size, xengine_tbl_options.block_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "BlockBasedTableOptions::block_size for XEngine",
                          nullptr, nullptr, DEFAULT_XENGINE_BLOCK_SIZE,
                          /* min */ 1L, /* max */ LONG_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_INT(
    block_size_deviation, xengine_tbl_options.block_size_deviation,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::block_size_deviation for XEngine", nullptr,
    nullptr, xengine_tbl_options.block_size_deviation,
    /* min */ 0, /* max */ INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    block_restart_interval, xengine_tbl_options.block_restart_interval,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::block_restart_interval for XEngine", nullptr,
    nullptr, xengine_tbl_options.block_restart_interval,
    /* min */ 1, /* max */ INT_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    whole_key_filtering, xengine_tbl_options.whole_key_filtering,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::whole_key_filtering for XEngine", nullptr, nullptr,
    xengine_tbl_options.whole_key_filtering);

static MYSQL_SYSVAR_STR(
    filter_policy, xengine_filter_policy,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "BlockBasedTableOptions::filter_policy for XEngine",
    nullptr, nullptr, "");

// begin splitted xengine default column family options
static MYSQL_SYSVAR_STR(
    prefix_extractor, xengine_prefix_extractor,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::prefix_extractor for XEngine",
    nullptr, nullptr, "");
#endif

static MYSQL_SYSVAR_ULONG(
    write_buffer_size,
    xengine_default_cf_options.write_buffer_size,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::write_buffer_size for XEngine",
    nullptr, xengine_set_write_buffer_size,
    DEFAULT_XENGINE_WRITE_BUFFER_SIZE, 4096, ULONG_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_ULONG(
    arena_block_size,
    xengine_default_cf_options.arena_block_size,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::arena_block_size for XEngine",
    nullptr, xengine_set_arena_block_size,
    DEFAULT_XENGINE_ARENA_BLOCK_SIZE, 0L, /* max=1G */ 1073741824, 0);
#endif

static MYSQL_SYSVAR_INT(
    max_write_buffer_number,
    xengine_default_cf_options.max_write_buffer_number,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::max_write_buffer_number for XEngine",
    nullptr, nullptr,
    DEFAULT_XENGINE_MAX_WRITE_BUFFER_NUMBER, 2, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    max_write_buffer_number_to_maintain,
    xengine_default_cf_options.max_write_buffer_number_to_maintain,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::max_write_buffer_number_to_maintain for XEngine",
    nullptr, nullptr,
    DEFAULT_XENGINE_MAX_WRITE_BUFFER_NUMBER_TO_MAINTAIN, -1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    min_write_buffer_number_to_merge,
    xengine_default_cf_options.min_write_buffer_number_to_merge,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::min_write_buffer_number_to_merge for XEngine",
    nullptr, nullptr,
    DEFAULT_XENGINE_MIN_WRITE_BUFFER_NUMBER_TO_MERGE, 0, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level0_file_num_compaction_trigger,
    xengine_default_cf_options.level0_file_num_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_file_num_compaction_trigger for XEngine",
    nullptr, xengine_set_level0_file_num_compaction_trigger,
    DEFAULT_XENGINE_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, -1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level0_layer_num_compaction_trigger,
    xengine_default_cf_options.level0_layer_num_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_layer_num_compaction_trigger for XEngine",
    nullptr, xengine_set_level0_layer_num_compaction_trigger,
    DEFAULT_XENGINE_LEVEL0_LAYER_NUM_COMPACTION_TRIGGER, -1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level1_extents_major_compaction_trigger,
    xengine_default_cf_options.level1_extents_major_compaction_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level1_extents_major_compaction_trigger for XEngine",
    nullptr, xengine_set_level1_extents_major_compaction_trigger,
    DEFAULT_XENGINE_LEVEL1_EXTENTS_MAJOR_COMPACTION_TRIGGER, -1, INT_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    disable_auto_compactions,
    xengine_default_cf_options.disable_auto_compactions,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::disable_auto_compaction for XEngine",
    nullptr, xengine_set_disable_auto_compactions,
    DEFAULT_XENGINE_DISABLE_AUTO_COMPACTIONS);

static MYSQL_SYSVAR_LONG(
    level2_usage_percent,
    xengine_default_cf_options.level2_usage_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level2_usage_percent for XEngine",
    nullptr, xengine_set_level2_usage_percent,
    DEFAULT_XENGINE_LEVEL2_USAGE_PERCENT, 0L, 100, 0);

static MYSQL_SYSVAR_INT(
    flush_delete_percent,
    xengine_default_cf_options.flush_delete_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_percent for XEngine",
    nullptr, xengine_set_flush_delete_percent,
    DEFAULT_XENGINE_FLUSH_DELETE_PERCENT, 0L, 100, 0);

static MYSQL_SYSVAR_INT(
    compaction_delete_percent,
    xengine_default_cf_options.compaction_delete_percent,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::compaction_delete_percent for XEngine",
    nullptr, xengine_set_compaction_delete_percent,
    DEFAULT_XENGINE_COMPACTION_DELETE_PERCENT, 0L, 100, 0);

static MYSQL_SYSVAR_INT(
    flush_delete_percent_trigger,
    xengine_default_cf_options.flush_delete_percent_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_percent_trigger for XEngine",
    nullptr, xengine_set_flush_delete_percent_trigger,
    DEFAULT_XENGINE_FLUSH_DELETE_PERCENT_TRIGGER, 10, 1<<30, 0);

static MYSQL_SYSVAR_INT(
    flush_delete_record_trigger,
    xengine_default_cf_options.flush_delete_record_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::flush_delete_record_trigger for XEngine",
    nullptr, xengine_set_flush_delete_record_trigger,
    DEFAULT_XENGINE_FLUSH_DELETE_RECORD_TRIGGER, 1, 1<<30, 0);

static MYSQL_SYSVAR_INT(
    compaction_task_extents_limit,
    xengine_default_cf_options.compaction_task_extents_limit,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::compaction_task_extents_limit for XEngine",
    nullptr, xengine_set_compaction_task_extents_limit,
    1000, 1, 1<<30, 0);

static MYSQL_SYSVAR_ULONG(
    scan_add_blocks_limit,
    xengine_default_cf_options.scan_add_blocks_limit,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::scan_add_blocks_limit for XEngine",
    nullptr, xengine_set_scan_add_blocks_limit,
    xengine_default_cf_options.scan_add_blocks_limit, /* 100 */
    0L, ULONG_MAX, 0);

static MYSQL_SYSVAR_INT(
    bottommost_level,
    xengine_default_cf_options.bottommost_level,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::bottommost_level for XEngine",
    nullptr, xengine_set_bottommost_level,
    xengine_default_cf_options.bottommost_level, /* 2 */
    0L, 2, 0);

static MYSQL_SYSVAR_STR(
    compression_per_level, xengine_cf_compression_per_level,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::compression_per_level for XEngine",
    nullptr, nullptr, DEFAULT_XENGINE_COMPRESSION_ALG);

static MYSQL_SYSVAR_STR(
    compression_options, xengine_cf_compression_opts,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::compression_opts for XEngine",
    nullptr, nullptr, DEFAULT_XENGINE_COMPRESSION_OPTS);

static MYSQL_SYSVAR_BOOL(
    level_compaction_dynamic_level_bytes,
    xengine_default_cf_options.level_compaction_dynamic_level_bytes,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::level_compaction_dynamic_level_bytes for XEngine",
    nullptr, nullptr, DEFAULT_XENGINE_LEVEL_COMPACTION_DYNAMIC_LEVEL_BYTES);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(
    optimize_filters_for_hits,
    xengine_default_cf_options.optimize_filters_for_hits,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "CFOptions::optimize_filters_for_hits for XEngine",
    nullptr, nullptr, DEFAULT_XENGINE_OPTIMIZE_FILTERS_FOR_HITS);

// added for some MTR cases
static MYSQL_SYSVAR_ULONG(
    max_bytes_for_level_base,
    xengine_default_cf_options.max_bytes_for_level_base,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::max_bytes_for_level_base for XEngine",
    nullptr, xengine_set_max_bytes_for_level_base,
    xengine_default_cf_options.max_bytes_for_level_base, /* 256 MB */
    1048576, ULONG_MAX, 0);

static MYSQL_SYSVAR_DOUBLE(
    max_bytes_for_level_multiplier,
    xengine_default_cf_options.max_bytes_for_level_multiplier,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::max_bytes_for_level_multiplier for XEngine",
    nullptr, xengine_set_max_bytes_for_level_multiplier,
    xengine_default_cf_options.max_bytes_for_level_multiplier, /* 10 */
    1.0, 1000, 0);

static MYSQL_SYSVAR_ULONG(
    target_file_size_base,
    xengine_default_cf_options.target_file_size_base,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::target_file_size_base for XEngine",
    nullptr, xengine_set_target_file_size_base,
    xengine_default_cf_options.target_file_size_base, /* 64MB */
    1048576, ULONG_MAX, 0);

static MYSQL_SYSVAR_INT(
    target_file_size_multiplier,
    xengine_default_cf_options.target_file_size_multiplier,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::target_file_size_multiplier for XEngine",
    nullptr, xengine_set_target_file_size_multiplier,
    xengine_default_cf_options.target_file_size_multiplier, /* 10 */
    1, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level0_slowdown_writes_trigger,
    xengine_default_cf_options.level0_slowdown_writes_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_slowdown_writes_trigger for XEngine",
    nullptr, xengine_set_level0_slowdown_writes_trigger,
    // fix #21280399 avoid warning in SanitizedOptions
    DEFAULT_XENGINE_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, /* 64 */
    INT_MIN, INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    level0_stop_writes_trigger,
    xengine_default_cf_options.level0_stop_writes_trigger,
    PLUGIN_VAR_RQCMDARG,
    "CFOptions::level0_stop_writes_trigger for XEngine",
    nullptr, xengine_set_level0_stop_writes_trigger,
    // fix #21280399 avoid warning in SanitizedOptions
    DEFAULT_XENGINE_LEVEL0_FILE_NUM_COMPACTION_TRIGGER, /* 64 */
    0, INT_MAX, 0);
// end for added for some MTR cases
// end splitted xengine default column family options
static MYSQL_SYSVAR_STR(override_cf_options, xengine_override_cf_options,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                        "option overrides per cf for XEngine", nullptr, nullptr,
                        "");
#endif

static MYSQL_SYSVAR_STR(
    memtable, xengine_cf_memtable_options,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "options for CFOptions::memtable in XEngine",
    nullptr, nullptr, "");

static MYSQL_SYSVAR_UINT(flush_log_at_trx_commit,
                         xengine_flush_log_at_trx_commit,
                         PLUGIN_VAR_RQCMDARG,
                         "Sync on transaction commit. Similar to "
                         "innodb_flush_log_at_trx_commit. 1: sync on commit, "
                         "0: not sync until wal_bytes_per_sync, "
                         "2: sync per second",
                         nullptr, nullptr, 1, 0, 2, 0);

static MYSQL_THDVAR_BOOL(write_disable_wal, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "WriteOptions::disableWAL for XEngine", nullptr,
                         nullptr, xengine::common::WriteOptions().disableWAL);

#if 0 // DEL-SYSVAR
static MYSQL_THDVAR_BOOL(
    write_ignore_missing_column_families, PLUGIN_VAR_RQCMDARG,
    "WriteOptions::ignore_missing_column_families for XEngine", nullptr,
    nullptr, xengine::common::WriteOptions().ignore_missing_column_families);

static MYSQL_THDVAR_BOOL(skip_fill_cache, PLUGIN_VAR_RQCMDARG,
                         "Skip filling block cache on read requests", nullptr,
                         nullptr, FALSE);
#endif

static MYSQL_THDVAR_BOOL(
    unsafe_for_binlog, PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Allowing statement based binary logging which may break consistency",
    nullptr, nullptr, FALSE);

#if 0 // DEL-SYSVAR
static MYSQL_THDVAR_UINT(records_in_range, PLUGIN_VAR_RQCMDARG,
                         "Used to override the result of records_in_range(). "
                         "Set to a positive number to override",
                         nullptr, nullptr, 0,
                         /* min */ 0, /* max */ INT_MAX, 0);

static MYSQL_THDVAR_UINT(force_index_records_in_range, PLUGIN_VAR_RQCMDARG,
                         "Used to override the result of records_in_range() "
                         "when FORCE INDEX is used.",
                         nullptr, nullptr, 0,
                         /* min */ 0, /* max */ INT_MAX, 0);

static MYSQL_SYSVAR_UINT(
    debug_optimizer_n_rows, xengine_debug_optimizer_n_rows,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY | PLUGIN_VAR_NOSYSVAR,
    "Test only to override xengine estimates of table size in a memtable",
    nullptr, nullptr, 0, /* min */ 0, /* max */ INT_MAX, 0);

static MYSQL_SYSVAR_INT(
    max_backup_snapshot_time, xengine_max_backup_snapshot_time,
    PLUGIN_VAR_RQCMDARG,
    "Max snapshot hold time of hotbackup",
    nullptr, nullptr, (2 * 60 * 60), /* min */ 1, /* max */ INT_MAX, 0);
#endif

static MYSQL_SYSVAR_INT(
    shrink_table_space, xengine_shrink_table_space, PLUGIN_VAR_RQCMDARG,
    "Shrink xengine table space",
    xengine_shrink_table_space_func, xengine_shrink_table_space_sub_func,
    -1, /* min */ -1, /* max */ INT_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(force_compute_memtable_stats,
    xengine_force_compute_memtable_stats,
    PLUGIN_VAR_RQCMDARG,
    "Force to always compute memtable stats",
    nullptr, nullptr, TRUE);

static MYSQL_SYSVAR_BOOL(
    debug_optimizer_no_zero_cardinality,
    xengine_debug_optimizer_no_zero_cardinality, PLUGIN_VAR_RQCMDARG,
    "In case if cardinality is zero, overrides it with some value", nullptr,
    nullptr, TRUE);

/* This enum needs to be kept up to date with xengine::util::InfoLogLevel */
static const char *manual_compact_type_names[] = {"all", "minor",
    "major",  "major_self", NullS};

static TYPELIB manual_compact_type_typelib = {
    array_elements(manual_compact_type_names) - 1, "manual_compact_type_typelib",
    manual_compact_type_names, nullptr};

static MYSQL_SYSVAR_ENUM(
    manual_compact_type, xengine_manual_compact_type, PLUGIN_VAR_RQCMDARG,
    "Compact type for next manual compaction. "
    "Valid values include 'intra', 'minor', 'major' and 'major_self'.",
    nullptr, xengine_set_xengine_manual_compact_type,
    1 /* default minor */, &manual_compact_type_typelib);
#endif

static MYSQL_SYSVAR_LONGLONG(compact_cf, xengine_compact_cf_id,
                             PLUGIN_VAR_RQCMDARG, "Compact sub table",
                             xengine_compact_column_family,
                             xengine_compact_column_family_stub,
                             0, /*min*/0, /*max*/INT_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(reset_pending_shrink,
                             xengine_pending_shrink_subtable_id,
                             PLUGIN_VAR_RQCMDARG,
                             "reset subtable's pending shrink",
                             xengine_reset_subtable_pending_shrink,
                             xengine_reset_subtable_pending_shrink_stub, 0,
                             /*min*/ 0, /*max*/ INT_MAX, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_STR(create_checkpoint, xengine_checkpoint_name,
                        PLUGIN_VAR_RQCMDARG, "Checkpoint directory",
                        xengine_create_checkpoint,
                        xengine_create_checkpoint_stub, "");


static MYSQL_SYSVAR_BOOL(signal_drop_index_thread,
                         xengine_signal_drop_index_thread, PLUGIN_VAR_RQCMDARG,
                         "Wake up drop index thread", nullptr,
                         xengine_drop_index_wakeup_thread, FALSE);
#endif

static MYSQL_SYSVAR_STR(hotbackup, xengine_hotbackup_name,
                        PLUGIN_VAR_RQCMDARG, "Hot Backup",
                        xengine_hotbackup,
                        xengine_hotbackup_stub, "");

static MYSQL_SYSVAR_BOOL(pause_background_work, xengine_pause_background_work,
                         PLUGIN_VAR_RQCMDARG,
                         "Disable all xengine background operations", nullptr,
                         xengine_set_pause_background_work, FALSE);

static MYSQL_SYSVAR_BOOL(enable_2pc, xengine_enable_2pc,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Enable two phase commit for XEngine", nullptr,
                         nullptr, TRUE);

static MYSQL_SYSVAR_BOOL(strict_collation_check, xengine_strict_collation_check,
                         PLUGIN_VAR_RQCMDARG,
                         "Enforce case sensitive collation for XEngine indexes",
                         nullptr, nullptr, TRUE);

static MYSQL_SYSVAR_STR(strict_collation_exceptions,
                        xengine_strict_collation_exceptions,
                        PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_MEMALLOC,
                        "List of tables (using regex) that are excluded "
                        "from the case sensitive collation enforcement",
                        nullptr, xengine_set_collation_exception_list, "");

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_BOOL(collect_sst_properties, xengine_collect_sst_properties,
                         PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                         "Enables collecting SST file properties on each flush",
                         nullptr, nullptr, xengine_collect_sst_properties);
#endif

static MYSQL_SYSVAR_BOOL(
    force_flush_memtable_now, xengine_force_flush_memtable_now_var,
    PLUGIN_VAR_RQCMDARG,
    "Forces memstore flush which may block all write requests so be careful",
    xengine_force_flush_memtable_now, xengine_force_flush_memtable_now_stub,
    FALSE);

#if 0 // DEL-SYSVAR
static MYSQL_THDVAR_BOOL(
    flush_memtable_on_analyze, PLUGIN_VAR_RQCMDARG,
    "Forces memtable flush on ANALZYE table to get accurate cardinality",
    nullptr, nullptr, true);

static MYSQL_SYSVAR_UINT(
    seconds_between_stat_computes, xengine_seconds_between_stat_computes,
    PLUGIN_VAR_RQCMDARG,
    "Sets a number of seconds to wait between optimizer stats recomputation. "
    "Only changed indexes will be refreshed.",
    nullptr, nullptr, xengine_seconds_between_stat_computes,
    /* min */ 0L, /* max */ UINT_MAX, 0);

static MYSQL_SYSVAR_LONGLONG(compaction_sequential_deletes,
                             xengine_compaction_sequential_deletes,
                             PLUGIN_VAR_RQCMDARG,
                             "XEngine will trigger compaction for the file if "
                             "it has more than this number sequential deletes "
                             "per window",
                             nullptr, xengine_set_compaction_options,
                             DEFAULT_COMPACTION_SEQUENTIAL_DELETES,
                             /* min */ 0L,
                             /* max */ MAX_COMPACTION_SEQUENTIAL_DELETES, 0);

static MYSQL_SYSVAR_LONGLONG(
    compaction_sequential_deletes_window,
    xengine_compaction_sequential_deletes_window, PLUGIN_VAR_RQCMDARG,
    "Size of the window for counting xengine_compaction_sequential_deletes",
    nullptr, xengine_set_compaction_options,
    DEFAULT_COMPACTION_SEQUENTIAL_DELETES_WINDOW,
    /* min */ 0L, /* max */ MAX_COMPACTION_SEQUENTIAL_DELETES_WINDOW, 0);

static MYSQL_SYSVAR_LONGLONG(
    compaction_sequential_deletes_file_size,
    xengine_compaction_sequential_deletes_file_size, PLUGIN_VAR_RQCMDARG,
    "Minimum file size required for compaction_sequential_deletes", nullptr,
    xengine_set_compaction_options, 0L,
    /* min */ -1L, /* max */ LLONG_MAX, 0);

static MYSQL_SYSVAR_BOOL(
    compaction_sequential_deletes_count_sd,
    xengine_compaction_sequential_deletes_count_sd, PLUGIN_VAR_RQCMDARG,
    "Counting SingleDelete as xengine_compaction_sequential_deletes", nullptr,
    nullptr, xengine_compaction_sequential_deletes_count_sd);

static MYSQL_SYSVAR_BOOL(
    print_snapshot_conflict_queries, xengine_print_snapshot_conflict_queries,
    PLUGIN_VAR_RQCMDARG,
    "Logging queries that got snapshot conflict errors into *.err log", nullptr,
    nullptr, xengine_print_snapshot_conflict_queries);

static MYSQL_THDVAR_INT(checksums_pct, PLUGIN_VAR_RQCMDARG,
                        "How many percentages of rows to be checksummed",
                        nullptr, nullptr, XDB_MAX_CHECKSUMS_PCT,
                        /* min */ 0, /* max */ XDB_MAX_CHECKSUMS_PCT, 0);

static MYSQL_THDVAR_BOOL(store_row_debug_checksums, PLUGIN_VAR_RQCMDARG,
                         "Include checksums when writing index/table records",
                         nullptr, nullptr, false /* default value */);

static MYSQL_THDVAR_BOOL(verify_row_debug_checksums, PLUGIN_VAR_RQCMDARG,
                         "Verify checksums when reading index/table records",
                         nullptr, nullptr, false /* default value */);

static MYSQL_THDVAR_BOOL(master_skip_tx_api, PLUGIN_VAR_RQCMDARG,
                         "Skipping holding any lock on row access. "
                         "Not effective on slave.",
                         nullptr, nullptr, false);

static MYSQL_SYSVAR_UINT(
    validate_tables, xengine_validate_tables,
    PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
    "Verify all .frm files match all XEngine tables (0 means no verification, "
    "1 means verify and fail on error, and 2 means verify but continue",
    nullptr, nullptr, 1 /* default value */, 0 /* min value */,
    2 /* max value */, 0);
#endif

static MYSQL_SYSVAR_STR(datadir, xengine_datadir,
                        PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
                        "XEngine data directory", nullptr, nullptr,
                        "./.xengine");

static const char *query_trace_sum_ops[] = {"OFF", "ON", "RESET",  NullS};
static TYPELIB query_trace_sum_ops_typelib = {array_elements(query_trace_sum_ops) - 1,
                                     "query_trace_sum_ops_typelib", query_trace_sum_ops,
                                     nullptr};
static MYSQL_SYSVAR_ENUM(query_trace_sum, xengine_query_trace_sum,
                         PLUGIN_VAR_RQCMDARG, "if record query detail in IS table for XEngine",
                         nullptr, xengine_set_query_trace_sum,
                         0 /* OFF */ , &query_trace_sum_ops_typelib);
static MYSQL_SYSVAR_BOOL(
    query_trace_print_slow, xengine_query_trace_print_slow,
    PLUGIN_VAR_RQCMDARG,
    "if print slow query detail in error log for XEngine", nullptr,
    xengine_set_query_trace_print_slow, 1 /* default ON */);


static MYSQL_SYSVAR_UINT(disable_online_ddl, xengine_disable_online_ddl,
                         PLUGIN_VAR_RQCMDARG,
                         "disable online ddl feature if necessary."
                         "0: not disable online ddl,"
                         "1: disable online-rebuild ddl, like modify pk,"
                         "2: disable all type online-ddl include rebuild/norebuild",
                         nullptr, nullptr, /*default*/0, /*min*/0, /*max*/2, 0);

static MYSQL_SYSVAR_BOOL(disable_instant_ddl, xengine_disable_instant_ddl,
                         PLUGIN_VAR_RQCMDARG,
                         "disable instant ddl feature if necessary.",
                         nullptr, nullptr, FALSE);

static MYSQL_SYSVAR_ULONG(sort_buffer_size, xengine_sort_buffer_size,
                          PLUGIN_VAR_RQCMDARG,
                          "Memory buffer size for index creation", NULL, NULL,
                          DEFAULT_XENGINE_SORT_BUF_SIZE/* default 4M */,
                          XENGINE_MIN_SORT_BUF_SIZE/* min 64k */, XENGINE_MAX_SORT_BUF_SIZE/* max 16G */,
                          0);

static MYSQL_SYSVAR_BOOL(purge_invalid_subtable_bg, opt_purge_invalid_subtable_bg,
                         PLUGIN_VAR_RQCMDARG,
                         "Turn on to enable purging invalid subtable in background",
                         nullptr, nullptr, true);

static MYSQL_SYSVAR_ULONG(table_cache_size, xengine_db_options.table_cache_size,
                          PLUGIN_VAR_RQCMDARG | PLUGIN_VAR_READONLY,
                          "DBOptions::table_cache_size for XEngine", nullptr,
                          nullptr, xengine_db_options.table_cache_size,
                          1 * 1024 * 1024 /*min*/, ULONG_MAX /*max*/, 0);

#if 0 // DEL-SYSVAR
static MYSQL_SYSVAR_UINT(
    table_stats_sampling_pct, xengine_table_stats_sampling_pct,
    PLUGIN_VAR_RQCMDARG,
    "Percentage of entries to sample when collecting statistics about table "
    "properties. Specify either 0 to sample everything or percentage "
    "[" STRINGIFY_ARG(XDB_TBL_STATS_SAMPLE_PCT_MIN) ".."
        STRINGIFY_ARG(XDB_TBL_STATS_SAMPLE_PCT_MAX) "]. "
    "By default " STRINGIFY_ARG(XDB_DEFAULT_TBL_STATS_SAMPLE_PCT) "% "
    "of entries are sampled.",
    nullptr, xengine_set_table_stats_sampling_pct, /* default */
    XDB_DEFAULT_TBL_STATS_SAMPLE_PCT, /* everything */ 0,
    /* max */ XDB_TBL_STATS_SAMPLE_PCT_MAX, 0);
#endif

static MYSQL_THDVAR_ULONG(parallel_read_threads, PLUGIN_VAR_RQCMDARG,
                          "Number of threads to do parallel read.", NULL, NULL,
                          4,   /* Default. */
                          1,   /* Minimum. */
                          256, /* Maxumum. */
                          0);

static MYSQL_SYSVAR_BOOL(disable_parallel_ddl, xengine_disable_parallel_ddl,
                         PLUGIN_VAR_RQCMDARG,
                         "disable parall ddl feature if necessary.",
                         nullptr, nullptr, false);

static const int XENGINE_ASSUMED_KEY_VALUE_DISK_SIZE = 100;

static struct SYS_VAR *xengine_system_variables[] = {
    MYSQL_SYSVAR(lock_wait_timeout), MYSQL_SYSVAR(deadlock_detect),
    MYSQL_SYSVAR(max_row_locks), MYSQL_SYSVAR(lock_scanned_rows),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(bulk_load),
    MYSQL_SYSVAR(skip_unique_check_tables),
    MYSQL_SYSVAR(trace_sst_api),
    MYSQL_SYSVAR(commit_in_the_middle),
    MYSQL_SYSVAR(blind_delete_primary_key),
    MYSQL_SYSVAR(read_free_rpl_tables),
    MYSQL_SYSVAR(rpl_skip_tx_api),
    MYSQL_SYSVAR(bulk_load_size),
    MYSQL_SYSVAR(merge_buf_size),
    MYSQL_SYSVAR(enable_bulk_load_api),
    MYSQL_SYSVAR(tmpdir),
    MYSQL_SYSVAR(merge_combine_read_size),
    MYSQL_SYSVAR(skip_bloom_filter_on_read),

    MYSQL_SYSVAR(create_if_missing),
    MYSQL_SYSVAR(create_missing_column_families),
    MYSQL_SYSVAR(error_if_exists),
    MYSQL_SYSVAR(paranoid_checks),
    MYSQL_SYSVAR(delayed_write_rate),
    MYSQL_SYSVAR(query_trace_print_slow),
    MYSQL_SYSVAR(query_trace_print_stats),
#endif
    MYSQL_SYSVAR(mutex_backtrace_threshold_ns),
    // MYSQL_SYSVAR(max_open_files),
    MYSQL_SYSVAR(max_total_wal_size),
    // MYSQL_SYSVAR(use_fsync),
    MYSQL_SYSVAR(wal_dir),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(persistent_cache_path),
    MYSQL_SYSVAR(persistent_cache_size),
    MYSQL_SYSVAR(delete_obsolete_files_period_micros),
#endif
    MYSQL_SYSVAR(base_background_compactions),
    MYSQL_SYSVAR(max_background_compactions),
    MYSQL_SYSVAR(max_background_flushes), MYSQL_SYSVAR(max_background_dumps),
    MYSQL_SYSVAR(dump_memtable_limit_size), MYSQL_SYSVAR(auto_shrink_enabled),
    MYSQL_SYSVAR(max_free_extent_percent),
    MYSQL_SYSVAR(shrink_allocate_interval),
    MYSQL_SYSVAR(max_shrink_extent_count),
    MYSQL_SYSVAR(total_max_shrink_extent_count),
    MYSQL_SYSVAR(auto_shrink_schedule_interval),
    MYSQL_SYSVAR(estimate_cost_depth),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(max_log_file_size),
    MYSQL_SYSVAR(max_subcompactions),
    MYSQL_SYSVAR(log_file_time_to_roll),
    MYSQL_SYSVAR(keep_log_file_num),
#endif
    MYSQL_SYSVAR(max_manifest_file_size),
    MYSQL_SYSVAR(table_cache_numshardbits),
#if 0  // DEL-SYSVAR
    MYSQL_SYSVAR(wal_ttl_seconds),
    MYSQL_SYSVAR(wal_size_limit_mb),
    MYSQL_SYSVAR(manifest_preallocation_size),
    MYSQL_SYSVAR(use_direct_reads),
    MYSQL_SYSVAR(use_direct_io_for_flush_and_compaction),
    MYSQL_SYSVAR(allow_mmap_reads),
    MYSQL_SYSVAR(allow_mmap_writes),
    MYSQL_SYSVAR(is_fd_close_on_exec),
#endif
    MYSQL_SYSVAR(stats_dump_period_sec),
    // MYSQL_SYSVAR(advise_random_on_open),
    MYSQL_SYSVAR(db_write_buffer_size),
    MYSQL_SYSVAR(db_total_write_buffer_size),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(use_adaptive_mutex),
    MYSQL_SYSVAR(avoid_flush_during_recovery),
    MYSQL_SYSVAR(bytes_per_sync),
    MYSQL_SYSVAR(wal_bytes_per_sync),
    MYSQL_SYSVAR(enable_thread_tracking),
    MYSQL_SYSVAR(perf_context_level),
#endif
    MYSQL_SYSVAR(wal_recovery_mode), MYSQL_SYSVAR(parallel_wal_recovery),
    MYSQL_SYSVAR(parallel_recovery_thread_num),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(access_hint_on_compaction_start),
    MYSQL_SYSVAR(new_table_reader_for_compaction_inputs),
    MYSQL_SYSVAR(compaction_readahead_size),
    MYSQL_SYSVAR(allow_concurrent_memtable_write),
    MYSQL_SYSVAR(enable_write_thread_adaptive_yield),
#endif

    MYSQL_SYSVAR(block_cache_size), MYSQL_SYSVAR(row_cache_size),
    MYSQL_SYSVAR(block_size),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(cache_index_and_filter_blocks),
    MYSQL_SYSVAR(pin_l0_filter_and_index_blocks_in_cache),
    MYSQL_SYSVAR(index_type),
    MYSQL_SYSVAR(hash_index_allow_collision),
    MYSQL_SYSVAR(no_block_cache),
    MYSQL_SYSVAR(block_size_deviation),
    MYSQL_SYSVAR(block_restart_interval),
    MYSQL_SYSVAR(whole_key_filtering),
    MYSQL_SYSVAR(filter_policy),
    // xengine default column family options
    MYSQL_SYSVAR(prefix_extractor),
#endif
    MYSQL_SYSVAR(write_buffer_size),
    // MYSQL_SYSVAR(arena_block_size),
    MYSQL_SYSVAR(max_write_buffer_number),
    MYSQL_SYSVAR(max_write_buffer_number_to_maintain),
    MYSQL_SYSVAR(min_write_buffer_number_to_merge),
    MYSQL_SYSVAR(compression_per_level), MYSQL_SYSVAR(compression_options),
#if 0 // DEL-SYSVAR
    // added for some MTR cases
    MYSQL_SYSVAR(max_bytes_for_level_base),
    MYSQL_SYSVAR(max_bytes_for_level_multiplier),
    MYSQL_SYSVAR(target_file_size_base),
    MYSQL_SYSVAR(target_file_size_multiplier),
#endif
    MYSQL_SYSVAR(level_compaction_dynamic_level_bytes),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(level0_slowdown_writes_trigger),
    MYSQL_SYSVAR(level0_stop_writes_trigger),
    // end for added for some MTR cases
    MYSQL_SYSVAR(optimize_filters_for_hits),
#endif
    MYSQL_SYSVAR(memtable),
    MYSQL_SYSVAR(level0_file_num_compaction_trigger),
    MYSQL_SYSVAR(level0_layer_num_compaction_trigger),
    MYSQL_SYSVAR(level1_extents_major_compaction_trigger),
    MYSQL_SYSVAR(disable_auto_compactions), MYSQL_SYSVAR(flush_delete_percent),
    MYSQL_SYSVAR(compaction_delete_percent),
    MYSQL_SYSVAR(flush_delete_percent_trigger),
    MYSQL_SYSVAR(flush_delete_record_trigger),
    MYSQL_SYSVAR(level2_usage_percent), MYSQL_SYSVAR(scan_add_blocks_limit),
    MYSQL_SYSVAR(bottommost_level), MYSQL_SYSVAR(rate_limiter_bytes_per_sec),
    MYSQL_SYSVAR(compaction_task_extents_limit),
    MYSQL_SYSVAR(idle_tasks_schedule_time),

    // MYSQL_SYSVAR(override_cf_options),

    MYSQL_SYSVAR(flush_log_at_trx_commit), MYSQL_SYSVAR(write_disable_wal),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(write_ignore_missing_column_families),
    MYSQL_SYSVAR(skip_fill_cache),
#endif
    MYSQL_SYSVAR(unsafe_for_binlog),

#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(records_in_range),
    MYSQL_SYSVAR(force_index_records_in_range),
    MYSQL_SYSVAR(debug_optimizer_n_rows),
    MYSQL_SYSVAR(max_backup_snapshot_time),
#endif
    MYSQL_SYSVAR(shrink_table_space),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(force_compute_memtable_stats),
    MYSQL_SYSVAR(debug_optimizer_no_zero_cardinality),
    MYSQL_SYSVAR(manual_compact_type),
#endif

    MYSQL_SYSVAR(compact_cf),
    MYSQL_SYSVAR(reset_pending_shrink),
    // MYSQL_SYSVAR(signal_drop_index_thread),
    MYSQL_SYSVAR(pause_background_work), MYSQL_SYSVAR(enable_2pc),
    MYSQL_SYSVAR(strict_collation_check),
    MYSQL_SYSVAR(strict_collation_exceptions),
    // MYSQL_SYSVAR(collect_sst_properties),
    MYSQL_SYSVAR(force_flush_memtable_now),
#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(flush_memtable_on_analyze),
    MYSQL_SYSVAR(seconds_between_stat_computes),

    MYSQL_SYSVAR(compaction_sequential_deletes),
    MYSQL_SYSVAR(compaction_sequential_deletes_window),
    MYSQL_SYSVAR(compaction_sequential_deletes_file_size),
    MYSQL_SYSVAR(compaction_sequential_deletes_count_sd),
    MYSQL_SYSVAR(print_snapshot_conflict_queries),
#endif

    MYSQL_SYSVAR(datadir), MYSQL_SYSVAR(hotbackup),

#if 0 // DEL-SYSVAR
    MYSQL_SYSVAR(create_checkpoint),
    MYSQL_SYSVAR(checksums_pct),
    MYSQL_SYSVAR(store_row_debug_checksums),
    MYSQL_SYSVAR(verify_row_debug_checksums),
    MYSQL_SYSVAR(master_skip_tx_api),

    MYSQL_SYSVAR(validate_tables),
    MYSQL_SYSVAR(table_stats_sampling_pct),
#endif
    MYSQL_SYSVAR(batch_group_slot_array_size),
    MYSQL_SYSVAR(batch_group_max_group_size),
    MYSQL_SYSVAR(batch_group_max_leader_wait_time_us),
    MYSQL_SYSVAR(concurrent_writable_file_buffer_num),
    MYSQL_SYSVAR(concurrent_writable_file_single_buffer_size),
    MYSQL_SYSVAR(concurrent_writable_file_buffer_switch_limit),
    MYSQL_SYSVAR(compaction_type),
    MYSQL_SYSVAR(compaction_mode),
    MYSQL_SYSVAR(cpu_compaction_thread_num),
    MYSQL_SYSVAR(fpga_compaction_thread_num),
    //MYSQL_SYSVAR(use_direct_write_for_wal),
    //MYSQL_SYSVAR(skip_unique_key_check_in_boost_insert),
    MYSQL_SYSVAR(dump_malloc_stats),
    MYSQL_SYSVAR(query_trace_sum),
    MYSQL_SYSVAR(query_trace_print_slow),
    //MYSQL_SYSVAR(disable_auto_index_per_cf),
    //MYSQL_SYSVAR(enable_print_ddl_log),
    MYSQL_SYSVAR(disable_online_ddl),
    MYSQL_SYSVAR(disable_instant_ddl),
    MYSQL_SYSVAR(sort_buffer_size),
    MYSQL_SYSVAR(parallel_read_threads),
    MYSQL_SYSVAR(purge_invalid_subtable_bg),
    MYSQL_SYSVAR(disable_parallel_ddl),
    MYSQL_SYSVAR(table_cache_size),
    nullptr};

static xengine::common::WriteOptions
xdb_get_xengine_write_options(my_core::THD *const thd) {
  xengine::common::WriteOptions opt;

  opt.sync = xengine_flush_log_at_trx_commit == 1;
  opt.disableWAL = THDVAR(thd, write_disable_wal);
  opt.ignore_missing_column_families = false;
      // THDVAR(thd, write_ignore_missing_column_families);

  return opt;
}

///////////////////////////////////////////////////////////////////////////////////////////
#if 0 // unused function
/**
  @brief
  Function we use in the creation of our hash to get key.
*/
uchar *
Xdb_open_tables_map::get_hash_key(Xdb_table_handler *const table_handler,
                                  size_t *const length,
                                  bool not_used MY_ATTRIBUTE((__unused__))) {
  *length = table_handler->m_table_name_length;
  return reinterpret_cast<uchar *>(table_handler->m_table_name);
}
#endif

/*
  The following is needed as an argument for mysql_stage_register,
  irrespectively of whether we're compiling with P_S or not.
*/
PSI_stage_info stage_waiting_on_row_lock = {0, "Waiting for row lock", 0};

#ifdef HAVE_PSI_INTERFACE
static PSI_thread_key xdb_background_psi_thread_key;
static PSI_thread_key xdb_drop_idx_psi_thread_key;

static PSI_stage_info *all_xengine_stages[] = {&stage_waiting_on_row_lock};

static my_core::PSI_mutex_key xdb_psi_open_tbls_mutex_key,
    xdb_signal_bg_psi_mutex_key, xdb_signal_drop_idx_psi_mutex_key,
    xdb_collation_data_mutex_key, xdb_mem_cmp_space_mutex_key,
    key_mutex_tx_list, xdb_sysvars_psi_mutex_key;

static PSI_mutex_info all_xengine_mutexes[] = {
    {&xdb_psi_open_tbls_mutex_key, "open tables", PSI_FLAG_SINGLETON},
    {&xdb_signal_bg_psi_mutex_key, "stop background", PSI_FLAG_SINGLETON},
    {&xdb_signal_drop_idx_psi_mutex_key, "signal drop index", PSI_FLAG_SINGLETON},
    {&xdb_collation_data_mutex_key, "collation data init", PSI_FLAG_SINGLETON},
    {&xdb_mem_cmp_space_mutex_key, "collation space char data init",
     PSI_FLAG_SINGLETON},
    {&key_mutex_tx_list, "tx_list", PSI_FLAG_SINGLETON},
    {&xdb_sysvars_psi_mutex_key, "setting sysvar", PSI_FLAG_SINGLETON},
};

static PSI_rwlock_key key_rwlock_collation_exception_list;
static PSI_rwlock_key key_rwlock_read_free_rpl_tables;
static PSI_rwlock_key key_rwlock_skip_unique_check_tables;

static PSI_rwlock_info all_xengine_rwlocks[] = {
    {&key_rwlock_collation_exception_list, "collation_exception_list",
     PSI_FLAG_SINGLETON},
    {&key_rwlock_read_free_rpl_tables, "read_free_rpl_tables", PSI_FLAG_SINGLETON},
    {&key_rwlock_skip_unique_check_tables, "skip_unique_check_tables",
     PSI_FLAG_SINGLETON},
};

PSI_cond_key xdb_signal_bg_psi_cond_key, xdb_signal_drop_idx_psi_cond_key;

static PSI_cond_info all_xengine_conds[] = {
    {&xdb_signal_bg_psi_cond_key, "cond signal background", PSI_FLAG_SINGLETON},
    {&xdb_signal_drop_idx_psi_cond_key, "cond signal drop index",
     PSI_FLAG_SINGLETON},
};

static PSI_thread_info all_xengine_threads[] = {
    {&xdb_background_psi_thread_key, "background", PSI_FLAG_SINGLETON},
    {&xdb_drop_idx_psi_thread_key, "drop index", PSI_FLAG_SINGLETON},
};

static void init_xengine_psi_keys() {
  const char *const category = "xengine";
  int count;

  count = array_elements(all_xengine_mutexes);
  mysql_mutex_register(category, all_xengine_mutexes, count);

  count = array_elements(all_xengine_rwlocks);
  mysql_rwlock_register(category, all_xengine_rwlocks, count);

  count = array_elements(all_xengine_conds);
  // TODO Disabling PFS for conditions due to the bug
  // https://github.com/MySQLOnXENGINE/mysql-5.6/issues/92
  // PSI_server->register_cond(category, all_xengine_conds, count);

  count = array_elements(all_xengine_stages);
  mysql_stage_register(category, all_xengine_stages, count);

  count = array_elements(all_xengine_threads);
  mysql_thread_register(category, all_xengine_threads, count);
}
#endif

#if 0 // DEL-SYSVAR
/*
  Drop index thread's control
*/
void xengine_drop_index_wakeup_thread(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr MY_ATTRIBUTE((__unused__)), const void *const save) {
  if (*static_cast<const bool *>(save)) {
    xdb_drop_idx_thread.signal();
  }
}
#endif

static inline uint32_t xengine_perf_context_level(THD *const thd) {
  DBUG_ASSERT(thd != nullptr);

  const int session_perf_context_level = xengine::monitor::PerfLevel::kUninitialized; //THDVAR(thd, perf_context_level);
  if (session_perf_context_level > xengine::monitor::PerfLevel::kUninitialized) {
    return session_perf_context_level;
  }

  /*
    Fallback to global thdvar, if session specific one was not set to a valid
    value.
  */

  const int global_perf_context_level = xengine::monitor::PerfLevel::kUninitialized; //THDVAR(nullptr, perf_context_level);
  if (global_perf_context_level > xengine::monitor::PerfLevel::kUninitialized) {
    return global_perf_context_level;
  }

  return xengine::monitor::PerfLevel::kDisable;
}

static char xengine_backup_status[][16] = {"checkpoint",
                                           "acquire",
                                           "incremental",
                                           "release",
                                           ""};
static int xengine_hotbackup(THD *const thd MY_ATTRIBUTE((__unused__)),
                             struct SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
                             void *const save MY_ATTRIBUTE((__unused__)),
                             struct st_mysql_value *const value)
{

  char buf[STRING_BUFFER_USUAL_SIZE];
  int len = sizeof(buf);
  int ret = 0;
  const char *cmd = value->val_str(value, buf, &len);
  Xdb_transaction *const tx = get_or_create_tx(thd);

  if (!my_core::thd_test_options(thd, OPTION_BEGIN)) {
    ret = xengine::common::Status::kNotSupported;
    my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should begin a trx first", MYF(0));
  } else if (ISNULL(backup_instance)) {
    XHANDLER_LOG(WARN, "backup_instance is nullptr", K(tx->get_backup_running()));
    if (tx->get_backup_running()) {
      ret = xengine::common::Status::kErrorUnexpected;
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: fatal error", MYF(0));
    } else if (FAILED(BackupSnapshot::create(backup_instance))) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: unexpected error", MYF(0));
    }
  }

  if (FAILED(ret)) {
    // do nothing
  } else if (0 == strcasecmp(cmd, xengine_backup_status[0])) {
    if (FAILED(backup_instance->init(xdb))) {
      if (ret == xengine::common::Status::kInitTwice) {
        my_printf_error(ER_UNKNOWN_ERROR, "XEngine: there is another backup job running\n", MYF(0));
      } else {
        my_printf_error(ER_UNKNOWN_ERROR, "XEngine: failed to init backup snapshot", MYF(0));
      }
    } else {
      xengine_register_tx(xengine_hton, thd, tx);
      tx->set_backup_running(true);
      if (FAILED(backup_instance->do_checkpoint(xdb))) {
        my_printf_error(ER_UNKNOWN_ERROR, "XEngine: failed to do checkpoint for backup", MYF(0));
      } else {
        xengine_hotbackup_name = xengine_backup_status[0];
      }
    }
  } else if (0 == strcasecmp(cmd, xengine_backup_status[1])) {
    if (0 != strcasecmp(xengine_hotbackup_name, xengine_backup_status[0])) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should execute command: %s before this command\n", MYF(0), xengine_backup_status[0]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->acquire_snapshots(xdb))) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: failed to acquire snapshots for backup", MYF(0));
    } else {
      xengine_hotbackup_name = xengine_backup_status[1];
    }
  } else if (0 == strcasecmp(cmd, xengine_backup_status[2])) {
    if (0 != strcasecmp(xengine_hotbackup_name, xengine_backup_status[1])) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should execute command: %s before this command\n", MYF(0), xengine_backup_status[1]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->record_incremental_extent_ids(xdb))) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: failed to record incremental extent ids for backup", MYF(0));
    } else {
      xengine_hotbackup_name = xengine_backup_status[2];
    }
  } else if (0 == strcasecmp(cmd, xengine_backup_status[3])) {
    if (0 != strcasecmp(xengine_hotbackup_name, xengine_backup_status[2])) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should execute command: %s before this command\n", MYF(0), xengine_backup_status[2]);
    } else if (!tx->get_backup_running()) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: should do checkpoint first\n", MYF(0));
    } else if (FAILED(backup_instance->release_snapshots(xdb))) {
      my_printf_error(ER_UNKNOWN_ERROR, "XEngine: failed to release snapshots for backup", MYF(0));
    } else {
      xengine_hotbackup_name = xengine_backup_status[4];
    }
  } else {
    my_printf_error(ER_UNKNOWN_ERROR, "XEngine: invalid command: %s\n", MYF(0), cmd);
    ret = xengine::common::Status::kInvalidArgument;
  }

  if (FAILED(ret)) {
    if (ret != xengine::common::Status::kInitTwice) {
      tx->rollback();
    }
    ret = HA_ERR_INTERNAL_ERROR;
  }
  return ret;
}

void Xdb_transaction::init_mutex() {
  mysql_mutex_init(key_mutex_tx_list, &s_tx_list_mutex, MY_MUTEX_INIT_FAST);
}

void Xdb_transaction::term_mutex()
{
  DBUG_ASSERT(s_tx_list.size() == 0);
  mysql_mutex_destroy(&s_tx_list_mutex);
}

void Xdb_transaction::walk_tx_list(Xdb_tx_list_walker *walker) {
    DBUG_ASSERT(walker != nullptr);

    XDB_MUTEX_LOCK_CHECK(s_tx_list_mutex);

    for (auto it : s_tx_list)
      walker->process_tran(it);

    XDB_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
}

  int Xdb_transaction::set_status_error(THD *const thd, const xengine::common::Status &s,
                       const Xdb_key_def &kd, Xdb_tbl_def *const tbl_def) {
    DBUG_ASSERT(!s.ok());
    DBUG_ASSERT(tbl_def != nullptr);

    if (s.IsTimedOut()) {
      __XHANDLER_LOG(WARN, "tx failed with timeout on key(%u)",
                     kd.get_index_number());
      /*
        SQL layer has weird expectations. If we return an error when
        doing a read in DELETE IGNORE, it will ignore the error ("because it's
        an IGNORE command!) but then will fail an assert, because "error code
        was returned, but no error happened".  Do what InnoDB's
        convert_error_code_to_mysql() does: force a statement
        rollback before returning HA_ERR_LOCK_WAIT_TIMEOUT:
        */
      my_core::thd_mark_transaction_to_rollback(thd, false /*just statement*/);
      m_detailed_error.copy(timeout_message(
          "index", tbl_def->full_tablename().c_str(), kd.get_name().c_str()));

      return HA_ERR_LOCK_WAIT_TIMEOUT;
    }

    if (s.IsDeadlock()) {
      __XHANDLER_LOG(WARN, "tx failed with deadlock on key(%u)",
                     kd.get_index_number());
      my_core::thd_mark_transaction_to_rollback(thd,
                                                true /* whole transaction */);
      return HA_ERR_LOCK_DEADLOCK;
    } else if (s.IsBusy()) {
      __XHANDLER_LOG(WARN, "tx failed with snapshot conflict on key(%u)",
                     kd.get_index_number());
      xengine_snapshot_conflict_errors++;
      #if 0 // DEL-SYSVAR
      if (xengine_print_snapshot_conflict_queries) {
        char user_host_buff[MAX_USER_HOST_SIZE + 1];
        make_user_name(thd->security_context(), user_host_buff);
        // NO_LINT_DEBUG
        sql_print_warning("Got snapshot conflict errors: User: %s "
                          "Query: %s",
                          user_host_buff, thd->query());
      }
      #endif
      return HA_ERR_LOCK_DEADLOCK;
    }

    if (s.IsLockLimit()) {
      __XHANDLER_LOG(WARN, "tx failed with too many locks on key(%u)",
                     kd.get_index_number());
      return HA_ERR_XENGINE_TOO_MANY_LOCKS;
    }

    if (s.IsIOError() || s.IsCorruption()) {
      xdb_handle_io_error(s, XDB_IO_ERROR_GENERAL);
    }
    my_error(ER_INTERNAL_ERROR, MYF(0), s.ToString().c_str());
    return HA_ERR_INTERNAL_ERROR;
  }


  bool Xdb_transaction::commit_or_rollback() {
    bool res;
    if (m_is_tx_failed) {
      rollback();
      res = false;
    } else
      res = commit();
    return res;
  }

  bool Xdb_transaction::commit() {
    if (get_write_count() == 0) {
      rollback();
      return false;
    } else if (m_rollback_only) {
      /*
        Transactions marked as rollback_only are expected to be rolled back at
        prepare(). But there are some exceptions like below that prepare() is
        never called and commit() is called instead.
         1. Binlog is disabled
         2. No modification exists in binlog cache for the transaction (#195)
        In both cases, rolling back transaction is safe. Nothing is written to
        binlog.
       */
      my_printf_error(ER_UNKNOWN_ERROR, ERRSTR_ROLLBACK_ONLY, MYF(0));
      rollback();
      return true;
    } else {
      //TODO comment by beilou, the same with percona-server 8.0
      //my_core::thd_binlog_pos(m_thd, &m_mysql_log_file_name,
      //                        &m_mysql_log_offset, &m_mysql_max_gtid);
      //binlog_manager.update(m_mysql_log_file_name, m_mysql_log_offset,
      //                      m_mysql_max_gtid, get_write_batch());

      return commit_no_binlog();
    }
  }

  void Xdb_transaction::snapshot_created(const xengine::db::Snapshot *const snapshot) {
    DBUG_ASSERT(snapshot != nullptr);

    m_read_opts.snapshot = snapshot;
    xdb->GetEnv()->GetCurrentTime(&m_snapshot_timestamp);
    m_is_delayed_snapshot = false;
  }


  int Xdb_transaction::finish_bulk_load() {
    int rc = 0;

    std::vector<ha_xengine *>::iterator it;
    while ((it = m_curr_bulk_load.begin()) != m_curr_bulk_load.end()) {
      int rc2 = (*it)->finalize_bulk_load();
      if (rc2 != 0 && rc == 0) {
        rc = rc2;
      }
    }

    DBUG_ASSERT(m_curr_bulk_load.size() == 0);

    return rc;
  }

  void Xdb_transaction::start_bulk_load(ha_xengine *const bulk_load) {
    /*
     If we already have an open bulk load of a table and the name doesn't
     match the current one, close out the currently running one.  This allows
     multiple bulk loads to occur on a partitioned table, but then closes
     them all out when we switch to another table.
    */
    DBUG_ASSERT(bulk_load != nullptr);

    if (!m_curr_bulk_load.empty() &&
        !bulk_load->same_table(*m_curr_bulk_load[0])) {
      const auto res = finish_bulk_load();
      SHIP_ASSERT(res == 0);
    }

    m_curr_bulk_load.push_back(bulk_load);
  }

  void Xdb_transaction::end_bulk_load(ha_xengine *const bulk_load) {
    for (auto it = m_curr_bulk_load.begin(); it != m_curr_bulk_load.end();
         it++) {
      if (*it == bulk_load) {
        m_curr_bulk_load.erase(it);
        return;
      }
    }

    // Should not reach here
    SHIP_ASSERT(0);
  }

  bool Xdb_transaction::flush_batch() {
    if (get_write_count() == 0)
      return false;

    /* Commit the current transaction */
    if (commit_no_binlog())
      return true;

    /* Start another one */
    start_tx();
    return false;
  }

  xengine::db::Iterator *
  Xdb_transaction::get_iterator(xengine::db::ColumnFamilyHandle *const column_family,
               bool skip_bloom_filter, bool fill_cache,
               bool read_current, bool create_snapshot,
               bool exclude_l2, bool unique_check) {
    // Make sure we are not doing both read_current (which implies we don't
    // want a snapshot) and create_snapshot which makes sure we create
    // a snapshot
    DBUG_ASSERT(column_family != nullptr);
    DBUG_ASSERT(!read_current || !create_snapshot);

    if (create_snapshot)
      acquire_snapshot(true);

    xengine::common::ReadOptions options = m_read_opts;

    if (skip_bloom_filter) {
      options.total_order_seek = true;
    } else {
      // With this option, Iterator::Valid() returns false if key
      // is outside of the prefix bloom filter range set at Seek().
      // Must not be set to true if not using bloom filter.
      options.prefix_same_as_start = true;
    }
    options.fill_cache = fill_cache;
    if (read_current) {
      options.snapshot = nullptr;
    }

    if(exclude_l2){
      options.read_level_ = xengine::common::kExcludeL2;
    }
    options.unique_check_ = unique_check;

    return get_iterator(options, column_family);
  }


  bool Xdb_transaction::can_prepare() const {
    if (m_rollback_only) {
      my_printf_error(ER_UNKNOWN_ERROR, ERRSTR_ROLLBACK_ONLY, MYF(0));
      return false;
    }
    return true;
  }

  int Xdb_transaction::rollback_to_savepoint(void *const savepoint) {
    if (has_modifications()) {
      my_printf_error(ER_UNKNOWN_ERROR,
                      "XEngine currently does not support ROLLBACK TO "
                      "SAVEPOINT if modifying rows.",
                      MYF(0));
      m_rollback_only = true;
      return HA_EXIT_FAILURE;
    }
    return HA_EXIT_SUCCESS;
  }


  Xdb_transaction::Xdb_transaction(THD *const thd) : m_thd(thd) {
    XDB_MUTEX_LOCK_CHECK(s_tx_list_mutex);
    s_tx_list.insert(this);
    XDB_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
  }

  Xdb_transaction::~Xdb_transaction() {
    XDB_MUTEX_LOCK_CHECK(s_tx_list_mutex);
    s_tx_list.erase(this);
    XDB_MUTEX_UNLOCK_CHECK(s_tx_list_mutex);
  }

/*
  This is a xengine transaction. Its members represent the current transaction,
  which consists of:
  - the snapshot
  - the changes we've made but are not seeing yet.

  The changes are made to individual tables, which store them here and then
  this object commits them on commit.
*/
class Xdb_transaction_impl : public Xdb_transaction {
  xengine::util::Transaction *m_xengine_tx = nullptr;
  xengine::util::Transaction *m_xengine_reuse_tx = nullptr;

public:
  void set_lock_timeout(int timeout_sec_arg) override {
    if (m_xengine_tx)
      m_xengine_tx->SetLockTimeout(xdb_convert_sec_to_ms(m_timeout_sec));
  }

  void set_sync(bool sync) override {
    m_xengine_tx->GetWriteOptions()->sync = sync;
  }

  void release_lock(xengine::db::ColumnFamilyHandle *const column_family,
                    const std::string &rowkey) override {
    if (!THDVAR(m_thd, lock_scanned_rows)) {
      m_xengine_tx->UndoGetForUpdate(column_family, xengine::common::Slice(rowkey));
    }
  }

  void detach_from_xengine() {
    m_xengine_tx = nullptr;
    m_xengine_reuse_tx = nullptr;
  }

  virtual bool is_writebatch_trx() const override { return false; }

  bool is_async_commit() {
    return m_xengine_tx->GetWriteOptions()->async_commit;
  }

private:
  void release_tx(void) {
    // We are done with the current active transaction object.  Preserve it
    // for later reuse.
    DBUG_ASSERT(m_xengine_reuse_tx == nullptr);
    m_xengine_reuse_tx = m_xengine_tx;
    m_xengine_tx = nullptr;
    m_prepared = false;
  }

  bool prepare(const xengine::util::TransactionName &name) override {
    xengine::common::Status s;
    s = m_xengine_tx->SetName(name);
    if (!s.ok()) {
      xdb_handle_io_error(s, XDB_IO_ERROR_TX_COMMIT);
      return false;
    }

    s = m_xengine_tx->Prepare();
    if (!s.ok()) {
      xdb_handle_io_error(s, XDB_IO_ERROR_TX_COMMIT);
      return false;
    }
    m_prepared = true;
    return true;
  }

  bool commit_no_binlog() override {
    bool res = false;
    release_snapshot();
     //comment by beilou, we have no async commit facility in 8.0 server
    //bool async_commit = m_thd->variables.opt_async_commit &&
    //                    !m_thd->force_sync_commit &&
    //                    (nullptr != this->get_thd()->connection);

    bool async_commit = false;

    xengine::common::AsyncCallback *cb_param = nullptr;
    if (async_commit) {
      m_xengine_tx->GetWriteOptions()->async_commit = async_commit;
      //assert(nullptr != this->get_thd()->connection);
      this->async_call_back.reset_thd(this->get_thd());
      cb_param = &this->async_call_back;
    } else {
      m_xengine_tx->GetWriteOptions()->async_commit = false;
      cb_param = nullptr;
    }

    const xengine::common::Status s = m_xengine_tx->CommitAsync(cb_param);

    if (async_commit) {//async commit return directly
      // TODO comment by beilou, will fix async commit later
      //this->m_thd->async_commit = true;
      abort();
      return res;
    }

    if (!s.ok()) {
      xdb_handle_io_error(s, XDB_IO_ERROR_TX_COMMIT);
      res = true;
    }

    /* Save the transaction object to be reused */
    release_tx();

    m_write_count = 0;
    m_lock_count = 0;
    set_tx_read_only(false);
    m_rollback_only = false;
    return res;
  }

  bool commit_no_binlog2() override {
    bool res = false;

    m_xengine_tx->GetWriteOptions()->async_commit = false;

    /* Save the transaction object to be reused */
    release_tx();

    m_write_count = 0;
    m_lock_count = 0;
    set_tx_read_only(false);
    m_rollback_only = false;
    return res;
  }

public:
  void rollback() override {
    m_write_count = 0;
    m_lock_count = 0;
    m_ddl_transaction = false;
    if (m_xengine_tx) {
      release_snapshot();
      /* This will also release all of the locks: */
      m_xengine_tx->Rollback();

      /* Save the transaction object to be reused */
      release_tx();

      set_tx_read_only(false);
      m_rollback_only = false;
    }
    if (m_backup_running) {
      if (nullptr != backup_instance) {
        backup_instance->release_snapshots(xdb);
      }
      xengine_hotbackup_name = xengine_backup_status[4];
      m_backup_running = false;
    }
  }

  void acquire_snapshot(bool acquire_now) override {
    if (m_read_opts.snapshot == nullptr) {
      if (is_tx_read_only()) {
        snapshot_created(xdb->GetSnapshot());
      } else if (acquire_now) {
        m_xengine_tx->SetSnapshot();
        snapshot_created(m_xengine_tx->GetSnapshot());
      } else if (!m_is_delayed_snapshot) {
        m_xengine_tx->SetSnapshotOnNextOperation(m_notifier);
        m_is_delayed_snapshot = true;
      }
    }
  }

  void release_snapshot() override {
    bool need_clear = m_is_delayed_snapshot;

    if (m_read_opts.snapshot != nullptr) {
      m_snapshot_timestamp = 0;
      if (is_tx_read_only()) {
        xdb->ReleaseSnapshot(m_read_opts.snapshot);
        need_clear = false;
      } else {
        need_clear = true;
      }
      m_read_opts.snapshot = nullptr;
    }

    if (need_clear && m_xengine_tx != nullptr)
      m_xengine_tx->ClearSnapshot();
  }

  bool has_snapshot() { return m_read_opts.snapshot != nullptr; }

  xengine::common::Status put(xengine::db::ColumnFamilyHandle *const column_family,
                      const xengine::common::Slice &key,
                      const xengine::common::Slice &value) override {
    if (UNLIKELY(key.size() + value.size() > XDB_MAX_ROW_SIZE)) {
      return xengine::common::Status::NotSupported("row too large");
    }
    ++m_write_count;
    ++m_lock_count;
    if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
      return xengine::common::Status(xengine::common::Status::kLockLimit);
    return m_xengine_tx->Put(column_family, key, value);
  }

  xengine::common::Status delete_key(xengine::db::ColumnFamilyHandle *const column_family,
                             const xengine::common::Slice &key) override {
    ++m_write_count;
    ++m_lock_count;
    if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
      return xengine::common::Status(xengine::common::Status::kLockLimit);
    return m_xengine_tx->Delete(column_family, key);
  }

  xengine::common::Status
  single_delete(xengine::db::ColumnFamilyHandle *const column_family,
                const xengine::common::Slice &key) override {
    ++m_write_count;
    ++m_lock_count;
    if (m_write_count > m_max_row_locks || m_lock_count > m_max_row_locks)
      return xengine::common::Status(xengine::common::Status::kLockLimit);
    return m_xengine_tx->SingleDelete(column_family, key);
  }

  bool has_modifications() const override {
    return m_xengine_tx->GetWriteBatch() &&
           m_xengine_tx->GetWriteBatch()->GetWriteBatch() &&
           m_xengine_tx->GetWriteBatch()->GetWriteBatch()->Count() > 0;
  }

  xengine::db::WriteBatchBase *get_write_batch() override {
    if (is_two_phase()) {
      return m_xengine_tx->GetCommitTimeWriteBatch();
    }
    return m_xengine_tx->GetWriteBatch()->GetWriteBatch();
  }

  /*
    Return a WriteBatch that one can write to. The writes will skip any
    transaction locking. The writes WILL be visible to the transaction.
  */
  xengine::db::WriteBatchBase *get_indexed_write_batch() override {
    ++m_write_count;
    return m_xengine_tx->GetWriteBatch();
  }

  xengine::common::Status get(xengine::db::ColumnFamilyHandle *const column_family,
                      const xengine::common::Slice &key,
                      std::string *value) const override {
    return m_xengine_tx->Get(m_read_opts, column_family, key, value);
  }

  xengine::common::Status get_latest(
      xengine::db::ColumnFamilyHandle *const column_family,
      const xengine::common::Slice &key, std::string *value) const override
  {
    xengine::common::ReadOptions read_opts = m_read_opts;
    // use latest seq to read
    read_opts.snapshot = nullptr;
    return m_xengine_tx->Get(read_opts, column_family, key, value);
  }

  xengine::common::Status get_for_update(
      xengine::db::ColumnFamilyHandle *const column_family,
      const xengine::common::Slice &key, std::string *const value,
      bool exclusive, bool only_lock = false) override
  {
    if (++m_lock_count > m_max_row_locks)
      return xengine::common::Status(xengine::common::Status::kLockLimit);

    if (only_lock) {
      return m_xengine_tx->TryLock(column_family, key, true /* read_only */, exclusive, false, only_lock);
    } else {
      return m_xengine_tx->GetForUpdate(m_read_opts, column_family, key, value,
                                        exclusive);
    }
  }

  xengine::common::Status lock_unique_key(xengine::db::ColumnFamilyHandle *const column_family,
                                          const xengine::common::Slice &key,
                                          const bool skip_bloom_filter,
                                          const bool fill_cache,
                                          const bool exclusive) override
  {
    if (++m_lock_count > m_max_row_locks) {
      return xengine::common::Status(xengine::common::Status::kLockLimit);
    } else {
      xengine::common::ReadOptions read_opts = m_read_opts;
      // use latest seq to read in TryLock
      read_opts.snapshot = nullptr;
      DBUG_ASSERT(column_family != nullptr);
      // copied from Xdb_transaction::get_iterator
      if (skip_bloom_filter) {
        read_opts.total_order_seek = true;
      } else {
        // With this option, Iterator::Valid() returns false if key
        // is outside of the prefix bloom filter range set at Seek().
        // Must not be set to true if not using bloom filter.
        read_opts.prefix_same_as_start = true;
      }
      read_opts.fill_cache = fill_cache;
      // should not skip deleted record when checking seq validation
      read_opts.skip_del_ = false;
      return m_xengine_tx->TryLock(column_family,
                                   key,
                                   true /* read_only */,
                                   exclusive,
                                   false,
                                   true /*lock_uk*/,
                                   &read_opts);
    }
  }

  xengine::db::Iterator *
  get_iterator(const xengine::common::ReadOptions &options,
               xengine::db::ColumnFamilyHandle *const column_family) override {
    return m_xengine_tx->GetIterator(options, column_family);
  }

  const xengine::util::Transaction *get_xdb_trx() const { return m_xengine_tx; }

  bool is_tx_started() const override { return (m_xengine_tx != nullptr); }

  void start_tx() override {
    xengine::util::TransactionOptions tx_opts;
    xengine::common::WriteOptions write_opts;
    tx_opts.set_snapshot = false;
    tx_opts.lock_timeout = xdb_convert_sec_to_ms(m_timeout_sec);
    tx_opts.deadlock_detect = THDVAR(m_thd, deadlock_detect);

    write_opts.sync = xengine_flush_log_at_trx_commit == 1;
    write_opts.disableWAL = THDVAR(m_thd, write_disable_wal);
    write_opts.ignore_missing_column_families = false;
        // THDVAR(m_thd, write_ignore_missing_column_families);
    m_is_two_phase = xengine_enable_2pc;

    /*
      If m_xengine_reuse_tx is null this will create a new transaction object.
      Otherwise it will reuse the existing one.
    */
    m_xengine_tx =
        xdb->BeginTransaction(write_opts, tx_opts, m_xengine_reuse_tx);
    m_xengine_reuse_tx = nullptr;

    m_read_opts = xengine::common::ReadOptions();

    m_prepared = false;
    m_ddl_transaction = false;
    m_is_delayed_snapshot = false;
  }

  /*
    Start a statement inside a multi-statement transaction.

    @todo: are we sure this is called once (and not several times) per
    statement start?

    For hooking to start of statement that is its own transaction, see
    ha_xengine::external_lock().
  */
  void start_stmt() override {
    // Set the snapshot to delayed acquisition (SetSnapshotOnNextOperation)
    acquire_snapshot(false);
    m_xengine_tx->SetSavePoint();
  }

  /*
    This must be called when last statement is rolled back, but the transaction
    continues
  */
  void rollback_stmt() override {
    /* TODO: here we must release the locks taken since the start_stmt() call */
    if (m_xengine_tx) {
      const xengine::db::Snapshot *const org_snapshot = m_xengine_tx->GetSnapshot();
      m_xengine_tx->RollbackToSavePoint();

      const xengine::db::Snapshot *const cur_snapshot = m_xengine_tx->GetSnapshot();
      if (org_snapshot != cur_snapshot) {
        if (org_snapshot != nullptr)
          m_snapshot_timestamp = 0;

        m_read_opts.snapshot = cur_snapshot;
        if (cur_snapshot != nullptr)
          xdb->GetEnv()->GetCurrentTime(&m_snapshot_timestamp);
        else
          m_is_delayed_snapshot = true;
      }
    }
  }

  explicit Xdb_transaction_impl(THD *const thd)
      : Xdb_transaction(thd), m_xengine_tx(nullptr) {
    // Create a notifier that can be called when a snapshot gets generated.
    m_notifier = std::make_shared<Xdb_snapshot_notifier>(this);
  }

  virtual ~Xdb_transaction_impl() {
    if (!m_prepared) {
      rollback();
    }

    // Theoretically the notifier could outlive the Xdb_transaction_impl
    // (because of the shared_ptr), so let it know it can't reference
    // the transaction anymore.
    m_notifier->detach();

    // Free any transaction memory that is still hanging around.
    delete m_xengine_reuse_tx;
  }
};

/* This is a xengine write batch. This class doesn't hold or wait on any
   transaction locks (skips xengine transaction API) thus giving better
   performance. The commit is done through xdb->GetBaseDB()->Commit().

   Currently this is only used for replication threads which are guaranteed
   to be non-conflicting. Any further usage of this class should completely
   be thought thoroughly.
*/
class Xdb_writebatch_impl : public Xdb_transaction {
  xengine::util::WriteBatchWithIndex *m_batch;
  xengine::common::WriteOptions write_opts;
  // Called after commit/rollback.
  void reset() {
    m_batch->Clear();
    m_read_opts = xengine::common::ReadOptions();
    m_ddl_transaction = false;
    m_prepared = false;
  }

private:
  bool prepare(const xengine::util::TransactionName &name) override {
    m_prepared = true;
    return true;
  }

  bool commit_no_binlog() override {
    bool res = false;
    release_snapshot();

    bool async_commit = false;
    //bool async_commit = m_thd->variables.opt_async_commit &&
    //                    !m_thd->force_sync_commit &&
    //                    (nullptr != this->get_thd()->connection);

    xengine::common::AsyncCallback *cb_param = nullptr;
    if (async_commit) {
      write_opts.async_commit = async_commit;
      this->async_call_back.reset_thd(this->get_thd());
      cb_param = &this->async_call_back;
    } else {
      write_opts.async_commit = false;
      cb_param = nullptr;
    }

    const xengine::common::Status s =
        xdb->GetBaseDB()->WriteAsync(write_opts,
                                     m_batch->GetWriteBatch(),
                                     cb_param);
    //TODO comment by beilou 5.7->8.0 we will fix it later
    //if (async_commit) {
    //  this->m_thd->async_commit = true;
    //  return res;
    //}

		if (!s.ok()) {
      xdb_handle_io_error(s, XDB_IO_ERROR_TX_COMMIT);
      res = true;
    }
    reset();

    m_write_count = 0;
    set_tx_read_only(false);
    m_rollback_only = false;
    return res;
  }

  bool commit_no_binlog2() override {
  	// rpl thread will not use async commit
    reset();

    write_opts.async_commit = false;

    m_write_count = 0;
    set_tx_read_only(false);
    m_rollback_only = false;
		return false;
  }

public:
  void detach_from_xengine() {}

  bool is_writebatch_trx() const override { return true; }

  void set_lock_timeout(int timeout_sec_arg) override {
    // Nothing to do here.
  }

  void set_sync(bool sync) override { write_opts.sync = sync; }

  void release_lock(xengine::db::ColumnFamilyHandle *const column_family,
                    const std::string &rowkey) override {
    // Nothing to do here since we don't hold any row locks.
  }

  void rollback() override {
    m_write_count = 0;
    m_lock_count = 0;
    release_snapshot();

    reset();
    set_tx_read_only(false);
    m_rollback_only = false;

    if (m_backup_running) {
      if (nullptr != backup_instance) {
        backup_instance->release_snapshots(xdb);
      }
      xengine_hotbackup_name = xengine_backup_status[4];
      m_backup_running = false;
    }
  }

  void acquire_snapshot(bool acquire_now) override {
    if (m_read_opts.snapshot == nullptr)
      snapshot_created(xdb->GetSnapshot());
  }

  void release_snapshot() override {
    if (m_read_opts.snapshot != nullptr) {
      xdb->ReleaseSnapshot(m_read_opts.snapshot);
      m_read_opts.snapshot = nullptr;
    }
  }

  xengine::common::Status put(xengine::db::ColumnFamilyHandle *const column_family,
                      const xengine::common::Slice &key,
                      const xengine::common::Slice &value) override {
    if (UNLIKELY(key.size() + value.size() > XDB_MAX_ROW_SIZE)) {
      return xengine::common::Status::NotSupported("row too large");
    }
    ++m_write_count;
    m_batch->Put(column_family, key, value);
    // Note Put/Delete in write batch doesn't return any error code. We simply
    // return OK here.
    return xengine::common::Status::OK();
  }

  xengine::common::Status delete_key(xengine::db::ColumnFamilyHandle *const column_family,
                             const xengine::common::Slice &key) override {
    ++m_write_count;
    m_batch->Delete(column_family, key);
    return xengine::common::Status::OK();
  }

  xengine::common::Status
  single_delete(xengine::db::ColumnFamilyHandle *const column_family,
                const xengine::common::Slice &key) override {
    ++m_write_count;
    m_batch->SingleDelete(column_family, key);
    return xengine::common::Status::OK();
  }

  bool has_modifications() const override {
    return m_batch->GetWriteBatch()->Count() > 0;
  }

  xengine::db::WriteBatchBase *get_write_batch() override { return m_batch; }

  xengine::db::WriteBatchBase *get_indexed_write_batch() override {
    ++m_write_count;
    return m_batch;
  }

  xengine::common::Status get(xengine::db::ColumnFamilyHandle *const column_family,
                      const xengine::common::Slice &key,
                      std::string *const value) const override {
    return m_batch->GetFromBatchAndDB(xdb, m_read_opts, column_family, key,
                                      value);
  }

  xengine::common::Status get_latest(
      xengine::db::ColumnFamilyHandle *const column_family,
      const xengine::common::Slice &key,
      std::string *const value) const override
  {
    xengine::common::ReadOptions read_opts = m_read_opts;
    // use latest seq to read
    read_opts.snapshot = nullptr;
    return m_batch->GetFromBatchAndDB(xdb, read_opts, column_family, key,
                                      value);
  }

  xengine::common::Status
  get_for_update(xengine::db::ColumnFamilyHandle *const column_family,
                 const xengine::common::Slice &key, std::string *const value,
                 bool exclusive, bool only_lock = false) override {
    return get(column_family, key, value);
  }

  xengine::common::Status lock_unique_key(xengine::db::ColumnFamilyHandle *const column_family,
                                          const xengine::common::Slice &key,
                                          const bool skip_bloom_filter,
                                          const bool fill_cache,
                                          const bool exclusive) override
  {
    return common::Status::NotSupported();
  }

  xengine::db::Iterator *
  get_iterator(const xengine::common::ReadOptions &options,
               xengine::db::ColumnFamilyHandle *const column_family) override {
    const auto it = xdb->NewIterator(options);
    return m_batch->NewIteratorWithBase(it);
  }

  bool is_tx_started() const override { return (m_batch != nullptr); }

  void start_tx() override {
    reset();
    write_opts.sync = xengine_flush_log_at_trx_commit == 1;
    write_opts.disableWAL = THDVAR(m_thd, write_disable_wal);
    write_opts.ignore_missing_column_families = false;
        // THDVAR(m_thd, write_ignore_missing_column_families);
  }

  void start_stmt() override { m_batch->SetSavePoint(); }

  void rollback_stmt() override {
    if (m_batch)
      m_batch->RollbackToSavePoint();
  }

  explicit Xdb_writebatch_impl(THD *const thd)
      : Xdb_transaction(thd), m_batch(nullptr) {
    m_batch = new xengine::util::WriteBatchWithIndex(xengine::util::BytewiseComparator(), 0,
                                               true);
  }

  virtual ~Xdb_writebatch_impl() {
    rollback();
    delete m_batch;
  }
};

void Xdb_snapshot_notifier::SnapshotCreated(
    const xengine::db::Snapshot *const snapshot) {
  if (m_owning_tx != nullptr) {
    m_owning_tx->snapshot_created(snapshot);
  }
}

std::multiset<Xdb_transaction *> Xdb_transaction::s_tx_list;
mysql_mutex_t Xdb_transaction::s_tx_list_mutex;

static Xdb_transaction *&get_tx_from_thd(THD *const thd) {
  return *reinterpret_cast<Xdb_transaction **>(
      my_core::thd_ha_data(thd, xengine_hton));
}

/*
  TODO: maybe, call this in external_lock() and store in ha_xengine..
*/
Xdb_transaction *get_or_create_tx(THD *const thd) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);
  // TODO: this is called too many times.. O(#rows)
  if (tx == nullptr) {
    if ((rpl_skip_tx_api_var && thd->rli_slave) ||
        /*(THDVAR(thd, master_skip_tx_api) && !thd->rli_slave) || */
         (xengine_skip_unique_key_check_in_boost_insert &&
          !my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))
    {
      tx = new Xdb_writebatch_impl(thd);
    }
    else
    {
      tx = new Xdb_transaction_impl(thd);
    }
    tx->set_params(THDVAR(thd, lock_wait_timeout), THDVAR(thd, max_row_locks));
    tx->start_tx();
  } else {
    tx->set_params(THDVAR(thd, lock_wait_timeout), THDVAR(thd, max_row_locks));
    if (!tx->is_tx_started()) {
      tx->start_tx();
    }
  }

  return tx;
}

static int xengine_close_connection(handlerton *const hton, THD *const thd) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);
  if (tx != nullptr) {
    int rc = tx->finish_bulk_load();
    if (rc != 0) {
      // NO_LINT_DEBUG
      sql_print_error("XEngine: Error %d finalizing last SST file while "
                      "disconnecting",
                      rc);
      abort_with_stack_traces();
    }

    delete tx;
    tx = nullptr;
  }
  return HA_EXIT_SUCCESS;
}

/*
 * Serializes an xid to a string so that it can
 * be used as a xengine transaction name
 */
static std::string xdb_xid_to_string(const XID &src) {
  DBUG_ASSERT(src.get_gtrid_length() >= 0 && src.get_gtrid_length() <= MAXGTRIDSIZE);
  DBUG_ASSERT(src.get_bqual_length() >= 0 && src.get_bqual_length() <= MAXBQUALSIZE);

  std::string buf;
  buf.reserve(XDB_XIDHDR_LEN + src.get_gtrid_length() + src.get_bqual_length());

  /*
   * expand formatID to fill 8 bytes if it doesn't already
   * then reinterpret bit pattern as unsigned and store in network order
   */
  uchar fidbuf[XDB_FORMATID_SZ];
  int64 signed_fid8 = src.get_format_id();
  const uint64 raw_fid8 = *reinterpret_cast<uint64 *>(&signed_fid8);
  xdb_netbuf_store_uint64(fidbuf, raw_fid8);
  buf.append(reinterpret_cast<const char *>(fidbuf), XDB_FORMATID_SZ);

  buf.push_back(src.get_gtrid_length());
  buf.push_back(src.get_bqual_length());
  buf.append(src.get_data(), (src.get_gtrid_length()) + (src.get_bqual_length()));
  return buf;
}

/**
  Called by hton->flush_logs after MySQL group commit prepares a set of
  transactions.
*/
static bool xengine_flush_wal(handlerton *const hton MY_ATTRIBUTE((__unused__)),
                              bool binlog_group_flush) {
  DBUG_ASSERT(xdb != nullptr);
  if (binlog_group_flush && xengine_flush_log_at_trx_commit == 0) {
    return HA_EXIT_SUCCESS;
  }
  xengine_wal_group_syncs++;
  const xengine::common::Status s = xdb->SyncWAL();
  if (!s.ok()) {
    return HA_EXIT_FAILURE;
  }
  return HA_EXIT_SUCCESS;
}

/*
 Copy from innodb.
 This function checks if the given db.tablename is a system table
 supported by Innodb and is used as an initializer for the data member
 is_supported_system_table of XENGINE storage engine handlerton.
 Currently we support only plugin, servers,  help- and time_zone- related
 system tables in XENGINE. Please don't add any SE-specific system tables here.

 @param db                            database name to check.
 @param table_name                    table name to check.
 @param is_sql_layer_system_table     if the supplied db.table_name is a SQL
                                      layer system table.
*/

static bool xengine_is_supported_system_table(const char *db,
                                              const char *table_name,
                                              bool is_sql_layer_system_table)
{
  static const char* supported_system_tables[]= { "help_topic",
                                                  "help_category",
                                                  "help_relation",
                                                  "help_keyword",
                                                  "plugin",
                                                  "servers",
                                                  "time_zone",
                                                  "time_zone_leap_second",
                                                  "time_zone_name",
                                                  "time_zone_transition",
                                                  "time_zone_transition_type",
                                                  (const char *)NULL };

  if (!is_sql_layer_system_table)
    return false;

  for (unsigned i= 0; supported_system_tables[i] != NULL; ++i)
  {
    if (!strcmp(table_name, supported_system_tables[i]))
      return true;
  }

  return false;
}

#if 0
static void xengine_replace_tx_in_thd(THD* thd, void* new_tx_arg,
				      void** ptr_tx_arg) {
  // Begin new transaction for current thd.
  Xdb_transaction *new_tx = get_or_create_tx(thd);
  // To modify the member tx of thd, get the reference.
  Xdb_transaction *&tx = get_tx_from_thd(thd);
  if (ptr_tx_arg != nullptr) {
    *ptr_tx_arg = tx;
  } else if (tx->has_prepared()) {
    tx->detach_from_xengine();
    delete tx;
  } else {
    delete tx;
  }
  tx = static_cast<Xdb_transaction*>(new_tx_arg);
}
#endif

/**
  For a slave, prepare() updates the slave_gtid_info table which tracks the
  replication progress.
*/
//TODO disable async temporaly we will fix it in the future
//static int xengine_prepare(handlerton *const hton, THD *const thd,
//                           bool prepare_tx, bool async) {
static int xengine_prepare(handlerton *const hton, THD *const thd,
                           bool prepare_tx) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);
  if (!tx->can_prepare()) {
    return HA_EXIT_FAILURE;
  }
  if (DBUG_EVALUATE_IF("simulate_xa_failure_prepare_in_engine", 1, 0)) {
    return HA_EXIT_FAILURE;
  }
  if (prepare_tx ||
      (!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))) {
    /* We were instructed to prepare the whole transaction, or
    this is an SQL statement end and autocommit is on */
    //COMMENT GTID temporarily
    /*
    std::vector<st_slave_gtid_info> slave_gtid_info;
    my_core::thd_slave_gtid_info(thd, &slave_gtid_info);
    for (const auto &it : slave_gtid_info) {
      xengine::db::WriteBatchBase *const write_batch = tx->get_blind_write_batch();
      binlog_manager.update_slave_gtid_info(it.id, it.db, it.gtid, write_batch);
    }
    */

    if (tx->is_two_phase()) {
      //if (thd->durability_property == HA_IGNORE_DURABILITY || async) {
      if (thd->durability_property == HA_IGNORE_DURABILITY) {
        tx->set_sync(false);
      }
      XID xid;
      thd_get_xid(thd, reinterpret_cast<MYSQL_XID *>(&xid));
      if (!tx->prepare(xdb_xid_to_string(xid))) {
        return HA_EXIT_FAILURE;
      }
      if (thd->durability_property == HA_IGNORE_DURABILITY &&
          xengine_flush_log_at_trx_commit == 1) {
        /**
          we set the log sequence as '1' just to trigger hton->flush_logs
        */
        //comment by beilou  TODO, we should fix this as percona 8.0
        //this is no thd_store_lsn in 8.0
       // thd_store_lsn(thd, 1, DB_TYPE_XENGINE);
      }
    }

    DEBUG_SYNC(thd, "xengine.prepared");
  }

  return HA_EXIT_SUCCESS;
}

/**
 do nothing for prepare/commit by xid
 this is needed to avoid crashes in XA scenarios
*/
static xa_status_code xengine_commit_by_xid(handlerton *const hton, XID *const xid) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(hton != nullptr);
  DBUG_ASSERT(xid != nullptr);

  const auto name = xdb_xid_to_string(*xid);
  xengine::util::Transaction *const trx = xdb->GetTransactionByName(name);
  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }
  const xengine::common::Status s = trx->Commit();
  if (!s.ok()) {
    DBUG_RETURN(XAER_RMERR);
  }
  delete trx;
  DBUG_RETURN(XA_OK);
}

static xa_status_code
xengine_rollback_by_xid(handlerton *const hton MY_ATTRIBUTE((__unused__)),
                        XID *const xid) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(hton != nullptr);
  DBUG_ASSERT(xid != nullptr);

  const auto name = xdb_xid_to_string(*xid);

  xengine::util::Transaction *const trx = xdb->GetTransactionByName(name);

  if (trx == nullptr) {
    DBUG_RETURN(XAER_NOTA);
  }
  const xengine::common::Status s = trx->Rollback();

  if (!s.ok()) {
    DBUG_RETURN(XAER_RMERR);
  }
  delete trx;
  DBUG_RETURN(XA_OK);
}

/**
  Rebuilds an XID from a serialized version stored in a string.
*/
static void xdb_xid_from_string(const std::string &src, XID *const dst) {
  DBUG_ASSERT(dst != nullptr);
  uint offset = 0;
  uint64 raw_fid8 =
      xdb_netbuf_to_uint64(reinterpret_cast<const uchar *>(src.data()));
  const int64 signed_fid8 = *reinterpret_cast<int64 *>(&raw_fid8);
  dst->set_format_id(signed_fid8);
  offset += XDB_FORMATID_SZ;
  dst->set_gtrid_length(src.at(offset));
  offset += XDB_GTRID_SZ;
  dst->set_bqual_length(src.at(offset));
  offset += XDB_BQUAL_SZ;

  DBUG_ASSERT(dst->get_gtrid_length() >= 0 && dst->get_gtrid_length() <= MAXGTRIDSIZE);
  DBUG_ASSERT(dst->get_bqual_length() >= 0 && dst->get_bqual_length() <= MAXBQUALSIZE);

  const std::string &tmp_data = src.substr(
      XDB_XIDHDR_LEN, (dst->get_gtrid_length()) + (dst->get_bqual_length()));
  dst->set_data(tmp_data.data(), tmp_data.length());
}

/**
  Reading last committed binary log info from XEngine system row.
  The info is needed for crash safe slave/master to work.
*/
//static int xengine_recover(handlerton *const hton, XID *const xid_list,
//                           uint len, char *const binlog_file,
//                           my_off_t *const binlog_pos,
//                           Gtid *const binlog_max_gtid) {
static int xengine_recover(handlerton *hton, XA_recover_txn *txn_list, uint len,
                           MEM_ROOT *mem_root) {
  // if (binlog_file && binlog_pos) {
  //  char file_buf[FN_REFLEN + 1] = {0};
  //  my_off_t pos;
  //  char gtid_buf[FN_REFLEN + 1] = {0};
  //  if (binlog_manager.read(file_buf, &pos, gtid_buf)) {
  //    // if (is_binlog_advanced(binlog_file, *binlog_pos, file_buf, pos)) {
  //      // memcpy(binlog_file, file_buf, FN_REFLEN + 1);
  //      // *binlog_pos = pos;
  //    fprintf(stderr, "XEngine: Last binlog file position %llu,"
  //        " file name %s\n", pos, file_buf);
  //    if (*gtid_buf) {
  //      global_sid_lock->rdlock();
  //      binlog_max_gtid->parse(global_sid_map, gtid_buf);
  //      global_sid_lock->unlock();
  //      fprintf(stderr, "XEngine: Last MySQL Gtid %s\n", gtid_buf);
  //    }
  //    // }
  //  }
  //}

  if (len == 0 || txn_list == nullptr) {
    return HA_EXIT_SUCCESS;
  }

  std::vector<xengine::util::Transaction *> trans_list;
  xdb->GetAllPreparedTransactions(&trans_list);

  uint count = 0;
  for (auto &trans : trans_list) {
    if (count >= len) {
      break;
    }
    auto name = trans->GetName();
    xdb_xid_from_string(name, &(txn_list[count].id));

    txn_list[count].mod_tables = new (mem_root) List<st_handler_tablename>();
    if (!txn_list[count].mod_tables) break;

    count++;
  }
  return count;
}

static int xengine_commit(handlerton *const hton, THD *const thd,
                          bool commit_tx) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(hton != nullptr);
  DBUG_ASSERT(thd != nullptr);

  /* note: h->external_lock(F_UNLCK) is called after this function is called) */
  Xdb_transaction *&tx = get_tx_from_thd(thd);

  if (tx != nullptr) {
    if (commit_tx || (!my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT |
                                                          OPTION_BEGIN))) {
      /*
        We get here
         - For a COMMIT statement that finishes a multi-statement transaction
         - For a statement that has its own transaction
      */
      bool ret = tx->commit();
      //TODO we will fix async commit later
      //if (thd->async_commit) {
      //  thd->async_commit = true;
      //  DBUG_RETURN(HA_EXIT_SUCCESS);
      //}

			// TODO: Leader thread put all followers to async queue

      if (ret)
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    } else {
      /*
        We get here when committing a statement within a transaction.

        We don't need to do anything here. tx->start_stmt() will notify
        Xdb_transaction_impl that another statement has started.
      */
      tx->set_tx_failed(false);
    }

    if (my_core::thd_tx_isolation(thd) <= ISO_READ_COMMITTED) {
      // For READ_COMMITTED, we release any existing snapshot so that we will
      // see any changes that occurred since the last statement.
      tx->release_snapshot();
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

#if 0 //TODO unused function in sync mode, we will fix async commit in the future
static int xengine_commit2(handlerton *const hton, THD *const thd,
                          bool commit_tx, bool) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(hton != nullptr);
  DBUG_ASSERT(thd != nullptr);

  /* note: h->external_lock(F_UNLCK) is called after this function is called) */
  Xdb_transaction *&tx = get_tx_from_thd(thd);

  bool ret = tx->commit2();
  if (ret)
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  if (my_core::thd_tx_isolation(thd) <= ISO_READ_COMMITTED) {
    // For READ_COMMITTED, we release any existing snapshot so that we will
    // see any changes that occurred since the last statement.
    tx->release_snapshot();
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}
#endif


static int xengine_rollback(handlerton *const hton, THD *const thd,
                            bool rollback_tx) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);

  if (tx != nullptr) {
    if (rollback_tx) {
      /*
        We get here, when
        - ROLLBACK statement is issued.

        Discard the changes made by the transaction
      */
      tx->rollback();
    } else {
      /*
        We get here when
        - a statement with AUTOCOMMIT=1 is being rolled back (because of some
          error)
        - a statement inside a transaction is rolled back
      */

      tx->rollback_stmt();
      tx->set_tx_failed(true);
    }

    if (my_core::thd_tx_isolation(thd) <= ISO_READ_COMMITTED) {
      // For READ_COMMITTED, we release any existing snapshot so that we will
      // see any changes that occurred since the last statement.
      tx->release_snapshot();
    }
  }
  return HA_EXIT_SUCCESS;
}

static bool print_stats(THD *const thd, std::string const &type,
                        std::string const &name, std::string const &status,
                        stat_print_fn *stat_print) {
  return stat_print(thd, type.c_str(), type.size(), name.c_str(), name.size(),
                    status.c_str(), status.size());
}

static std::string format_string(const char *const format, ...) {
  std::string res;
  va_list args;
  va_list args_copy;
  char static_buff[256];

  DBUG_ASSERT(format != nullptr);

  va_start(args, format);
  va_copy(args_copy, args);

  // Calculate how much space we will need
  int len = vsnprintf(nullptr, 0, format, args);
  va_end(args);

  if (len < 0) {
    res = std::string("<format error>");
  } else if (len == 0) {
    // Shortcut for an empty string
    res = std::string("");
  } else {
    // For short enough output use a static buffer
    char *buff = static_buff;
    std::unique_ptr<char[]> dynamic_buff = nullptr;

    len++; // Add one for null terminator

    // for longer output use an allocated buffer
    if (static_cast<uint>(len) > sizeof(static_buff)) {
      dynamic_buff.reset(new char[len]);
      buff = dynamic_buff.get();
    }

    // Now re-do the vsnprintf with the buffer which is now large enough
    (void)vsnprintf(buff, len, format, args_copy);

    // Convert to a std::string.  Note we could have created a std::string
    // large enough and then converted the buffer to a 'char*' and created
    // the output in place.  This would probably work but feels like a hack.
    // Since this isn't code that needs to be super-performant we are going
    // with this 'safer' method.
    res = std::string(buff);
  }

  va_end(args_copy);

  return res;
}

class Xdb_snapshot_status : public Xdb_tx_list_walker {
private:
  std::string m_data;

  static std::string current_timestamp(void) {
    static const char *const format = "%d-%02d-%02d %02d:%02d:%02d";
    time_t currtime;
    struct tm currtm;

    time(&currtime);

    localtime_r(&currtime, &currtm);

    return format_string(format, currtm.tm_year + 1900, currtm.tm_mon + 1,
                         currtm.tm_mday, currtm.tm_hour, currtm.tm_min,
                         currtm.tm_sec);
  }

  static std::string get_header(void) {
    return "\n============================================================\n" +
           current_timestamp() +
           " XENGINE TRANSACTION MONITOR OUTPUT\n"
           "============================================================\n"
           "---------\n"
           "SNAPSHOTS\n"
           "---------\n"
           "LIST OF SNAPSHOTS FOR EACH SESSION:\n";
  }

  static std::string get_footer(void) {
    return "-----------------------------------------\n"
           "END OF XENGINE TRANSACTION MONITOR OUTPUT\n"
           "=========================================\n";
  }

public:
  Xdb_snapshot_status() : m_data(get_header()) {}

  std::string getResult() { return m_data + get_footer(); }

  /* Implement Xdb_transaction interface */
  /* Create one row in the snapshot status table */
  void process_tran(const Xdb_transaction *const tx) override {
    DBUG_ASSERT(tx != nullptr);

    /* Calculate the duration the snapshot has existed */
    int64_t snapshot_timestamp = tx->m_snapshot_timestamp;
    if (snapshot_timestamp != 0) {
      int64_t curr_time;
      xdb->GetEnv()->GetCurrentTime(&curr_time);

      THD *thd = tx->get_thd();
      char buffer[1024];
      thd_security_context(thd, buffer, sizeof buffer, 0);
      m_data += format_string("---SNAPSHOT, ACTIVE %lld sec\n"
                              "%s\n"
                              "lock count %llu, write count %llu\n",
                              curr_time - snapshot_timestamp, buffer,
                              tx->get_lock_count(), tx->get_write_count());
    }
  }
};

/**
 * @brief
 * walks through all non-replication transactions and copies
 * out relevant information for information_schema.xengine_trx
 */
class Xdb_trx_info_aggregator : public Xdb_tx_list_walker {
private:
  std::vector<Xdb_trx_info> *m_trx_info;

public:
  explicit Xdb_trx_info_aggregator(std::vector<Xdb_trx_info> *const trx_info)
      : m_trx_info(trx_info) {}

  void process_tran(const Xdb_transaction *const tx) override {
    static const std::map<int, std::string> state_map = {
        {xengine::util::Transaction::STARTED, "STARTED"},
        {xengine::util::Transaction::AWAITING_PREPARE, "AWAITING_PREPARE"},
        {xengine::util::Transaction::PREPARED, "PREPARED"},
        {xengine::util::Transaction::AWAITING_COMMIT, "AWAITING_COMMIT"},
        {xengine::util::Transaction::COMMITED, "COMMITED"},
        {xengine::util::Transaction::AWAITING_ROLLBACK, "AWAITING_ROLLBACK"},
        {xengine::util::Transaction::ROLLEDBACK, "ROLLEDBACK"},
    };
    static const size_t trx_query_max_len = 1024;  // length stolen from InnoDB

    DBUG_ASSERT(tx != nullptr);

    THD *const thd = tx->get_thd();
    ulong thread_id = thd_thread_id(thd);

    if (tx->is_writebatch_trx()) {
      const auto wb_impl = static_cast<const Xdb_writebatch_impl *>(tx);
      DBUG_ASSERT(wb_impl);
      m_trx_info->push_back(
          {"",                            /* name */
           0,                             /* trx_id */
           wb_impl->get_write_count(), 0, /* lock_count */
           0,                             /* timeout_sec */
           "",                            /* state */
           "",                            /* waiting_key */
           0,                             /* waiting_cf_id */
           1,                             /*is_replication */
           1,                             /* skip_trx_api */
           wb_impl->is_tx_read_only(), 0, /* deadlock detection */
           wb_impl->num_ongoing_bulk_load(), thread_id, "" /* query string */});
    } else {
      const auto tx_impl = static_cast<const Xdb_transaction_impl *>(tx);
      DBUG_ASSERT(tx_impl);
      const xengine::util::Transaction *xdb_trx = tx_impl->get_xdb_trx();

      if (xdb_trx == nullptr) {
        return;
      }

      char query[trx_query_max_len + 1];
      std::string query_str;
      if (thd_query_safe(thd, query, trx_query_max_len) > 0) {
        query_str.assign(query);
      }

      const auto state_it = state_map.find(xdb_trx->GetState());
      DBUG_ASSERT(state_it != state_map.end());
      const int is_replication = (thd->rli_slave != nullptr);
      uint32_t waiting_cf_id;
      std::string waiting_key;
      xdb_trx->GetWaitingTxns(&waiting_cf_id, &waiting_key),

          m_trx_info->push_back(
              {xdb_trx->GetName(), xdb_trx->GetID(), tx_impl->get_write_count(),
               tx_impl->get_lock_count(), tx_impl->get_timeout_sec(),
               state_it->second, waiting_key, waiting_cf_id, is_replication,
               0, /* skip_trx_api */
               tx_impl->is_tx_read_only(), xdb_trx->IsDeadlockDetect(),
               tx_impl->num_ongoing_bulk_load(), thread_id, query_str});
    }
  }
};

/*
  returns a vector of info for all non-replication threads
  for use by information_schema.xengine_trx
*/
std::vector<Xdb_trx_info> xdb_get_all_trx_info() {
  std::vector<Xdb_trx_info> trx_info;
  Xdb_trx_info_aggregator trx_info_agg(&trx_info);
  Xdb_transaction::walk_tx_list(&trx_info_agg);
  return trx_info;
}

#if 0 //TODO  unused function from 5.7->8.0
/* Generate the snapshot status table */
static bool xengine_show_snapshot_status(handlerton *const hton, THD *const thd,
                                         stat_print_fn *const stat_print) {
  Xdb_snapshot_status showStatus;

  Xdb_transaction::walk_tx_list(&showStatus);

  /* Send the result data back to MySQL */
  return print_stats(thd, "SNAPSHOTS", "xengine", showStatus.getResult(),
                     stat_print);
}
#endif

/*
  This is called for SHOW ENGINE XENGINE STATUS|LOGS|etc.

  For now, produce info about live files (which gives an imprecise idea about
  what column families are there)
*/

static bool xengine_show_status(handlerton *const hton, THD *const thd,
                                stat_print_fn *const stat_print,
                                enum ha_stat_type stat_type) {
  bool res = false;
  if (stat_type == HA_ENGINE_STATUS) {
    std::string str;

    /* Per DB stats */
    if (xdb->GetProperty("xengine.dbstats", &str)) {
      res |= print_stats(thd, "DBSTATS", "xengine", str, stat_print);
    }

    //TODO, information_schema need use cf_name
#if 0
    /* Per column family stats */
    for (const auto &cf_name : cf_manager.get_cf_names()) {
      xengine::db::ColumnFamilyHandle *cfh;
      bool is_automatic;

      /*
        Only the cf name is important. Whether it was generated automatically
        does not matter, so is_automatic is ignored.
      */
      cfh = cf_manager.get_cf(cf_name.c_str(), "", nullptr, &is_automatic);
      if (cfh == nullptr)
        continue;

      if (!xdb->GetProperty(cfh, "xengine.cfstats", &str))
        continue;

      res |= print_stats(thd, "CF_COMPACTION", cf_name, str, stat_print);
    }
#endif

    /* Memory Statistics */
    std::vector<xengine::db::DB *> dbs;
    std::unordered_set<const xengine::cache::Cache *> cache_set;
    size_t internal_cache_count = 0;
    size_t kDefaultInternalCacheSize = 8 * 1024 * 1024;
    char buf[100];

    dbs.push_back(xdb);
    cache_set.insert(xengine_tbl_options.block_cache.get());
    std::unique_ptr<xengine::db::ColumnFamilyHandle> cf_ptr;
    for (const auto &cf_handle : cf_manager.get_all_cf()) {
      cf_ptr.reset(cf_handle);
      xengine::db::ColumnFamilyDescriptor cf_desc;
      cf_handle->GetDescriptor(&cf_desc);
      auto *const table_factory = cf_desc.options.table_factory.get();
      if (table_factory != nullptr) {
        std::string tf_name = table_factory->Name();
        if (tf_name.find("BlockBasedTable") != std::string::npos) {
          const xengine::table::BlockBasedTableOptions *const bbt_opt =
              reinterpret_cast<xengine::table::BlockBasedTableOptions *>(
                  table_factory->GetOptions());
          if (bbt_opt != nullptr) {
            if (bbt_opt->block_cache.get() != nullptr) {
              cache_set.insert(bbt_opt->block_cache.get());
            } else {
              internal_cache_count++;
            }
            cache_set.insert(bbt_opt->block_cache_compressed.get());
          }
        }
      }
    }
    std::map<xengine::util::MemoryStat::MemoryStatType, uint64_t> temp_stat_by_type;
    xengine::util::MemoryStat::GetApproximateMemoryStatByType(dbs, temp_stat_by_type);
    str.clear();
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalNumber: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kActiveMemTableTotalNumber]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalMemoryAllocated: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kActiveMemTableTotalMemoryAllocated]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nActiveMemTableTotalUsed: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kActiveMemTableTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalNumber: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kUnflushedImmTableTotalNumber]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalMemoryAllocated: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kUnflushedImmTableTotalMemoryAllocated]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nUnflushedImmTableTotalMemoryUsed: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kUnflushedImmTableTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nTableReaderTotalNumber: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kTableReaderTotalNumber]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nTableReaderTotalMemoryUsed: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kTableReaderTotalMemoryUsed]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nBlockCacheTotalPinnedMemory: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kBlockCacheTotalPinnedMemory]);
    str.append(buf);

    snprintf(buf, sizeof(buf), "\nBlockCacheTotalMemoryUsed: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kBlockCacheTotalMemoryUsed]);
    str.append(buf);
    snprintf(buf, sizeof(buf), "\nDBTotalMemoryAllocated: %lu",
            temp_stat_by_type[xengine::util::MemoryStat::kDBTotalMemoryAllocated]);
    str.append(buf);
    res |= print_stats(thd, "Memory_Stats", "xengine", str, stat_print);
  }
#if 0
//comment by beilou for 5.7->8.0
//we have to chagne the sql/handle.h ha_stat_type{}  to support
// HA_ENGINE_TRX , HA_ENGINE_COMPACTION , HA_ENGINE_META
// for a better compatiable feature, we should find another way
// to show this information
else if (stat_type == HA_ENGINE_TRX) {
    /* Handle the SHOW ENGINE XENGINE TRANSACTION STATUS command */
    res |= xengine_show_snapshot_status(hton, thd, stat_print);
  } else if (stat_type == HA_ENGINE_COMPACTIONS) {
    std::string str;

    /* Per column family stats */
    for (const auto &cf_name : cf_manager.get_cf_names()) {
      xengine::db::ColumnFamilyHandle *cfh;
      bool is_automatic;

      /*
        Only the cf name is important. Whether it was generated automatically
        does not matter, so is_automatic is ignored.
      */
      cfh = cf_manager.get_cf(cf_name.c_str(), "", nullptr, &is_automatic);
      if (cfh == nullptr)
        continue;

      if (!xdb->GetProperty(cfh, "xengine.cfstats-no-file-histogram", &str))
        continue;

      res |= print_stats(thd, "CF_COMPACTION", cf_name, str, stat_print);
    }
  } else if (stat_type == HA_ENGINE_META){
    std::string str = "While there's too much information, mysql chose log rather than the screen to print it. check the log please.";
    res |= print_stats(thd, "storage manager", "meta", str, stat_print);
    for (const auto &cf_name : cf_manager.get_cf_names()) {
      xengine::db::ColumnFamilyHandle *cfh;
      bool is_automatic;

      /*
        Only the cf name is important. Whether it was generated automatically
        does not matter, so is_automatic is ignored.
      */
      cfh = cf_manager.get_cf(cf_name.c_str(), "", nullptr, &is_automatic);
      if (cfh == nullptr)
        continue;

      if (! xdb->GetProperty(cfh, "xengine.meta", &str))
        continue;
    }
  }
#endif

  return res;
}

void xengine_register_tx(handlerton *const hton, THD *const thd,
                                       Xdb_transaction *const tx) {
  DBUG_ASSERT(tx != nullptr);

  trans_register_ha(thd, FALSE, xengine_hton, NULL);
  if (my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
    tx->start_stmt();
    trans_register_ha(thd, TRUE, xengine_hton, NULL);
  }
}

/*
    Supporting START TRANSACTION WITH CONSISTENT [XENGINE] SNAPSHOT

    Features:
    1. Supporting START TRANSACTION WITH CONSISTENT SNAPSHOT
    2. Getting current binlog position in addition to #1.

    The second feature is done by START TRANSACTION WITH
    CONSISTENT XENGINE SNAPSHOT. This is Facebook's extension, and
    it works like existing START TRANSACTION WITH CONSISTENT INNODB SNAPSHOT.

    - When not setting engine, START TRANSACTION WITH CONSISTENT SNAPSHOT
    takes both InnoDB and XENGINE snapshots, and both InnoDB and XENGINE
    participate in transaction. When executing COMMIT, both InnoDB and
    XENGINE modifications are committed. Remember that XA is not supported yet,
    so mixing engines is not recommended anyway.

    - When setting engine, START TRANSACTION WITH CONSISTENT.. takes
    snapshot for the specified engine only. But it starts both
    InnoDB and XENGINE transactions.
*/
static int xengine_start_tx_and_assign_read_view(
    handlerton *const hton,          /*!< in: XENGINE handlerton */
    THD *const thd                  /*!< in: MySQL thread handle of the
                                     user for whom the transaction should
                                     be committed */
    )
#if 0
    char *const binlog_file,         /* out: binlog file for last commit */
    ulonglong *const binlog_pos,     /* out: binlog pos for last commit */
    char **gtid_executed,            /* out: Gtids logged until last commit */
    int *const gtid_executed_length) /*out: Length of gtid_executed string */
#endif
{

  ulong const tx_isolation = my_core::thd_tx_isolation(thd);

  if (tx_isolation != ISO_REPEATABLE_READ) {
    // here just a warning, do not return error
    push_warning_printf(thd, Sql_condition::SL_WARNING, HA_ERR_UNSUPPORTED,
                        "Only REPEATABLE READ isolation level is supported "
                        "for START TRANSACTION WITH CONSISTENT SNAPSHOT "
                        "in XEngine Storage Engine. "
                        "Snapshot has not been taken");
  }
  //TODO comment by beilou 5.7 -> 8.0
  // we should do the same action with percona server
  //if (binlog_file) {
  //  if (binlog_pos && mysql_bin_log_is_open())
  //    mysql_bin_log_lock_commits();
  //  else
  //    return HA_EXIT_FAILURE;
  //}

  Xdb_transaction *const tx = get_or_create_tx(thd);
  DBUG_ASSERT(!tx->has_snapshot());
  tx->set_tx_read_only(true);
  xengine_register_tx(hton, thd, tx);
  tx->acquire_snapshot(true);

  //TODO comment by beilou 5.7->8.0
  //we should implement this function like percona server
  //if (binlog_file)
  //  mysql_bin_log_unlock_commits(binlog_file, binlog_pos, gtid_executed,
  //                               gtid_executed_length);

  return HA_EXIT_SUCCESS;
}

/* Dummy SAVEPOINT support. This is needed for long running transactions
 * like mysqldump (https://bugs.mysql.com/bug.php?id=71017).
 * Current SAVEPOINT does not correctly handle ROLLBACK and does not return
 * errors. This needs to be addressed in future versions (Issue#96).
 */
static int xengine_savepoint(handlerton *const hton, THD *const thd,
                             void *const savepoint) {
  return HA_EXIT_SUCCESS;
}

static int xengine_rollback_to_savepoint(handlerton *const hton, THD *const thd,
                                         void *const savepoint) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);
  return tx->rollback_to_savepoint(savepoint);
}

static bool
xengine_rollback_to_savepoint_can_release_mdl(handlerton *const hton,
                                              THD *const thd) {
  return true;
}

static void xengine_post_ddl(THD *thd) {
  ddl_log_manager.post_ddl(thd);
  return;
}

static void xengine_post_recover() {
  ddl_log_manager.recover();
  return;
}

#if 0 //TODO unused function, we comment this, from 5.7 -> 8.0
/*
  This is called for INFORMATION_SCHEMA
*/
static void xengine_update_table_stats(
    /* per-table stats callback */
    void (*cb)(const char *db, const char *tbl, bool is_partition,
               my_io_perf_t *r, my_io_perf_t *w, my_io_perf_t *r_blob,
               my_io_perf_t *r_primary, my_io_perf_t *r_secondary,
               page_stats_t *page_stats, comp_stats_t *comp_stats,
               int n_lock_wait, int n_lock_wait_timeout, const char *engine)) {
  my_io_perf_t io_perf_read;
  my_io_perf_t io_perf;
  page_stats_t page_stats;
  comp_stats_t comp_stats;
  std::vector<std::string> tablenames;

  /*
    Most of these are for innodb, so setting them to 0.
    TODO: possibly separate out primary vs. secondary index reads
   */
  memset(&io_perf, 0, sizeof(io_perf));
  memset(&page_stats, 0, sizeof(page_stats));
  memset(&comp_stats, 0, sizeof(comp_stats));

  tablenames = xdb_open_tables.get_table_names();

  for (const auto &it : tablenames) {
    Xdb_table_handler *table_handler;
    std::string str, dbname, tablename, partname;
    char dbname_sys[NAME_LEN + 1];
    char tablename_sys[NAME_LEN + 1];
    bool is_partition;

    if (xdb_normalize_tablename(it, &str)) {
      /* Function needs to return void because of the interface and we've
       * detected an error which shouldn't happen. There's no way to let
       * caller know that something failed.
      */
      SHIP_ASSERT(false);
      return;
    }

    if (xdb_split_normalized_tablename(str, &dbname, &tablename, &partname)) {
      continue;
    }

    is_partition = (partname.size() != 0);

    table_handler = xdb_open_tables.get_table_handler(it.c_str());
    if (table_handler == nullptr) {
      continue;
    }

    /*
      Convert from xengine timer to mysql timer. XENGINE values are
      in nanoseconds, but table statistics expect the value to be
      in my_timer format.
     */
    xdb_open_tables.release_table_handler(table_handler);

    /*
      Table stats expects our database and table name to be in system encoding,
      not filename format. Convert before calling callback.
     */
    my_core::filename_to_tablename(dbname.c_str(), dbname_sys,
                                   sizeof(dbname_sys));
    my_core::filename_to_tablename(tablename.c_str(), tablename_sys,
                                   sizeof(tablename_sys));
    (*cb)(dbname_sys, tablename_sys, is_partition, &io_perf_read, &io_perf,
          &io_perf, &io_perf, &io_perf, &page_stats, &comp_stats, 0, 0,
          xengine_hton_name);
  }
}

static xengine::common::Status check_xengine_options_compatibility(
    const char *const dbpath, const xengine::common::Options &main_opts,
    const std::vector<xengine::db::ColumnFamilyDescriptor> &cf_descr) {
  DBUG_ASSERT(xengine_datadir != nullptr);

  xengine::common::DBOptions loaded_db_opt;
  std::vector<xengine::db::ColumnFamilyDescriptor> loaded_cf_descs;
  xengine::common::Status status = LoadLatestOptions(dbpath, xengine::util::Env::Default(),
                                             &loaded_db_opt, &loaded_cf_descs);

  // If we're starting from scratch and there are no options saved yet then this
  // is a valid case. Therefore we can't compare the current set of options to
  // anything.
  if (status.IsNotFound()) {
    return xengine::common::Status::OK();
  }

  if (!status.ok()) {
    return status;
  }

  if (loaded_cf_descs.size() != cf_descr.size()) {
    return xengine::common::Status::NotSupported("Mismatched size of sub table "
                                         "descriptors.");
  }

  // Please see XENGINE documentation for more context about why we need to set
  // user-defined functions and pointer-typed options manually.
  for (size_t i = 0; i < loaded_cf_descs.size(); i++) {
    loaded_cf_descs[i].options.compaction_filter =
        cf_descr[i].options.compaction_filter;
    loaded_cf_descs[i].options.compaction_filter_factory =
        cf_descr[i].options.compaction_filter_factory;
    loaded_cf_descs[i].options.comparator = cf_descr[i].options.comparator;
    loaded_cf_descs[i].options.memtable_factory =
        cf_descr[i].options.memtable_factory;
    loaded_cf_descs[i].options.merge_operator =
        cf_descr[i].options.merge_operator;
    loaded_cf_descs[i].options.prefix_extractor =
        cf_descr[i].options.prefix_extractor;
    loaded_cf_descs[i].options.table_factory =
        cf_descr[i].options.table_factory;
  }

  // This is the essence of the function - determine if it's safe to open the
  // database or not.
  status = CheckOptionsCompatibility(dbpath, xengine::util::Env::Default(), main_opts,
                                     loaded_cf_descs);

  return status;
}
#endif


/** Perform post-commit/rollback cleanup after DDL statement.
@param[in,out]	thd	connection thread */
static void xengine_post_ddl(THD *thd);

/** Perform post-commit/rollback before instance startup.*/
static void xengine_post_recover();

/*
  Storage Engine initialization function, invoked when plugin is loaded.
*/
static int xengine_init_func(void *const p) {
  DBUG_ENTER_FUNC();

  // Validate the assumption about the size of XENGINE_SIZEOF_HIDDEN_PK_COLUMN.
  static_assert(sizeof(longlong) == 8, "Assuming that longlong is 8 bytes.");

#ifdef HAVE_PSI_INTERFACE
  init_xengine_psi_keys();
#endif

  xengine_hton = (handlerton *)p;
  mysql_mutex_init(xdb_psi_open_tbls_mutex_key, &xdb_open_tables.m_mutex,
                   MY_MUTEX_INIT_FAST);
#ifdef HAVE_PSI_INTERFACE
  xdb_bg_thread.init(xdb_signal_bg_psi_mutex_key, xdb_signal_bg_psi_cond_key);
  xdb_drop_idx_thread.init(xdb_signal_drop_idx_psi_mutex_key,
                           xdb_signal_drop_idx_psi_cond_key);
#else
  xdb_bg_thread.init();
  xdb_drop_idx_thread.init();
#endif
  mysql_mutex_init(xdb_collation_data_mutex_key, &xdb_collation_data_mutex,
                   MY_MUTEX_INIT_FAST);
  mysql_mutex_init(xdb_mem_cmp_space_mutex_key, &xdb_mem_cmp_space_mutex,
                   MY_MUTEX_INIT_FAST);

#if defined(HAVE_PSI_INTERFACE)
  xdb_collation_exceptions =
      new Regex_list_handler(key_rwlock_collation_exception_list);
#else
  xdb_collation_exceptions = new Regex_list_handler();
#endif

  mysql_mutex_init(xdb_sysvars_psi_mutex_key, &xdb_sysvars_mutex,
                   MY_MUTEX_INIT_FAST);
 // xdb_open_tables.init_hash();
  Xdb_transaction::init_mutex();

  xengine_hton->state = SHOW_OPTION_YES;
  xengine_hton->create = xengine_create_handler;
  xengine_hton->close_connection = xengine_close_connection;
  xengine_hton->prepare = xengine_prepare;
  xengine_hton->commit_by_xid = xengine_commit_by_xid;
  xengine_hton->rollback_by_xid = xengine_rollback_by_xid;
  xengine_hton->recover = xengine_recover;
  xengine_hton->commit = xengine_commit;
  //TODO comment beilou 5.7->8.0 we will fix async commit later
  //xengine_hton->commit2 = xengine_commit2;
  xengine_hton->rollback = xengine_rollback;
  xengine_hton->db_type = DB_TYPE_XENGINE;
  xengine_hton->show_status = xengine_show_status;
  xengine_hton->start_consistent_snapshot =
      xengine_start_tx_and_assign_read_view;
  xengine_hton->savepoint_set = xengine_savepoint;
  xengine_hton->savepoint_rollback = xengine_rollback_to_savepoint;
  xengine_hton->savepoint_rollback_can_release_mdl =
      xengine_rollback_to_savepoint_can_release_mdl;
  //TODO comment by beilou, there is no update_table_stas in 8.0
  //     we will
  //xengine_hton->update_table_stats = xengine_update_table_stats;
  xengine_hton->flush_logs = xengine_flush_wal;
  xengine_hton->is_supported_system_table = xengine_is_supported_system_table;
  // xengine_hton->replace_native_transaction_in_thd = xengine_replace_tx_in_thd;
  xengine_hton->flags = HTON_TEMPORARY_NOT_SUPPORTED |
                        HTON_SUPPORTS_EXTENDED_KEYS | HTON_CAN_RECREATE |
                        HTON_SUPPORTS_ATOMIC_DDL | HTON_SUPPORTS_RECYCLE_BIN;

  xengine_hton->post_recover = xengine_post_recover;
  xengine_hton->post_ddl = xengine_post_ddl;

  xengine_hton->data = xengine_api_cb;

  //DBUG_ASSERT(!mysqld_embedded);

  xengine_stats = xengine::monitor::CreateDBStatistics();
  xengine_db_options.statistics = xengine_stats;

  if (xengine_row_cache_size > 0) {
    // set row_cache
    xengine_db_options.row_cache =
        xengine::cache::NewRowCache(xengine_row_cache_size, -1, false, 0, 0,
            xengine::memory::ModId::kRowCache, true);
  }
  if (xengine_rate_limiter_bytes_per_sec != 0) {
    xengine_rate_limiter.reset(
        xengine::util::NewGenericRateLimiter(xengine_rate_limiter_bytes_per_sec));
    xengine_db_options.rate_limiter = xengine_rate_limiter;
  }

  //xengine_db_options.delayed_write_rate = xengine_delayed_write_rate;

  int log_init_ret = 0;
  auto log_level = get_xengine_log_level(log_error_verbosity);
  // according to setup_error_log() in sql/mysqld.cc
  // log_error_dest is set to "stderr" if log_errors_to_file is false
  // bool log_errors_to_file =
  //     !is_help_or_validate_option() && (log_error_dest != disabled_my_option))
  // for example, run exec mysqld in some MTR cases
  if ((NULL != log_error_dest) && (0 != strcmp(log_error_dest, "stderr"))) {
    // using redirected stderr, refer to open_error_log in sql/log.cc
    fsync(STDERR_FILENO);
    log_init_ret = xengine::logger::Logger::get_log().init(STDERR_FILENO, log_level);
  } else {
    // Write XEngine log to a file
    std::ostringstream oss;
    oss << xengine_datadir << "/Log";
    log_init_ret = xengine::logger::Logger::get_log().init(oss.str().c_str(),
                                                      log_level,
                                                      256 * 1024 * 1024);
  }
  if (0 != log_init_ret) {
    sql_print_information("Failed to initialize XEngine logger at handler level,"
                          " DB:Open will init it with %s/Log", xengine_datadir);
  }

  xengine_db_options.wal_dir = xengine_wal_dir;

  xengine_db_options.wal_recovery_mode =
      static_cast<xengine::common::WALRecoveryMode>(xengine_wal_recovery_mode);
  xengine_db_options.parallel_wal_recovery = xengine_parallel_wal_recovery;
  xengine_db_options.parallel_recovery_thread_num = xengine_parallel_recovery_thread_num;

#if 0 // DEL-SYSVAR
  xengine_db_options.access_hint_on_compaction_start =
      static_cast<xengine::common::Options::AccessHint>(
          xengine_access_hint_on_compaction_start);
#endif

  if (xengine_db_options.allow_mmap_reads &&
      xengine_db_options.use_direct_reads) {
    // allow_mmap_reads implies !use_direct_reads and XENGINE will not open if
    // mmap_reads and direct_reads are both on.   (NO_LINT_DEBUG)
    sql_print_error("XEngine: Can't enable both use_direct_reads "
                    "and allow_mmap_reads\n");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  if (xengine_db_options.allow_mmap_writes &&
      xengine_db_options.use_direct_io_for_flush_and_compaction) {
    // See above comment for allow_mmap_reads. (NO_LINT_DEBUG)
    sql_print_error("XEngine: Can't enable both use_direct_writes "
                    "and allow_mmap_writes\n");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  std::vector<std::string> cf_names;
  xengine::common::Status status;
  status = xengine::db::DB::ListColumnFamilies(xengine_db_options, xengine_datadir,
                                           &cf_names);
  if (!status.ok()) {
    /*
      When we start on an empty datadir, ListColumnFamilies returns IOError,
      and XENGINE doesn't provide any way to check what kind of error it was.
      Checking system errno happens to work right now.
    */
    if (status.IsIOError() && errno == ENOENT) {
      sql_print_information("XEngine: Got ENOENT when listing column families");
      sql_print_information(
          "XEngine:   assuming that we're creating a new database");
    } else {
      std::string err_text = status.ToString();
      sql_print_error("XEngine: Error listing column families: %s",
                      err_text.c_str());
      xdb_open_tables.free_hash();
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
  } else
    sql_print_information("XEngine: %ld column families found",
                          cf_names.size());

  std::vector<xengine::db::ColumnFamilyDescriptor> cf_descr;
  std::vector<xengine::db::ColumnFamilyHandle *> cf_handles;

#if 0 // DEL-SYSVAR
  xengine_tbl_options.index_type =
      (xengine::table::BlockBasedTableOptions::IndexType)xengine_index_type;
#endif

  if (!xengine_tbl_options.no_block_cache) {
    xengine_tbl_options.block_cache =
        xengine::cache::NewLRUCache(xengine_block_cache_size, -1, false, 0.1, 0.375,
            xengine::memory::ModId::kDefaultBlockCache);
  }
  // Using newer ExtentBasedTable format version for reuse of block and SST.
  xengine_tbl_options.format_version = 3;

  if (xengine_collect_sst_properties) {
    properties_collector_factory =
        std::make_shared<Xdb_tbl_prop_coll_factory>(&ddl_manager);

    xengine_set_compaction_options(nullptr, nullptr, nullptr, nullptr);

    XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

    DBUG_ASSERT(xengine_table_stats_sampling_pct <=
                XDB_TBL_STATS_SAMPLE_PCT_MAX);
    properties_collector_factory->SetTableStatsSamplingPct(
        xengine_table_stats_sampling_pct);

    XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
  }

#if 0 // DEL-SYSVAR
  if (xengine_persistent_cache_size > 0) {
    std::shared_ptr<xengine::cache::PersistentCache> pcache;
    xengine::cache::NewPersistentCache(
        xengine::util::Env::Default(), std::string(xengine_persistent_cache_path),
        xengine_persistent_cache_size, true, &pcache);
    xengine_tbl_options.persistent_cache = pcache;
  } else if (strlen(xengine_persistent_cache_path)) {
    sql_print_error("XEngine: Must specify xengine_persistent_cache_size");
    DBUG_RETURN(1);
  }
  if (nullptr != xengine_filter_policy &&
      !xengine::common::GetTableFilterPolicy(
          std::string(xengine_filter_policy),
          &xengine_tbl_options.filter_policy)) {
    sql_print_error("XEngine: Failed to parse input filter_policy");
    DBUG_RETURN(1);
  }
#endif
  // set default value for some internal options
  xengine_tbl_options.cache_index_and_filter_blocks = true;
  xengine_tbl_options.cache_index_and_filter_blocks_with_high_priority = true;
  xengine_tbl_options.pin_l0_filter_and_index_blocks_in_cache = false;
  xengine_default_cf_options.arena_block_size = DEFAULT_XENGINE_ARENA_BLOCK_SIZE;
  xengine_default_cf_options.optimize_filters_for_hits = DEFAULT_XENGINE_OPTIMIZE_FILTERS_FOR_HITS;
  // xengine_db_options.table_cache_numshardbits =
  // DEFAULT_XENGINE_TABLE_CACHE_NUMSHARDBITS;
  xengine_db_options.allow_concurrent_memtable_write = true;
  xengine_db_options.use_direct_write_for_wal = false;
  xengine_db_options.concurrent_writable_file_buffer_num =
      DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_NUMBER;
  xengine_db_options.concurrent_writable_file_single_buffer_size =
      DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_SINGLE_BUFFER_SIZE; // 64KB
  xengine_db_options.concurrent_writable_file_buffer_switch_limit =
      DEFAULT_XENGINE_CONCURRENT_WRITABLE_FILE_BUFFER_SWITCH_LIMIT; // 32 KB
  // bug #21280399, avoid warning message in error log about:
  // level0_stop_writes_trigger >= level0_slowdown_writes_trigger >= level0_file_num_compaction_trigger
  if (xengine_default_cf_options.level0_slowdown_writes_trigger <
      xengine_default_cf_options.level0_file_num_compaction_trigger) {
    xengine_default_cf_options.level0_slowdown_writes_trigger =
        xengine_default_cf_options.level0_file_num_compaction_trigger;
  }
  if (xengine_default_cf_options.level0_stop_writes_trigger <
      xengine_default_cf_options.level0_slowdown_writes_trigger) {
    xengine_default_cf_options.level0_stop_writes_trigger =
      xengine_default_cf_options.level0_slowdown_writes_trigger;
  }
#if 0 // DEL-SYSVAR
  if (nullptr != xengine_prefix_extractor &&
       !xengine::common::GetPrefixExtractor(
          std::string(xengine_prefix_extractor),
          &xengine_default_cf_options.prefix_extractor)) {
    sql_print_error("XEngine: Failed to parse prefix extractor");
    DBUG_RETURN(1);
  }
#endif
  if (nullptr != xengine_cf_compression_per_level &&
      !xengine::common::GetVectorCompressionType(
          std::string(xengine_cf_compression_per_level),
          &xengine_default_cf_options.compression_per_level)) {
    sql_print_error("XEngine: Failed to parse input compression_per_level");
    DBUG_RETURN(1);
  }
  if (nullptr != xengine_cf_compression_opts &&
      !xengine::common::GetCompressionOptions(
          std::string(xengine_cf_compression_opts),
          &xengine_default_cf_options.compression_opts)) {
    sql_print_error("XEngine: Failed to parse input compress_opts");
    DBUG_RETURN(1);
  }
// #if 0 // DEL-SYSVAR
  if (nullptr != xengine_cf_memtable_options &&
      !xengine::common::GetMemTableFactory(
          std::string(xengine_cf_memtable_options),
          &xengine_default_cf_options.memtable_factory)) {
    sql_print_error("XEngine: Failed to parse input memtable options");
    DBUG_RETURN(1);
  }
// #endif

  if (!xengine_cf_options_map.init(
          xengine_tbl_options, properties_collector_factory,
          xengine_default_cf_options, xengine_override_cf_options)) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Failed to initialize CF options map.");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  /*
    If there are no column families, we're creating the new database.
    Create one column family named "default".
  */
  if (cf_names.size() == 0)
    cf_names.push_back(DEFAULT_CF_NAME);

  std::vector<int> compaction_enabled_cf_indices;
  sql_print_information("XEngine: Column Families at start:");
  for (size_t i = 0; i < cf_names.size(); ++i) {
    xengine::common::ColumnFamilyOptions opts;
    xengine_cf_options_map.get_cf_options(cf_names[i], &opts);

    sql_print_information("  cf=%s", cf_names[i].c_str());
    sql_print_information("    write_buffer_size=%ld", opts.write_buffer_size);
    //sql_print_information("    target_file_size_base=%" PRIu64,
    sql_print_information("    target_file_size_base= %lu",
                          opts.target_file_size_base);

    /*
      Temporarily disable compactions to prevent a race condition where
      compaction starts before compaction filter is ready.
    */
    if (!opts.disable_auto_compactions) {
      compaction_enabled_cf_indices.push_back(i);
      opts.disable_auto_compactions = true;
    }
    cf_descr.push_back(xengine::db::ColumnFamilyDescriptor(cf_names[i], opts));

  }

  xengine::common::Options main_opts(xengine_db_options,
                             xengine_cf_options_map.get_defaults());
  // 1 more bg thread for create extent space
  main_opts.env->SetBackgroundThreads(main_opts.max_background_flushes + 1,
                                      xengine::util::Env::Priority::HIGH);
  // 1 more bg thread for delete extent space, 1 more for cache purge, 1 more for free memtable
  main_opts.env->SetBackgroundThreads(main_opts.max_background_compactions +
                                      main_opts.max_background_dumps + 3,
                                      xengine::util::Env::Priority::LOW);

  xengine::util::TransactionDBOptions tx_db_options;
  tx_db_options.transaction_lock_timeout = 2; // 2 seconds
  tx_db_options.custom_mutex_factory = std::make_shared<Xdb_mutex_factory>();

  status = xengine::util::TransactionDB::Open(
      main_opts, tx_db_options, xengine_datadir, cf_descr, &cf_handles, &xdb);

  if (!status.ok()) {
    std::string err_text = status.ToString();
    sql_print_error("XEngine: Error opening instance: %s", err_text.c_str());
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }
  cf_manager.init(&xengine_cf_options_map, &cf_handles, &ddl_log_manager);

  if (dict_manager.init(xdb->GetBaseDB(), xengine_default_cf_options)) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Failed to initialize data dictionary.");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  if (binlog_manager.init(&dict_manager)) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Failed to initialize binlog manager.");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  THD *const thd = my_core::thd_get_current_thd();
  if (ddl_manager.init(thd, &dict_manager)) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Failed to initialize DDL manager.");
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  if (ddl_log_manager.init(&dict_manager, &ddl_manager, &cf_manager, &xdb_drop_idx_thread)) {
    sql_print_error("XEngine: Failed to initialize ddl_log manager.");
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  Xdb_sst_info::init(xdb);

  /*
    Enable auto compaction, things needed for compaction filter are finished
    initializing
  */
  std::vector<xengine::db::ColumnFamilyHandle *> compaction_enabled_cf_handles;
  compaction_enabled_cf_handles.reserve(compaction_enabled_cf_indices.size());
  for (const auto &index : compaction_enabled_cf_indices) {
    compaction_enabled_cf_handles.push_back(cf_handles[index]);
  }

  //status = xdb->EnableAutoCompaction(compaction_enabled_cf_handles);
  //allow auto compactions for all subtable
  xdb->SetOptions({{"disable_auto_compactions", "false"}});

  if (!status.ok()) {
    const std::string err_text = status.ToString();
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Error enabling compaction: %s", err_text.c_str());
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  auto err = xdb_bg_thread.create_thread(BG_THREAD_NAME
#ifdef HAVE_PSI_INTERFACE
                                         ,
                                         xdb_background_psi_thread_key
#endif
                                         );
  if (err != 0) {
    sql_print_error("XEngine: Couldn't start the background thread: (errno=%d)",
                    err);
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  err = xdb_drop_idx_thread.create_thread(INDEX_THREAD_NAME
#ifdef HAVE_PSI_INTERFACE
                                          ,
                                          xdb_drop_idx_psi_thread_key
#endif
                                          );
  if (err != 0) {
    sql_print_error("XEngine: Couldn't start the drop index thread: (errno=%d)",
                    err);
    xdb_open_tables.free_hash();
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  xdb_set_collation_exception_list(xengine_strict_collation_exceptions);

  if (xengine_pause_background_work) {
    xdb->PauseBackgroundWork();
  }

  if (xengine::common::Status::kOk != BackupSnapshot::create(backup_instance)) {
    sql_print_error("XEngine: Failed to create backup instance.");
    DBUG_RETURN(HA_EXIT_FAILURE);
  } else if (nullptr != backup_instance) {
    // remove hard links in backup tmp dir if exists
    backup_instance->init(xdb);
    backup_instance->release_snapshots(xdb);
  }

  // NO_LINT_DEBUG
  sql_print_information("XEngine: global statistics using %s indexer",
                        STRINGIFY_ARG(XDB_INDEXER));
#if defined(HAVE_SCHED_GETCPU)
  if (sched_getcpu() == -1) {
    // NO_LINT_DEBUG
    sql_print_information(
        "XEngine: sched_getcpu() failed - "
        "global statistics will use thread_id_indexer_t instead");
  }
#endif

  sql_print_information("XEngine instance opened");
  xdb_initialized = true;
  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  Storage Engine deinitialization function, invoked when plugin is unloaded.
*/

static int xengine_done_func(void *const p) {
  DBUG_ENTER_FUNC();

  int error = 0;

  // signal the drop index thread to stop
  xdb_drop_idx_thread.signal(true);

  // Flush all memtables for not losing data, even if WAL is disabled.
  xengine_flush_all_memtables();

  // Stop all xengine background work
  xengine::common::CancelAllBackgroundWork(xdb->GetBaseDB(), true);

  // Signal the background thread to stop and to persist all stats collected
  // from background flushes and compactions. This will add more keys to a new
  // memtable, but since the memtables were just flushed, it should not trigger
  // a flush that can stall due to background threads being stopped. As long
  // as these keys are stored in a WAL file, they can be retrieved on restart.
  xdb_bg_thread.signal(true);

  // Wait for the background thread to finish.
  auto err = xdb_bg_thread.join();
  if (err != 0) {
    // We'll log the message and continue because we're shutting down and
    // continuation is the optimal strategy.
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Couldn't stop the background thread: (errno=%d)",
                    err);
  }

  // Wait for the drop index thread to finish.
  err = xdb_drop_idx_thread.join();
  if (err != 0) {
    // NO_LINT_DEBUG
    sql_print_error("XEngine: Couldn't stop the index thread: (errno=%d)", err);
  }

  if (xdb_open_tables.m_hash.size()) {
    // Looks like we are getting unloaded and yet we have some open tables
    // left behind.
    error = 1;
  }

  xdb_open_tables.free_hash();
  mysql_mutex_destroy(&xdb_open_tables.m_mutex);
  mysql_mutex_destroy(&xdb_sysvars_mutex);

  delete xdb_collation_exceptions;
  mysql_mutex_destroy(&xdb_collation_data_mutex);
  mysql_mutex_destroy(&xdb_mem_cmp_space_mutex);

  Xdb_transaction::term_mutex();

  for (auto &it : xdb_collation_data) {
    delete it;
    it = nullptr;
  }

  ddl_manager.cleanup();
  binlog_manager.cleanup();
  dict_manager.cleanup();
  cf_manager.cleanup();

  delete xdb;
  xdb = nullptr;

// Disown the cache data since we're shutting down.
// This results in memory leaks but it improved the shutdown time.
// Don't disown when running under valgrind
#ifndef HAVE_purify
  if (xengine_tbl_options.block_cache) {
    xengine_tbl_options.block_cache->DisownData();
    xengine_tbl_options.block_cache->destroy();
  }
  if (xengine_db_options.row_cache) {
    xengine_db_options.row_cache->DisownData();
    xengine_db_options.row_cache->destroy();
  }
#else /* HAVE_purify */
  if (xengine_tbl_options.block_cache) {
    xengine_tbl_options.block_cache->EraseUnRefEntries();
    xengine_tbl_options.block_cache->destroy();
  }
  if (xengine_db_options.row_cache) {
    xengine_db_options.row_cache->destroy();
  }
#endif

  DBUG_RETURN(error);
}

/**
  @brief
  Example of simple lock controls. The "table_handler" it creates is a
  structure we will pass to each ha_xengine handler. Do you have to have
  one of these? Well, you have pieces that are used for locking, and
  they are needed to function.
*/

Xdb_table_handler *
Xdb_open_tables_map::get_table_handler(const char *const table_name) {

  DBUG_ASSERT(table_name != nullptr);

  const std::string s_table_name(table_name);

  uint length;
  length = s_table_name.length();

  // First, look up the table in the hash map.
  XDB_MUTEX_LOCK_CHECK(m_mutex);
  Xdb_table_handler *table_handler = nullptr;
  const auto &it = m_hash.find(s_table_name);
  if (it != m_hash.end()) {
    table_handler = it->second;
  } else {
    // Since we did not find it in the hash map, attempt to create and add it
    // to the hash map.
    char *tmp_name;
    if (!(table_handler = reinterpret_cast<Xdb_table_handler *>(my_multi_malloc(
              PSI_NOT_INSTRUMENTED,
              MYF(MY_WME | MY_ZEROFILL), &table_handler, sizeof(*table_handler),
              &tmp_name, length + 1, NullS)))) {
      // Allocating a new Xdb_table_handler and a new table name failed.
      XDB_MUTEX_UNLOCK_CHECK(m_mutex);
      return nullptr;
    }

    table_handler->m_ref_count = 0;
    table_handler->m_table_name_length = length;
    table_handler->m_table_name = tmp_name;
    my_stpmov(table_handler->m_table_name, table_name);

    m_hash.insert({s_table_name, table_handler});

    thr_lock_init(&table_handler->m_thr_lock);
  }
  DBUG_ASSERT(table_handler->m_ref_count >= 0);
  table_handler->m_ref_count++;

  XDB_MUTEX_UNLOCK_CHECK(m_mutex);

  return table_handler;
}

std::vector<std::string> xdb_get_open_table_names(void) {
  return xdb_open_tables.get_table_names();
}

std::vector<std::string> Xdb_open_tables_map::get_table_names(void) const {
  ulong i = 0;
  const Xdb_table_handler *table_handler;
  std::vector<std::string> names;

  XDB_MUTEX_LOCK_CHECK(m_mutex);
  for (const auto &it : m_hash) {
    table_handler = it.second;
    DBUG_ASSERT(table_handler != nullptr);
    names.push_back(table_handler->m_table_name);
    i++;
  }
  DBUG_ASSERT(i == m_hash.size());
  XDB_MUTEX_UNLOCK_CHECK(m_mutex);

  return names;
}


/*
  Inspired by innobase_get_int_col_max_value from InnoDB. This returns the
  maximum value a type can take on.
*/
static ulonglong xdb_get_int_col_max_value(const Field *field) {
  ulonglong max_value = 0;
  switch (field->key_type()) {
  case HA_KEYTYPE_BINARY:
    max_value = 0xFFULL;
    break;
  case HA_KEYTYPE_INT8:
    max_value = 0x7FULL;
    break;
  case HA_KEYTYPE_USHORT_INT:
    max_value = 0xFFFFULL;
    break;
  case HA_KEYTYPE_SHORT_INT:
    max_value = 0x7FFFULL;
    break;
  case HA_KEYTYPE_UINT24:
    max_value = 0xFFFFFFULL;
    break;
  case HA_KEYTYPE_INT24:
    max_value = 0x7FFFFFULL;
    break;
  case HA_KEYTYPE_ULONG_INT:
    max_value = 0xFFFFFFFFULL;
    break;
  case HA_KEYTYPE_LONG_INT:
    max_value = 0x7FFFFFFFULL;
    break;
  case HA_KEYTYPE_ULONGLONG:
    max_value = 0xFFFFFFFFFFFFFFFFULL;
    break;
  case HA_KEYTYPE_LONGLONG:
    max_value = 0x7FFFFFFFFFFFFFFFULL;
    break;
  case HA_KEYTYPE_FLOAT:
    max_value = 0x1000000ULL;
    break;
  case HA_KEYTYPE_DOUBLE:
    max_value = 0x20000000000000ULL;
    break;
  default:
    abort();
  }

  return max_value;
}

bool ha_xengine::is_using_prohibited_gap_locks(TABLE *table,
                                            bool using_full_primary_key)
{
  THD* thd = table->in_use;
  thr_lock_type lock_type = table->reginfo.lock_type;
  const char *table_name = table->s->table_name.str;
  //TODO implement this function like MySQL 8.0
  if (!using_full_primary_key
      && ht
      && (ht->db_type == DB_TYPE_INNODB || ht->db_type == DB_TYPE_XENGINE)
      && !thd->rli_slave
      //&& (thd->variables.gap_lock_raise_error
      //    || thd->variables.gap_lock_write_log)
      && (thd->lex->table_count >= 2 || thd->in_multi_stmt_transaction_mode())
      //&& can_hold_locks_on_trans(thd, lock_type)
      /*&& !gap_lock_exceptions->matches(table_name)*/) //COMMENT temporarily
  {
    //gap_lock_log_write(thd, COM_QUERY, thd->query().str, thd->query().length);
    //if (thd->variables.gap_lock_raise_error)
    //{
    //  my_printf_error(ER_UNKNOWN_ERROR,
    //                  "Using Gap Lock without full unique key in multi-table "
    //                  "or multi-statement transactions is not "
    //                  "allowed. You need either 1: Execute 'SET SESSION "
    //                  "gap_lock_raise_error=0' if you are sure that "
    //                  "your application does not rely on Gap Lock. "
    //                  "2: Rewrite queries to use "
    //                  "all unique key columns in WHERE equal conditions. "
    //                  "3: Rewrite to single-table, single-statement "
    //                  "transaction.  Query: %s",
    //                  MYF(0), thd->query().length);
    //  return true;
    //}
  }
  return false;
}

bool ha_xengine::is_using_full_key(key_part_map keypart_map,
                                uint actual_key_parts)
{
  return (keypart_map == HA_WHOLE_KEY) ||
         (keypart_map == ((key_part_map(1) << actual_key_parts)
                        - 1));
}

bool ha_xengine::is_using_full_unique_key(uint index,
                                        key_part_map keypart_map,
                                        enum ha_rkey_function find_flag)
{
  return (is_using_full_key(keypart_map,
                            table->key_info[index].actual_key_parts)
          && find_flag == HA_READ_KEY_EXACT
          && (index == table->s->primary_key
              || (table->key_info[index].flags & HA_NOSAME)));
}

void ha_xengine::load_auto_incr_value() {

  //TODO this api should implemented like mysql 8.0
  const int save_active_index = active_index;
  active_index = table->s->next_number_index;
  //const uint8 save_table_status = table->status;

  /*
    load_auto_incr_value() may be called by statements that
    do not execute implicit commits (i.e. SHOW CREATE TABLE).
    index_last() creates a snapshot. When a snapshot is created
    here, it has to be released as well. (GitHub issue#189)
  */
  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();

  // Do a lookup. We only need index column, so it should be index-only.
  // (another reason to make it index-only is that table->read_set is
  //  not set appropriately and non-index-only lookup will not read the value)
  const bool save_keyread_only = m_keyread_only;
  m_keyread_only = true;

  if (!index_last(table->record[0]))
    update_auto_incr_val();

  m_keyread_only = save_keyread_only;
  if (is_new_snapshot) {
    tx->release_snapshot();
  }

  //table->status = save_table_status;
  active_index = save_active_index;

  /*
    Do what ha_xengine::index_end() does.
    (Why don't we use index_init/index_end? class handler defines index_init
     as private, for some reason).
  */
  release_scan_iterator();
}

/* Get PK value from table->record[0]. */
/*
  TODO(alexyang): No existing support for auto_increment on non-pk columns, see
  end of ha_xengine::create. Also see opened issue here:
  https://github.com/facebook/mysql-5.6/issues/153
*/
void ha_xengine::update_auto_incr_val() {
  Field *field;
  ulonglong new_val, max_val;
  field = table->key_info[table->s->next_number_index].key_part[0].field;
  max_val = xdb_get_int_col_max_value(field);

  my_bitmap_map *const old_map =
      dbug_tmp_use_all_columns(table, table->read_set);

  new_val = field->val_int();
  if (new_val != max_val) {
    new_val++;
  }

  dbug_tmp_restore_column_map(table->read_set, old_map);

  if (new_val <= max_val) {
    ulonglong auto_incr_val = m_tbl_def->m_auto_incr_val;
    while (auto_incr_val < new_val &&
         !m_tbl_def->m_auto_incr_val.compare_exchange_weak(auto_incr_val,
                                                           new_val)) {
    // Do nothing - just loop until auto_incr_val is >= new_val or
    // we successfully set it
    }
  }
}

int ha_xengine::load_hidden_pk_value() {
  const int save_active_index = active_index;
  active_index = m_tbl_def->m_key_count - 1;
  //const uint8 save_table_status = table->status;

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();

  // Do a lookup.
  if (!index_last(table->record[0])) {
    /*
      Decode PK field from the key
    */
    longlong hidden_pk_id = 0;
    if (read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
      if (is_new_snapshot) {
        tx->release_snapshot();
      }
      return HA_ERR_INTERNAL_ERROR;
    }

    hidden_pk_id++;
    longlong old = m_tbl_def->m_hidden_pk_val;
    while (
        old < hidden_pk_id &&
        !m_tbl_def->m_hidden_pk_val.compare_exchange_weak(old, hidden_pk_id)) {
    }
  }

  if (is_new_snapshot) {
    tx->release_snapshot();
  }

  //table->status = save_table_status;
  active_index = save_active_index;

  release_scan_iterator();

  return HA_EXIT_SUCCESS;
}

/* Get PK value from m_tbl_def->m_hidden_pk_info. */
longlong ha_xengine::update_hidden_pk_val() {
  DBUG_ASSERT(has_hidden_pk(table));
  const longlong new_val = m_tbl_def->m_hidden_pk_val++;
  return new_val;
}

/* Get the id of the hidden pk id from m_last_rowkey */
int ha_xengine::read_hidden_pk_id_from_rowkey(longlong *const hidden_pk_id, const String *key) {
  DBUG_ASSERT(hidden_pk_id != nullptr);
  DBUG_ASSERT(table != nullptr);
  DBUG_ASSERT(has_hidden_pk(table));

  xengine::common::Slice rowkey_slice(key->ptr(), key->length());

  // Get hidden primary key from old key slice
  Xdb_string_reader reader(&rowkey_slice);
  if ((!reader.read(Xdb_key_def::INDEX_NUMBER_SIZE)))
    return HA_EXIT_FAILURE;

  const int length = Field_longlong::PACK_LENGTH;
  const uchar *from = reinterpret_cast<const uchar *>(reader.read(length));
  if (from == nullptr) {
    return HA_EXIT_FAILURE; /* Mem-comparable image doesn't have enough bytes */
  }

  *hidden_pk_id = xdb_netbuf_read_uint64(&from);
  return HA_EXIT_SUCCESS;
}

/**
  @brief
  Free lock controls. We call this whenever we close a table. If the table had
  the last reference to the table_handler, then we free the memory associated
  with it.
*/

void Xdb_open_tables_map::release_table_handler(
    Xdb_table_handler *const table_handler) {
  XDB_MUTEX_LOCK_CHECK(m_mutex);

  DBUG_ASSERT(table_handler != nullptr);
  DBUG_ASSERT(table_handler->m_ref_count > 0);
  if (!--table_handler->m_ref_count) {
    // Last rereference was released. Tear down the hash entry.
    const auto ret MY_ATTRIBUTE((__unused__)) =
        m_hash.erase(std::string(table_handler->m_table_name));
    DBUG_ASSERT(ret == 1); // the hash entry must actually be found and deleted
    my_core::thr_lock_delete(&table_handler->m_thr_lock);
    my_free(table_handler);
  }

  XDB_MUTEX_UNLOCK_CHECK(m_mutex);
}

static handler *xengine_create_handler(my_core::handlerton *const hton,
                                       my_core::TABLE_SHARE *const table_arg,
                                       bool partitioned,
                                       my_core::MEM_ROOT *const mem_root) {
  if (partitioned) {
    //TODO we should implement partion in the future
    my_printf_error(ER_NOT_SUPPORTED_YET,
        "Create partitioned table is not supported yet in xengine.", MYF(0));
    return nullptr;
  }
  return new (mem_root) ha_xengine(hton, table_arg);
}

ha_xengine::ha_xengine(my_core::handlerton *const hton,
                       my_core::TABLE_SHARE *const table_arg)
    : handler(hton, table_arg),
      m_table_handler(nullptr),
      m_scan_it(nullptr),
      m_cmp_end_key(true),
      m_max_packed_sk_len(0),
      m_scan_it_skips_bloom(false),
      m_scan_it_snapshot(nullptr),
      m_tbl_def(nullptr),
      m_fields_no_needed_to_decode(0),
      m_pk_descr(nullptr),
      m_key_descr_arr(nullptr),
      m_pk_can_be_decoded(false),
      m_maybe_unpack_info(false),
      m_pk_tuple(nullptr),
      m_pk_packed_tuple(nullptr),
      m_pk_packed_tuple_old(nullptr),
      m_sk_packed_tuple(nullptr),
      m_end_key_packed_tuple(nullptr),
      m_sk_match_prefix(nullptr),
      m_sk_match_prefix_buf(nullptr),
      m_sk_packed_tuple_old(nullptr),
      m_dup_sk_packed_tuple(nullptr),
      m_dup_sk_packed_tuple_old(nullptr),
      m_pack_buffer(nullptr),
      m_new_pk_descr(nullptr),
      m_new_record(nullptr),
      m_lock_rows(XDB_LOCK_NONE),
      m_keyread_only(FALSE),
      m_bulk_load_tx(nullptr),
      m_encoder_arr(nullptr),
      m_row_checksums_checked(0),
      m_in_rpl_delete_rows(false),
      m_in_rpl_update_rows(false),
      m_force_skip_unique_check(false)
{
  // TODO(alexyang): create a valid PSI_mutex_key for this mutex
  mysql_mutex_init(0, &m_bulk_load_mutex, MY_MUTEX_INIT_FAST);
}

//static const char *ha_xengine_exts[] = {NullS};
//
//const char **ha_xengine::bas_ext() const {
//  DBUG_ENTER_FUNC();
//
//  DBUG_RETURN(ha_xengine_exts);
//}

bool ha_xengine::same_table(const ha_xengine &other) const {
  return m_tbl_def->base_tablename() == other.m_tbl_def->base_tablename();
}

bool ha_xengine::init_with_fields() {
  DBUG_ENTER_FUNC();

  const uint pk = table_share->primary_key;
  if (pk != MAX_KEY) {
    const uint key_parts = table_share->key_info[pk].user_defined_key_parts;
    check_keyread_allowed(pk /*PK*/, key_parts - 1, true);
  } else
    m_pk_can_be_decoded = false;

  cached_table_flags = table_flags();

  DBUG_RETURN(false); /* Ok */
}

bool ha_xengine::rpl_can_handle_stm_event() const noexcept {
  return !(rpl_skip_tx_api_var && !super_read_only);
}

#ifndef NDEBUG
void dbug_append_garbage_at_end(std::string &on_disk_rec) {
  on_disk_rec.append("abc");
}

void dbug_truncate_record(std::string &on_disk_rec) { on_disk_rec.resize(0); }

void dbug_modify_rec_varchar12(std::string &on_disk_rec) {
  std::string res;
  // The record is NULL-byte followed by VARCHAR(10).
  // Put the NULL-byte
  res.append("\0", 1);
  // Then, add a valid VARCHAR(12) value.
  res.append("\xC", 1);
  res.append("123456789ab", 12);

  on_disk_rec.assign(res);
}

void dbug_modify_key_varchar8(String &on_disk_rec) {
  std::string res;
  // The key starts with index number
  res.append(on_disk_rec.ptr(), Xdb_key_def::INDEX_NUMBER_SIZE);

  // Then, a mem-comparable form of a varchar(8) value.
  res.append("ABCDE\0\0\0\xFC", 9);
  on_disk_rec.length(0);
  on_disk_rec.append(res.data(), res.size());
}
#endif



/**
  Convert record from table->record[0] form into a form that can be written
  into xengine.

  @param pk_packed_slice      Packed PK tuple. We need it in order to compute
                              and store its CRC.
  @param packed_rec      OUT  Data slice with record data.
*/

int ha_xengine::convert_record_to_storage_format(
    const xengine::common::Slice &pk_packed_slice,
    Xdb_string_writer *const pk_unpack_info, xengine::common::Slice *const packed_rec) {
  DBUG_ASSERT_IMP(m_maybe_unpack_info, pk_unpack_info);
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_CONVERT_TO);
  m_storage_record.length(0);

  // reserve XENGINE_RECORD_HEADER_LENGTH bytes for future use
  m_storage_record.fill(XENGINE_RECORD_HEADER_LENGTH, 0);
  int field_number_len = 0;
  // store instant ddl flag and field numbers if necessary
  if (m_instant_ddl_info.have_instantly_added_columns()) {
    char *const begin_point = (char *)m_storage_record.ptr();
    begin_point[0] |= INSTANT_DDL_FLAG;
    char field_number_str[2];
    char nullable_bytes_str[2];
    int2store(field_number_str, table->s->fields);
    int2store(nullable_bytes_str, m_null_bytes_in_rec);
    m_storage_record.append(field_number_str ,XENGINE_RECORD_FIELD_NUMBER_LENGTH);
    m_storage_record.append(nullable_bytes_str ,XENGINE_RECORD_NULLABLE_BYTES);
    field_number_len = XENGINE_RECORD_FIELD_NUMBER_LENGTH + XENGINE_RECORD_NULLABLE_BYTES;
  }

  /* All NULL bits are initially 0 */
  String null_bytes_str;
  null_bytes_str.length(0);
  null_bytes_str.fill(m_null_bytes_in_rec, 0);
  m_storage_record.append(null_bytes_str);

  // If a primary key may have non-empty unpack_info for certain values,
  // (m_maybe_unpack_info=TRUE), we write the unpack_info block. The block
  // itself was prepared in Xdb_key_def::pack_record.
  if (m_maybe_unpack_info) {
    m_storage_record.append(reinterpret_cast<char *>(pk_unpack_info->ptr()),
                            pk_unpack_info->get_current_pos());
  }

  for (uint i = 0; i < table->s->fields; i++) {
    /* Don't pack decodable PK key parts */
    if (m_encoder_arr[i].m_storage_type != Xdb_field_encoder::STORE_ALL) {
      continue;
    }

    Field *const field = table->field[i];
    if (m_encoder_arr[i].maybe_null()) {
      char *const data = (char *)m_storage_record.ptr() +
                         XENGINE_RECORD_HEADER_LENGTH +
                         field_number_len; /*0 or XENGINE_RECORD_FIELD_NUMBER_LENGTH*/
      if (field->is_null()) {
        data[m_encoder_arr[i].m_null_offset] |= m_encoder_arr[i].m_null_mask;
        /* Don't write anything for NULL values */
        continue;
      }
    }

    if (m_encoder_arr[i].m_field_type == MYSQL_TYPE_BLOB ||
        m_encoder_arr[i].m_field_type == MYSQL_TYPE_JSON) {
      append_blob_to_storage_format(m_storage_record,
                                    (my_core::Field_blob *)field);
    } else if (m_encoder_arr[i].m_field_type == MYSQL_TYPE_VARCHAR) {
      append_varchar_to_storage_format(m_storage_record,
                                       (Field_varstring *)field);
    } else {
      /* Copy the field data */
      const uint len = field->pack_length_in_rec();
      m_storage_record.append(reinterpret_cast<char *>(field->ptr), len);
    }
  }

  if (should_store_row_debug_checksums()) {
    const uint32_t key_crc32 = my_core::crc32(
        0, xdb_slice_to_uchar_ptr(&pk_packed_slice), pk_packed_slice.size());
    const uint32_t val_crc32 =
        my_core::crc32(0, xdb_mysql_str_to_uchar_str(&m_storage_record),
                       m_storage_record.length());
    uchar key_crc_buf[XDB_CHECKSUM_SIZE];
    uchar val_crc_buf[XDB_CHECKSUM_SIZE];
    xdb_netbuf_store_uint32(key_crc_buf, key_crc32);
    xdb_netbuf_store_uint32(val_crc_buf, val_crc32);
    m_storage_record.append((const char *)&XDB_CHECKSUM_DATA_TAG, 1);
    m_storage_record.append((const char *)key_crc_buf, XDB_CHECKSUM_SIZE);
    m_storage_record.append((const char *)val_crc_buf, XDB_CHECKSUM_SIZE);
  }

  *packed_rec =
      xengine::common::Slice(m_storage_record.ptr(), m_storage_record.length());

  return HA_EXIT_SUCCESS;
}

/*
  @brief
    Setup which fields will be unpacked when reading rows

  @detail
    Two special cases when we still unpack all fields:
    - When this table is being updated (m_lock_rows==XDB_LOCK_WRITE).
    - When @@xengine_verify_row_debug_checksums is ON (In this mode, we need to
  read all
      fields to find whether there is a row checksum at the end. We could skip
      the fields instead of decoding them, but currently we do decoding.)

  @seealso
    ha_xengine::setup_field_converters()
    ha_xengine::convert_record_from_storage_format()
*/

int ha_xengine::setup_read_decoders() {
  // bitmap is cleared on index merge, but it still needs to decode columns
  bool force_decode_all_fields = (m_lock_rows == XDB_LOCK_WRITE || m_verify_row_debug_checksums || bitmap_is_clear_all(table->read_set));

  return setup_read_decoders(this->table, m_encoder_arr, m_decoders_vect, force_decode_all_fields);
}

/*
  setup information for decode xengine record, if force_decode_all_fields is set,
  all fields need to be decoded.
*/
int ha_xengine::setup_read_decoders(const TABLE *table, Xdb_field_encoder *encoder_arr, std::vector<READ_FIELD> &decoders_vect, bool force_decode_all_fields) {
  decoders_vect.clear();

  int last_useful = 0;
  int skip_size = 0;

  for (uint i = 0; i < table->s->fields; i++) {
    // We only need the decoder if the whole record is stored.
    if (encoder_arr[i].m_storage_type != Xdb_field_encoder::STORE_ALL) {
      continue;
    }

    if (force_decode_all_fields ||
        bitmap_is_set(table->read_set, table->field[i]->field_index)) {
      // We will need to decode this field
      decoders_vect.push_back({&encoder_arr[i], true, skip_size});
      last_useful = decoders_vect.size();
      skip_size = 0;
    } else {
      if (m_instant_ddl_info.have_instantly_added_columns() ||
          encoder_arr[i].uses_variable_len_encoding() ||
          encoder_arr[i].maybe_null()) {
        // For variable-length field, we need to read the data and skip it
        // Or if this table hash instantly added column, we must record every field's info here
        decoders_vect.push_back({&encoder_arr[i], false, skip_size});
        skip_size = 0;
      } else {
        // Fixed-width field can be skipped without looking at it.
        // Add appropriate skip_size to the next field.
        skip_size += encoder_arr[i].m_pack_length_in_rec;
      }
    }
  }

  // It could be that the last few elements are varchars that just do
  // skipping. Remove them.
  if (!m_instant_ddl_info.have_instantly_added_columns()) {
    decoders_vect.erase(decoders_vect.begin() + last_useful,
                        decoders_vect.end());
  }
  return HA_EXIT_SUCCESS;
}

int ha_xengine::convert_record_from_storage_format(
    const xengine::common::Slice *key, std::string& retrieved_record,
    uchar *const buf, TABLE* tbl) {
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read1",
                  dbug_append_garbage_at_end(retrieved_record););
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read2",
                  dbug_truncate_record(retrieved_record););
  DBUG_EXECUTE_IF("myx_simulate_bad_row_read3",
                  dbug_modify_rec_varchar12(retrieved_record););

  const xengine::common::Slice retrieved_rec_slice(&retrieved_record.front(),
                                                   retrieved_record.size());
  return convert_record_from_storage_format(key, &retrieved_rec_slice, buf, tbl);
}

int ha_xengine::convert_blob_from_storage_format(
  my_core::Field_blob *const blob,
  Xdb_string_reader *const   reader,
  bool                       decode)
{
  /* Get the number of bytes needed to store length*/
  const uint length_bytes = blob->pack_length() - portable_sizeof_char_ptr;

  const char *data_len_str;
  if (!(data_len_str = reader->read(length_bytes))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  memcpy(blob->ptr, data_len_str, length_bytes);

  const uint32 data_len = blob->get_length(
      reinterpret_cast<const uchar*>(data_len_str), length_bytes, table->s->db_low_byte_first);
  const char *blob_ptr;
  if (!(blob_ptr = reader->read(data_len))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (decode) {
    // set 8-byte pointer to 0, like innodb does (relevant for 32-bit
    // platforms)
    memset(blob->ptr + length_bytes, 0, 8);
    memcpy(blob->ptr + length_bytes, &blob_ptr, sizeof(uchar **));
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::convert_varchar_from_storage_format(
  my_core::Field_varstring *const field_var,
  Xdb_string_reader *const        reader,
  bool                            decode)
{
  const char *data_len_str;
  if (!(data_len_str = reader->read(field_var->length_bytes)))
    return HA_ERR_INTERNAL_ERROR;

  uint data_len;
  /* field_var->length_bytes is 1 or 2 */
  if (field_var->length_bytes == 1) {
    data_len = (uchar)data_len_str[0];
  } else {
    DBUG_ASSERT(field_var->length_bytes == 2);
    data_len = uint2korr(data_len_str);
  }

  if (data_len > field_var->field_length) {
    /* The data on disk is longer than table DDL allows? */
    return HA_ERR_INTERNAL_ERROR;
  }

  if (!reader->read(data_len)) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (decode) {
    memcpy(field_var->ptr, data_len_str, field_var->length_bytes + data_len);
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::convert_field_from_storage_format(
  my_core::Field *const    field,
  Xdb_string_reader *const reader,
  bool                     decode,
  uint                     len)
{
  const char *data_bytes;
  if (len > 0) {
    if ((data_bytes = reader->read(len)) == nullptr) {
      return HA_ERR_INTERNAL_ERROR;
    }

    if (decode)
      memcpy(field->ptr, data_bytes, len);
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::append_blob_to_storage_format(String &storage_record,
                                              my_core::Field_blob *const blob)
{
  /* Get the number of bytes needed to store length*/
  const uint length_bytes = blob->pack_length() - portable_sizeof_char_ptr;

  /* Store the length of the value */
  storage_record.append(reinterpret_cast<char *>(blob->ptr), length_bytes);

  /* Store the blob value itself */
  char *data_ptr;
  memcpy(&data_ptr, blob->ptr + length_bytes, sizeof(uchar **));
  storage_record.append(data_ptr, blob->get_length());

  return HA_EXIT_SUCCESS;
}

int ha_xengine::append_varchar_to_storage_format(
    String &storage_record, my_core::Field_varstring *const field_var)
{
  uint data_len;
  /* field_var->length_bytes is 1 or 2 */
  if (field_var->length_bytes == 1) {
    data_len = field_var->ptr[0];
  } else {
    DBUG_ASSERT(field_var->length_bytes == 2);
    data_len = uint2korr(field_var->ptr);
  }
  storage_record.append(reinterpret_cast<char *>(field_var->ptr),
                        field_var->length_bytes + data_len);

  return HA_EXIT_SUCCESS;
}

/*
  @brief
  Unpack the record in this->m_retrieved_record and this->m_last_rowkey from
  storage format into buf (which can be table->record[0] or table->record[1]).

  @param  key   Table record's key in mem-comparable form.
  @param  buf   Store record in table->record[0] format here

  @detail
    If the table has blobs, the unpacked data in buf may keep pointers to the
    data in this->m_retrieved_record.

    The key is only needed to check its checksum value (the checksum is in
    m_retrieved_record).

  @seealso
    ha_xengine::setup_read_decoders()  Sets up data structures which tell which
    columns to decode.

  @return
    0      OK
    other  Error inpacking the data
*/

int ha_xengine::convert_record_from_storage_format(
    const xengine::common::Slice *key, const xengine::common::Slice *value,
    uchar *const buf, TABLE* t, uint32_t null_bytes_in_rec,
    bool maybe_unpack_info, const Xdb_key_def *pk_descr,
    std::vector<READ_FIELD>& decoders_vect,
    InstantDDLInfo* instant_ddl_info, bool verify_row_debug_checksums,
    my_core::ha_rows& row_checksums_checked) {

  DBUG_ASSERT(key != nullptr);
  DBUG_ASSERT(buf != nullptr);
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_CONVERT_FROM);

  Xdb_string_reader reader(value);

  // Used for DEBUG EXECUTION
  String debug_key;
  xengine::common::Slice debug_key_slice;
  /*
    Decode PK fields from the key
  */
  DBUG_EXECUTE_IF("myx_simulate_bad_pk_read1",{
    debug_key.copy(key->data(), key->size(), &my_charset_bin);
    dbug_modify_key_varchar8(debug_key);
    debug_key_slice = xengine::common::Slice (debug_key.ptr(), debug_key.length());
    key = &debug_key_slice;
  });

  const char *unpack_info = nullptr;
  uint16 unpack_info_len = 0;
  xengine::common::Slice unpack_slice;

  const char *header = reader.read(XENGINE_RECORD_HEADER_LENGTH);
  // if column_number_need_supplement <= 0 , it means no instantly added columns should be extracted
  int column_number_need_supplement = 0;
  int offset_for_default_vect = 0;
  if (instant_ddl_info && instant_ddl_info->have_instantly_added_columns()) {
    // number of fields stored in a X-Engine row
    uint16_t field_num = 0;
    if (header[0] & INSTANT_DDL_FLAG) {
      const char *field_num_str = reader.read(XENGINE_RECORD_FIELD_NUMBER_LENGTH);
      const char *nullable_bytes_str = reader.read(XENGINE_RECORD_NULLABLE_BYTES);
      field_num = uint2korr(field_num_str);
      column_number_need_supplement = decoders_vect.size() - (field_num - m_fields_no_needed_to_decode);
      null_bytes_in_rec = uint2korr(nullable_bytes_str);
    } else {
      // inserted before instant ddl
      field_num = instant_ddl_info->m_instant_cols;
      column_number_need_supplement = decoders_vect.size() - (field_num - m_fields_no_needed_to_decode);
      null_bytes_in_rec = instant_ddl_info->m_null_bytes;
    }
    // the first element should be get from default value array
    offset_for_default_vect = field_num - instant_ddl_info->m_instant_cols;
    assert(offset_for_default_vect >= 0 &&
           offset_for_default_vect <= (int)(instant_ddl_info->instantly_added_default_values.size()));
    assert(null_bytes_in_rec != std::numeric_limits<uint32_t>::max());
  }

  /* Other fields are decoded from the value */
  const char *null_bytes = nullptr;
  if (null_bytes_in_rec && !(null_bytes = reader.read(null_bytes_in_rec))) {
    return HA_ERR_INTERNAL_ERROR;
  }

  if (maybe_unpack_info) {
    unpack_info = reader.read(XDB_UNPACK_HEADER_SIZE);

    if (!unpack_info || unpack_info[0] != XDB_UNPACK_DATA_TAG) {
      return HA_ERR_INTERNAL_ERROR;
    }

    unpack_info_len =
        xdb_netbuf_to_uint16(reinterpret_cast<const uchar *>(unpack_info + 1));
    unpack_slice = xengine::common::Slice(unpack_info, unpack_info_len);

    reader.read(unpack_info_len - XDB_UNPACK_HEADER_SIZE);
  }

  if (!unpack_info && !pk_descr->table_has_unpack_info(t)) {
      if (pk_descr->unpack_record_1(t, buf, key, nullptr,
                                  false /* verify_checksum */)) {
          return HA_ERR_INTERNAL_ERROR;
      }
  } else if (pk_descr->unpack_record(t, buf, key,
                                unpack_info? &unpack_slice : nullptr,
                                false /* verify_checksum */)) {
      return HA_ERR_INTERNAL_ERROR;
  }

  int err = HA_EXIT_SUCCESS;
  int fields_have_been_extracted = 0;
  int extracted_normally = decoders_vect.size();
  if (column_number_need_supplement > 0) {
    extracted_normally = decoders_vect.size() - column_number_need_supplement;
  }

  for (auto it = decoders_vect.begin(); it != decoders_vect.end(); it++) {
    assert(extracted_normally >= 0);
    const Xdb_field_encoder *const field_dec = it->m_field_enc;
    const bool decode = it->m_decode;

    Field *const field = t->field[field_dec->m_field_index];

    /* Skip the bytes we need to skip */
    if (it->m_skip && !reader.read(it->m_skip))
      return HA_ERR_INTERNAL_ERROR;

    uint field_offset = field->ptr - t->record[0];
    uint null_offset = field->null_offset();
    bool maybe_null = field->real_maybe_null();
    field->move_field(buf + field_offset,
                      maybe_null ? buf + null_offset : nullptr,
                      field->null_bit);
    // WARNING! - Don't return before restoring field->ptr and field->null_ptr!

    if (fields_have_been_extracted < extracted_normally) {

      const bool isNull = field_dec->maybe_null() &&
          ((null_bytes[field_dec->m_null_offset] & field_dec->m_null_mask) != 0);

      if (isNull) {
        if (decode) {
          /* This sets the NULL-bit of this record */
          field->set_null();
          /*
            Besides that, set the field value to default value. CHECKSUM t
            depends on this.
          */
          memcpy(field->ptr, t->s->default_values + field_offset,
                field->pack_length());
        }
      } else {
        if (decode) {
          field->set_notnull();
        }

        if (field_dec->m_field_type == MYSQL_TYPE_BLOB ||
            field_dec->m_field_type == MYSQL_TYPE_JSON) {
          err = convert_blob_from_storage_format(
              (my_core::Field_blob *) field, &reader, decode);
        } else if (field_dec->m_field_type == MYSQL_TYPE_VARCHAR) {
          err = convert_varchar_from_storage_format(
              (my_core::Field_varstring *) field, &reader, decode);
        } else {
          err = convert_field_from_storage_format(
              field, &reader, decode, field_dec->m_pack_length_in_rec);
        }
      }
    } else if (instant_ddl_info) {
      // instantly added columns
      const bool is_default_null = instant_ddl_info->instantly_added_default_value_null[offset_for_default_vect];
      if (decode) {
        if (is_default_null) {
          field->set_null();
        } else {
          field->set_notnull();
          const char *data_bytes = m_instant_ddl_info.instantly_added_default_values[offset_for_default_vect].c_str();
          int len = m_instant_ddl_info.instantly_added_default_values[offset_for_default_vect].size();
          memcpy(field->ptr, data_bytes, len);
        }
      }
      offset_for_default_vect++;
    } else {
      __XHANDLER_LOG(ERROR, "Decode value failed for unkown err");
      DBUG_ASSERT(0);
    }

    // Restore field->ptr and field->null_ptr
    field->move_field(t->record[0] + field_offset,
                      maybe_null ? t->record[0] + null_offset : nullptr,
                      field->null_bit);
    fields_have_been_extracted++;

    if (err != HA_EXIT_SUCCESS) {
      return err;
    }
  }

  // if decode ddl pk, pass checksums verification
  if (verify_row_debug_checksums) {
    if (reader.remaining_bytes() == XDB_CHECKSUM_CHUNK_SIZE &&
        reader.read(1)[0] == XDB_CHECKSUM_DATA_TAG) {
      uint32_t stored_key_chksum =
          xdb_netbuf_to_uint32((const uchar *)reader.read(XDB_CHECKSUM_SIZE));
      uint32_t stored_val_chksum =
          xdb_netbuf_to_uint32((const uchar *)reader.read(XDB_CHECKSUM_SIZE));

      const uint32_t computed_key_chksum =
          my_core::crc32(0, xdb_slice_to_uchar_ptr(key), key->size());
      const uint32_t computed_val_chksum =
          my_core::crc32(0, xdb_slice_to_uchar_ptr(value),
                         value->size() - XDB_CHECKSUM_CHUNK_SIZE);

      DBUG_EXECUTE_IF("myx_simulate_bad_pk_checksum1",
                      stored_key_chksum++;);

      if (stored_key_chksum != computed_key_chksum) {
        pk_descr->report_checksum_mismatch(true, key->data(), key->size());
        return HA_ERR_INTERNAL_ERROR;
      }

      DBUG_EXECUTE_IF("myx_simulate_bad_pk_checksum2",
                      stored_val_chksum++;);
      if (stored_val_chksum != computed_val_chksum) {
        pk_descr->report_checksum_mismatch(false, value->data(),
                                             value->size());
        return HA_ERR_INTERNAL_ERROR;
      }

      row_checksums_checked++;
    }
    if (reader.remaining_bytes())
      return HA_ERR_INTERNAL_ERROR;
  }

  return HA_EXIT_SUCCESS;
}

/*
 param OUT maybe_unpack_info, true if storage record contains unpack_info
 */
void ha_xengine::get_storage_type(Xdb_field_encoder *const encoder,
                                  std::shared_ptr<Xdb_key_def> pk_descr,
                                  const uint &kp,
                                  bool &maybe_unpack_info) {
  // STORE_SOME uses unpack_info.
  if (pk_descr->has_unpack_info(kp)) {
    DBUG_ASSERT(pk_descr->can_unpack(kp));
    encoder->m_storage_type = Xdb_field_encoder::STORE_SOME;
    maybe_unpack_info = true;
  } else if (pk_descr->can_unpack(kp)) {
    encoder->m_storage_type = Xdb_field_encoder::STORE_NONE;
  }
}

/*
  Setup data needed to convert table->record[] to and from record storage
  format.

  @seealso
     ha_xengine::convert_record_to_storage_format,
     ha_xengine::convert_record_from_storage_format
*/
int ha_xengine::setup_field_converters() {
  DBUG_ASSERT(this->table != nullptr);
  DBUG_ASSERT(this->m_encoder_arr == nullptr);
  DBUG_ASSERT(this->m_pk_descr != nullptr);

  m_fields_no_needed_to_decode = 0;
  m_null_bytes_in_rec = 0;
  return setup_field_converters(table, m_pk_descr, m_encoder_arr, m_fields_no_needed_to_decode, m_null_bytes_in_rec, m_maybe_unpack_info);

}

int ha_xengine::setup_field_converters(const TABLE *table, std::shared_ptr<Xdb_key_def> pk_descr, Xdb_field_encoder* &encoder_arr, uint &fields_no_needed_to_decode, uint &null_bytes_in_rec, bool &maybe_unpack_info) {
  uint i;
  uint null_bytes = 0;
  uchar cur_null_mask = 0x1;

  encoder_arr = static_cast<Xdb_field_encoder *>(
      my_malloc(PSI_NOT_INSTRUMENTED, table->s->fields * sizeof(Xdb_field_encoder), MYF(0)));
  if (encoder_arr == nullptr) {
    __XHANDLER_LOG(ERROR, "allcate from memory failed");
    return HA_EXIT_FAILURE;
  }

  for (i = 0; i < table->s->fields; i++) {
    Field *const field = table->field[i];
    encoder_arr[i].m_storage_type = Xdb_field_encoder::STORE_ALL;

    /*
      Check if this field is
      - a part of primary key, and
      - it can be decoded back from its key image.
      If both hold, we don't need to store this field in the value part of
      XENGINE's key-value pair.

      If hidden pk exists, we skip this check since the field will never be
      part of the hidden pk.
    */
    if (!has_hidden_pk(table) &&
        field->part_of_key.is_set(table->s->primary_key)) {
      KEY *const pk_info = &table->key_info[table->s->primary_key];
      for (uint kp = 0; kp < pk_info->user_defined_key_parts; kp++) {
        /* key_part->fieldnr is counted from 1 */
        if (field->field_index + 1 == pk_info->key_part[kp].fieldnr) {
          get_storage_type(&encoder_arr[i], pk_descr, kp, maybe_unpack_info);
          break;
        }
      }
    }

    if (encoder_arr[i].m_storage_type != Xdb_field_encoder::STORE_ALL) {
      fields_no_needed_to_decode++;
    }

    encoder_arr[i].m_field_type = field->real_type();
    encoder_arr[i].m_field_index = i;
    encoder_arr[i].m_pack_length_in_rec = field->pack_length_in_rec();

    if (field->real_maybe_null()) {
      encoder_arr[i].m_null_mask = cur_null_mask;
      encoder_arr[i].m_null_offset = null_bytes;
      if (cur_null_mask == 0x80) {
        cur_null_mask = 0x1;
        null_bytes++;
      } else
        cur_null_mask = cur_null_mask << 1;
    } else {
      encoder_arr[i].m_null_mask = 0;
    }
  }

  /* Count the last, unfinished NULL-bits byte */
  if (cur_null_mask != 0x1)
    null_bytes++;

  null_bytes_in_rec = null_bytes;

  return HA_EXIT_SUCCESS;
}

int ha_xengine::alloc_key_buffers(const TABLE *const table_arg,
                                  const Xdb_tbl_def *const tbl_def_arg,
                                  bool alloc_alter_buffers) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(m_pk_tuple == nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);

  std::shared_ptr<Xdb_key_def> *const kd_arr = tbl_def_arg->m_key_descr_arr;

  uint key_len = 0;
  m_max_packed_sk_len = 0;
  m_pack_key_len = 0;

  m_pk_descr = kd_arr[pk_index(table_arg, tbl_def_arg)];
  if (has_hidden_pk(table_arg)) {
    m_pk_key_parts = 1;
  } else {
    m_pk_key_parts =
        table->key_info[table->s->primary_key].user_defined_key_parts;
    key_len = table->key_info[table->s->primary_key].key_length;
  }

  // move this into get_table_handler() ??
  m_pk_descr->setup(table_arg, tbl_def_arg);

  m_pk_tuple = reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, key_len, MYF(0)));
  if (m_pk_tuple == nullptr) {
    goto error;
  }

  //packed pk length
  m_pack_key_len = m_pk_descr->max_storage_fmt_length();

  /* Sometimes, we may use m_sk_packed_tuple for storing packed PK */
  m_max_packed_sk_len = m_pack_key_len;
  for (uint i = 0; i < table_arg->s->keys; i++) {
    if (i == table_arg->s->primary_key) /* Primary key was processed above */
      continue;

    // TODO: move this into get_table_handler() ??
    kd_arr[i]->setup(table_arg, tbl_def_arg);

    const uint packed_len = kd_arr[i]->max_storage_fmt_length();
    if (packed_len > m_max_packed_sk_len) {
      m_max_packed_sk_len = packed_len;
    }
  }

  // the cached TABLE(s) were dropped at prepare_inplace_alter_table, so the
  // new SQLs will open the table again and call into this function. It makes
  // sure that enough space is allocated for the inplace index.
  for (auto &k : m_tbl_def->m_added_key) {
    const Xdb_key_def &kd = *k.first;
    const uint packed_len = kd.max_storage_fmt_length();
    if (packed_len > m_max_packed_sk_len) {
      m_max_packed_sk_len = packed_len;
    }
  }

  //for online-copy-ddl, we need allocate enough space for new indexes and record
  if (m_tbl_def->m_inplace_new_tdef) {
    Xdb_tbl_def* new_tdef = m_tbl_def->m_inplace_new_tdef;
    for (uint i = 0; i < new_tdef->m_key_count; i++) {
      std::shared_ptr<Xdb_key_def> kd = new_tdef->m_key_descr_arr[i];
      const uint packed_len = kd->max_storage_fmt_length();
      if (packed_len > m_max_packed_sk_len) {
        m_max_packed_sk_len = packed_len;
      }
    }

    DBUG_ASSERT(!m_tbl_def->m_inplace_new_keys.empty());
    auto iter = m_tbl_def->m_inplace_new_keys.begin();
    TABLE *altered_table = iter->second.altered_table;
    DBUG_ASSERT(altered_table != nullptr);


  //pk maybe changed, enlarge pk_buffer if necessary
    m_new_pk_descr = new_tdef->m_key_descr_arr[pk_index(altered_table, new_tdef)];
    DBUG_ASSERT(m_new_pk_descr != nullptr);

    uint new_packed_pk_len = m_new_pk_descr->max_storage_fmt_length();
    m_pack_key_len = m_pack_key_len < new_packed_pk_len ? new_packed_pk_len : m_pack_key_len;

    uint new_record_len = altered_table->s->rec_buff_length + altered_table->s->null_bytes;

#if 0
    if (!(m_new_record = reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, new_record_len, MYF(0))))) {
      __XHANDLER_LOG(ERROR, "allocate new buffers error");
      goto error;
    }
#endif

    // allocate memory for old_pk in new table update
    m_pk_packed_tuple_old = reinterpret_cast<uchar *>(
        my_malloc(PSI_NOT_INSTRUMENTED, m_pack_key_len, MYF(0)));
    if (m_pk_packed_tuple_old == nullptr) {
      goto error;
    }
  }

  //allocate memory for primary key
  m_pk_packed_tuple =
      reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_pack_key_len, MYF(0)));
  if (m_pk_packed_tuple == nullptr) {
    goto error;
  }

  //allocate memory for secondary keys
  if (!(m_sk_packed_tuple =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_sk_match_prefix_buf =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_sk_packed_tuple_old =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !(m_end_key_packed_tuple =
            reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
      !((m_pack_buffer = reinterpret_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))))) {
    goto error;
  }

  /*
    If inplace alter is happening, allocate special buffers for unique
    secondary index duplicate checking.
  */
  if (alloc_alter_buffers &&
      (!(m_dup_sk_packed_tuple =
             reinterpret_cast<uchar *>(my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))) ||
       !(m_dup_sk_packed_tuple_old = reinterpret_cast<uchar *>(
             my_malloc(PSI_NOT_INSTRUMENTED, m_max_packed_sk_len, MYF(0)))))) {
    goto error;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);

error:
  // If we're here then this means that at some point above an allocation may
  // have failed. To avoid any resource leaks and maintain a clear contract
  // we'll clean up before returning the error code.
  free_key_buffers();

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

void ha_xengine::free_key_buffers() {
  my_free(m_pk_tuple);
  m_pk_tuple = nullptr;

  my_free(m_pk_packed_tuple);
  m_pk_packed_tuple = nullptr;

  my_free(m_pk_packed_tuple_old);
  m_pk_packed_tuple_old = nullptr;

  my_free(m_sk_packed_tuple);
  m_sk_packed_tuple = nullptr;

  my_free(m_sk_match_prefix_buf);
  m_sk_match_prefix_buf = nullptr;

  my_free(m_sk_packed_tuple_old);
  m_sk_packed_tuple_old = nullptr;

  my_free(m_end_key_packed_tuple);
  m_end_key_packed_tuple = nullptr;

  my_free(m_pack_buffer);
  m_pack_buffer = nullptr;

  my_free(m_dup_sk_packed_tuple);
  m_dup_sk_packed_tuple = nullptr;

  my_free(m_dup_sk_packed_tuple_old);
  m_dup_sk_packed_tuple_old = nullptr;

  my_free(m_new_record);
  m_new_record = nullptr;
}


void ha_xengine::set_skip_unique_check_tables(const char *const whitelist) {
  DBUG_ASSERT(whitelist != nullptr);

#if defined(HAVE_PSI_INTERFACE)
  Regex_list_handler regex_handler(key_rwlock_skip_unique_check_tables);
#else
  Regex_list_handler regex_handler;
#endif

  if (!regex_handler.set_patterns(whitelist)) {
    warn_about_bad_patterns(&regex_handler, "skip_unique_check_tables");
  }

  m_skip_unique_check = regex_handler.matches(m_tbl_def->base_tablename());
}

int ha_xengine::open(const char *const name, int mode, uint test_if_locked,
                     const dd::Table *table_def) {
  //TODO by beilou this function should be implementd like mysql 8.0
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_OPEN);

  if (close()) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  m_table_handler = xdb_open_tables.get_table_handler(name);

  if (m_table_handler == nullptr) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  my_core::thr_lock_data_init(&m_table_handler->m_thr_lock, &m_db_lock,
                              nullptr);

  std::string fullname;
  if (xdb_normalize_tablename(name, &fullname)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  // For concurrent scenario,
  //
  // stage 1 check table cache
  // T1 set rdlock on ddl_manager
  // T2 set rdlock on ddl_manager
  // T1 failed to find from table cache
  // T1 unset rdlock on ddl_manager
  // T2 failed to find from table cache
  // T2 unset rdlock on ddl_manager
  //
  // stage 2 load from dictionary
  // T1 load table_def from dictionary referenced from ha_xengine::m_tbl_def
  // T2 load table_def from dictionary referenced from ha_xengine::m_tbl_def
  //
  // stage 3 put into cache
  // T1 set wrlock on ddl_manager
  // T1 put table_def into table cache,
  //    the reference of T1(ha_xengine::m_tbl_def) will be increased to 2.
  // T1 unset wrlock on ddl_manager
  // T2 set wrlock on ddl_manager
  // T2 put table_def into table cache
  //    the reference of T1(ha_xengine::m_tbl_def) will be decreased to 1.
  //    the reference of T2(ha_xengine::m_tbl_def) will be increased to 2.
  // T2 unset wrlock on ddl_manager
  bool from_dict = false;
  if (nullptr != table_def)
    m_tbl_def = ddl_manager.find(table_def, fullname, &from_dict);
  else
    m_tbl_def = ddl_manager.find(ha_thd(), fullname, &from_dict);
  if (m_tbl_def == nullptr) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to open a table that is not present in XEngine-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  } else if (from_dict) {
    ddl_manager.put(m_tbl_def);
  }

  m_lock_rows = XDB_LOCK_NONE;

  m_key_descr_arr = m_tbl_def->m_key_descr_arr;

  /*
    Full table scan actually uses primary key
    (UPDATE needs to know this, otherwise it will go into infinite loop on
    queries like "UPDATE tbl SET pk=pk+100")
  */
  key_used_on_scan = table->s->primary_key;

  // close() above has already called free_key_buffers(). No need to do it here.
  int err = alloc_key_buffers(table, m_tbl_def.get());

  if (err) {
    DBUG_RETURN(err);
  }

  /*
    init_with_fields() is used to initialize table flags based on the field
    definitions in table->field[].
    It is called by open_binary_frm(), but that function calls the method for
    a temporary ha_xengine object which is later destroyed.

    If we are here in ::open(), then init_with_fields() has not been called
    for this object. Call it ourselves, we want all member variables to be
    properly initialized.
  */
  init_with_fields();

  setup_field_converters();

  info(HA_STATUS_NO_LOCK | HA_STATUS_VARIABLE | HA_STATUS_CONST);

  /*
    The following load_XXX code calls row decode functions, and they do
    that without having done ::external_lock() or index_init()/rnd_init().
    (Note: this also means we're doing a read when there was no
    setup_field_converters() call)

    Initialize the necessary variables for them:
  */
  m_verify_row_debug_checksums = false;

  /* TODO: move the following to where TABLE_SHARE is opened: */
  if (table->found_next_number_field)
    load_auto_incr_value();

  if (has_hidden_pk(table) && load_hidden_pk_value()) {
    free_key_buffers();
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  /* Index block size in XEngine: used by MySQL in query optimization */
  stats.block_size = xengine_tbl_options.block_size;

  /* Determine at open whether we can use Read Free Replication or not */
  //set_use_read_free_rpl(THDVAR(ha_thd(), read_free_rpl_tables));

  /* Determine at open whether we should skip unique checks for this table */
  set_skip_unique_check_tables(".*"/*THDVAR(ha_thd(), skip_unique_check_tables)*/);

  if (nullptr != table_def) {
    // sometimes null is passed here, for example, handler::clone->handler::ha_open
    get_instant_ddl_info_if_needed(table_def);
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::close(void) {
  DBUG_ENTER_FUNC();

  m_tbl_def.reset();
  m_pk_descr = nullptr;
  m_key_descr_arr = nullptr;

  free_key_buffers();

  my_free(m_encoder_arr);
  m_encoder_arr = nullptr;

  if (m_table_handler != nullptr) {
    xdb_open_tables.release_table_handler(m_table_handler);
    m_table_handler = nullptr;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

bool ha_xengine::get_error_message(const int error, String *const buf) {
  DBUG_ENTER_FUNC();

  static_assert(HA_ERR_XENGINE_LAST > HA_ERR_FIRST,
                "HA_ERR_XENGINE_LAST > HA_ERR_FIRST");
  static_assert(HA_ERR_XENGINE_LAST > HA_ERR_LAST,
                "HA_ERR_XENGINE_LAST > HA_ERR_LAST");

  DBUG_ASSERT(buf != nullptr);

  Xdb_transaction *const tx = get_tx_from_thd(ha_thd());
  bool temp_error = false;

  switch (error) {
  case HA_ERR_XENGINE_PK_REQUIRED:
    buf->append("Table must have a PRIMARY KEY.");
    break;
  case HA_ERR_XENGINE_UNIQUE_NOT_SUPPORTED:
    buf->append("Unique indexes are not supported.");
    break;
  case HA_ERR_XENGINE_TOO_MANY_LOCKS:
    buf->append("Number of locks held reached @@xengine_max_row_locks.");
    break;
  case HA_ERR_LOCK_WAIT_TIMEOUT:
    DBUG_ASSERT(tx != nullptr);
    buf->append(tx->m_detailed_error);
    temp_error = true;
    break;
  case HA_ERR_XENGINE_TABLE_DATA_DIRECTORY_NOT_SUPPORTED:
    buf->append("Specifying DATA DIRECTORY for an individual table is not "
                "supported.");
    break;
  case HA_ERR_XENGINE_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED:
    buf->append("Specifying INDEX DIRECTORY for an individual table is not "
                "supported.");
    break;
  case HA_ERR_XENGINE_OUT_OF_SORTMEMORY:
    buf->append("Sort memory is too small.");
    break;
  default:
    // We can be called with the values which are < HA_ERR_FIRST because most
    // MySQL internal functions will just return HA_EXIT_FAILURE in case of
    // an error.
    break;
  }

  DBUG_RETURN(temp_error);
}

/* XEngine supports only the following collations for indexed columns */
static const std::set<const my_core::CHARSET_INFO *> XDB_INDEX_COLLATIONS =
  {&my_charset_bin, &my_charset_latin1_bin,
   &my_charset_utf8_bin, &my_charset_utf8_general_ci,
   &my_charset_gbk_bin, &my_charset_gbk_chinese_ci,
   &my_charset_utf8mb4_bin, &my_charset_utf8mb4_general_ci,
   &my_charset_utf8mb4_0900_ai_ci};

static std::string& gen_xdb_supported_collation_string() {
  static std::string xdb_supported_collation_string;
  if (xdb_supported_collation_string.empty()) {
    // sort by name -- only do once
    std::set<std::string> names;
    for (const auto &coll : XDB_INDEX_COLLATIONS) {
      names.insert(coll->name);
    }
    bool first_coll = true;
    std::ostringstream oss;
    for (const auto &name: names) {
      if (!first_coll) {
        oss << ", ";
      } else {
        first_coll = false;
      }
      oss << name;
    }
    xdb_supported_collation_string = oss.str();
  }
  return xdb_supported_collation_string;
}

static bool
xdb_is_index_collation_supported(const my_core::Field *const field) {
  const my_core::enum_field_types type = field->real_type();
  /* Handle [VAR](CHAR|BINARY) or TEXT|BLOB */
  if (type == MYSQL_TYPE_VARCHAR || type == MYSQL_TYPE_STRING ||
      type == MYSQL_TYPE_BLOB) {
    return XDB_INDEX_COLLATIONS.find(field->charset()) !=
           XDB_INDEX_COLLATIONS.end();
  }
  return true;
}

/*
  Create structures needed for storing data in xengine. This is called when the
  table is created. The structures will be shared by all TABLE* objects.

  @param
    table_arg        Table with definition
    db_table         "dbname.tablename"
    len              strlen of the above
    tbl_def_arg      tbl_def whose key_descr is being created/populated
    old_tbl_def_arg  tbl_def from which keys are being copied over from
                     (for use during inplace alter)

  @return
    0      - Ok
    other  - error, either given table ddl is not supported by xengine or OOM.
*/
int ha_xengine::create_key_defs(
    const TABLE *const table_arg, Xdb_tbl_def *tbl_def_arg,
    const char *old_table_name,
    const TABLE *const old_table_arg /* = nullptr */,
    const Xdb_tbl_def *const old_tbl_def_arg /* = nullptr */,
    const bool need_rebuild /* = false */) const {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(table_arg->s != nullptr);

  uint i;

  /*
    These need to be one greater than MAX_INDEXES since the user can create
    MAX_INDEXES secondary keys and no primary key which would cause us
    to generate a hidden one.
  */
  std::array<key_def_cf_info, MAX_INDEXES + 1> cfs;
  std::array<uint32, MAX_INDEXES + 1> index_ids;

  /*
    NOTE: All new column families must be created before new index numbers are
    allocated to each key definition. See below for more details.
    http://github.com/MySQLOnXENGINE/mysql-5.6/issues/86#issuecomment-138515501
  */
  const char *table_name = nullptr;
  if (old_tbl_def_arg) {
    table_name = old_tbl_def_arg->base_tablename().c_str();
  } else {
    table_name = old_table_name;
  }

  if (create_cfs(table_arg, tbl_def_arg, &cfs, &index_ids, table_name,
                 old_table_arg, old_tbl_def_arg, need_rebuild)) {
    DBUG_RETURN(HA_EXIT_FAILURE);
  };

  if (!old_tbl_def_arg) {
    /**
       old_tbl_def doesn't exist. this means we are in the process of creating
       a new  table.
    */
    for (i = 0; i < tbl_def_arg->m_key_count; i++) {
      if (create_key_def(table_arg, i, tbl_def_arg, &m_key_descr_arr[i],
                         cfs[i], index_ids[i])) {
        DBUG_RETURN(HA_EXIT_FAILURE);
      }
    }
  } else if (need_rebuild) { /** for copy-online ddl */
    for (i = 0; i < tbl_def_arg->m_key_count; i++) {
      if (create_key_def(table_arg, i, tbl_def_arg, &tbl_def_arg->m_key_descr_arr[i],
                         cfs[i], index_ids[i])) {
        DBUG_RETURN(HA_EXIT_FAILURE);
      }

      DBUG_ASSERT(tbl_def_arg->m_key_descr_arr[i] != nullptr);
      tbl_def_arg->m_key_descr_arr[i]->setup(table_arg, tbl_def_arg);
    }
  } else {
    /*
      old_tbl_def exists.  This means we are creating a new tbl_def as part of
      in-place alter table.  Copy over existing keys from the old_tbl_def and
      generate the necessary new key definitions if any.
    */
    if (create_inplace_key_defs(table_arg, tbl_def_arg, old_table_arg,
                                old_tbl_def_arg, cfs, index_ids)) {
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  Checks index parameters and creates column families needed for storing data
  in xengine if necessary.

  @param in
    table_arg     Table with definition
    db_table      Table name
    tbl_def_arg   Table def structure being populated

  @param out
    cfs           CF info for each key definition in 'key_info' order

  @return
    0      - Ok
    other  - error
*/
int ha_xengine::create_cfs(
    const TABLE *const table_arg, Xdb_tbl_def *tbl_def_arg,
    std::array<struct key_def_cf_info, MAX_INDEXES + 1> *const cfs,
    std::array<uint32_t, MAX_INDEXES + 1> *const index_ids,
    const char *table_name,
    const TABLE *const old_table_arg,
    const Xdb_tbl_def *const old_tbl_def_arg,
    const bool need_rebuild) const {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(table_arg->s != nullptr);

  char tablename_sys[NAME_LEN + 1];

  my_core::filename_to_tablename(tbl_def_arg->base_tablename().c_str(),
                                 tablename_sys, sizeof(tablename_sys));

  bool create_table_space = (nullptr == old_tbl_def_arg) ? true : false;
  /*
    The first loop checks the index parameters and creates
    column families if necessary.
  */
  for (uint i = 0; i < tbl_def_arg->m_key_count; i++) {
    xengine::db::ColumnFamilyHandle *cf_handle;

    if (xengine_strict_collation_check &&
        !is_hidden_pk(i, table_arg, tbl_def_arg)) {
      for (uint part = 0; part < table_arg->key_info[i].actual_key_parts;
           part++) {
        /* for alter and create, we all need check collation is supported or not */
        if (!xdb_is_index_collation_supported(
                table_arg->key_info[i].key_part[part].field) &&
            !xdb_collation_exceptions->matches(tablename_sys)) {
          my_printf_error(
              ER_UNKNOWN_ERROR, "Unsupported collation on string indexed column"
                                " %s. Consider change to other collation (%s).",
              MYF(0), table_arg->key_info[i].key_part[part].field->field_name,
              gen_xdb_supported_collation_string().c_str());
          DBUG_RETURN(HA_EXIT_FAILURE);
        }
      }
    }

    /*
      index comment has Column Family name. If there was no comment, we get
      NULL, and then we use dbname_tablename_indexno fake comment for each index.
    */
    const char *const key_name = get_key_name(i, table_arg, tbl_def_arg);

    char comment[1024]; // use cf_index_number fake cf_name

    // get old indexid if the index was found in old_table_arg
    bool found_old_index_id = false;
    if( nullptr != old_tbl_def_arg && !need_rebuild) {
      assert(nullptr != old_table_arg);
      const std::unordered_map<std::string, uint> old_key_pos =
            get_old_key_positions(table_arg, tbl_def_arg, old_table_arg,
                                  old_tbl_def_arg);

      const auto &it = old_key_pos.find(key_name);
      if (it != old_key_pos.end()){
        const Xdb_key_def &okd = *old_tbl_def_arg->m_key_descr_arr[it->second];
        const GL_INDEX_ID gl_index_id = okd.get_gl_index_id();
        (*index_ids)[i] = gl_index_id.index_id;
        found_old_index_id = true;
      }
    }

    if(!found_old_index_id){
      (*index_ids)[i] = ddl_manager.get_and_update_next_number(&dict_manager);
    }
    // use old table name when tmp table
    if (!strncmp(tbl_def_arg->base_tablename().c_str(), "#sql-", strlen("#sql-")) &&
      table_name != nullptr) {
      snprintf(comment, 1024, "%s.%s_%u", tbl_def_arg->base_dbname().c_str(),
              table_name, (*index_ids)[i]);
    } else {
      const char *dbname_tablename = tbl_def_arg->full_tablename().c_str();
      snprintf(comment, 1024, "%s_%u", dbname_tablename, (*index_ids)[i]);
    }

    if (looks_like_per_index_cf_typo(comment)) {
      my_error(ER_NOT_SUPPORTED_YET, MYF(0),
               "sub table name looks like a typo of $per_index_cf");
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    /* Prevent create from using the system column family */
    if (strcmp(DEFAULT_SYSTEM_SUBTABLE_NAME, comment) == 0) {
      my_error(ER_WRONG_ARGUMENTS, MYF(0),
               "column family not valid for storing index data");
      DBUG_RETURN(HA_EXIT_FAILURE);
    }
    bool is_auto_cf_flag;

    THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
    Xdb_transaction *const tx = get_or_create_tx(thd);
    xengine_register_tx(ht, thd, tx);
    auto batch = dynamic_cast<xengine::db::WriteBatch *>(tx->get_blind_write_batch());

    uint subtable_id = (*index_ids)[i];
    int64_t table_space_id = 0;
    if (!create_table_space) {
      table_space_id = old_tbl_def_arg->space_id;
    }
    cf_handle = cf_manager.get_or_create_cf(
        xdb, batch, thd_thread_id(thd), (*index_ids)[i], comment, tbl_def_arg->full_tablename(),
        key_name, &is_auto_cf_flag, xengine_default_cf_options, create_table_space,
        table_space_id);
    if (create_table_space) {
      tbl_def_arg->space_id = table_space_id;
    }

    if (!cf_handle)
      DBUG_RETURN(HA_EXIT_FAILURE);

    auto &cf = (*cfs)[i];
    cf.cf_handle = cf_handle;
    cf.is_reverse_cf = Xdb_cf_manager::is_cf_name_reverse(comment);
    cf.is_auto_cf = false;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}


/*
  Create key definition needed for storing data in xengine.
  This can be called either during CREATE table or doing ADD index operations.

  @param in
    table_arg     Table with definition
    i             Position of index being created inside table_arg->key_info
    tbl_def_arg   Table def structure being populated
    cf_info       Struct which contains column family information

  @param out
    new_key_def  Newly created index definition.

  @return
    0      - Ok
    other  - error, either given table ddl is not supported by xengine or OOM.
*/
int ha_xengine::create_key_def(const TABLE *const table_arg, const uint &i,
                               const Xdb_tbl_def *const tbl_def_arg,
                               std::shared_ptr<Xdb_key_def> *const new_key_def,
                               const struct key_def_cf_info &cf_info,
                               const uint32_t index_id) const {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(new_key_def != nullptr);
  DBUG_ASSERT(*new_key_def == nullptr);

  const uint16_t index_dict_version = Xdb_key_def::INDEX_INFO_VERSION_LATEST;
  uchar index_type;
  uint16_t kv_version;

  if (is_hidden_pk(i, table_arg, tbl_def_arg)) {
    index_type = Xdb_key_def::INDEX_TYPE_HIDDEN_PRIMARY;
    kv_version = Xdb_key_def::PRIMARY_FORMAT_VERSION_LATEST;
  } else if (i == table_arg->s->primary_key) {
    index_type = Xdb_key_def::INDEX_TYPE_PRIMARY;
    uint16 pk_latest_version = Xdb_key_def::PRIMARY_FORMAT_VERSION_LATEST;
    kv_version = pk_latest_version;
  } else {
    index_type = Xdb_key_def::INDEX_TYPE_SECONDARY;
    uint16 sk_latest_version = Xdb_key_def::SECONDARY_FORMAT_VERSION_LATEST;
    kv_version = sk_latest_version;
  }

  const char *const key_name = get_key_name(i, table_arg, tbl_def_arg);
  *new_key_def = std::make_shared<Xdb_key_def>(
      index_id, i, cf_info.cf_handle, index_dict_version, index_type,
      kv_version, cf_info.is_reverse_cf, cf_info.is_auto_cf, key_name);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int xdb_normalize_tablename(const std::string &tablename,
                            std::string *const strbuf) {
  DBUG_ASSERT(strbuf != nullptr);

  if (tablename.size() < 2 || tablename[0] != '.' || tablename[1] != '/') {
    DBUG_ASSERT(0); // We were not passed table name?
    return HA_ERR_INTERNAL_ERROR;
  }

  size_t pos = tablename.find_first_of('/', 2);
  if (pos == std::string::npos) {
    DBUG_ASSERT(0); // We were not passed table name?
    return HA_ERR_INTERNAL_ERROR;
  }

  *strbuf = tablename.substr(2, pos - 2) + "." + tablename.substr(pos + 1);

  return HA_EXIT_SUCCESS;
}

/*
  Check to see if the user's original statement includes foreign key
  references
*/
bool ha_xengine::contains_foreign_key(THD *const thd) {
  bool success;
  size_t query_length = thd->query().length + 1;
  std::shared_ptr<char> query_str(new char[query_length], std::default_delete<char []>());
  (void)thd_query_safe(thd, query_str.get(), query_length);
  const char *str = query_str.get();

  DBUG_ASSERT(str != nullptr);

  while (*str != '\0') {
    // Scan from our current pos looking for 'FOREIGN'
    str = xdb_find_in_string(str, "FOREIGN", &success);
    if (!success) {
      return false;
    }

    // Skip past the found "FOREIGN'
    str = xdb_check_next_token(&my_charset_bin, str, "FOREIGN", &success);
    DBUG_ASSERT(success);

    if (!my_isspace(&my_charset_bin, *str)) {
      return false;
    }

    // See if the next token is 'KEY'
    str = xdb_check_next_token(&my_charset_bin, str, "KEY", &success);
    if (!success) {
      continue;
    }

    // See if the next token is '('
    str = xdb_check_next_token(&my_charset_bin, str, "(", &success);
    if (!success) {
      // There is an optional index id after 'FOREIGN KEY', skip it
      str = xdb_skip_id(&my_charset_bin, str);

      // Now check for '(' again
      str = xdb_check_next_token(&my_charset_bin, str, "(", &success);
    }

    // If we have found 'FOREIGN KEY [<word>] (' we can be confident we have
    // a foreign key clause.
    return success;
  }

  // We never found a valid foreign key clause
  return false;
}

/**
  @brief
  splits the normalized table name of <dbname>.<tablename>#P#<part_no> into
  the <dbname>, <tablename> and <part_no> components.

  @param dbbuf returns database name/table_schema
  @param tablebuf returns tablename
  @param partitionbuf returns partition suffix if there is one
  @return HA_EXIT_SUCCESS on success, non-zero on failure to split
*/
int xdb_split_normalized_tablename(const std::string &fullname,
                                   std::string *const db,
                                   std::string *const table,
                                   std::string *const partition) {
  DBUG_ASSERT(!fullname.empty());

#define XDB_PARTITION_STR "#P#"

  /* Normalize returns dbname.tablename. */
  size_t dotpos = fullname.find('.');

  /* Invalid table name? */
  if (dotpos == std::string::npos) {
    return HA_ERR_INTERNAL_ERROR;
  }

  // Table must have a database name associated with it.
  DBUG_ASSERT(dotpos > 0);

  if (db != nullptr) {
    *db = fullname.substr(0, dotpos);
  }

  dotpos++;

  const size_t partpos =
      fullname.find(XDB_PARTITION_STR, dotpos, strlen(XDB_PARTITION_STR));

  if (partpos != std::string::npos) {
    DBUG_ASSERT(partpos >= dotpos);

    if (table != nullptr) {
      *table = fullname.substr(dotpos, partpos - dotpos);
    }

    if (partition != nullptr) {
      *partition = fullname.substr(partpos + strlen(XDB_PARTITION_STR));
    }
  } else if (table != nullptr) {
    *table = fullname.substr(dotpos);
  }

  return HA_EXIT_SUCCESS;
}

/** for ddl transaction, we write atomic-ddl log in xengine, so
    mark xengine modified.
 */
void ha_xengine::mark_ddl_trx_read_write() {
  Ha_trx_info *ha_info = &ha_thd()->get_ha_data(ht->slot)->ha_info[0];

  /** make sure xengine registed by xengine_register_tx */
  if (ha_info->is_started()) {
    ha_info->set_trx_read_write();
  }
}

/**
  @brief
  create() is called to create a table. The variable name will have the name
  of the table.

  @details
  When create() is called you do not need to worry about
  opening the table. Also, the .frm file will have already been
  created so adjusting create_info is not necessary. You can overwrite
  the .frm file at this point if you wish to change the table
  definition, but there are no methods currently provided for doing
  so.

  Called from handle.cc by ha_create_table().

  @see
  ha_create_table() in handle.cc
*/

int ha_xengine::create(const char *const name, TABLE *const table_arg,
                       HA_CREATE_INFO *const create_info,
                       dd::Table *table_def) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(create_info != nullptr);

  if (create_info->data_file_name) {
    // DATA DIRECTORY is used to create tables under a specific location
    // outside the MySQL data directory. XEngine doesn't support this.
    // The `xengine_datadir` setting should be used to configure XENGINE data
    // directory.
    DBUG_RETURN(HA_ERR_XENGINE_TABLE_DATA_DIRECTORY_NOT_SUPPORTED);
  }

  if (create_info->index_file_name) {
    // Similar check for INDEX DIRECTORY as well.
    DBUG_RETURN(HA_ERR_XENGINE_TABLE_INDEX_DIRECTORY_NOT_SUPPORTED);
  }

  std::string str;
  int err;
  THD *const thd = my_core::thd_get_current_thd();

  /*
    Construct dbname.tablename ourselves, because parititioning
    passes strings like "./test/t14#P#p0" for individual partitions,
    while table_arg->s->table_name has none of that.
  */
  if (xdb_normalize_tablename(name, &str)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool do_truncate = (thd->lex->sql_command == SQLCOM_TRUNCATE);
  bool from_dict = false;
  auto tbl_def = table_def ? ddl_manager.find(table_def, str, &from_dict)
                           : ddl_manager.find(thd, str, &from_dict);
  if (tbl_def) {
    // for truncate, delete the table first
    if (do_truncate) {
      // put it into cache for later usage in delete_table
      if (from_dict) ddl_manager.put(tbl_def);
      err = delete_table(name, table_def);
      if (err != HA_EXIT_SUCCESS) {
        DBUG_RETURN(err);
      }

      DBUG_EXECUTE_IF("ddl_log_crash_truncate_after_delete_table", DBUG_SUICIDE(););
    } else if (is_prefix(tbl_def->base_tablename().c_str(), tmp_file_prefix)) {
      XHANDLER_LOG(WARN, "XEngine: find garbage temporary table in dictionary",
                   "table_name", str);
      DBUG_ASSERT(!from_dict);
      // remove cache entry of the table
      ddl_manager.remove_cache(tbl_def->full_tablename());
      xdb_drop_idx_thread.signal();
    } else {
      my_printf_error(ER_UNKNOWN_ERROR, "Table '%s' doesn't exist in XEngine, "
          "but metadata information exists. This means that data inconsistency "
          "happens. Please contact IT support to try to restore it.",
          MYF(0), str.c_str());
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
  }

  if (contains_foreign_key(thd)) {
    my_printf_error(ER_NOT_SUPPORTED_YET, "XEngine currently doesn't support "
                                          "foreign key constraints",
                    MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool write_err = false;
  //const std::unique_ptr<xengine::db::WriteBatch> wb = dict_manager.begin();
  //xengine::db::WriteBatch *const batch = wb.get();
  Xdb_transaction *const tx = get_or_create_tx(thd);
  xengine_register_tx(ht, thd, tx);
  xengine::db::WriteBatch *const batch = dynamic_cast<xengine::db::WriteBatch *>(tx->get_blind_write_batch());

  /* Create table/key descriptions and put them into the data dictionary */
  m_tbl_def = std::make_shared<Xdb_tbl_def>(str);

  uint n_keys = table_arg->s->keys;

  /*
    If no primary key found, create a hidden PK and place it inside table
    definition
  */
  if (has_hidden_pk(table_arg)) {
    n_keys += 1;
  }

  if (do_truncate && (dd::INVALID_OBJECT_ID != table_def->se_private_id())) {
    // for truncate table, use old table_id
    m_tbl_def->set_table_id(table_def->se_private_id());
  } else if (m_tbl_def->init_table_id(ddl_manager)) {
    goto error;
  }

  m_key_descr_arr = new std::shared_ptr<Xdb_key_def>[n_keys];
  m_tbl_def->m_key_count = n_keys;
  m_tbl_def->m_key_descr_arr = m_key_descr_arr;

  if (create_key_defs(table_arg, m_tbl_def.get(), create_info->alias)) {
    goto error;
  }

  if (m_tbl_def->write_dd_table(table_def)) {
    goto error;
  }

  m_pk_descr = m_key_descr_arr[pk_index(table_arg, m_tbl_def.get())];

  dict_manager.lock();
  write_err = ddl_manager.put_and_write(m_tbl_def, batch, &ddl_log_manager,
                                        thd_thread_id(thd), true);
  dict_manager.unlock();

  if (write_err) {
    goto error;
  }

  if (create_info->auto_increment_value)
    m_tbl_def->m_auto_incr_val = create_info->auto_increment_value;

  /*
    We only support auto_increment at start of the PRIMARY KEY.
  */
  // Field *field;
  // if ((field= table_arg->next_number_field))
  /* TODO mdcallag: disable this for now to let UNIQUE indexes kind of work
  if ((field= table_arg->found_next_number_field))
  {
    int pk= table_arg->s->primary_key;
    Field *pk_field= table_arg->key_info[pk].key_part[0].field;
    if (field->field_index !=  pk_field->field_index)
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  */

  DBUG_EXECUTE_IF("ddl_log_crash_create_after_xengine_success", DBUG_SUICIDE(););

  /* mark xengine modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);

error:
  /* Delete what we have allocated so far */
  m_tbl_def.reset();
  m_pk_descr = nullptr;
  m_key_descr_arr = nullptr;

  DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
}

/**
  @note
  This function is used only when the table has not yet been opened, and
  keyread_allowed bitmap doesn't have the correct values yet.

  See comment in ha_xengine::index_flags() for details.
*/

bool ha_xengine::check_keyread_allowed(uint inx, uint part,
                                       bool all_parts) const {
  bool res = true;
  KEY *const key_info = &table_share->key_info[inx];

  Xdb_field_packing dummy1;
  res = dummy1.setup(nullptr, key_info->key_part[part].field, inx, part,
                     key_info->key_part[part].length);

  if (res && all_parts) {
    for (uint i = 0; i < part; i++) {
      Field *field;
      if ((field = key_info->key_part[i].field)) {
        Xdb_field_packing dummy;
        if (!dummy.setup(nullptr, field, inx, i,
                         key_info->key_part[i].length)) {
          /* Cannot do index-only reads for this column */
          res = false;
          break;
        }
      }
    }
  }

  const uint pk = table_share->primary_key;
  if (inx == pk && all_parts &&
      part + 1 == table_share->key_info[pk].user_defined_key_parts) {
    m_pk_can_be_decoded = res;
    // reset the cached_table_flags
    const_cast<ha_xengine*>(this)->init();
  }

  return res;
}

int ha_xengine::read_key_exact(const Xdb_key_def &kd,
                               xengine::db::Iterator *const iter,
                               const bool &full_key_match,
                               const xengine::common::Slice &key_slice) const {
  DBUG_ASSERT(iter != nullptr);

  /*
    We are looking for the first record such that
      index_tuple= lookup_tuple.
    lookup_tuple may be a prefix of the index.
  */
  if (kd.m_is_reverse_cf) {
    if (!full_key_match) {
      if (!iter->Valid())
        iter->SeekToLast();
      else
        iter->Prev();
    }
  }

  if (!iter->Valid() || !kd.value_matches_prefix(iter->key(), key_slice)) {
    /*
      Got a record that is not equal to the lookup value, or even a record
      from another table.index.
    */
    return HA_ERR_KEY_NOT_FOUND;
  }
  return HA_EXIT_SUCCESS;
}

int ha_xengine::read_before_key(const Xdb_key_def &kd,
                                const bool &full_key_match,
                                const xengine::common::Slice &key_slice) {
  /*
    We are looking for record with the biggest t.key such that
    t.key < lookup_tuple.
  */
  if (kd.m_is_reverse_cf) {
    if (m_scan_it->Valid() && full_key_match &&
        kd.value_matches_prefix(m_scan_it->key(), key_slice)) {
      /* We are using full key and we've hit an exact match */
      m_scan_it->Next();
    }
  } else {
    if (m_scan_it->Valid())
      m_scan_it->Prev();
    else
      m_scan_it->SeekToLast();
  }

  return m_scan_it->Valid() ? HA_EXIT_SUCCESS : HA_ERR_KEY_NOT_FOUND;
}

int ha_xengine::read_after_key(const Xdb_key_def &kd,
                               const bool &full_key_match,
                               const xengine::common::Slice &key_slice) {
  /*
    We are looking for the first record such that

      index_tuple $GT lookup_tuple

    with HA_READ_AFTER_KEY, $GT = '>',
    with HA_READ_KEY_OR_NEXT, $GT = '>='
  */
  if (kd.m_is_reverse_cf) {
    if (!m_scan_it->Valid()) {
      m_scan_it->SeekToLast();
    } else {
      /*
        We should step back
         - when not using full extended key
         - when using full extended key and when we've got an exact match
      */
      if (!full_key_match ||
          !kd.value_matches_prefix(m_scan_it->key(), key_slice)) {
        m_scan_it->Prev();
      }
    }
  }

  return m_scan_it->Valid() ? HA_EXIT_SUCCESS : HA_ERR_KEY_NOT_FOUND;
}

int ha_xengine::position_to_correct_key(const Xdb_key_def &kd,
                                        enum ha_rkey_function &find_flag,
                                        bool &full_key_match,
                                        const uchar *key,
                                        key_part_map &keypart_map,
                                        xengine::common::Slice &key_slice,
                                        bool *move_forward) {
  int rc = 0;

  *move_forward = true;

  switch (find_flag) {
  case HA_READ_KEY_EXACT:
    rc = read_key_exact(kd, m_scan_it, full_key_match, key_slice);
    break;
  case HA_READ_BEFORE_KEY:
    *move_forward = false;
    rc = read_before_key(kd, full_key_match, key_slice);
    if (rc == 0 && !kd.covers_key(m_scan_it->key())) {
      /* The record we've got is not from this index */
      rc = HA_ERR_KEY_NOT_FOUND;
    }
    break;
  case HA_READ_AFTER_KEY:
  case HA_READ_KEY_OR_NEXT:
    rc = read_after_key(kd, full_key_match, key_slice);
    if (rc == 0 && !kd.covers_key(m_scan_it->key())) {
      /* The record we've got is not from this index */
      rc = HA_ERR_KEY_NOT_FOUND;
    }
    break;
  case HA_READ_KEY_OR_PREV:
  case HA_READ_PREFIX:
    /* This flag is not used by the SQL layer, so we don't support it yet. */
    rc = HA_ERR_UNSUPPORTED;
    break;
  case HA_READ_PREFIX_LAST:
  case HA_READ_PREFIX_LAST_OR_PREV:
    *move_forward = false;
    /*
      Find the last record with the specified index prefix lookup.
      - HA_READ_PREFIX_LAST requires that the record has the
        prefix=lookup (if there are no such records,
        HA_ERR_KEY_NOT_FOUND should be returned).
      - HA_READ_PREFIX_LAST_OR_PREV has no such requirement. If there are no
        records with prefix=lookup, we should return the last record
        before that.
    */
    rc = read_before_key(kd, full_key_match, key_slice);
    if (rc == 0) {
      const xengine::common::Slice &rkey = m_scan_it->key();
      if (!kd.covers_key(rkey)) {
        /* The record we've got is not from this index */
        rc = HA_ERR_KEY_NOT_FOUND;
      } else if (find_flag == HA_READ_PREFIX_LAST) {
        uint size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                        key, keypart_map);
        xengine::common::Slice lookup_tuple(reinterpret_cast<char *>(m_sk_packed_tuple),
                                    size);

        // We need to compare the key we've got with the original search prefix.
        if (!kd.value_matches_prefix(rkey, lookup_tuple)) {
          rc = HA_ERR_KEY_NOT_FOUND;
        }
      }
    }
    break;
  default:
    DBUG_ASSERT(0);
    break;
  }

  return rc;
}

int ha_xengine::calc_eq_cond_len(const Xdb_key_def &kd,
                                 enum ha_rkey_function &find_flag,
                                 xengine::common::Slice &slice,
                                 int &bytes_changed_by_succ,
                                 const key_range *end_key,
                                 uint *end_key_packed_size) {
  if (find_flag == HA_READ_KEY_EXACT)
    return slice.size();

  if (find_flag == HA_READ_PREFIX_LAST) {
    /*
      We have made the kd.successor(m_sk_packed_tuple) call above.

      The slice is at least Xdb_key_def::INDEX_NUMBER_SIZE bytes long.
    */
    return slice.size() - bytes_changed_by_succ;
  }

  if (end_key) {
    *end_key_packed_size =
        kd.pack_index_tuple(table, m_pack_buffer, m_end_key_packed_tuple,
                            end_key->key, (const_cast<key_range*>(end_key))->keypart_map);

    /*
      Calculating length of the equal conditions here. 4 byte index id is
      included.
      Example1: id1 BIGINT, id2 INT, id3 BIGINT, PRIMARY KEY (id1, id2, id3)
       WHERE id1=1 AND id2=1 AND id3>=2 => eq_cond_len= 4+8+4= 16
       WHERE id1=1 AND id2>=1 AND id3>=2 => eq_cond_len= 4+8= 12
      Example2: id1 VARCHAR(30), id2 INT, PRIMARY KEY (id1, id2)
       WHERE id1 = 'AAA' and id2 < 3; => eq_cond_len=13 (varchar used 9 bytes)
    */
    xengine::common::Slice end_slice(reinterpret_cast<char *>(m_end_key_packed_tuple),
                             *end_key_packed_size);
    return slice.difference_offset(end_slice);
  }

  /*
    On range scan without any end key condition, there is no
    eq cond, and eq cond length is the same as index_id size (4 bytes).
    Example1: id1 BIGINT, id2 INT, id3 BIGINT, PRIMARY KEY (id1, id2, id3)
     WHERE id1>=1 AND id2 >= 2 and id2 <= 5 => eq_cond_len= 4
  */
  return Xdb_key_def::INDEX_NUMBER_SIZE;
}

int ha_xengine::read_row_from_primary_key(uchar *const buf) {
  DBUG_ASSERT(buf != nullptr);
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_READ_ROW);

  int rc;
  const xengine::common::Slice &rkey = m_scan_it->key();
  const uint pk_size = rkey.size();
  const char *pk_data = rkey.data();

  memcpy(m_pk_packed_tuple, pk_data, pk_size);
  m_last_rowkey.copy(pk_data, pk_size, &my_charset_bin);

  if (m_lock_rows != XDB_LOCK_NONE) {
    /* We need to put a lock and re-read */
    rc = get_row_by_rowid(buf, m_pk_packed_tuple, pk_size);
  } else {
    /* Unpack from the row we've read */
    const xengine::common::Slice &value = m_scan_it->value();
    rc = convert_record_from_storage_format(&rkey, &value, buf, table);
  }

  return rc;
}

int ha_xengine::read_row_from_secondary_key(uchar *const buf,
                                            const Xdb_key_def &kd,
                                            bool move_forward) {
  DBUG_ASSERT(buf != nullptr);
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_READ_ROW);
  int rc = 0;
  uint pk_size;

  if (m_keyread_only && m_lock_rows == XDB_LOCK_NONE && !has_hidden_pk(table)) {
    /* Get the key columns and primary key value */
    const xengine::common::Slice &rkey = m_scan_it->key();
    pk_size =
        kd.get_primary_key_tuple(table, *m_pk_descr, &rkey, m_pk_packed_tuple);
    const xengine::common::Slice &value = m_scan_it->value();
    if (pk_size == XDB_INVALID_KEY_LEN ||
        kd.unpack_record(table, buf, &rkey, &value,
                         m_verify_row_debug_checksums)) {
      rc = HA_ERR_INTERNAL_ERROR;
    }
  } else {
    if (kd.m_is_reverse_cf)
      move_forward = !move_forward;

    rc = find_icp_matching_index_rec(move_forward, buf);
    if (!rc) {
      const xengine::common::Slice &rkey = m_scan_it->key();
      pk_size = kd.get_primary_key_tuple(table, *m_pk_descr, &rkey,
                                         m_pk_packed_tuple);
      if (pk_size == XDB_INVALID_KEY_LEN) {
        rc = HA_ERR_INTERNAL_ERROR;
      } else {
        rc = get_row_by_rowid(buf, m_pk_packed_tuple, pk_size);
      }
    }
  }

  if (!rc) {
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, pk_size,
                       &my_charset_bin);
  }

  return rc;
}

/**
  @note
    The problem with this function is that SQL layer calls it, when
     - the table has not been yet opened (no ::open() call done)
     - this->table_share already exists, but it is in the process of being
       filled, so some of fields are still NULL.
     - In particular, table_share->key_info[inx].key_part[] is filled only up
       to part #part. Subsequent key parts are not yet filled.

    To complicate things further, SQL layer will call index_flags() with
    all_parts=TRUE. Essentially, we're asked to provide flags for reading
    keyparts whose datatype is not yet known.

    We walk around this problem by using check_keyread_allowed(), which uses
    table_share object and is careful not to step on unitialized data.

    When we get a call with all_parts=TRUE, we try to analyze all parts but
    ignore those that have key_part->field==nullptr (these are not initialized
    yet).
*/

ulong ha_xengine::index_flags(uint inx, uint part, bool all_parts) const {
  DBUG_ENTER_FUNC();

  ulong base_flags = HA_READ_NEXT | // doesn't seem to be used
                     HA_READ_ORDER | HA_READ_RANGE | HA_READ_PREV;

  if (check_keyread_allowed(inx, part, all_parts))
    base_flags |= HA_KEYREAD_ONLY;

  if (inx == table_share->primary_key) {
    /*
      Index-only reads on primary key are the same as table scan for us. Still,
      we need to explicitly "allow" them, otherwise SQL layer will miss some
      plans.
    */
    base_flags |= HA_KEYREAD_ONLY;
  } else {
    /*
      We can Index Condition Pushdown any key except the primary. With primary
      key, we get (pk, record) pair immediately, there is no place to put the
      ICP check.
    */
    base_flags |= HA_DO_INDEX_COND_PUSHDOWN;
  }

  DBUG_RETURN(base_flags);
}

/**
  @brief
  Read next index tuple through the secondary index.

  @details
  m_scan_it points at the index key-value pair that we should read the (pk,row)
  pair for.
*/
int ha_xengine::secondary_index_read(const int keyno, uchar *const buf) {
  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(table != nullptr);
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_SECONDARY_INDEX_READ);
  //stats.rows_requested++;

  /* Use STATUS_NOT_FOUND when record not found or some error occurred */
  //table->status = STATUS_NOT_FOUND;

  if (m_scan_it->Valid()) {
    xengine::common::Slice key = m_scan_it->key();

    /* Check if we've ran out of records of this index */
    if (m_key_descr_arr[keyno]->covers_key(key)) {
      int rc = 0;

      // TODO: We could here check if we have ran out of range we're scanning
      const uint size = m_key_descr_arr[keyno]->get_primary_key_tuple(
          table, *m_pk_descr, &key, m_pk_packed_tuple);
      if (size == XDB_INVALID_KEY_LEN) {
        return HA_ERR_INTERNAL_ERROR;
      }

      m_last_rowkey.copy((const char *)m_pk_packed_tuple, size,
                         &my_charset_bin);

      if (m_keyread_only && m_lock_rows == XDB_LOCK_NONE &&
          !has_hidden_pk(table)) {
        xengine::common::Slice value = m_scan_it->value();
        if (m_key_descr_arr[keyno]->unpack_record(
                table, buf, &key, &value, m_verify_row_debug_checksums)) {
          return HA_ERR_INTERNAL_ERROR;
        }
      } else {
        rc = get_row_by_rowid(buf, m_pk_packed_tuple, size);
      }

      if (!rc) {
        //table->status = 0;
       // stats.rows_read++;
        //stats.rows_index_next++;
        update_row_stats(ROWS_READ);
      }
      return rc;
    }
  }
  return HA_ERR_END_OF_FILE;
}

/*
  ha_xengine::read_range_first overrides handler::read_range_first.
  The only difference from handler::read_range_first is that
  ha_xengine::read_range_first passes end_key to
  ha_xengine::index_read_map_impl function.
*/
int ha_xengine::read_range_first(const key_range *const start_key,
                                 const key_range *const end_key,
                                 bool eq_range_arg, bool sorted) {
  DBUG_ENTER_FUNC();

  int result;

  eq_range = eq_range_arg;
  set_end_range(end_key, RANGE_SCAN_ASC);

  range_key_part = table->key_info[active_index].key_part;

  if (!start_key) // Read first record
    result = ha_index_first(table->record[0]);
  else {
    // TODO comment by beilou
    // we should implement the following logic with MySQL 8.0
    if (is_using_prohibited_gap_locks(
            table,
            is_using_full_unique_key(active_index, start_key->keypart_map,
                                     start_key->flag))) {
      DBUG_RETURN(HA_ERR_LOCK_DEADLOCK);
    }

    result = index_read_map_impl(table->record[0], start_key->key,
                                 start_key->keypart_map, start_key->flag,
                                 end_key);
    // temporarily comment statistics
    //MYSQL_TABLE_IO_WAIT(m_psi, PSI_TABLE_FETCH_ROW, active_index, 0, result);
  }
  if (result)
    DBUG_RETURN((result == HA_ERR_KEY_NOT_FOUND) ? HA_ERR_END_OF_FILE : result);

  if (compare_key(end_range) <= 0) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  } else {
    /*
      The last read row does not fall in the range. So request
      storage engine to release row lock if possible.
    */
    unlock_row();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
}

int ha_xengine::read_range_next()
{
  int result;
  DBUG_ENTER("ha_xengine::read_range_next");

  if (eq_range)
  {
    /* We trust that index_next_same always gives a row in range */
    DBUG_RETURN(ha_index_next_same(table->record[0],
                                   end_range->key,
                                   end_range->length));
  }
  result= ha_index_next(table->record[0]);

  if (result) {
    // index_next will return HA_ERR_END_OF_FILE, when meets end key
    if (HA_ERR_END_OF_FILE == result) {
      unlock_row();
    }
    DBUG_RETURN(result);
  }

  if (!m_cmp_end_key) {
    DBUG_RETURN(0);
  } else if (compare_key(end_range) <= 0) {
    DBUG_RETURN(0);
  } else {
    /*
      The last read row does not fall in the range. So request
      storage engine to release row lock if possible.
    */
    unlock_row();
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }
}

int ha_xengine::index_read_map(uchar *const buf, const uchar *const key,
                               key_part_map keypart_map,
                               enum ha_rkey_function find_flag) {
  DBUG_ENTER_FUNC();

  DBUG_RETURN(index_read_map_impl(buf, key, keypart_map, find_flag, nullptr));
}

/*
   See storage/xengine/xengine-range-access.txt for description of how MySQL
   index navigation commands are converted into XENGINE lookup commands.

   This function takes end_key as an argument, and it is set on range scan.
   XEngine needs to decide whether prefix bloom filter can be used or not.
   To decide to use prefix bloom filter or not, calculating equal condition
   length
   is needed. On equal lookups (find_flag == HA_READ_KEY_EXACT), equal
   condition length is the same as xengine::common::Slice.size() of the start key.
   On range scan, equal condition length is MIN(start_key, end_key) of the
   xengine::common::Slice expression.
*/
int ha_xengine::index_read_map_impl(uchar *const buf, const uchar *const key,
                                    key_part_map keypart_map,
                                    enum ha_rkey_function find_flag,
                                    const key_range *end_key) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_INDEX_READ_MAP);
  int rc = 0;

  ha_statistic_increment(&SSV::ha_read_key_count);
  const Xdb_key_def &kd = *m_key_descr_arr[active_index];
  const uint actual_key_parts = kd.get_key_parts();
  bool using_full_key = is_using_full_key(keypart_map, actual_key_parts);



  /* By default, we don't need the retrieved records to match the prefix */
  m_sk_match_prefix = nullptr;
  //stats.rows_requested++;

  if (active_index == table->s->primary_key && find_flag == HA_READ_KEY_EXACT &&
      using_full_key) {
    /*
      Equality lookup over primary key, using full tuple.
      This is a special case, use DB::Get.
    */
    const uint size = kd.pack_index_tuple(table, m_pack_buffer,
                                          m_pk_packed_tuple, key, keypart_map);
    bool skip_lookup = is_blind_delete_enabled();
    rc = get_row_by_rowid(buf, m_pk_packed_tuple, size, skip_lookup);
    if (!rc && !skip_lookup) {
      //stats.rows_read++;
      //stats.rows_index_first++;
      update_row_stats(ROWS_READ);
    }
    DBUG_RETURN(rc);
  }

  /*
    Unique secondary index performs lookups without the extended key fields
  */
  uint packed_size;
  if (active_index != table->s->primary_key &&
      table->key_info[active_index].flags & HA_NOSAME &&
      find_flag == HA_READ_KEY_EXACT && using_full_key) {
    key_part_map tmp_map = (key_part_map(1) << table->key_info[active_index]
                                                   .user_defined_key_parts) -
                           1;
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, tmp_map);
    if (table->key_info[active_index].user_defined_key_parts !=
        kd.get_key_parts())
      using_full_key = false;
  } else {
    packed_size = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                      key, keypart_map);
  }

  if ((pushed_idx_cond && pushed_idx_cond_keyno == active_index) &&
      (find_flag == HA_READ_KEY_EXACT || find_flag == HA_READ_PREFIX_LAST)) {
    /*
      We are doing a point index lookup, and ICP is enabled. It is possible
      that this call will be followed by ha_xengine->index_next_same() call.

      Do what InnoDB does: save the lookup tuple now. We will need it in
      index_next_same/find_icp_matching_index_rec in order to stop scanning
      as soon as index record doesn't match the lookup tuple.

      When not using ICP, handler::index_next_same() will make sure that rows
      that don't match the lookup prefix are not returned.
      row matches the lookup prefix.
    */
    m_sk_match_prefix = m_sk_match_prefix_buf;
    m_sk_match_length = packed_size;
    memcpy(m_sk_match_prefix, m_sk_packed_tuple, packed_size);
  }

  int bytes_changed_by_succ = 0;
  if (find_flag == HA_READ_PREFIX_LAST_OR_PREV ||
      find_flag == HA_READ_PREFIX_LAST || find_flag == HA_READ_AFTER_KEY) {
    /* See below */
    bytes_changed_by_succ = kd.successor(m_sk_packed_tuple, packed_size);
  }

  xengine::common::Slice slice(reinterpret_cast<const char *>(m_sk_packed_tuple),
                       packed_size);

  uint end_key_packed_size = 0;
  const uint eq_cond_len =
      calc_eq_cond_len(kd, find_flag, slice, bytes_changed_by_succ, end_key,
                       &end_key_packed_size);

  bool use_all_keys = false;
  if (find_flag == HA_READ_KEY_EXACT &&
      my_count_bits(keypart_map) == kd.get_key_parts())
    use_all_keys = true;

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  const bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    /*
      This will open the iterator and position it at a record that's equal or
      greater than the lookup tuple.
    */
    xengine::common::Slice end_key_slice;
    xengine::common::Slice* end_key_slice_ptr = nullptr;
    const bool is_ascending_scan = is_ascending(kd, find_flag);
    // check whether can push down end_key, if false, end_key_slice_ptr would be nullptr
    // NOTE that, now we only support end_key pushdown for ascending (i.e. forward) scan
    if (is_ascending_scan && end_key && end_key_packed_size > 0) {
      if (end_key->flag == HA_READ_AFTER_KEY) {
        // including end key, thus the m_end_key_packed_tuple should be
        // the successor of m_end_key_packed_tuple (mem-comparable format)
        kd.successor(m_end_key_packed_tuple, end_key_packed_size);
      }
      end_key_slice.assign(reinterpret_cast<const char *>(m_end_key_packed_tuple), end_key_packed_size);
      end_key_slice_ptr = &end_key_slice;
    }
    setup_scan_iterator(kd, &slice, use_all_keys, is_ascending_scan,
                        eq_cond_len, end_key_slice_ptr);

    /*
      Once we are positioned on from above, move to the position we really
      want: See storage/xengine/xengine-range-access.txt
    */
    bool move_forward;
    rc = position_to_correct_key(kd, find_flag, using_full_key, key,
                                 keypart_map, slice, &move_forward);

    if (rc) {
      /* This status is returned on any error */
      //table->status = STATUS_NOT_FOUND;
      DBUG_RETURN(rc);
    }

    m_skip_scan_it_next_call = false;

    /*
      Now get the data for the row into 'buf'.  If we were using a primary key
      then we have all the rows we need.  For a secondary key we now need to
      lookup the primary key.
    */
    if (active_index == table->s->primary_key)
      rc = read_row_from_primary_key(buf);
    else
      rc = read_row_from_secondary_key(buf, kd, move_forward);

    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; /* Exit the loop */

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (rc) {
    /* the only possible error condition is record-not-found */
    //table->status = STATUS_NOT_FOUND;
  } else {
    //table->status = 0;
    //stats.rows_read++;
    //stats.rows_index_first++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

/*
  @brief
  Scan the secondary index until we find an index record that satisfies ICP

  @param move_forward   TRUE  <=> move m_scan_it forward
                        FALSE <=> move m_scan_it backward
  @param buf            Record buffer (must be the same buffer that
                        pushed index condition points to, in practice
                        it is table->record[0])

  @detail
  Move the current iterator m_scan_it until we get an index tuple that
  satisfies the pushed Index Condition.
  (if there is no pushed index condition, return right away)

  @return
    0     - Index tuple satisfies ICP, can do index read.
    other - error code
*/

int ha_xengine::find_icp_matching_index_rec(const bool &move_forward,
                                            uchar *const buf) {
  DBUG_ASSERT(buf != nullptr);

  if (pushed_idx_cond && pushed_idx_cond_keyno == active_index) {
    const Xdb_key_def &kd = *m_key_descr_arr[active_index];

    while (1) {
      if (!m_scan_it->Valid()) {
        //table->status = STATUS_NOT_FOUND;
        return HA_ERR_END_OF_FILE;
      }
      const xengine::common::Slice rkey = m_scan_it->key();

      if (!kd.covers_key(rkey)) {
        //table->status = STATUS_NOT_FOUND;
        return HA_ERR_END_OF_FILE;
      }

      if (m_sk_match_prefix) {
        const xengine::common::Slice prefix((const char *)m_sk_match_prefix,
                                    m_sk_match_length);
        if (!kd.value_matches_prefix(rkey, prefix)) {
          //table->status = STATUS_NOT_FOUND;
          return HA_ERR_END_OF_FILE;
        }
      }

      const xengine::common::Slice value = m_scan_it->value();
      if (kd.unpack_record(table, buf, &rkey, &value,
                           m_verify_row_debug_checksums)) {
        return HA_ERR_INTERNAL_ERROR;
      }

      const enum icp_result icp_status = check_index_cond();
      if (icp_status == ICP_NO_MATCH) {
        if (move_forward)
          m_scan_it->Next();
        else
          m_scan_it->Prev();
        continue; /* Get the next (or prev) index tuple */
      } else if (icp_status == ICP_OUT_OF_RANGE) {
        /* We have walked out of range we are scanning */
        //table->status = STATUS_NOT_FOUND;
        return HA_ERR_END_OF_FILE;
      } else /* icp_status == ICP_MATCH */
      {
        /* Index Condition is satisfied. We have rc==0, proceed to fetch the
         * row. */
        break;
      }
      /*
        TODO: should we have this here, or RockDB handles this internally?
        if (my_core::thd_killed(current_thd))
        {
          rc= HA_ERR_INTERNAL_ERROR; // doesn't matter
          break;
        }
      */
    }
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::index_read_last_map(uchar *const buf, const uchar *const key,
                                    key_part_map keypart_map) {
  DBUG_ENTER_FUNC();

  DBUG_RETURN(index_read_map(buf, key, keypart_map, HA_READ_PREFIX_LAST));
}

static void print_error_row(const char *const db_name,
                            const char *const table_name,
                            const String &rowkey, const String &sec_key,
                            const std::string &retrieved_record = "") {
  std::string buf;
  buf = xdb_hexdump(rowkey.ptr(), rowkey.length(), XDB_MAX_HEXDUMP_LEN);
  // NO_LINT_DEBUG
  sql_print_error("CHECK TABLE %s.%s:   rowkey: %s", db_name, table_name,
                  buf.c_str());

  buf = xdb_hexdump(retrieved_record.data(), retrieved_record.size(),
                    XDB_MAX_HEXDUMP_LEN);
  // NO_LINT_DEBUG
  sql_print_error("CHECK TABLE %s.%s:   record: %s", db_name, table_name,
                  buf.c_str());

  buf = xdb_hexdump(sec_key.ptr(), sec_key.length(), XDB_MAX_HEXDUMP_LEN);
  // NO_LINT_DEBUG
  sql_print_error("CHECK TABLE %s.%s:   index: %s", db_name, table_name,
                  buf.c_str());
}

static int advice_fix_index(TABLE *table, int64_t keyno, int64_t hidden_pk_id,
                            uchar *data, const char *op) {
  if (nullptr == table || nullptr == table->s ||
      keyno >= table->s->keys || nullptr == data) {
    return 1;
  }

  // Get the useful columns including sk key parts and pk key parts.
  bool has_hidden_pk = MAX_INDEXES == table->s->primary_key;
  std::set<int64_t> fix_fields;
  if (!has_hidden_pk) {
    KEY *pk_info = &(table->key_info[table->s->primary_key]);
    for (int64_t i = 0; i < pk_info->actual_key_parts; ++i) {
      fix_fields.insert(pk_info->key_part[i].field->field_index);
    }
  }
  KEY *sk_info = &(table->key_info[keyno]);
  for (int64_t i = 0; i < sk_info->actual_key_parts; ++i) {
    fix_fields.insert(sk_info->key_part[i].field->field_index);
  }

  std::string fix_sql_str;
  std::string field_name_str;
  std::string target_value_str;
  fix_sql_str.reserve(101);
  field_name_str.reserve(101);
  target_value_str.reserve(101);
  fix_sql_str.append(op);
  fix_sql_str.push_back('(');
  fix_sql_str.append(sk_info->name);
  if (has_hidden_pk) {
    fix_sql_str.push_back(' ');
    fix_sql_str.append(std::to_string(hidden_pk_id));
  }
  fix_sql_str.append(")  INTO ");
  fix_sql_str.append(table->s->db.str);
  fix_sql_str.push_back('.');
  fix_sql_str.append(table->s->table_name.str);

  field_name_str.push_back('(');
  target_value_str.push_back('(');
  for (int64_t field_index = 0; field_index < table->s->fields; ++field_index) {
    Field *const field = table->field[field_index];
    enum_field_types field_type = field->real_type();
    uint field_offset = field->ptr - table->record[0];
    uint null_offset = field->null_offset();
    bool maybe_null = field->real_maybe_null();

    // If this field is one of the indexed ones, get user data from data which
    // is from table->record[0], else, use table->s->default_values.
    uchar *buf = fix_fields.find(field_index) != fix_fields.end() ? data :
                  table->s->default_values;

    field_name_str.append(field->field_name);
    field_name_str.push_back(',');
    field->move_field(buf + field_offset,
                      maybe_null ? buf + null_offset : nullptr,
                      field->null_bit);
    if (field->is_real_null()) {
      target_value_str.append("NULL");
    } else if (MYSQL_TYPE_BLOB == field->type()) {
      // BLOB can not be index, use default empty string.
      target_value_str.append("\'\'");
    } else if (MYSQL_TYPE_VARCHAR == field->real_type() && field->binary()) {
      // For BINARY or BIT type, output using hex format.
      auto field_varstring = static_cast<Field_varstring *>(field);
      uint32_t data_len = 0;
      if (1 == field_varstring->length_bytes) {
        data_len = static_cast<uint32_t>(field->ptr[0]);
      } else {
        data_len = static_cast<uint32_t>(uint2korr(field->ptr));
      }
      if (data_len > 0) {
        char *data_ptr =
            reinterpret_cast<char*>(field->ptr) + field_varstring->length_bytes;
        target_value_str.append("0x");
        target_value_str.append(xdb_hexdump(data_ptr, data_len, data_len * 2));
      } else {
        target_value_str.append("0x0");
      }
    } else if ((MYSQL_TYPE_STRING == field->real_type() ||
                MYSQL_TYPE_BIT == field->real_type()) && field->binary()) {
      target_value_str.append("0x");
      target_value_str.append(xdb_hexdump(reinterpret_cast<char*>(field->ptr),
                                          field->pack_length(),
                                          field->pack_length() * 2));
    } else {
      String buffer;
      String value;
      String *convert_result = field->val_str(&buffer, &value);
      if (field->str_needs_quotes()) {
        target_value_str.push_back('\'');
      }
      target_value_str.append(convert_result->c_ptr_safe());
      if (field->str_needs_quotes()) {
        target_value_str.push_back('\'');
      }
    }
    field->move_field(table->record[0] + field_offset,
                      maybe_null ? table->record[0] + null_offset : nullptr,
                      field->null_bit);
    target_value_str.push_back(',');
  }
  field_name_str.back() = ')';
  target_value_str.back() = ')';

  fix_sql_str.append(field_name_str);
  fix_sql_str.append(" VALUES ");
  fix_sql_str.append(target_value_str);
  fix_sql_str.push_back(';');

  // NO_LINT_DEBUG
  sql_print_error("CHECK TABLE %s.%s: ADVICE %s",
                  table->s->db.str, table->s->table_name.str,
                  fix_sql_str.data());
  return 0;
}

static int advice_del_index(TABLE *table, int64_t keyno,
                            int64_t hidden_pk_id, uchar *data) {
  return advice_fix_index(table, keyno, hidden_pk_id, data,
                          "DELETE INDEX ROW");
}

static int advice_add_index(TABLE *table, int64_t keyno,
                            int64_t hidden_pk_id, uchar *data) {
  return advice_fix_index(table, keyno, hidden_pk_id, data,
                          "INSERT INDEX ROW");
}

int ha_xengine::scan_parallel(Xdb_key_def *kd, Xdb_transaction *const tx,
                              xengine::common::ParallelReader::F &&f) {

  int ret = HA_EXIT_SUCCESS;
  uint size_start = 0;
  uint size_end = 0;

  kd->get_infimum_key(m_sk_packed_tuple, &size_start);
  kd->get_supremum_key(m_sk_packed_tuple_old, &size_end);

  const common::Slice user_key_start((const char *)m_sk_packed_tuple,
                                     size_start);
  const common::Slice user_key_end((const char *)m_sk_packed_tuple_old,
                                   size_end);

  size_t max_threads = THDVAR(ha_thd(), parallel_read_threads);
  DBUG_EXECUTE_IF("parallel_scan_kill", table->in_use->killed = THD::KILL_QUERY;);
  common::ParallelReader preader(max_threads, table->in_use);

  // generate range with internal_key [start,end)
  storage::Range range;
  preader.build_internal_key_range(user_key_start, user_key_end, range);

  common::ReadOptions read_options = tx->get_read_opts();
  read_options.total_order_seek = true;

  db::ColumnFamilyHandle *column_family_handle = kd->get_cf();
  common::ParallelReader::Config config(column_family_handle, range,
                                        read_options);

  const auto tx_impl = dynamic_cast<const Xdb_transaction_impl *>(tx);
  DBUG_ASSERT(tx_impl);

  util::Transaction *trx =
      const_cast<util::Transaction *>(tx_impl->get_xdb_trx());

  ret = preader.add_scan(trx, config, std::move(f));

  if (ret) {
    XHANDLER_LOG(WARN, "prepare for parallel scan failed", K(ret));
  } else if ((ret = preader.run())) {
    XHANDLER_LOG(WARN, "do parallel scan failed", K(ret));
  }

  return ret;
}

size_t ha_xengine::get_parallel_read_threads() {
  return THDVAR(ha_thd(), parallel_read_threads);
}

int ha_xengine::check_parallel(THD *const thd, HA_CHECK_OPT *const check_opt) {
  sql_print_information("--------- PARALLEL CHECK TABLE START ----------");
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_CHECK);
  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(check_opt != nullptr);
  const char *const table_name = table->s->table_name.str;
  const char *const db_name = table->s->db.str;
  bool error = false;

  for (uint i = 0; i < table->s->fields; i++) {
    bitmap_set_bit(table->read_set, i);
  }

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);
  tx->acquire_snapshot(true);

  for (uint iter_keyno = 0; iter_keyno < table->s->keys; iter_keyno++) {
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   Checking index %s", db_name,
                          table_name, table->key_info[iter_keyno].name);

    Xdb_key_def *kd = m_key_descr_arr[iter_keyno].get();
    extra(HA_EXTRA_KEYREAD);
    ha_index_init(iter_keyno, true);

    size_t max_threads = THDVAR(ha_thd(), parallel_read_threads);

    std::vector<std::unique_ptr<ParallelScanCtx>> check_ctx_set(max_threads);
    for (int i = 0; i < static_cast<int>(check_ctx_set.size()); i++) {
      check_ctx_set[i].reset(new ParallelScanCtx(this));
      if (check_ctx_set[i]->init())
        DBUG_RETURN(HA_ADMIN_CORRUPT);
    }
    // traverse func def
    auto f = [&](const xengine::common::ParallelReader::ExecuteCtx *ctx,
                 db::Iterator *db_iter) {
      ParallelScanCtx &check_ctx = *(check_ctx_set[ctx->thread_id_].get());
      TABLE* tbl = &check_ctx.thd_table;
      xengine::common::Slice key, value;
      int ret = Status::kOk;

      key = db_iter->key();
      value = db_iter->value();

      if (!is_pk(iter_keyno, tbl, m_tbl_def.get())) {
        // sk
        check_ctx.secondary_key.copy(key.data(), key.size(), &my_charset_bin);
        // pk size
        const uint pk_size = kd->get_primary_key_tuple(tbl, *m_pk_descr, &key, check_ctx.pk_packed_tuple);
        // pk
        check_ctx.primary_key.copy((const char *)(check_ctx.pk_packed_tuple), pk_size,
                         &my_charset_bin);

        // TODO(qimu): checksum counter
        if (kd->unpack_info_has_checksum(value)) {
          check_ctx.checksums++;
        }

        // Step 1:
        // Make a xengine Get using rowkey_copy and put the mysql format
        // result in table->record[0]
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(tbl) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &check_ctx.primary_key)) {
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        if ((ret = get_row_by_rowid(tbl->record[0], check_ctx.primary_key.ptr(),
                                    check_ctx.primary_key.length(),
                                    check_ctx.record, tbl,
                                    check_ctx.primary_key, false))) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, get_row_by_rowid failed, thread %d",
              db_name, table_name, check_ctx.rows, ctx->thread_id_);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // Step 2:
        // Convert the pk parts in table->record[0] to storage format then
        // compare it with rowkey_copy. table->record[0] is from pk subtable
        // get while the rowkey_copy is from sk subtable iteration.
        uint packed_size = m_pk_descr->pack_record(
            tbl, check_ctx.pack_buffer, tbl->record[0],
            check_ctx.pk_packed_tuple,
            nullptr, false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != check_ctx.primary_key.length() ||
            memcmp(check_ctx.pk_packed_tuple, check_ctx.primary_key.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, "
              "pk decoded from m_scan_it and get_row_by_rowid mismatch",
              db_name, table_name, check_ctx.rows);
          print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // Step 3:
        // Convert the sk[iter_keyno], pk parts in table->record[0] to storage
        // format then compare it with sec_key_copy. table->record[0] is from pk
        // subtable get while the sec_key_copy is from sk subtable iteration.
        packed_size = kd->pack_record(tbl, check_ctx.pack_buffer,
                                      tbl->record[0],
                                      check_ctx.sk_packed_tuple, &check_ctx.sk_tails, false,
                                      hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != check_ctx.secondary_key.length() ||
            memcmp(check_ctx.sk_packed_tuple, check_ctx.secondary_key.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error(
              "CHECK TABLE %s.%s:   .. row %lld: "
              "iter sk and get from pk, "
              "sk from m_scan_it and sk decoded from get_row_by_rowid mismatch",
              db_name, table_name, check_ctx.rows);
          print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
          advice_del_index(tbl, iter_keyno, hidden_pk_id,
                           tbl->record[0]);
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // the scan thread Step 4: if index is unique index, we should check
        // whether unique attribute is ok
        if (tbl->key_info[iter_keyno].flags & HA_NOSAME) {
          uint n_null_fields = 0;
          uint current_sk_size =
              kd->get_memcmp_sk_size(tbl, key, &n_null_fields);
          if (!check_ctx.last_key.is_empty() && n_null_fields == 0) {
            if (check_ctx.last_key.length() == current_sk_size &&
                memcmp(check_ctx.last_key.ptr(), check_ctx.secondary_key.ptr(),
                       current_sk_size) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              DBUG_ASSERT(0);
              goto one_row_checked;
            }
          }

          // if no-null fields, there may be duplicated key error
          if (n_null_fields == 0) {
            check_ctx.last_key.copy(check_ctx.secondary_key.ptr(), current_sk_size,
                                   &my_charset_bin);
            if (check_ctx.first_key.is_empty()) {
              check_ctx.first_key.copy(check_ctx.secondary_key.ptr(), current_sk_size,
                                      &my_charset_bin);
            }
          }
        }
      } else {
        // pk
        check_ctx.primary_key.copy(key.data(), key.size(), &my_charset_bin);
        // record
        int rc =
            convert_record_from_storage_format(&key, &value, tbl->record[0], tbl);

        longlong hidden_pk_id = 0;
        if (has_hidden_pk(tbl) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &check_ctx.primary_key)) {
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // for pk-index and not hidden-pk, we check every key is different or
        // not
        if (!has_hidden_pk(tbl)) {
          if (!check_ctx.last_key.is_empty()) {
            if (check_ctx.last_key.length() == check_ctx.primary_key.length() &&
                memcmp(check_ctx.last_key.ptr(), check_ctx.primary_key.ptr(),
                       check_ctx.last_key.length()) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              DBUG_ASSERT(0);
              goto one_row_checked;
            }
          }
          check_ctx.last_key.copy(check_ctx.primary_key.ptr(), check_ctx.primary_key.length(),
                                 &my_charset_bin);
          if (check_ctx.first_key.is_empty()) {
            check_ctx.first_key.copy(check_ctx.primary_key.ptr(), check_ctx.primary_key.length(),
                                    &my_charset_bin);
          }
        }

        for (uint keyno = 0; keyno < tbl->s->keys; keyno++) {
          if (is_pk(keyno, tbl, m_tbl_def.get())) {
            // Assume the iterator and get would be consistency. Do not do a pk
            // get again.
            continue;
          }

          Xdb_key_def *keydef = m_key_descr_arr[keyno].get();
          uint packed_size = keydef->pack_record(
              tbl, check_ctx.pack_buffer, tbl->record[0],
              check_ctx.sk_packed_tuple,
              &check_ctx.sk_tails, false, hidden_pk_id);

          std::string sk_value_str;
          xengine::common::Status s = tx->get(
              keydef->get_cf(),
              xengine::common::Slice(
                  reinterpret_cast<char *>(check_ctx.sk_packed_tuple), packed_size),
              &sk_value_str);

          // The secondary key kv does not exist.
          if (!s.ok()) {
            sql_print_error(
                "CHECK TABLE %s.%s:   .. row %lld: "
                "iter pk and get from sk, "
                "sk encoded from pk value does not exist",
                db_name, table_name, check_ctx.rows);
            print_error_row(db_name, table_name, check_ctx.primary_key, check_ctx.secondary_key);
            advice_add_index(tbl, keyno, hidden_pk_id,
                             tbl->record[0]);
            error = true;
            DBUG_ASSERT(0);
            goto one_row_checked;
          }
        }
      }
      one_row_checked:
      ++check_ctx.rows;
      if (my_core::thd_killed(current_thd)) {
        return 1;
      }
      return 0;
    };
    int ret = scan_parallel(kd, tx, f);

    // unique check in border
    for (int i = 0; i < static_cast<int>(check_ctx_set.size()) - 1; i++) {
      String& first = check_ctx_set[i]->last_key, &next = check_ctx_set[i + 1]->first_key;
      if (!first.is_empty() && first.length() == next.length() && memcmp(first.ptr(), next.ptr(), first.length()) == 0) {
        error = true;
        break;
      }
      // statistics
      check_ctx_set[check_ctx_set.size() - 1]->rows += check_ctx_set[i]->rows;
      check_ctx_set[check_ctx_set.size() - 1]->checksums +=
          check_ctx_set[i]->checksums;
    }

    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   ... %lld index entries checked "
                          "(%lld had checksums)",
                          db_name, table_name, check_ctx_set[max_threads - 1]->rows,
        check_ctx_set[max_threads - 1]->checksums);

    ha_index_end();
    if (my_core::thd_killed(current_thd)) {
      break;
    }
  }

  // TODO(qimu): row_checksums
//  if (row_checksums != ha_rows(-1)) {
//    // NO_LINT_DEBUG
//    sql_print_information(" CHECK TABLE %s.%s:   %lld "
//                          "table records had checksums",
//                          db_name, table_name, row_checksums);
//  }
  extra(HA_EXTRA_NO_KEYREAD);

  if (my_core::thd_killed(current_thd)) {
    sql_print_information("--------- CHECK TABLE KILLED ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  if (error) {
    // TODO(qimu): row_checksums
//    m_verify_row_debug_checksums = save_verify_row_debug_checksums;
    ha_index_or_rnd_end();
    extra(HA_EXTRA_NO_KEYREAD);
    sql_print_information("--------- CHECK TABLE END ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  sql_print_information("--------- PARALLEL CHECK TABLE END ----------");
  DBUG_RETURN(HA_ADMIN_OK);
}

int ha_xengine::check(THD *const thd, HA_CHECK_OPT *const check_opt) {
  if (THDVAR(ha_thd(), parallel_read_threads) > 1)
    return check_parallel(thd, check_opt);
  sql_print_information("--------- CHECK TABLE START ----------");
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_CHECK);
  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(check_opt != nullptr);

  const uint pk = pk_index(table, m_tbl_def.get());
  uint32_t record_size = table->s->reclength;
  std::unique_ptr<uchar[]> key_record {new uchar[record_size]};

  xengine::common::Slice key;
  String rowkey_copy;
  String sec_key_copy;
  const char *const table_name = table->s->table_name.str;
  const char *const db_name = table->s->db.str;
  bool error = false;

  // Only when debugging: don't use snapshot when reading
  // Xdb_transaction *tx= get_or_create_tx(table->in_use);
  // tx->snapshot= nullptr;

  const bool save_verify_row_debug_checksums = m_verify_row_debug_checksums;
  m_verify_row_debug_checksums = true;

  for (uint i = 0; i < table->s->fields; i++) {
    bitmap_set_bit(table->read_set, i);
  }

  /* For each secondary index, check that we can get a PK value from it */
  // NO_LINT_DEBUG
  sql_print_information(" CHECK TABLE %s.%s: Checking table %s",
                        db_name, table_name, table_name);
  ha_rows row_checksums_at_start = ha_rows(-1); // set/used iff first_index==true
  ha_rows row_checksums = ha_rows(-1);
  bool first_index = true;

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);
  tx->acquire_snapshot(true);

  for (uint iter_keyno = 0; iter_keyno < table->s->keys; iter_keyno++) {
    Xdb_key_def *iter_keydef = m_key_descr_arr[iter_keyno].get();
    extra(HA_EXTRA_KEYREAD);
    ha_index_init(iter_keyno, true);
    ha_rows rows = 0;
    ha_rows checksums = 0;
    if (first_index)
      row_checksums_at_start = m_row_checksums_checked;
    int res = 0;
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   Checking index %s", db_name,
                          table_name, table->key_info[iter_keyno].name);
#ifndef NDEBUG
    while (1) {
      if (!rows)
        res = index_first(table->record[0]);
      else
        res = index_next(table->record[0]);
      if (res == HA_ERR_END_OF_FILE) break;
      ++rows;
    }
#endif

    String last_key_copy; //used to check whether unique attribute is ok
    rows = 0;
    while (1) {
      if (!rows)
        res = index_first(table->record[0]);
      else
        res = index_next(table->record[0]);

      if (res == HA_ERR_END_OF_FILE)
        break;
      if (res) {
        // error
        // NO_LINT_DEBUG
        sql_print_error("CHECK TABLE %s.%s:   .. row %lld: index scan error %d",
                        db_name, table_name, rows, res);
        error = true;
        DBUG_ASSERT(0);
        goto one_row_checked;
      }
      key = m_scan_it->key();
      sec_key_copy.copy(key.data(), key.size(), &my_charset_bin);
      rowkey_copy.copy(m_last_rowkey.ptr(), m_last_rowkey.length(),
                       &my_charset_bin);

      if (!is_pk(iter_keyno, table, m_tbl_def.get())) {
        // Here, we have m_last_rowkey which should be unpack_record from
        // iterator key to table->record[0]. So the sk + pk parts in
        // table->record[0] is valid.
        memcpy(key_record.get(), table->record[0], record_size);

        if (iter_keydef->unpack_info_has_checksum(m_scan_it->value())) {
          checksums++;
        }

        // Step 1:
        // Make a xengine Get using rowkey_copy and put the mysql format
        // result in table->record[0]
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(table) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        if ((res = get_row_by_rowid(table->record[0], rowkey_copy.ptr(),
                                    rowkey_copy.length()))) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, get_row_by_rowid failed",
                          db_name, table_name, rows);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // Step 2:
        // Convert the pk parts in table->record[0] to storage format then
        // compare it with rowkey_copy. table->record[0] is from pk subtable
        // get while the rowkey_copy is from sk subtable iteration.
        uint packed_size = m_pk_descr->pack_record(
            table, m_pack_buffer, table->record[0], m_pk_packed_tuple, nullptr,
            false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != rowkey_copy.length() ||
            memcmp(m_pk_packed_tuple, rowkey_copy.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, "
                          "pk decoded from m_scan_it and get_row_by_rowid mismatch",
                          db_name, table_name, rows);
          print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                          m_retrieved_record);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // Step 3:
        // Convert the sk[iter_keyno], pk parts in table->record[0] to storage
        // format then compare it with sec_key_copy. table->record[0] is from pk
        // subtable get while the sec_key_copy is from sk subtable iteration.
        packed_size = iter_keydef->pack_record(
            table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
            &m_sk_tails, false, hidden_pk_id);
        assert(packed_size <= m_max_packed_sk_len);

        if (packed_size != sec_key_copy.length() ||
            memcmp(m_sk_packed_tuple, sec_key_copy.ptr(), packed_size)) {
          // NO_LINT_DEBUG
          sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                          "iter sk and get from pk, "
                          "sk from m_scan_it and sk decoded from get_row_by_rowid mismatch",
                          db_name, table_name, rows);
          print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                          m_retrieved_record);
          advice_del_index(table, iter_keyno, hidden_pk_id, key_record.get());
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        // Step 4:
        // if index is unique index, we should check whether unique attribute is ok
        if (table->key_info[iter_keyno].flags & HA_NOSAME) {
          uint n_null_fields = 0;
          uint current_sk_size = iter_keydef->get_memcmp_sk_size(table, key, &n_null_fields);
          if (!last_key_copy.is_empty() && n_null_fields == 0) {
            if (last_key_copy.length() == current_sk_size &&
                memcmp(last_key_copy.ptr(), sec_key_copy.ptr(),
                       current_sk_size) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              DBUG_ASSERT(0);
              goto one_row_checked;
            }
          }

          //if no-null fields, there may be duplicated key error
          if (n_null_fields == 0) {
            last_key_copy.copy(sec_key_copy.ptr(), current_sk_size, &my_charset_bin);
          }
        }
      } else {
        longlong hidden_pk_id = 0;
        if (has_hidden_pk(table) &&
            read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey)) {
          error = true;
          DBUG_ASSERT(0);
          goto one_row_checked;
        }

        //for pk-index and not hidden-pk, we check every key is different or not
        if (!has_hidden_pk(table)) {
          if (!last_key_copy.is_empty()) {
            if (last_key_copy.length() == rowkey_copy.length() &&
                memcmp(last_key_copy.ptr(), rowkey_copy.ptr(),
                       last_key_copy.length()) == 0) {
              sql_print_error("CHECK TABLE %s.%s:  unique check failed",
                              db_name, table_name);
              error = true;
              DBUG_ASSERT(0);
              goto one_row_checked;
            }
          }
          last_key_copy.copy(rowkey_copy.ptr(), rowkey_copy.length(), &my_charset_bin);
        }

        for (uint keyno = 0; keyno < table->s->keys; keyno++) {
          if (is_pk(keyno, table, m_tbl_def.get())) {
            // Assume the iterator and get would be consistency. Do not do a pk
            // get again.
            continue;
          }
          Xdb_key_def *keydef = m_key_descr_arr[keyno].get();
          uint packed_size = keydef->pack_record(
            table, m_pack_buffer, table->record[0], m_sk_packed_tuple,
            &m_sk_tails, false, hidden_pk_id);

          std::string sk_value_str;
          xengine::common::Status s = tx->get(keydef->get_cf(),
                xengine::common::Slice(reinterpret_cast<char*>(m_sk_packed_tuple),
                packed_size), &sk_value_str);

          // The secondary key kv does not exist.
          if (!s.ok()) {
            sql_print_error("CHECK TABLE %s.%s:   .. row %lld: "
                            "iter pk and get from sk, "
                            "sk encoded from pk value does not exist",
                            db_name, table_name, rows);
            print_error_row(db_name, table_name, rowkey_copy, sec_key_copy,
                            m_scan_it->value().ToString());
            advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
            error = true;
            DBUG_ASSERT(0);
            goto one_row_checked;
          }
          // The key of a fast check uk exists, but the value is another pk.
#if 0     // TODO: need port fast uk.
          if (keydef->is_support_fast_unique_check()) {
            uint32_t pk_size = 0;
            if (get_pkvalue_from_fast_uk(sk_value_slice, pk_size)) {
              XHANDLER_LOG(ERROR, "get pkvalue from fast_uk error!");
              advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
              error = true;
              goto one_row_checked;
            }
            if (rowkey_copy.length() != pk_size ||
                memcmp(rowkey_copy.ptr(), m_pk_packed_tuple, pk_size)) {
              advice_add_index(table, keyno, hidden_pk_id, table->record[0]);
              error = true;
              goto one_row_checked;
            }
          }
#endif
        }
      }
one_row_checked:
      ++rows;
      DBUG_EXECUTE_IF("serial_check_kill_1", table->in_use->killed = THD::KILL_QUERY;);
      if (my_core::thd_killed(current_thd)) {
        break;
      }
    }
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   ... %lld index entries checked "
                          "(%lld had checksums)",
                          db_name, table_name, rows, checksums);

    if (first_index) {
      row_checksums = m_row_checksums_checked - row_checksums_at_start;
      first_index = false;
    }
    ha_index_end();
    DBUG_EXECUTE_IF("serial_check_kill_2", table->in_use->killed = THD::KILL_QUERY;);
    if (my_core::thd_killed(current_thd)) {
      break;
    }
  }

  if (row_checksums != ha_rows(-1)) {
    // NO_LINT_DEBUG
    sql_print_information(" CHECK TABLE %s.%s:   %lld "
                          "table records had checksums",
                          db_name, table_name, row_checksums);
  }
  extra(HA_EXTRA_NO_KEYREAD);

  m_verify_row_debug_checksums = save_verify_row_debug_checksums;

  if (my_core::thd_killed(current_thd)) {
    sql_print_information("--------- CHECK TABLE KILLED ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  if (error) {
    m_verify_row_debug_checksums = save_verify_row_debug_checksums;
    ha_index_or_rnd_end();
    extra(HA_EXTRA_NO_KEYREAD);
    sql_print_information("--------- CHECK TABLE END ----------");
    DBUG_RETURN(HA_ADMIN_CORRUPT);
  }

  sql_print_information("--------- CHECK TABLE END ----------");
  DBUG_RETURN(HA_ADMIN_OK);
}

static void dbug_dump_str(FILE *const out, const char *const str, int len) {
  fprintf(out, "\"");
  for (int i = 0; i < len; i++) {
    if (str[i] > 32)
      fprintf(out, "%c", str[i]);
    else
      fprintf(out, "\\%d", str[i]);
  }
  fprintf(out, "\"");
}

/*
  Debugging help: dump the whole database into a human-readable file.
  Usage:
    dbug_dump_database(xdb);
*/

void dbug_dump_database(xengine::db::DB *const db) {
  FILE *const out = fopen("/tmp/xengine.dump", "wt");
  if (!out)
    return;

  IteratorUptr it(db->NewIterator(xengine::common::ReadOptions()));
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    xengine::common::Slice key = it->key();
    xengine::common::Slice val = it->value();
    dbug_dump_str(out, key.data(), key.size());
    fprintf(out, " -> ");
    dbug_dump_str(out, val.data(), val.size());
    fprintf(out, "\n");
  }

  fclose(out);
}

xengine::common::Status ha_xengine::get_for_update(
    Xdb_transaction *const tx, xengine::db::ColumnFamilyHandle *const column_family,
    const xengine::common::Slice &key, std::string *const value, bool lock_unique) const {
  DBUG_ASSERT(m_lock_rows != XDB_LOCK_NONE);
  const bool exclusive = m_lock_rows != XDB_LOCK_READ;
  QUERY_TRACE_BEGIN(xengine::monitor::TracePoint::HA_GET_FOR_UPDATE);

  xengine::common::Status s = tx->get_for_update(column_family, key, value, exclusive, lock_unique);

  // If we have a lock conflict and we are running in READ COMMITTTED mode
  // release and reacquire the snapshot and then retry the get_for_update().
  if (s.IsBusy() && !s.IsDeadlock() &&
      my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
    tx->release_snapshot();
    tx->acquire_snapshot(false);

    s = tx->get_for_update(column_family, key, value, exclusive, lock_unique);
  }

  QUERY_TRACE_END();
  return s;
}

xengine::common::Status ha_xengine::lock_unique_key(Xdb_transaction *const tx,
                                                    xengine::db::ColumnFamilyHandle *const column_family,
                                                    const xengine::common::Slice &key,
                                                    const bool total_order_seek,
                                                    const bool fill_cache) const
{
  DBUG_ASSERT(m_lock_rows != XDB_LOCK_NONE);
  const bool exclusive = m_lock_rows != XDB_LOCK_READ;

  xengine::common::Status s = tx->lock_unique_key(column_family, key, total_order_seek, fill_cache, exclusive);

  // If we have a lock conflict and we are running in READ COMMITTED mode
  // release and reacquire the snapshot and then retry the get_for_update().
  if (s.IsBusy() && !s.IsDeadlock() && my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
    tx->release_snapshot();
    tx->acquire_snapshot(false);
    s = tx->lock_unique_key(column_family, key, total_order_seek, fill_cache, exclusive);
  }
  return s;
}

bool ha_xengine::is_blind_delete_enabled() {
  return false;
}

/*
  Given a rowid (i.e. packed PK) as a parameter, get the record.

  @return
    0      - Ok
    other  - HA_ERR error code.
*/

int ha_xengine::get_row_by_rowid(uchar *const buf, const char *const rowid,
                                 const uint rowid_size,
                                 std::string& retrieved_record, TABLE* tbl,
                                 String& key, const bool skip_lookup) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(rowid != nullptr);
  DBUG_ASSERT(tbl != nullptr);

  int rc;

  xengine::common::Slice key_slice(rowid, rowid_size);

  Xdb_transaction *const tx = get_or_create_tx(tbl->in_use);
  DBUG_ASSERT(tx != nullptr);

  // only check ha_thd() with no parallelization cause current_thd is thread_local
  if (tbl == table)
    DEBUG_SYNC(ha_thd(), "xengine.get_row_by_rowid");
  DBUG_EXECUTE_IF("dbug.xengine.get_row_by_rowid", {
    THD *thd = ha_thd();
    const char act[] = "now signal Reached "
                       "wait_for signal.xengine.get_row_by_rowid_let_running";
    DBUG_ASSERT(opt_debug_sync_timeout > 0);
    DBUG_ASSERT(!debug_sync_set_action(thd, STRING_WITH_LEN(act)));
  };);

  bool found;
  xengine::common::Status s;

  /* Pretend row found without looking up */
  if (skip_lookup)
  {
   // stats.rows_deleted_blind++;
#if 0 // DEL-SYSSAT
    update_row_stats(ROWS_DELETED_BLIND);
#endif
    key.copy((const char *)rowid, rowid_size, &my_charset_bin);
    //table->status = 0;
    DBUG_RETURN(0);
  }

  if (m_lock_rows == XDB_LOCK_NONE) {
    tx->acquire_snapshot(true);
    s = tx->get(m_pk_descr->get_cf(), key_slice, &retrieved_record);
  } else {
    s = get_for_update(tx, m_pk_descr->get_cf(), key_slice,
                       &retrieved_record);
  }

  if (!s.IsNotFound() && !s.ok()) {
    __XHANDLER_LOG(WARN, "DML: lock failed for key(%s) on index %u with error %s, table_name is: %s",
                   key_slice.ToString(true).c_str(), m_pk_descr->get_index_number(),
                   s.ToString().c_str(), tbl->s->table_name.str);
    DBUG_RETURN(tx->set_status_error(tbl->in_use, s, *m_pk_descr, m_tbl_def.get()));
  }
  found = !s.IsNotFound();

  //table->status = STATUS_NOT_FOUND;
  if (found) {
    key.copy((const char *)rowid, rowid_size, &my_charset_bin);
    rc = convert_record_from_storage_format(&key_slice, retrieved_record, buf, tbl);
    //if (!rc)
    //  table->status = 0;
  } else {
    /*
      Note: we don't need to unlock the row. It is intentional that we keep
      locks on rows that don't exist.
    */
    rc = HA_ERR_KEY_NOT_FOUND;
  }

  DBUG_RETURN(rc);
}

int ha_xengine::index_next(uchar *const buf) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(xengine::monitor::TracePoint::HA_INDEX_NEXT);

  bool moves_forward = true;
  ha_statistic_increment(&SSV::ha_read_next_count);
  if (m_key_descr_arr[active_index]->m_is_reverse_cf) {
    moves_forward = false;
  }

  int rc = index_next_with_direction(buf, moves_forward);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

int ha_xengine::index_prev(uchar *const buf) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(xengine::monitor::TracePoint::HA_INDEX_NEXT);

  bool moves_forward = false;
  ha_statistic_increment(&SSV::ha_read_prev_count);
  if (m_key_descr_arr[active_index]->m_is_reverse_cf) {
    moves_forward = true;
  }

  int rc = index_next_with_direction(buf, moves_forward);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

int ha_xengine::index_next_with_direction(uchar *const buf, bool move_forward) {
  DBUG_ENTER_FUNC();

  int rc;

  if (active_index == pk_index(table, m_tbl_def.get())) {
    rc = rnd_next_with_direction(buf, move_forward);
  } else {
    if (m_skip_scan_it_next_call) {
      m_skip_scan_it_next_call = false;
    } else {
      if (move_forward)
        m_scan_it->Next(); /* this call cannot fail */
      else
        m_scan_it->Prev();
    }
    rc = find_icp_matching_index_rec(move_forward, buf);
    if (!rc)
      rc = secondary_index_read(active_index, buf);
  }

  DBUG_RETURN(rc);
}

int ha_xengine::index_first(uchar *const buf) {
  DBUG_ENTER_FUNC();

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&SSV::ha_read_first_count);
  int rc = m_key_descr_arr[active_index]->m_is_reverse_cf
               ? index_last_intern(buf)
               : index_first_intern(buf);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  DBUG_RETURN(rc);
}

int ha_xengine::index_last(uchar *const buf) {
  DBUG_ENTER_FUNC();

  m_sk_match_prefix = nullptr;
  ha_statistic_increment(&SSV::ha_read_last_count);
  int rc = m_key_descr_arr[active_index]->m_is_reverse_cf
               ? index_first_intern(buf)
               : index_last_intern(buf);
  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  DBUG_RETURN(rc);
}

/*
  Start scanning from the "first" value.

  The 'first' here means "the first from start of the key space".
  For reverse-ordered key spaces, the first value will be the biggest, 'zzzz'.

  An picture of a forward-ordered keyspace (remember, the keys have form
  'indexnr-keyval'. Suppose the index we are at has number n)

      (n-1) - ...
      ( n )          <--- 1. (n) doesn't exist in the db but it would be here.
      ( n ) - aaa       <--- 2. Seek("n") will put us here on the first index
      ( n ) - bbb               record.
      ( n ) - cc

  So, need to do: Seek(n);

  A backward-ordered keyspace:

      (n+1) - bbb
      (n+1) - aaa
      (n+1)        <--- (n+1) doesn't exist in the db but would be here.
      ( n ) - ccc       <--- 1. We need to be here.
      ( n ) - bbb
      ( n ) - aaa
      ( n )

  So, need to: Seek(n+1);

*/

int ha_xengine::index_first_intern(uchar *const buf) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);

  uchar *key;
  uint key_size;
  int rc;

  if (is_pk(active_index, table, m_tbl_def.get())) {
    key = m_pk_packed_tuple;
  } else {
    key = m_sk_packed_tuple;
  }

  DBUG_ASSERT(key != nullptr);

  const Xdb_key_def &kd = *m_key_descr_arr[active_index];
  if (kd.m_is_reverse_cf) {
    kd.get_supremum_key(key, &key_size);
  } else {
    kd.get_infimum_key(key, &key_size);
  }

  xengine::common::Slice index_key((const char *)key, key_size);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);

  const bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    setup_scan_iterator(kd, &index_key, false, !kd.m_is_reverse_cf,
                        Xdb_key_def::INDEX_NUMBER_SIZE);
    m_skip_scan_it_next_call = true;

    rc = index_next_with_direction(buf, true);
    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; // exit the loop

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (!rc) {
    /*
      index_next is always incremented on success, so decrement if it is
      index_first instead
     */
    //stats.rows_index_first++;
    //stats.rows_index_next--;
  }

  DBUG_RETURN(rc);
}

/**
  @details
  Start scanning from the "last" value

  The 'last' here means "the last from start of the key space".
  For reverse-ordered key spaces, we will actually read the smallest value.

  An picture of a forward-ordered keyspace (remember, the keys have form
  'indexnr-keyval'. Suppose the we are at a key that has number n)

     (n-1)-something
     ( n )-aaa
     ( n )-bbb
     ( n )-ccc            <----------- Need to seek to here.
     (n+1)      <---- Doesn't exist, but would be here.
     (n+1)-smth, or no value at all

   XENGINE's Iterator::Seek($val) seeks to "at $val or first value that's
   greater". We can't see to "(n)-ccc" directly, because we don't know what
   is the value of 'ccc' (the biggest record with prefix (n)). Instead, we seek
   to "(n+1)", which is the least possible value that's greater than any value
   in index #n. Then we step one record back.

   So, need to:  it->Seek(n+1) || it->SeekToLast(); it->Prev();

   A backward-ordered keyspace:

      (n+1)-something
      ( n ) - ccc
      ( n ) - bbb
      ( n ) - aaa       <---------------- (*) Need to seek here.
      ( n ) <--- Doesn't exist, but would be here.
      (n-1)-smth, or no value at all

   So, need to:  it->Seek(n) || it->SeekToLast(); it->Prev();
*/

int ha_xengine::index_last_intern(uchar *const buf) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);

  uchar *key;
  uint key_size;
  int rc;

  if (is_pk(active_index, table, m_tbl_def.get())) {
    key = m_pk_packed_tuple;
  } else {
    key = m_sk_packed_tuple;
  }

  DBUG_ASSERT(key != nullptr);

  const Xdb_key_def &kd = *m_key_descr_arr[active_index];
  if (kd.m_is_reverse_cf) {
    kd.get_infimum_key(key, &key_size);
  } else {
    kd.get_supremum_key(key, &key_size);
  }

  xengine::common::Slice index_key((const char *)key, key_size);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);

  bool is_new_snapshot = !tx->has_snapshot();
  // Loop as long as we get a deadlock error AND we end up creating the
  // snapshot here (i.e. it did not exist prior to this)
  for (;;) {
    setup_scan_iterator(kd, &index_key, false, kd.m_is_reverse_cf,
                        Xdb_key_def::INDEX_NUMBER_SIZE);

    if (!m_scan_it->Valid()) {
      /*
        XENGINE keyspace has no records that follow our index. It could be, our
        index is the last in the keyspace. Navigate to last record in the
        keyspace.
      */
      m_scan_it->SeekToLast();
    } else {
      /*
        Iterator is pointing at some record right after the end of the index.
        Move one record back.
      */
      m_scan_it->Prev();
    }
    m_skip_scan_it_next_call = false;

    if (is_pk(active_index, table, m_tbl_def.get())) {
      m_skip_scan_it_next_call = true;
      rc = rnd_next_with_direction(buf, false);
    } else {
      rc = find_icp_matching_index_rec(false /*move_forward*/, buf);
      if (!rc)
        rc = secondary_index_read(active_index, buf);
    }

    if (rc != HA_ERR_LOCK_DEADLOCK || !is_new_snapshot)
      break; /* exit the loop */

    // release the snapshot and iterator so they will be regenerated
    tx->release_snapshot();
    release_scan_iterator();
  }

  if (!rc) {
    /*
      index_next is always incremented on success, so decrement if it is
      index_first instead
     */
    //stats.rows_index_first++;
    //stats.rows_index_next--;
  }

  DBUG_RETURN(rc);
}

void ha_xengine::unlock_row() {
  DBUG_ENTER_FUNC();

  if (m_lock_rows != XDB_LOCK_NONE) {
    Xdb_transaction *const tx = get_or_create_tx(table->in_use);
    tx->release_lock(m_pk_descr->get_cf(),
                     std::string(m_last_rowkey.ptr(), m_last_rowkey.length()));
  }

  DBUG_VOID_RETURN;
}

/*
  Returning true if SingleDelete can be used.
  - Secondary Indexes can always use SingleDelete.
  - If the index is PRIMARY KEY, and if all of the columns of the table
    are covered by the PRIMARY KEY, SingleDelete can be used.
*/
bool ha_xengine::can_use_single_delete(const uint &index) const {
  return (index != pk_index(table, m_tbl_def.get()) ||
          (!has_hidden_pk(table) &&
           table->key_info[index].actual_key_parts == table->s->fields));
}

bool ha_xengine::skip_unique_check() const {
  /*
    We want to skip unique checks if:
      1) bulk_load is on
      2) this table is in the whitelist of tables to skip and the replication
         lag has reached a large enough value (see unique_check_lag_threshold
         and unique_check_lage_reset_threshold)
      3) the user set unique_checks option to 0, and the table does not have
         any indexes. If the table has secondary keys, then those might becomes
         inconsisted/corrupted
  */
  return /*THDVAR(table->in_use, bulk_load) ||*/
         (m_force_skip_unique_check && m_skip_unique_check) ||
         (my_core::thd_test_options(table->in_use,
                                    OPTION_RELAXED_UNIQUE_CHECKS) &&
          m_tbl_def->m_key_count == 1) ||
         (xengine_skip_unique_key_check_in_boost_insert &&
          !my_core::thd_test_options(table->in_use, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) ;
}

//void ha_xengine::set_force_skip_unique_check(bool skip) {
//  DBUG_ENTER_FUNC();
//
//  m_force_skip_unique_check = skip;
//
//  DBUG_VOID_RETURN;
//}

bool ha_xengine::commit_in_the_middle() {
  return true; /* THDVAR(table->in_use, bulk_load) ||
         THDVAR(table->in_use, commit_in_the_middle); */
}

/*
  Executing bulk commit if it should.
  @retval true if bulk commit failed
  @retval false if bulk commit was skipped or succeeded
*/
bool ha_xengine::do_bulk_commit(Xdb_transaction *const tx) {
  DBUG_ASSERT(tx != nullptr);

  if (commit_in_the_middle()) {
    bool ret = tx->get_write_count() >= XDB_DEFAULT_BULK_LOAD_SIZE /*THDVAR(table->in_use, bulk_load_size)*/ &&
           tx->flush_batch();

    DBUG_EXECUTE_IF("ddl_log_crash_after_commit_in_the_middle", DBUG_SUICIDE(););
    return ret;
  }

  return false;
}

/*
  If table was created without primary key, SQL layer represents the primary
  key number as MAX_INDEXES.  Hence, this function returns true if the table
  does not contain a primary key. (In which case we generate a hidden
  'auto-incremented' pk.)
*/
bool ha_xengine::has_hidden_pk(const TABLE *const table) const {
  DBUG_ASSERT(table != nullptr);
  return Xdb_key_def::table_has_hidden_pk(table);
}

/*
  Returns true if given index number is a hidden_pk.
  - This is used when a table is created with no primary key.
*/
bool ha_xengine::is_hidden_pk(const uint index, const TABLE *const table_arg,
                              const Xdb_tbl_def *const tbl_def_arg) {
  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(table_arg->s != nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);

  return (table_arg->s->primary_key == MAX_INDEXES &&
          index == tbl_def_arg->m_key_count - 1);
}

/* Returns index of primary key */
uint ha_xengine::pk_index(const TABLE *const table_arg,
                          const Xdb_tbl_def *const tbl_def_arg) {
  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(table_arg->s != nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);

  return table_arg->s->primary_key == MAX_INDEXES ? tbl_def_arg->m_key_count - 1
                                                  : table_arg->s->primary_key;
}

/* Returns true if given index number is a primary key */
bool ha_xengine::is_pk(const uint index, const TABLE *const table_arg,
                       const Xdb_tbl_def *const tbl_def_arg) {
  DBUG_ASSERT(table_arg != nullptr);
  DBUG_ASSERT(table_arg->s != nullptr);
  DBUG_ASSERT(tbl_def_arg != nullptr);

  return index == table_arg->s->primary_key ||
         is_hidden_pk(index, table_arg, tbl_def_arg);
}

const char *ha_xengine::get_key_name(const uint index,
                                     const TABLE *const table_arg,
                                     const Xdb_tbl_def *const tbl_def_arg) {
  DBUG_ASSERT(table_arg != nullptr);

  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    return HIDDEN_PK_NAME;
  }

  return table_arg->key_info[index].name;
}

const char *ha_xengine::get_key_comment(const uint index,
                                        const TABLE *const table_arg,
                                        const Xdb_tbl_def *const tbl_def_arg) {
  DBUG_ASSERT(table_arg != nullptr);

  if (is_hidden_pk(index, table_arg, tbl_def_arg)) {
    return nullptr;
  }

  return table_arg->key_info[index].comment.str;
}

int ha_xengine::write_row(uchar *const buf) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);
  DBUG_ASSERT(buf == table->record[0]);
  DBUG_ASSERT(m_lock_rows == XDB_LOCK_WRITE);

  ha_statistic_increment(&SSV::ha_write_count);
  /*
    Note: "buf == table->record[0]" is copied from innodb. I am not aware of
    any use cases where this condition is not true.
  */
  if (table->next_number_field && buf == table->record[0]) {
    int err;
    if ((err = update_auto_increment())) {
      DBUG_RETURN(err);
    }
  }

  const int rv = update_write_row(nullptr, buf, skip_unique_check());

  if (rv == 0) {
    //stats.rows_inserted++;
    update_row_stats(ROWS_INSERTED);
  }

  DBUG_RETURN(rv);
}

/**
  Constructing m_last_rowkey (XEngine key expression) from
  before_update|delete image (MySQL row expression).
  m_last_rowkey is normally set during lookup phase, such as
  rnd_next_with_direction() and rnd_pos(). With Read Free Replication,
  these read functions are skipped and update_rows(), delete_rows() are
  called without setting m_last_rowkey. This function sets m_last_rowkey
  for Read Free Replication.
*/
void ha_xengine::set_last_rowkey(const uchar *const old_data) {
  if (old_data && use_read_free_rpl()) {
    const uint old_pk_size = m_pk_descr->pack_record(
        table, m_pack_buffer, old_data, m_pk_packed_tuple, nullptr, false);

    assert(old_pk_size <= m_max_packed_sk_len);
    m_last_rowkey.copy((const char *)m_pk_packed_tuple, old_pk_size,
                       &my_charset_bin);
  }
}

int ha_xengine::get_pk_for_update(struct update_row_info *const row_info) {
  uint size;

  /*
    Get new row key for any insert, and any update where the pk is not hidden.
    Row key for updates with hidden pk is handled below.
  */
  if (!has_hidden_pk(table)) {
    row_info->hidden_pk_id = 0;

    row_info->new_pk_unpack_info = &m_pk_unpack_info;

    size = m_pk_descr->pack_record(table, m_pack_buffer, row_info->new_data,
                                   m_pk_packed_tuple,
                                   row_info->new_pk_unpack_info, false);
    assert(size <= m_max_packed_sk_len);
  } else if (row_info->old_data == nullptr) {
    row_info->hidden_pk_id = update_hidden_pk_val();
    size =
        m_pk_descr->pack_hidden_pk(row_info->hidden_pk_id, m_pk_packed_tuple);
  } else {
    /*
      If hidden primary key, rowkey for new record will always be the same as
      before
    */
    size = row_info->old_pk_slice.size();
    memcpy(m_pk_packed_tuple, row_info->old_pk_slice.data(), size);
    if (read_hidden_pk_id_from_rowkey(&row_info->hidden_pk_id, &m_last_rowkey)) {
      return HA_ERR_INTERNAL_ERROR;
    }
  }

  row_info->new_pk_slice =
      xengine::common::Slice((const char *)m_pk_packed_tuple, size);

  return HA_EXIT_SUCCESS;
}

int ha_xengine::check_and_lock_unique_pk(const uint &key_id,
                                         const struct update_row_info &row_info,
                                         bool *const found,
                                         bool *const pk_changed) {
  DEBUG_SYNC(ha_thd(), "xengine.check_and_lock_unique_pk");
  DBUG_ASSERT(found != nullptr);
  DBUG_ASSERT(pk_changed != nullptr);

  *pk_changed = false;

  /*
    For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
    always require locking.
  */
  if (row_info.old_pk_slice.size() > 0) {
    /*
      If the keys are the same, then no lock is needed
    */
    if (!Xdb_pk_comparator::bytewise_compare(row_info.new_pk_slice,
                                             row_info.old_pk_slice)) {
      *found = false;
      return HA_EXIT_SUCCESS;
    }

    *pk_changed = true;
  }

  /*
    Perform a read to determine if a duplicate entry exists. For primary
    keys, a point lookup will be sufficient.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  /*
    To prevent race conditions like below, it is necessary to
    take a lock for a target row. get_for_update() holds a gap lock if
    target key does not exist, so below conditions should never
    happen.

    1) T1 Get(empty) -> T2 Get(empty) -> T1 Put(insert) -> T1 commit
       -> T2 Put(overwrite) -> T2 commit
    2) T1 Get(empty) -> T1 Put(insert, not committed yet) -> T2 Get(empty)
       -> T2 Put(insert, blocked) -> T1 commit -> T2 commit(overwrite)
  */
  const xengine::common::Status s =
      get_for_update(row_info.tx, m_pk_descr->get_cf(), row_info.new_pk_slice,
                     &m_retrieved_record);
  if (!s.ok() && !s.IsNotFound()) {
    __XHANDLER_LOG(WARN, "DML: get_for_update for key(%s) on index(%u) failed with error:%s, table_name: %s",
                   row_info.new_pk_slice.ToString(true).c_str(),
                   m_pk_descr->get_index_number(), s.ToString().c_str(),
                   table->s->table_name.str);
    return row_info.tx->set_status_error(table->in_use, s,
                                         *m_key_descr_arr[key_id], m_tbl_def.get());
  }

  *found = !s.IsNotFound();
  return HA_EXIT_SUCCESS;
}

int ha_xengine::check_and_lock_sk(const uint &key_id,
                                  const struct update_row_info &row_info,
                                  bool *const found) const {
  DEBUG_SYNC(ha_thd(), "xengine.check_and_lock_sk");
  DBUG_ASSERT(found != nullptr);
  *found = false;

  /*
    Can skip checking this key if none of the key fields have changed.
  */
  if (row_info.old_data != nullptr && !m_update_scope.is_set(key_id)) {
    return HA_EXIT_SUCCESS;
  }

  KEY *key_info = nullptr;
  uint n_null_fields = 0;
  uint user_defined_key_parts = 1;

  key_info = &table->key_info[key_id];
  user_defined_key_parts = key_info->user_defined_key_parts;
  /*
    If there are no uniqueness requirements, there's no need to obtain a
    lock for this key.
  */
  if (!(key_info->flags & HA_NOSAME)) {
    return HA_EXIT_SUCCESS;
  }

  const Xdb_key_def &kd = *m_key_descr_arr[key_id];

  /*
    Calculate the new key for obtaining the lock

    For unique secondary indexes, the key used for locking does not
    include the extended fields.
  */
  uint size =
      kd.pack_record(table, m_pack_buffer, row_info.new_data, m_sk_packed_tuple,
                     nullptr, false, 0, user_defined_key_parts, &n_null_fields);
  assert(size <= m_max_packed_sk_len);

  if (n_null_fields > 0) {
    /*
      If any fields are marked as NULL this will never match another row as
      to NULL never matches anything else including another NULL.
     */
    return HA_EXIT_SUCCESS;
  }

  const xengine::common::Slice new_slice =
      xengine::common::Slice((const char *)m_sk_packed_tuple, size);

  /*
    For UPDATEs, if the key has changed, we need to obtain a lock. INSERTs
    always require locking.
  */
  if (row_info.old_data != nullptr) {
    size = kd.pack_record(table, m_pack_buffer, row_info.old_data,
                          m_sk_packed_tuple_old, nullptr, false,
                          0, user_defined_key_parts);
    assert(size <= m_max_packed_sk_len);
    const xengine::common::Slice old_slice =
        xengine::common::Slice((const char *)m_sk_packed_tuple_old, size);

    /*
      For updates, if the keys are the same, then no lock is needed

      Also check to see if the key has any fields set to NULL. If it does, then
      this key is unique since NULL is not equal to each other, so no lock is
      needed.
    */
    if (!Xdb_pk_comparator::bytewise_compare(new_slice, old_slice)) {
      return HA_EXIT_SUCCESS;
    }
  }

  /*
    Perform a read to determine if a duplicate entry exists - since this is
    a secondary indexes a range scan is needed.

    note: we intentionally don't set options.snapshot here. We want to read
    the latest committed data.
  */

  const bool all_parts_used = (user_defined_key_parts == kd.get_key_parts());

  /*
    This iterator seems expensive since we need to allocate and free
    memory for each unique index.

    If this needs to be optimized, for keys without NULL fields, the
    extended primary key fields can be migrated to the value portion of the
    key. This enables using Get() instead of Seek() as in the primary key
    case.

    The bloom filter may need to be disabled for this lookup.
  */
  const bool total_order_seek = !can_use_bloom_filter(
      ha_thd(), kd, new_slice, all_parts_used,
      is_ascending(*m_key_descr_arr[key_id], HA_READ_KEY_EXACT));
  const bool fill_cache = true; // !THDVAR(ha_thd(), skip_fill_cache);

  /*
    psergey-todo: we just need to take lock, lookups not needed:
  */
  //std::string dummy_value;
  DBUG_ASSERT(kd.m_is_reverse_cf == false);
  const xengine::common::Status s =
      lock_unique_key(row_info.tx, kd.get_cf(), new_slice, total_order_seek, fill_cache);
  if (!s.ok() && !s.IsNotFound()) {
    __XHANDLER_LOG(WARN, "DML: lock_unique_key for key(%s) on index:%d failed with error:%s, table_name: %s",
                   new_slice.ToString(true).c_str(),
                   kd.get_index_number(), s.ToString().c_str(),
                   table->s->table_name.str);
    return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def.get());
  }

  IteratorUptr iter(row_info.tx->get_iterator(kd.get_cf(), total_order_seek,
      fill_cache, true /* read current data */, false /* acquire snapshot */));
  /*
    Need to scan the transaction to see if there is a duplicate key.
    Also need to scan XENGINE and verify the key has not been deleted
    in the transaction.
  */
  iter->Seek(new_slice);
  *found = (HA_EXIT_SUCCESS == read_key_exact(kd, iter.get(), all_parts_used, new_slice));

  return HA_EXIT_SUCCESS;
}

int ha_xengine::check_uniqueness_and_lock(
    const struct update_row_info &row_info, bool *const pk_changed)
{
  int rc = HA_EXIT_SUCCESS;
  /*
    Go through each index and determine if the index has uniqueness
    requirements. If it does, then try to obtain a row lock on the new values.
    Once all locks have been obtained, then perform the changes needed to
    update/insert the row.
  */
  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    bool found;

    if (is_pk(key_id, table, m_tbl_def.get())) {
      rc = check_and_lock_unique_pk(key_id, row_info, &found, pk_changed);
    } else {
      rc = check_and_lock_sk(key_id, row_info, &found);
    }

    if (rc != 0) {
      return rc;
    }

    if (found) {
      /* There is a row with this key already, so error out. */
      errkey = key_id;
      m_dupp_errkey = errkey;
      return HA_ERR_FOUND_DUPP_KEY;
    }
  }

  for (auto &k : m_tbl_def->m_added_key) {
    const Xdb_key_def &kd = *k.first;
    TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab->s->primary_key) {
      continue;
    }

    bool found = false;
    rc = lock_and_check_new_index_sk(kd, k.second, row_info, &found);

    if (rc != HA_EXIT_SUCCESS){
      __XHANDLER_LOG(WARN, "XEngineDDL: check duplicated key failed for new added key %u, errcode is %d, table_name:%s",
                     kd.get_index_number(), rc, table->s->table_name.str);
      break;
    }
  }

  return rc;
}

int ha_xengine::bulk_load_key(Xdb_transaction *const tx, const Xdb_key_def &kd,
                              const xengine::common::Slice &key,
                              const xengine::common::Slice &value,
                              int level) {
  xengine::db::ColumnFamilyHandle *const cf = kd.get_cf();
  DBUG_ASSERT(cf != nullptr);

  if (mtables_ == nullptr) {
    mtables_ = std::make_shared<xengine::db::MiniTables>();
    m_change_info = std::make_unique<storage::ChangeInfo>();
    mtables_->change_info_ = m_change_info.get();
    mtables_->level = level;

  }

  if (m_sst_info == nullptr) {
    m_sst_info = std::make_shared<Xdb_sst_info>(
        xdb, m_table_handler->m_table_name, kd.get_name(), cf,
        xengine_db_options, false/*THDVAR(ha_thd(), trace_sst_api)*/, mtables_.get());
    tx->start_bulk_load(this);
    m_bulk_load_tx = tx;
  }

  DBUG_ASSERT(m_sst_info != nullptr);

  int rc = m_sst_info->put(key, value);
  if (rc != 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                    "Failed to add a key to sst file writer(%s)", MYF(0),
                    m_sst_info->error_message().c_str());
    rc = HA_ERR_INTERNAL_ERROR;
  }

  return rc;
}

int ha_xengine::finalize_bulk_load() {
  int rc = 0;

  /* Skip if there are no possible ongoing bulk loads */
  if (m_sst_info == nullptr && m_bulk_load_tx == nullptr) {
    return rc;
  }

  XDB_MUTEX_LOCK_CHECK(m_bulk_load_mutex);

  /*
    We need this check because it's possible that m_sst_info has been
    flushed and cleared by another thread by the time the mutex has been
    acquired.
  */
  if (m_sst_info != nullptr) {
    rc = m_sst_info->commit();
    if (rc != 0) {
      /*
        Log the error immediately here in case the server crashes before
        mysql prints via my_printf_error.
      */
      sql_print_error("Failed to commit bulk loaded sst file to the "
                      "data store (%s)",
                      m_sst_info->error_message().c_str());

      my_printf_error(ER_UNKNOWN_ERROR,
                      "Failed to commit bulk loaded sst file to the "
                      "data store (%s)",
                      MYF(0), m_sst_info->error_message().c_str());
      rc = HA_ERR_INTERNAL_ERROR;
    }

    m_sst_info = nullptr;
    mtables_ = nullptr;
    m_change_info.reset();
    m_bulk_load_tx->end_bulk_load(this);
    m_bulk_load_tx = nullptr;
  }

  XDB_MUTEX_UNLOCK_CHECK(m_bulk_load_mutex);

  return rc;
}

int ha_xengine::update_pk(const Xdb_key_def &kd,
                          const struct update_row_info &row_info,
                          const bool &pk_changed) {
  int rc = HA_EXIT_SUCCESS;

  const uint key_id = kd.get_keyno();
  const bool hidden_pk = is_hidden_pk(key_id, table, m_tbl_def.get());

  if (!hidden_pk && pk_changed) {
    /*
      The old key needs to be deleted.
    */
    const xengine::common::Status s = delete_or_singledelete(
        key_id, row_info.tx, kd.get_cf(), row_info.old_pk_slice);
    if (!s.ok()) {
      __XHANDLER_LOG(WARN, "DML: failed to delete old record(%s) for updating pk(%u) with error %s, table_name: %s",
                     row_info.old_pk_slice.ToString(true).c_str(),
                     kd.get_index_number(), s.ToString().c_str(),
                     table->s->table_name.str);
      return row_info.tx->set_status_error(table->in_use, s, kd, m_tbl_def.get());
    }
  }

  if (table->found_next_number_field) {
    update_auto_incr_val();
  }

  xengine::common::Slice value_slice;
  if ((rc = convert_record_to_storage_format(
           row_info.new_pk_slice, row_info.new_pk_unpack_info, &value_slice))) {
    __XHANDLER_LOG(ERROR, "convert record to xengine format error, code is %d",
                   rc);
    return rc;
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
        __XHANDLER_LOG(WARN, "DML: duplicate entry is found for key(%s) on pk:%d, table_name:%s",
                       row_info.new_pk_slice.ToString(true).c_str(),
                       kd.get_index_number(), table->s->table_name.str);
        errkey = table->s->primary_key;
        m_dupp_errkey = errkey;
        rc = HA_ERR_FOUND_DUPP_KEY;
      } else {
        __XHANDLER_LOG(WARN, "DML: failed to put record(%s:%s) for updating pk(%u) with error %s, table_name: %s",
                       row_info.new_pk_slice.ToString(true).c_str(),
                       value_slice.ToString(true).c_str(),
                       kd.get_index_number(), s.ToString().c_str(),
                       table->s->table_name.str);
        rc = row_info.tx->set_status_error(table->in_use, s, *m_pk_descr,
                                           m_tbl_def.get());
      }
    }
  }

  return rc;
}

int ha_xengine::update_sk(const TABLE *const table_arg, const Xdb_key_def &kd,
                          const struct update_row_info &row_info,
                          const TABLE *const altered_table) {
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

  new_packed_size = kd.pack_record(
      table_arg, m_pack_buffer, row_info.new_data, m_sk_packed_tuple,
      &m_sk_tails, store_row_debug_checksums, row_info.hidden_pk_id,
      0, nullptr, altered_table);
  assert(new_packed_size <= m_max_packed_sk_len);
  if (row_info.old_data != nullptr) {
    // The old value
    old_packed_size = kd.pack_record(
        table_arg, m_pack_buffer, row_info.old_data, m_sk_packed_tuple_old,
        &m_sk_tails_old, store_row_debug_checksums, row_info.hidden_pk_id,
              0, nullptr, altered_table);
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

#if 0  /* Now we have already disable bulk load and can't skip unique check */
  /*
    We're writing a new entry for secondary key. We can skip locking; we
    should write to
    - WriteBatchWithIndex normally (so that transaction sees the new row)
    - non-indexed WriteBatch, when we don't need to see the new row:
       = when doing a DDL operation and writing to a non-unique index, or
       = when doing a bulk load
  */
  xengine::db::WriteBatchBase *write_batch;
  if ((row_info.tx->m_ddl_transaction &&
       !(table_arg->key_info[key_id].flags & HA_NOSAME)) ||
      row_info.skip_unique_check) {
    write_batch = row_info.tx->get_blind_write_batch();
  } else {
    write_batch = row_info.tx->get_indexed_write_batch();
  }
#endif

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


int ha_xengine::update_indexes(const struct update_row_info &row_info,
                               const bool &pk_changed) {
  int rc = 0;

  for (uint key_id = 0; key_id < m_tbl_def->m_key_count; key_id++) {
    const Xdb_key_def &kd = *m_key_descr_arr[key_id];
    if (is_pk(key_id, table, m_tbl_def.get())) {
      rc = update_pk(kd, row_info, pk_changed);
    } else {
      rc = update_sk(table, kd, row_info);
    }

    if (rc != 0) {
      return rc;
    }
  }

  //update the indexes added-in-progress if necessary
  for (auto &k : m_tbl_def->m_added_key) {
    const Xdb_key_def &kd = *k.first;
    const TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab ->s->primary_key) {
      continue;
    }

    if (k.second.step >= Added_key_info::BUILDING_BASE_INDEX) {
      rc = update_sk(table, kd, row_info, atab);
    }

    DBUG_ASSERT(rc == HA_EXIT_SUCCESS);
    // it looks like update_sk() never returns error, but anyway
    //if (rc != 0) {
    //  k.second.status = HA_ERR_INTERNAL_ERROR;
    //}
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::update_write_row(const uchar *const old_data,
                                 const uchar *const new_data,
                                 const bool skip_unique_check) {
  DBUG_ENTER_FUNC();

  struct update_row_info row_info;
  int rc = HA_EXIT_SUCCESS;

  row_info.old_data = old_data;
  row_info.new_data = new_data;
  row_info.skip_unique_check = skip_unique_check;
  row_info.new_pk_unpack_info = nullptr;

  set_last_rowkey(old_data);

  row_info.tx = get_or_create_tx(table->in_use);

  if (old_data != nullptr) {
    row_info.old_pk_slice =
        xengine::common::Slice(m_last_rowkey.ptr(), m_last_rowkey.length());

    /* Determine which indexes need updating. */
    if ((rc = calc_updated_indexes())) {
      __XHANDLER_LOG(ERROR, "calculate update indexes error, code is %d", rc);
      DBUG_RETURN(rc);
    }
  }

  /*
    Get the new row key into row_info.new_pk_slice
   */
  rc = get_pk_for_update(&row_info);
  if (rc) {
    DBUG_RETURN(rc);
  }

  bool pk_changed = false;
  if (!skip_unique_check) {
    /*
      Check to see if we are going to have failures because of unique
      keys.  Also lock the appropriate key values.
    */
    rc = check_uniqueness_and_lock(row_info, &pk_changed);
    if (rc) {
      DBUG_RETURN(rc);
    }
  }

  DEBUG_SYNC(ha_thd(), "xengine.update_write_row_after_unique_check");

  /*
    At this point, all locks have been obtained, and all checks for duplicate
    keys have been performed. No further errors can be allowed to occur from
    here because updates to the transaction will be made and those updates
    cannot be easily removed without rolling back the entire transaction.
  */
  rc = update_indexes(row_info, pk_changed);
  if (rc) {
    DBUG_RETURN(rc);
  }

  DBUG_ASSERT(m_tbl_def->m_added_key.empty() || m_tbl_def->m_inplace_new_keys.empty());

  if (!m_tbl_def->m_inplace_new_keys.empty()) {
    TABLE *atab = m_tbl_def->m_inplace_new_keys.begin()->second.altered_table;
    if ((rc = get_new_pk_for_update(&row_info, atab))) {
      __XHANDLER_LOG(ERROR, "XEngineDDL: get_new_pk_for_update faield with %d, table_name: %s",
                     rc, table->s->table_name.str);
      DBUG_RETURN(rc);
    }
  }
  pk_changed = false;
  if (!skip_unique_check &&
      (rc = check_uniqueness_and_lock_rebuild(row_info, &pk_changed))) {
    DBUG_RETURN(rc);
  }
  rc = update_new_table(row_info, pk_changed);
  if (rc) {
    DBUG_RETURN(rc);
  }

  bool write_batch_iter_invalid_flag = false;
  if (commit_in_the_middle() &&
      (row_info.tx)->get_write_count() >= XDB_DEFAULT_BULK_LOAD_SIZE /*THDVAR(table->in_use, bulk_load_size)*/)
  {
    write_batch_iter_invalid_flag = true;
  }

  if (do_bulk_commit(row_info.tx)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (write_batch_iter_invalid_flag && m_scan_it)
  {
    util::BaseDeltaIterator* p_scan;
    p_scan = dynamic_cast<util::BaseDeltaIterator*>(m_scan_it);
    if (p_scan) {
      p_scan->InvalidDelta();
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  Open a cursor and position it at the passed record
*/

void ha_xengine::setup_scan_iterator(const Xdb_key_def &kd,
                                     xengine::common::Slice *const slice,
                                     const bool use_all_keys,
                                     const bool is_ascending,
                                     const uint eq_cond_len,
                                     const xengine::common::Slice* end_key) {
  DBUG_ASSERT(slice != nullptr);
  DBUG_ASSERT(slice->size() >= eq_cond_len);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);

  bool skip_bloom = true;
  m_cmp_end_key = true;

  const xengine::common::Slice eq_cond(slice->data(), eq_cond_len);
  if (can_use_bloom_filter(ha_thd(), kd, eq_cond, use_all_keys, is_ascending)) {
    skip_bloom = false;
  }

  /*
    In some cases, setup_scan_iterator() is called multiple times from
    the same query but bloom filter can not always be used.
    Suppose the following query example. id2 is VARCHAR(30) and PRIMARY KEY
    (id1, id2).
     select count(*) from t2 WHERE id1=100 and id2 IN ('00000000000000000000',
    '100');
    In this case, setup_scan_iterator() is called twice, the first time is for
    (id1, id2)=(100, '00000000000000000000') and the second time is for (100,
    '100').
    If prefix bloom filter length is 24 bytes, prefix bloom filter can be used
    for the
    first condition but not for the second condition.
    If bloom filter condition is changed, currently it is necessary to destroy
    and
    re-create Iterator.
  */
  if (m_scan_it_skips_bloom != skip_bloom) {
    release_scan_iterator();
  }

  /*
    SQL layer can call rnd_init() multiple times in a row.
    In that case, re-use the iterator, but re-position it at the table start.
  */
  if (!m_scan_it) {
    const bool fill_cache = true; // !THDVAR(ha_thd(), skip_fill_cache);
    m_scan_it = tx->get_iterator(kd.get_cf(), skip_bloom, fill_cache);
    m_scan_it_skips_bloom = skip_bloom;
  }

  if (nullptr != end_key) {
    m_cmp_end_key = false;
    int ret = m_scan_it->set_end_key(*end_key);
    assert(0 == ret);
  } else {
    int ret = m_scan_it->set_end_key(common::Slice() /*empty end key*/);
    assert(0 == ret);
  }
  /*
    Seek() will "Position at the first key in the source that at or past
    target".
    The operation cannot fail.
  */
  m_scan_it->Seek(*slice);
}

void ha_xengine::release_scan_iterator() {
  // delete m_scan_it;
  MOD_DELETE_OBJECT(Iterator, m_scan_it);
  m_scan_it = nullptr;

  if (m_scan_it_snapshot) {
    xdb->ReleaseSnapshot(m_scan_it_snapshot);
    m_scan_it_snapshot = nullptr;
  }
}

void ha_xengine::setup_iterator_for_rnd_scan() {
  uint key_size;

  if (m_pk_descr->m_is_reverse_cf)
    m_pk_descr->get_supremum_key(m_pk_packed_tuple, &key_size);
  else
    m_pk_descr->get_infimum_key(m_pk_packed_tuple, &key_size);

  xengine::common::Slice table_key((const char *)m_pk_packed_tuple, key_size);

  setup_scan_iterator(*m_pk_descr, &table_key);
  m_skip_scan_it_next_call = true;
}

int ha_xengine::rnd_init(bool scan) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_RND_INIT);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  setup_read_decoders();

  if (scan) {
    m_rnd_scan_is_new_snapshot = !tx->has_snapshot();
    setup_iterator_for_rnd_scan();
  } else {
    /* We don't need any preparations for rnd_pos() calls. */
  }

  // If m_lock_rows is on then we will be doing a get_for_update when accessing
  // the index, so don't acquire the snapshot right away.  Otherwise acquire
  // the snapshot immediately.
  tx->acquire_snapshot(m_lock_rows == XDB_LOCK_NONE);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::rnd_next(uchar *const buf) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_BEGIN(xengine::monitor::TracePoint::HA_RND_NEXT);

  int rc;
  ha_statistic_increment(&SSV::ha_read_rnd_next_count);
  for (;;) {
    rc = rnd_next_with_direction(buf, true);
    if (rc != HA_ERR_LOCK_DEADLOCK || !m_rnd_scan_is_new_snapshot)
      break; /* exit the loop */

    // release the snapshot and iterator and then regenerate them
    Xdb_transaction *tx = get_or_create_tx(table->in_use);
    tx->release_snapshot();
    release_scan_iterator();
    setup_iterator_for_rnd_scan();
  }

  m_rnd_scan_is_new_snapshot = false;

  if (rc == HA_ERR_KEY_NOT_FOUND)
    rc = HA_ERR_END_OF_FILE;

  QUERY_TRACE_END();
  DBUG_RETURN(rc);
}

/*
  See also secondary_index_read().
*/
int ha_xengine::rnd_next_with_direction(uchar *const buf, bool move_forward) {
  DBUG_ENTER_FUNC();

  int rc;

  //table->status = STATUS_NOT_FOUND;
  //stats.rows_requested++;

  if (!m_scan_it || !m_scan_it->Valid()) {
    /*
      We can get here when SQL layer has called

        h->index_init(PRIMARY);
        h->index_read_map(full index tuple, HA_READ_KEY_EXACT);

      In this case, we should return EOF.
    */
    DBUG_RETURN(HA_ERR_END_OF_FILE);
  }

  for (;;) {
    if (m_skip_scan_it_next_call) {
      m_skip_scan_it_next_call = false;
    } else {
      if (move_forward)
        m_scan_it->Next(); /* this call cannot fail */
      else
        m_scan_it->Prev(); /* this call cannot fail */
    }

    if (!m_scan_it->Valid()) {
      rc = HA_ERR_END_OF_FILE;
      break;
    }

    /* check if we're out of this table */
    const xengine::common::Slice key = m_scan_it->key();
    if (!m_pk_descr->covers_key(key)) {
      rc = HA_ERR_END_OF_FILE;
      break;
    }

    if (m_lock_rows != XDB_LOCK_NONE) {
      /*
        Lock the row we've just read.

        Now we call get_for_update which will 1) Take a lock and 2) Will fail
        if the row was deleted since the snapshot was taken.
      */
      Xdb_transaction *const tx = get_or_create_tx(table->in_use);
      DEBUG_SYNC(ha_thd(), "xengine_concurrent_delete");
      const xengine::common::Status s =
          get_for_update(tx, m_pk_descr->get_cf(), key, &m_retrieved_record);
      if (s.IsNotFound() &&
          my_core::thd_tx_isolation(ha_thd()) == ISO_READ_COMMITTED) {
        // This occurs if we accessed a row, tried to lock it, failed,
        // released and reacquired the snapshot (because of READ COMMITTED
        // mode) and the row was deleted by someone else in the meantime.
        // If so, we just want to move on to the next row.
        continue;
      }

      if (!s.ok()) {
        DBUG_RETURN(
            tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
      }

      // If we called get_for_update() use the value from that call not from
      // the iterator as it may be stale since we don't have a snapshot
      // when m_lock_rows is not XDB_LOCK_NONE.
      m_last_rowkey.copy(key.data(), key.size(), &my_charset_bin);
      rc = convert_record_from_storage_format(&key, m_retrieved_record, buf, table);
    } else {
      // Use the value from the iterator
      xengine::common::Slice value = m_scan_it->value();
      m_last_rowkey.copy(key.data(), key.size(), &my_charset_bin);
      rc = convert_record_from_storage_format(&key, &value, buf, table);
    }

    //table->status = 0;
    break;
  }

  if (!rc) {
    //stats.rows_read++;
    //stats.rows_index_next++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

int ha_xengine::rnd_end() {
  DBUG_ENTER_FUNC();

  release_scan_iterator();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::index_init(uint idx, bool sorted) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_INDEX_INIT);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  DBUG_ASSERT(tx != nullptr);

  setup_read_decoders();

  // If m_lock_rows is not XDB_LOCK_NONE then we will be doing a get_for_update
  // when accessing the index, so don't acquire the snapshot right away.
  // Otherwise acquire the snapshot immediately.
  tx->acquire_snapshot(m_lock_rows == XDB_LOCK_NONE);

  active_index = idx;

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::index_end() {
  DBUG_ENTER_FUNC();

  release_scan_iterator();

  active_index = MAX_KEY;
  in_range_check_pushed_down = FALSE;

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::truncate(dd::Table *table_def) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(m_tbl_def != nullptr);

  m_tbl_def->m_auto_incr_val = 1;
  m_tbl_def->m_hidden_pk_val = 1;

  remove_rows(m_tbl_def.get());

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
 @param old_record, record to be delete
 @param tx, transaction context
 @param pk_slice, pk to b delete
 @param hidden_pk_id, if table has no hidden_pk, then hidden_pk_id is 0, otherwise > 0
*/
int ha_xengine::delete_indexes(const uchar *const old_record)
{
  DBUG_ENTER_FUNC();

  xengine::common::Slice key_slice(m_last_rowkey.ptr(), m_last_rowkey.length());
  Xdb_transaction *const tx = get_or_create_tx(table->in_use);

  const uint index = pk_index(table, m_tbl_def.get());
  xengine::common::Status s =
      delete_or_singledelete(index, tx, m_pk_descr->get_cf(), key_slice);
  if (!s.ok()) {
    __XHANDLER_LOG(WARN, "DML: failed to delete record(%s) on pk (%u) with error %s, table_name: %s",
                   key_slice.ToString(true).c_str(),
                   m_pk_descr->get_index_number(), s.ToString().c_str(),
                   table->s->table_name.str);
    DBUG_RETURN(tx->set_status_error(table->in_use, s, *m_pk_descr, m_tbl_def.get()));
  }

  longlong hidden_pk_id = 0;
  assert(m_tbl_def->m_key_count >= 1);
  if (has_hidden_pk(table) && read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey))
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);

  // Delete the record for every secondary index
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (!is_pk(i, table, m_tbl_def.get())) {
      uint packed_size;
      const Xdb_key_def &kd = *m_key_descr_arr[i];
      packed_size = kd.pack_record(table, m_pack_buffer, old_record, m_sk_packed_tuple,
                                   nullptr, false, hidden_pk_id);
      assert(packed_size <= m_max_packed_sk_len);

      xengine::common::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      /* Deleting on secondary key doesn't need any locks: */
      tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                  secondary_key_slice);
    }
  }

  // Delete the record for new added index
  for (auto &k : m_tbl_def->m_added_key) {
    const Xdb_key_def &kd = *k.first;
    const TABLE *atab = k.second.altered_table;
    if (table->s->primary_key != atab->s->primary_key) {
      continue;
    }

    if (k.second.step >= Added_key_info::BUILDING_BASE_INDEX) {
      uint packed_size =
          kd.pack_record(table, m_pack_buffer, old_record, m_sk_packed_tuple, nullptr,
                         false, hidden_pk_id, 0, nullptr, atab);
      assert(packed_size <= m_max_packed_sk_len);
      xengine::common::Slice secondary_key_slice(
          reinterpret_cast<const char *>(m_sk_packed_tuple), packed_size);
      /* Deleting on secondary key doesn't need any locks: */
      tx->get_indexed_write_batch()->SingleDelete(kd.get_cf(),
                                                  secondary_key_slice);
    }
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/*
  Delete the row we've last read. The row is also passed as parameter.

  @detail
    The caller guarantees table buf points to the row that was just read.
    The row is either table->record[0] or table->record[1].
    (Check out InnoDB: row_update_for_mysql() has "UT_NOT_USED(mysql_rec)"
*/
int ha_xengine::delete_row(const uchar *const buf) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(buf != nullptr);

  ha_statistic_increment(&SSV::ha_delete_count);
  set_last_rowkey(buf);

  Xdb_transaction *const tx = get_or_create_tx(table->in_use);

  int ret = HA_EXIT_SUCCESS;
  if ((ret = delete_indexes(buf))) {
    __XHANDLER_LOG(ERROR, "delete failed, errcode is %d", ret);
    DBUG_RETURN(ret);
  } else if (m_tbl_def->m_inplace_new_tdef != nullptr) {
    DBUG_ASSERT(!m_tbl_def->m_inplace_new_keys.empty());
    struct update_row_info del_row_info;
    del_row_info.tx = tx;
    del_row_info.old_data = buf;

    //delete the record for rebuild new table
    if ((ret = delete_row_new_table(del_row_info))) {
      __XHANDLER_LOG(WARN, "XEngineDDL: failed to delete record for new table, errcode is %d, table_name:%s",
                     ret, table->s->table_name.str);
      DBUG_RETURN(ret);
    }
  }

  bool write_batch_iter_invalid_flag = false;
  if (commit_in_the_middle() &&
      tx->get_write_count() >=
          XDB_DEFAULT_BULK_LOAD_SIZE /*THDVAR(table->in_use, bulk_load_size)*/) {
    write_batch_iter_invalid_flag = true;
  }

  if (do_bulk_commit(tx)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (write_batch_iter_invalid_flag == true && m_scan_it)
  {
    util::BaseDeltaIterator* p_scan;
    p_scan = dynamic_cast<util::BaseDeltaIterator*>(m_scan_it);
    if (p_scan) {
      p_scan->InvalidDelta();
    }
  }

  //stats.rows_deleted++;
  update_row_stats(ROWS_DELETED);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

xengine::common::Status ha_xengine::delete_or_singledelete(
    uint index, Xdb_transaction *const tx,
    xengine::db::ColumnFamilyHandle *const column_family,
    const xengine::common::Slice &key) {
  if (can_use_single_delete(index))
    return tx->single_delete(column_family, key);
  return tx->delete_key(column_family, key);
}

void ha_xengine::update_stats(void) {
  DBUG_ENTER_FUNC();

  stats.records = 0;
  stats.index_file_length = 0ul;
  stats.data_file_length = 0ul;
  stats.mean_rec_length = 0;
  Xdb_index_stats index_stats;

  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    if (is_pk(i, table, m_tbl_def.get())) {
      index_stats.m_gl_index_id = m_pk_descr->get_gl_index_id();
      if (xdb->get_columnfamily_stats(m_pk_descr->get_cf(), index_stats.m_data_size,
                                      index_stats.m_rows, index_stats.m_entry_deletes, index_stats.m_actual_disk_size)) {
        ddl_manager.adjust_stats(index_stats);
        index_stats.reset();
      }
      stats.data_file_length = m_pk_descr->m_stats.m_actual_disk_size;
      stats.records = m_pk_descr->m_stats.m_rows;
    } else {
      index_stats.m_gl_index_id = m_key_descr_arr[i]->get_gl_index_id();
      if (xdb->get_columnfamily_stats(m_key_descr_arr[i]->get_cf(), index_stats.m_data_size,
                                      index_stats.m_rows, index_stats.m_entry_deletes, index_stats.m_actual_disk_size)) {
        ddl_manager.adjust_stats(index_stats);
        index_stats.reset();
      }
      stats.index_file_length += m_key_descr_arr[i]->m_stats.m_actual_disk_size;
    }
  }

  DBUG_VOID_RETURN;
}

int ha_xengine::info(uint flag) {
  DBUG_ENTER_FUNC();

  if (!table)
    return HA_EXIT_FAILURE;

  if (flag & HA_STATUS_VARIABLE) {
    /*
      Test only to simulate corrupted stats
    */
    DBUG_EXECUTE_IF("myx_simulate_negative_stats",
                    m_pk_descr->m_stats.m_actual_disk_size =
                        -m_pk_descr->m_stats.m_actual_disk_size;);

    update_stats();

    /*
      If any stats are negative due to bad cached stats, re-run analyze table
      and re-retrieve the stats.
    */
    if (static_cast<longlong>(stats.data_file_length) < 0 ||
        static_cast<longlong>(stats.index_file_length) < 0 ||
        static_cast<longlong>(stats.records) < 0) {
      if (analyze(nullptr, nullptr)) {
        DBUG_RETURN(HA_EXIT_FAILURE);
      }

      update_stats();
    }

    // if number of records is hardcoded, we do not want to force computation
    // of memtable cardinalities
    if (stats.records == 0 || stats.records == 1 ||
        (xengine_force_compute_memtable_stats &&
         xengine_debug_optimizer_n_rows == 0))
    {
      // First, compute SST files stats
      uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2];
      auto r = get_range(pk_index(table, m_tbl_def.get()), buf);
      uint64_t sz = 0;

      uint8_t include_flags = xengine::db::DB::INCLUDE_FILES;
      // recompute SST files stats only if records count is 0
    if (stats.records == 0 || stats.records == 1) {
        xdb->GetApproximateSizes(m_pk_descr->get_cf(), &r, 1, &sz,
                                 include_flags);
        stats.records+= sz/XENGINE_ASSUMED_KEY_VALUE_DISK_SIZE;
        stats.data_file_length+= sz;
      }

      // Second, compute memtable stats
      uint64_t memtableCount;
      uint64_t memtableSize;
      xdb->GetApproximateMemTableStats(m_pk_descr->get_cf(), r,
                                            &memtableCount, &memtableSize);
      stats.records += memtableCount;
      stats.data_file_length += memtableSize;

      if (xengine_debug_optimizer_n_rows > 0)
        stats.records = xengine_debug_optimizer_n_rows;
    }

    if (stats.records != 0)
      stats.mean_rec_length = stats.data_file_length / stats.records;
  }
  if (flag & HA_STATUS_CONST) {
    ref_length = m_pk_descr->max_storage_fmt_length();

    // TODO: Needs to reimplement after having real index statistics
    for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
      if (is_hidden_pk(i, table, m_tbl_def.get())) {
        continue;
      }
      KEY *const k = &table->key_info[i];
      for (uint j = 0; j < k->actual_key_parts; j++) {
        const Xdb_index_stats &k_stats = m_key_descr_arr[i]->m_stats;
        uint x = k_stats.m_distinct_keys_per_prefix.size() > j &&
                         k_stats.m_distinct_keys_per_prefix[j] > 0
                     ? k_stats.m_rows / k_stats.m_distinct_keys_per_prefix[j]
                     : 0;
        if (x > stats.records)
          x = stats.records;
        if ((x == 0/* && xengine_debug_optimizer_no_zero_cardinality*/) ||
            xengine_debug_optimizer_n_rows > 0) {
          // Fake cardinality implementation. For example, (idx1, idx2, idx3)
          // index
          // will have rec_per_key for (idx1)=4, (idx1,2)=2, and (idx1,2,3)=1.
          // rec_per_key for the whole index is 1, and multiplied by 2^n if
          // n suffix columns of the index are not used.
          x = 1 << (k->actual_key_parts - j - 1);
        }
        k->rec_per_key[j] = x;
      }
    }
  }

  if (flag & HA_STATUS_ERRKEY) {
    /*
      Currently we support only primary keys so we know which key had a
      uniqueness violation.
    */
    errkey = m_dupp_errkey;
    dup_ref = m_pk_tuple; // TODO(?): this should store packed PK.
  }

  if (flag & HA_STATUS_AUTO) {
    stats.auto_increment_value = m_tbl_def->m_auto_incr_val;
  }

  /*
    optimizer will make some decisions depending on stats.records.
    0 may lead to unexpected results, for example, semi-join strategy is
    materialized with stats.records being 0 while firstmatch with stats.records
    being 1.
  */

  if (stats.records == 0 && !(flag & HA_STATUS_TIME) &&
        table_share->table_category != TABLE_CATEGORY_TEMPORARY) {
    stats.records = 1;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

void ha_xengine::position(const uchar *const record) {
  DBUG_ENTER_FUNC();
  longlong hidden_pk_id = 0;
  if (has_hidden_pk(table) && read_hidden_pk_id_from_rowkey(&hidden_pk_id, &m_last_rowkey))
    DBUG_ASSERT(false); // should never reach here

  /*
    Get packed primary key value from the record.

    (Note: m_last_rowkey has the packed PK of last-read-row, which allows to
    handle most cases, but there is an exception to this: when slave applies
    RBR events, it fills the record and then calls position(); rnd_pos())

    Also note that we *can* rely on PK column values being available. This is
    because table_flags() includes HA_PRIMARY_KEY_REQUIRED_FOR_POSITION bit.
    When that is true, table->prepare_for_position() adds PK columns into the
    read set (this may potentially disable index-only access when PK column
    cannot be restored from its mem-comparable form in the secondary indexes).
  */
  const uint packed_size = m_pk_descr->pack_record(
      table, m_pack_buffer, record, ref, nullptr, false, hidden_pk_id);
  assert(packed_size <= m_max_packed_sk_len);

  /*
    It could be that mem-comparable form of PK occupies less than ref_length
    bytes. Fill the remainder with zeros.
  */
  if (ref_length > packed_size)
    memset(ref + packed_size, 0, ref_length - packed_size);

  DBUG_VOID_RETURN;
}

int ha_xengine::rnd_pos(uchar *const buf, uchar *const pos) {
  DBUG_ENTER_FUNC();
  QUERY_TRACE_SCOPE(xengine::monitor::TracePoint::HA_READ_ROW);
  int rc;
  size_t len;

  ha_statistic_increment(&SSV::ha_read_rnd_count);
  //stats.rows_requested++;
  len = m_pk_descr->key_length(table,
                               xengine::common::Slice((const char *)pos, ref_length));
  if (len == size_t(-1)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR); /* Data corruption? */
  }

  rc = get_row_by_rowid(buf, pos, len);

  if (!rc) {
    //stats.rows_read++;
    update_row_stats(ROWS_READ);
  }

  DBUG_RETURN(rc);
}

/*
  @brief
    Calculate (if needed) the bitmap of indexes that are modified by the
    current query

  @detail
    The calculation done by checking index definitions against the
  table->write_set
    column bitmap.
*/

int ha_xengine::calc_updated_indexes() {
  if (!m_update_scope_is_valid) {
    m_update_scope_is_valid = true;
    m_update_scope.clear_all();

    for (uint keynr = 0; keynr < table->s->keys; keynr++) {
      const Xdb_key_def &kd = *m_key_descr_arr[keynr];
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(table) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(table, kp);
        if (bitmap_is_set(table->write_set, field->field_index)) {
          m_update_scope.set_bit(keynr);
          break;
        }
      }
    }

    /* set update-bit for added indexes during inplace-norebuild ddl */
    for (auto &k : m_tbl_def->m_added_key) {
      const Xdb_key_def &kd = *k.first;
      TABLE *atab = k.second.altered_table;
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(atab) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(atab, kp);
        if (field == nullptr) {
          XHANDLER_LOG(ERROR, "table object is mismatch with m_tbl_def");
          return HA_EXIT_FAILURE;
        } else if (bitmap_is_set(table->write_set, field->field_index)) {
          m_update_scope.set_bit(kd.get_keyno());
          break;
        }
      }
    }

    /* set update-bit for added indexes during inplace-rebuild ddl */
    for (auto &k : m_tbl_def->m_inplace_new_keys) {
      const Xdb_key_def &kd = *k.first;
      TABLE *atab = k.second.altered_table;
      /* Walk over all key parts, including the "extended key" suffix */
      const uint key_parts = kd.get_key_parts();
      for (uint kp = 0; kp < key_parts; kp++) {
        if (has_hidden_pk(atab) && kp + 1 == key_parts)
          break;

        Field *const field = kd.get_table_field_for_part_no(atab, kp);
        if (field == nullptr) {
          XHANDLER_LOG(ERROR, "table object is mismatch with m_tbl_def");
          return HA_EXIT_FAILURE;
        }

        /*
           field in old table be modified should also set bitmap
           if field is still exists in the altered table.
        */
        uint *col_map = m_tbl_def->m_dict_info->m_col_map;
        uint old_field_index = col_map[field->field_index];

        if (old_field_index != uint(-1) &&
            bitmap_is_set(table->write_set, old_field_index)) {
          m_update_scope.set_bit(kd.get_keyno());
          break;
        }
      }
    }

    // it may be possible to set m_update_scope for the index to be added,
    // but it should be after calling kd.setup(). Skip it now for simplicity,
    // and suppose there is little performance drop.
  }

  return HA_EXIT_SUCCESS;
}

int ha_xengine::update_row(const uchar *const old_data, uchar *const new_data) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(old_data != nullptr);
  DBUG_ASSERT(new_data != nullptr);
  DBUG_ASSERT(m_lock_rows == XDB_LOCK_WRITE);
  /*
    old_data points to record we're updating. It is the same as the record
    we've just read (for multi-table UPDATE, too, because SQL layer will make
    an rnd_pos() call to re-read the record before calling update_row())
  */
  DBUG_ASSERT(new_data == table->record[0]);

  ha_statistic_increment(&SSV::ha_update_count);
  const int rv = update_write_row(old_data, new_data, false);

  if (rv == 0) {
    //stats.rows_updated++;
    update_row_stats(ROWS_UPDATED);
  }

  DBUG_RETURN(rv);
}

static bool can_hold_read_locks_on_select(THD *thd, thr_lock_type lock_type)
{
  return (lock_type == TL_READ_WITH_SHARED_LOCKS) ||
         (lock_type == TL_READ_NO_INSERT) ||
         (lock_type != TL_IGNORE && thd->lex->sql_command != SQLCOM_SELECT);
}

/* The following function was copied from ha_blackhole::store_lock: */
THR_LOCK_DATA **ha_xengine::store_lock(THD *const thd, THR_LOCK_DATA **to,
                                       enum thr_lock_type lock_type) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(to != nullptr);

  bool in_lock_tables = my_core::thd_in_lock_tables(thd);

  /* First, make a decision about XEngine's internal locking */
  if (lock_type >= TL_WRITE_ALLOW_WRITE) {
    m_lock_rows = XDB_LOCK_WRITE;
  } else if (lock_type == TL_READ_WITH_SHARED_LOCKS) {
    m_lock_rows = XDB_LOCK_READ;
  } else {
    m_lock_rows = XDB_LOCK_NONE;
    if (THDVAR(thd, lock_scanned_rows)) {
      /*
        The following logic was copied directly from
        ha_innobase::store_lock_with_x_type() in
        storage/innobase/handler/ha_innodb.cc and causes XEngine to leave
        locks in place on rows that are in a table that is not being updated.
      */
      const uint sql_command = my_core::thd_sql_command(thd);
      if ((lock_type == TL_READ && in_lock_tables) ||
          (lock_type == TL_READ_HIGH_PRIORITY && in_lock_tables) ||
          can_hold_read_locks_on_select(thd, lock_type)) {
        ulong tx_isolation = my_core::thd_tx_isolation(thd);
        if (sql_command != SQLCOM_CHECKSUM &&
            ((my_core::thd_test_options(thd, OPTION_BIN_LOG) &&
              tx_isolation > ISO_READ_COMMITTED) ||
             tx_isolation == ISO_SERIALIZABLE ||
             (lock_type != TL_READ && lock_type != TL_READ_NO_INSERT) ||
             (sql_command != SQLCOM_INSERT_SELECT &&
              sql_command != SQLCOM_REPLACE_SELECT &&
              sql_command != SQLCOM_UPDATE && sql_command != SQLCOM_DELETE &&
              sql_command != SQLCOM_CREATE_TABLE))) {
          m_lock_rows = XDB_LOCK_READ;
        }
      }
    }
  }

  /* Then, tell the SQL layer what kind of locking it should use: */
  if (lock_type != TL_IGNORE && m_db_lock.type == TL_UNLOCK) {
    /*
      Here is where we get into the guts of a row level lock.
      If TL_UNLOCK is set
      If we are not doing a LOCK TABLE or DISCARD/IMPORT
      TABLESPACE, then allow multiple writers
    */

    if ((lock_type >= TL_WRITE_CONCURRENT_INSERT && lock_type <= TL_WRITE) &&
        !in_lock_tables && !my_core::thd_tablespace_op(thd)) {
      lock_type = TL_WRITE_ALLOW_WRITE;
    }

    /*
      In queries of type INSERT INTO t1 SELECT ... FROM t2 ...
      MySQL would use the lock TL_READ_NO_INSERT on t2, and that
      would conflict with TL_WRITE_ALLOW_WRITE, blocking all inserts
      to t2. Convert the lock to a normal read lock to allow
      concurrent inserts to t2.
    */

    if (lock_type == TL_READ_NO_INSERT && !in_lock_tables) {
      lock_type = TL_READ;
    }

    m_db_lock.type = lock_type;
  }

  *to++ = &m_db_lock;

  DBUG_RETURN(to);
}

void ha_xengine::read_thd_vars(THD *const thd) {
  m_store_row_debug_checksums = false; // THDVAR(thd, store_row_debug_checksums);
  m_verify_row_debug_checksums = false; // THDVAR(thd, verify_row_debug_checksums);
  m_checksums_pct = XDB_MAX_CHECKSUMS_PCT; // THDVAR(thd, checksums_pct);

#ifndef NDEBUG
  m_store_row_debug_checksums = true;
  m_verify_row_debug_checksums = true;
#endif
}

const char *ha_xengine::thd_xengine_tmpdir() {
  const char *tmp_dir = nullptr; // THDVAR(ha_thd(), tmpdir);

  /*
    We want to treat an empty string as nullptr, in these cases DDL operations
    will use the default --tmpdir passed to mysql instead.
  */
  if (tmp_dir != nullptr && *tmp_dir == '\0') {
    tmp_dir = nullptr;
  }

  return (tmp_dir);
}

int ha_xengine::external_lock(THD *const thd, int lock_type) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);

  //for fast path
  read_thd_vars(thd);
  if (thd->lex->sql_command == SQLCOM_SELECT &&
      !thd->in_multi_stmt_transaction_mode())
  {
    if (lock_type == F_UNLCK)
    {
      Xdb_transaction *tx = get_or_create_tx(table->in_use);
      tx->release_snapshot();
    }
    DBUG_RETURN(0);
  }

  int res = 0;
  int binlog_format = my_core::thd_binlog_format(thd);
  bool unsafe_for_binlog = THDVAR(ha_thd(), unsafe_for_binlog);
  if (lock_type == F_WRLCK && !thd->rli_slave && !unsafe_for_binlog &&
      binlog_format != BINLOG_FORMAT_ROW &&
      binlog_format != BINLOG_FORMAT_UNSPEC &&
      my_core::thd_binlog_filter_ok(thd)) {
    my_printf_error(
        ER_UNKNOWN_ERROR,
        "Can't execute updates on master with binlog_format != ROW.", MYF(0));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (lock_type == F_UNLCK) {
    Xdb_transaction *const tx = get_or_create_tx(thd);
    tx->dec_table_in_use();
    int64_t table_in_use= tx->get_table_in_use();

    if (table_in_use == 0 &&
        !my_core::thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)) {
      /*
        Do like InnoDB: when we get here, it's time to commit a
        single-statement transaction.

        If the statement involved multiple tables, this code will be executed
        for each of them, but that's ok because non-first tx->commit() calls
        will be no-ops.
      */
      res = tx->commit_or_rollback();
    }
    DEBUG_SYNC_C("ha_xengine_end_statement");
  } else {
    if (my_core::thd_tx_isolation(thd) < ISO_READ_COMMITTED ||
        my_core::thd_tx_isolation(thd) > ISO_REPEATABLE_READ) {
      my_printf_error(
          ER_UNKNOWN_ERROR, "XEngine only supports READ COMMITTED and "
                            "REPEATABLE READ isolation levels. "
                            "Please change from current isolation level %s",
          MYF(0), tx_isolation_names[my_core::thd_tx_isolation(thd)]);
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
    }
    /*
      It's nice to do the following on start of every statement. The problem
      is, handler->start_stmt() is not called for INSERTs.
      So, we put this code here.
    */
    Xdb_transaction *const tx = get_or_create_tx(thd);
    tx->add_table_in_use();

    m_update_scope_is_valid = false;

    if (skip_unique_check()) {
      if ((thd->lex->sql_command == SQLCOM_INSERT ||
           thd->lex->sql_command == SQLCOM_LOAD ||
           thd->lex->sql_command == SQLCOM_REPLACE) &&
          (thd->lex->duplicates == DUP_REPLACE ||
           thd->lex->duplicates == DUP_UPDATE)) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "When unique checking is disabled in XEngine, INSERT,"
                        "UPDATE,LOAD statements with clauses that update or "
                        "replace the key (i.e. "
                        "INSERT ON DUPLICATE KEY UPDATE, REPLACE) are not "
                        "allowed. Query: %s",
                        MYF(0), thd->query().str);
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    }

    if (lock_type == F_WRLCK) {
      if (tx->is_tx_read_only()) {
        my_printf_error(ER_UNKNOWN_ERROR,
                        "Can't execute updates when you started a transaction "
                        "with START TRANSACTION WITH CONSISTENT [XENGINE] "
                        "SNAPSHOT.",
                        MYF(0));
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }

      /*
        SQL layer signals us to take a write lock. It does so when starting DML
        statement. We should put locks on the rows we're reading.

        Note: sometimes, external_lock() can be called without a prior
        ::store_lock call.  That's why we need to set lock_* members here, too.
      */
      m_lock_rows = XDB_LOCK_WRITE;

      if (thd->lex->sql_command == SQLCOM_CREATE_INDEX ||
          thd->lex->sql_command == SQLCOM_DROP_INDEX ||
          thd->lex->sql_command == SQLCOM_ALTER_TABLE) {
        tx->m_ddl_transaction = true;
      }
    }
    xengine_register_tx(xengine_hton, thd, tx);
  }

  DBUG_RETURN(res);
}

/**
  @note
  A quote from ha_innobase::start_stmt():
  <quote>
  MySQL calls this function at the start of each SQL statement inside LOCK
  TABLES. Inside LOCK TABLES the ::external_lock method does not work to
  mark SQL statement borders.
  </quote>
*/

int ha_xengine::start_stmt(THD *const thd, thr_lock_type lock_type) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);

  Xdb_transaction *const tx = get_or_create_tx(thd);
  read_thd_vars(thd);
  xengine_register_tx(ht, thd, tx);

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

xengine::db::Range get_range(uint32_t i,
                         uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2],
                         int offset1, int offset2) {
  uchar *buf_begin = buf;
  uchar *buf_end = buf + Xdb_key_def::INDEX_NUMBER_SIZE;
  xdb_netbuf_store_index(buf_begin, i + offset1);
  xdb_netbuf_store_index(buf_end, i + offset2);

  return xengine::db::Range(
      xengine::common::Slice((const char *)buf_begin, Xdb_key_def::INDEX_NUMBER_SIZE),
      xengine::common::Slice((const char *)buf_end, Xdb_key_def::INDEX_NUMBER_SIZE));
}

static xengine::db::Range get_range(const Xdb_key_def &kd,
                                uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2],
                                int offset1, int offset2) {
  return get_range(kd.get_index_number(), buf, offset1, offset2);
}

xengine::db::Range get_range(const Xdb_key_def &kd,
                         uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2]) {
  if (kd.m_is_reverse_cf) {
    return myx::get_range(kd, buf, 1, 0);
  } else {
    return myx::get_range(kd, buf, 0, 1);
  }
}

xengine::db::Range
ha_xengine::get_range(const int &i,
                      uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2]) const {
  return myx::get_range(*m_key_descr_arr[i], buf);
}

#if 0
static bool is_myx_index_empty(
  xengine::db::ColumnFamilyHandle *cfh, const bool is_reverse_cf,
  const xengine::common::ReadOptions &read_opts,
  const uint index_id)
{
  bool index_removed = false;
  uchar key_buf[Xdb_key_def::INDEX_NUMBER_SIZE] = {0};
  xdb_netbuf_store_uint32(key_buf, index_id);
  const xengine::common::Slice key =
    xengine::common::Slice(reinterpret_cast<char *>(key_buf), sizeof(key_buf));
  IteratorUptr it(xdb->NewIterator(read_opts, cfh));
  it->Seek(key);
  if (is_reverse_cf) {
    if (!it->Valid()) {
      it->SeekToLast();
    } else {
      it->Prev();
    }
  }
  if (!it->Valid()) {
    index_removed = true;
  } else {
    if (memcmp(it->key().data(), key_buf,
        Xdb_key_def::INDEX_NUMBER_SIZE)) {
      // Key does not have same prefix
      index_removed = true;
    }
  }
  return index_removed;
}
#endif

// purge garbaged subtables due to commit-in-the-middle
static void purge_invalid_subtables()
{
  static timespec start = {0, 0};
  if (start.tv_sec == 0) {
    clock_gettime(CLOCK_REALTIME, &start);
  } else {
    timespec now;
    clock_gettime(CLOCK_REALTIME, &now);
    // scan invalid subtables per DURATION_PER_RUN seconds
    if (now.tv_sec < start.tv_sec + purge_schedule_interval) return;

    start = now;
  }

  Global_THD_manager *thd_manager = Global_THD_manager::get_instance();
  THD* thd = new THD;
  thd->thread_stack = (char *)&thd;
  thd->slave_thread = 0;
  thd->set_command(COM_DAEMON);
  thd->set_new_thread_id();
  thd->store_globals();
  thd_manager->add_thd(thd);
  thd_manager->inc_thread_running();

  // get subtable ids from engine first
  std::vector<int32_t> ids_from_engine = cf_manager.get_subtable_ids();

  // collect id of subtables from global data dictionary
  std::set<uint32_t> ids_from_dd;
  if (!Xdb_dd_helper::get_xengine_subtable_ids(thd, purge_acquire_lock_timeout,
                                               ids_from_dd)) {
    // pre-defined subtables for internal usage
    ids_from_dd.insert(0);                           // default cf
    ids_from_dd.insert(DEFAULT_SYSTEM_SUBTABLE_ID);  // system cf

    // saved invalid subtable ids for last run
    static std::set<uint32_t> last_invalid_subtables;
    // collect invalid id of invalid subtables
    std::set<uint32_t> current_invalid_subtables;
    for (auto& id : ids_from_engine) {
      uint32_t subtable_id = id;
      GL_INDEX_ID gl_index_id = {.cf_id = subtable_id,
                                 .index_id = subtable_id};
      // filter out create/drop-ongoing index
      if (ids_from_dd.find(subtable_id) == ids_from_dd.end() &&
          ddl_manager.can_purge_subtable(thd, gl_index_id)) {
        current_invalid_subtables.insert(subtable_id);
      }
    }

    if (!last_invalid_subtables.empty() && !current_invalid_subtables.empty()) {
      // purge invalid subtables present during both last and this run
      auto wb = dict_manager.begin();
      auto write_batch = wb.get();
      for (auto& subtable_id : last_invalid_subtables) {
        auto it = current_invalid_subtables.find(subtable_id);
        if (it != current_invalid_subtables.end()) {
          XHANDLER_LOG(WARN, "XEngine: going to purge trashy subtable",
                       K(subtable_id));
          dict_manager.delete_index_info(write_batch,
                            {.cf_id=subtable_id, .index_id = subtable_id});
          auto cfh = cf_manager.get_cf(subtable_id);
          if (cfh != nullptr) {
            xdb->Flush(xengine::common::FlushOptions(), cfh);
            cf_manager.drop_cf(xdb, subtable_id);
          }
          current_invalid_subtables.erase(it);
        }
      }
      dict_manager.commit(write_batch);
    }
    last_invalid_subtables = current_invalid_subtables;
  } else {
    XHANDLER_LOG(WARN, "XEngine: failed to collect subtable id from DD!");
  }

  thd->release_resources();
  thd_manager->remove_thd(thd);
  thd_manager->dec_thread_running();
  delete thd;
}

/*
  Drop index thread's main logic
*/

void Xdb_drop_index_thread::run() {
  XDB_MUTEX_LOCK_CHECK(m_signal_mutex);

  for (;;) {
    // The stop flag might be set by shutdown command
    // after drop_index_thread releases signal_mutex
    // (i.e. while executing expensive Seek()). To prevent drop_index_thread
    // from entering long cond_timedwait, checking if stop flag
    // is true or not is needed, with drop_index_interrupt_mutex held.
    if (m_stop) {
      break;
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    ts.tv_sec += purge_schedule_interval;

    // enter wait
    if (m_run_) {
      m_run_ = false;
    }

    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts);
    if (m_stop) {
      break;
    }
    // FIXME: shrink exent space cann't do with drop index
    if (xengine_shrink_table_space >= 0) {
      sql_print_information("Wait for shrink extent space finish");
      continue;
    }
    // make sure, no program error is returned
    DBUG_ASSERT(ret == 0 || ret == ETIMEDOUT);
    // may be work
    m_run_ = true;
    XDB_MUTEX_UNLOCK_CHECK(m_signal_mutex);

#if 0
    std::unordered_set<GL_INDEX_ID> indices;
    dict_manager.get_ongoing_drop_indexes(&indices);
    if (!indices.empty()) {
      std::unordered_set<GL_INDEX_ID> finished;
      xengine::common::ReadOptions read_opts;
      read_opts.total_order_seek = true; // disable bloom filter

      for (const auto d : indices) {
        uint32 cf_flags = 0;
        if (!dict_manager.get_cf_flags(d.cf_id, &cf_flags)) {
          /* when fail to create index, we can't find cf_flags in dict */
          finished.insert(d);
          continue;
        }
        xengine::db::ColumnFamilyHandle *cfh = cf_manager.get_cf(d.cf_id);
        DBUG_ASSERT(cfh);
        const bool is_reverse_cf = cf_flags & Xdb_key_def::REVERSE_CF_FLAG;

        if (is_myx_index_empty(cfh, is_reverse_cf, read_opts, d.index_id))
        {
          finished.insert(d);
          sql_print_information("XEngine: Finished index (%d,%d), cfd name: %s",
                      d.cf_id, d.index_id, cfh->GetName().c_str());
          continue;
        }

        /* 1.trigger memtable-flush */
        xdb->Flush(xengine::common::FlushOptions(), cfh);

        /* 2.delete extents from the cf */
        uchar buf[Xdb_key_def::INDEX_NUMBER_SIZE * 2];
        xengine::db::Range range = get_range(d.index_id, buf, is_reverse_cf ? 1 : 0,
                                         is_reverse_cf ? 0 : 1);
        xengine::common::CompactRangeOptions compact_range_options;
        compact_range_options.bottommost_level_compaction =
            xengine::common::BottommostLevelCompaction::kForce;
        compact_range_options.exclusive_manual_compaction = false;
        DBUG_EXECUTE_IF("sleep_in_drop_index", sleep(5););
        xengine::common::Status status = DeleteFilesInRange(xdb->GetBaseDB(), cfh,
                                                    &range.start, &range.limit);
        if (!status.ok()) {
          if (status.IsShutdownInProgress()) {
            break;
          } else if (status.IsTryAgain()) { // background work interrupted
            continue;
          }
          xdb_handle_io_error(status, XDB_IO_ERROR_BG_THREAD);
        }

        /* TODO without compactionFilter, disable CompactRange temperally.
        status = xdb->CompactRange(compact_range_options, cfh, &range.start,
                                   &range.limit);
        if (!status.ok()) {
          if (status.IsShutdownInProgress()) {
            break;
          }
          xdb_handle_io_error(status, XDB_IO_ERROR_BG_THREAD);
        }
        */

        //we expect that after we do DeleteFilesInRange, we already finish clear the index.
        //TODO after we support compaction-filter and adjust compaction schedule, we can check whether index is cleared or not.
        finished.insert(d);
        sql_print_information("XEngine: Finished index (%d,%d), cfd name: %s",
              d.cf_id, d.index_id, cfh->GetName().c_str());
        /*
        if (is_myx_index_empty(cfh, is_reverse_cf, read_opts, d.index_id))
        {
          finished.insert(d);
        }
        */
      }

      if (!finished.empty()) {
        dict_manager.finish_drop_indexes(finished);
        /* drop the column families of the indexes */
        for (const auto &gl_index_id : finished) {
          cf_manager.drop_cf(xdb, gl_index_id.cf_id);
        }
      }
    }
#endif

    if (opt_purge_invalid_subtable_bg)
      purge_invalid_subtables();

    XDB_MUTEX_LOCK_CHECK(m_signal_mutex);
    m_run_ = false;
  }

  XDB_MUTEX_UNLOCK_CHECK(m_signal_mutex);
}

/*
  Note: the following function is called when the table is not open. That is,
  this->table==nullptr, pk_key_descr==nullptr, etc.

  tablename points to line in form "./dbname/tablename".
*/

int ha_xengine::delete_table(const char *const tablename,
                             const dd::Table *table_def
                                 MY_ATTRIBUTE((__unused__))) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(tablename != nullptr);

  THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
  Xdb_transaction *const tx = get_or_create_tx(thd);
  xengine_register_tx(ht, thd, tx);

  xengine::db::WriteBatch *const batch =
      dynamic_cast<xengine::db::WriteBatch *>(tx->get_blind_write_batch());

  DBUG_EXECUTE_IF("ddl_log_crash_before_xengine_delete_success", DBUG_SUICIDE(););

#ifndef NDEBUG
  static int delete_func_enter_counts = 0;
  DBUG_EXECUTE_IF("ddl_log_crash_delete_funcs", { delete_func_enter_counts++; };);
  if (delete_func_enter_counts > 1) {
    DBUG_SUICIDE();
  }
#endif

  std::string dbname_tablename;
  if (xdb_normalize_tablename(tablename, &dbname_tablename)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }
  bool from_dict = false;
  auto tbl = table_def ? ddl_manager.find(table_def, dbname_tablename, &from_dict)
                       : ddl_manager.find(thd, dbname_tablename, &from_dict);
  if (!tbl) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to delete a table that is not present in XEngine-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  for (uint i = 0; i < tbl->m_key_count; i++) {
    GL_INDEX_ID gl_index_id = tbl->m_key_descr_arr[i]->get_gl_index_id();
    assert(gl_index_id.cf_id == gl_index_id.index_id);

    xengine::db::ColumnFamilyHandle *cf_handle;
    cf_handle = cf_manager.get_cf(gl_index_id.cf_id);
    if (cf_handle == nullptr) {
      assert(false);
      sql_print_error("error find subtable: %d", gl_index_id.cf_id);
      DBUG_RETURN(HA_EXIT_FAILURE);
    }

    ddl_log_manager.write_drop_subtable_log(
        batch, gl_index_id.cf_id, thd_thread_id(thd), true);
  }

  // Remove the table entry in from table cache
  ddl_manager.remove_cache(dbname_tablename);

  DBUG_EXECUTE_IF("ddl_log_crash_after_xengine_delete_success", DBUG_SUICIDE(););

  /* mark xengine modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

void ha_xengine::remove_rows(Xdb_tbl_def *const tbl) {
  const xengine::common::WriteOptions wo =
      xdb_get_xengine_write_options(handler::ha_thd());

  xengine::common::ReadOptions opts;
  opts.total_order_seek = true;
  IteratorUptr it(xdb->NewIterator(opts));

  char key_buf[MAX_KEY_LENGTH];
  uint key_len;
  /*
    Remove all records in each index.
    (This is is not crash-safe, but it doesn't matter, because bulk row
    deletion will be handled on xengine side)
  */
  for (uint i = 0; i < tbl->m_key_count; i++) {
    const Xdb_key_def &kd = *tbl->m_key_descr_arr[i];
    kd.get_infimum_key(reinterpret_cast<uchar *>(key_buf), &key_len);

    const xengine::common::Slice table_key(key_buf, key_len);
    it->Seek(table_key);
    while (it->Valid()) {
      const xengine::common::Slice key = it->key();
      if (!kd.covers_key(key)) {
        break;
      }

      if (can_use_single_delete(i))
        xdb->SingleDelete(wo, key);
      else
        xdb->Delete(wo, key);
      it->Next();
    }
  }
}

int ha_xengine::rename_table(const char *const from, const char *const to,
                             const dd::Table *from_table_def,
                             dd::Table *to_table_def) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(from != nullptr);
  DBUG_ASSERT(to != nullptr);
  DBUG_ASSERT(from_table_def != nullptr);
  DBUG_ASSERT(to_table_def != nullptr);
  DBUG_ASSERT(from_table_def->se_private_id() == to_table_def->se_private_id());
  DBUG_ASSERT(from_table_def->se_private_data().raw_string() ==
              to_table_def->se_private_data().raw_string());

  std::string from_str;
  std::string to_str;
  std::string from_db;
  std::string to_db;

  if (xdb_normalize_tablename(from, &from_str) ||
      xdb_split_normalized_tablename(from_str, &from_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  if (xdb_normalize_tablename(to, &to_str) ||
      xdb_split_normalized_tablename(to_str, &to_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

#ifndef NDEBUG
  static int rename_func_enter_counts = 0;
  DBUG_EXECUTE_IF("ddl_log_crash_rename_funcs", { rename_func_enter_counts++; };);
  if (rename_func_enter_counts > 1) {
    DBUG_SUICIDE();
  }
#endif

  // If the user changed the database part of the name then validate that the
  // 'to' database exists.
  if (from_db != to_db && !xdb_database_exists(to_db)) {
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  bool from_dict = false;
  auto from_tbl_def = ddl_manager.find(from_table_def, from_str, &from_dict);
  if (nullptr == from_tbl_def) {
    my_error(ER_INTERNAL_ERROR, MYF(0),
             "Attempt to rename a table that is not present in XEngine-SE data "
             "dictionary");
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  /* Check if any index has a per-index column family */
  {
    /*
      We can't check this directly, because
      1. the object that referred to the old table has been close()d.
      2. this ha_xengine object has no connection to any table at all, it has
         been just created with (TABLE*)nullptr.

      So, we have to find the old table in the ddl_manager (it's there because
      it has been opened by this statement), and check is_auto_cf there.
    */

    Xdb_tbl_def *tdef = from_tbl_def.get();
    DBUG_ASSERT(tdef->m_key_descr_arr != nullptr);
    for (uint i = 0; i < tdef->m_key_count; i++) {
      if (tdef->m_key_descr_arr[i]->m_is_auto_cf) {
        my_error(ER_NOT_SUPPORTED_YET, MYF(0),
                 "ALTER TABLE on table with per-index CF");
        DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
      }
    }
  }

  DBUG_EXECUTE_IF("ddl_log_crash_before_write_rename_log", DBUG_SUICIDE(););

  THD *const thd = table ? table->in_use : my_core::thd_get_current_thd();
  Xdb_transaction *const tx = get_or_create_tx(thd);
  xengine_register_tx(ht, thd, tx);
  xengine::db::WriteBatch *const batch = dynamic_cast<xengine::db::WriteBatch *>(tx->get_blind_write_batch());

  if (ddl_log_manager.write_rename_cache_log(batch, from_str, to_str,
                                             thd_thread_id(thd))) {
    sql_print_error("write rename_cache ddl_log error, table_name(%s), "
                    "dst_table_name(%s), thread_id(%d)",
                    from_str.c_str(), to_str.c_str(),
                    thd_thread_id(thd));
    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  DBUG_EXECUTE_IF("ddl_log_crash_after_write_rename_log", DBUG_SUICIDE(););

  dict_manager.lock();

  if (ddl_manager.rename_cache(from_tbl_def.get(), to_str)) {
    sql_print_error("rename dictionary error, table_name(%s), dst_table_name(%s), thread_id(%d)", from_str.c_str(), to_str.c_str(), thd_thread_id(thd));
    dict_manager.unlock();

    DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  dict_manager.unlock();

  DBUG_EXECUTE_IF("ddl_log_crash_after_xengine_rename_success", DBUG_SUICIDE(););

  /* mark xengine modified that write atomic_ddl_log, make sure hton already
     registed. if not, mark_ddl_trx_read_write won't do anything.
   */
  mark_ddl_trx_read_write();

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
  check_if_incompatible_data() called if ALTER TABLE can't detect otherwise
  if new and old definition are compatible

  @details If there are no other explicit signs like changed number of
  fields this function will be called by compare_tables()
  (sql/sql_tables.cc) to decide should we rewrite whole table or only .frm
  file.

*/

bool ha_xengine::check_if_incompatible_data(HA_CREATE_INFO *const info,
                                            uint table_changes) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(info != nullptr);

  // this function is needed only for online alter-table
  DBUG_RETURN(COMPATIBLE_DATA_NO);
}

int ha_xengine::extra(enum ha_extra_function operation) {
  DBUG_ENTER_FUNC();

  switch (operation) {
  case HA_EXTRA_KEYREAD:
    m_keyread_only = true;
    break;
  case HA_EXTRA_NO_KEYREAD:
    m_keyread_only = false;
    break;
  case HA_EXTRA_FLUSH:
    /*
      If the table has blobs, then they are part of m_retrieved_record.
      This call invalidates them.
    */
    m_retrieved_record.clear();
    break;
  default:
    break;
  }

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

/**
  Returns the exact number of records, use primary index
*/
int ha_xengine::records(ha_rows *num_rows) {
  int ret = HA_EXIT_SUCCESS;

  uint index = pk_index(table, m_tbl_def.get());

  size_t max_threads = THDVAR(ha_thd(), parallel_read_threads);
  if (max_threads == 1) {
    return handler::records(num_rows); 
  }

  return records_from_index(num_rows, index);
}

int ha_xengine::records_from_index_single(ha_rows *num_rows, uint index) {
  int error = 0;
  ha_rows rows = 0;
  uchar *buf = table->record[0];
  start_psi_batch_mode();

  /* set index_only flag for xengine, see more in task #25925477 */
  extra(HA_EXTRA_KEYREAD);
  if (!(error = ha_index_init(index, false))) {
    if (!(error = ha_index_first(buf))) {
      rows = 1;
      while (!table->in_use->killed) {
        DBUG_EXECUTE_IF("bug28079850",
                        table->in_use->killed = THD::KILL_QUERY;);
        if ((error = ha_index_next(buf))) {
          if (error == HA_ERR_RECORD_DELETED)
            continue;
          else
            break;
        }
        ++rows;
      }
    }
  }
  *num_rows = rows;
  end_psi_batch_mode();
  int ha_index_end_error = 0;
  if (error != HA_ERR_END_OF_FILE) *num_rows = HA_POS_ERROR;
  // Call ha_index_end() only if handler has been initialized.
  if (inited && (ha_index_end_error = ha_index_end())) *num_rows = HA_POS_ERROR;

  /* reset index_only flag */
  extra(HA_EXTRA_NO_KEYREAD);
  return (error != HA_ERR_END_OF_FILE) ? error : ha_index_end_error;
}

/**
  Returns the exact number of records, use chosen key
*/
int ha_xengine::records_from_index(ha_rows *num_rows, uint index) {
  int ret = HA_EXIT_SUCCESS;

  uint size_start = 0;
  uint size_end = 0;

  std::shared_ptr<Xdb_key_def> kd = m_key_descr_arr[index];
  kd->get_infimum_key(m_sk_packed_tuple, &size_start);
  kd->get_supremum_key(m_sk_packed_tuple_old, &size_end);

  const common::Slice user_key_start((const char *)m_sk_packed_tuple, size_start);
  const common::Slice user_key_end((const char *)m_sk_packed_tuple_old, size_end);

  size_t max_threads = THDVAR(ha_thd(), parallel_read_threads);
  if (max_threads == 1) {
    return records_from_index_single(num_rows, index); 
  }
  DBUG_EXECUTE_IF("parallel_scan_kill", table->in_use->killed = THD::KILL_QUERY;);
  common::ParallelReader preader(max_threads, table->in_use);

  //generate range with internal_key [start,end)
  storage::Range range;
  preader.build_internal_key_range(user_key_start, user_key_end, range);

  //get snapshot if necessary
  Xdb_transaction *const tx = get_or_create_tx(table->in_use);
  tx->acquire_snapshot(true);
  common::ReadOptions read_options = tx->get_read_opts();
  read_options.total_order_seek = true;

  db::ColumnFamilyHandle *column_family_handle = kd->get_cf();  
  common::ParallelReader::Config config(column_family_handle, range, read_options);
 
  const auto tx_impl = dynamic_cast<const Xdb_transaction_impl *>(tx);
  DBUG_ASSERT(tx_impl);

  Counter::Shards n_recs;
  Counter::clear(n_recs);

  util::Transaction *trx =
      const_cast<util::Transaction *>(tx_impl->get_xdb_trx());

  /* Check for transaction interrupted every 1000 rows. */
  size_t counter = 1000;

  THD *current_thd = my_core::thd_get_current_thd();
  ret = preader.add_scan(
      trx, config, [&](const common::ParallelReader::ExecuteCtx *ctx, xengine::db::Iterator* it) {
        int ret = Status::kOk;

        /* Only check the THD state for the first thread. */
        if (ctx->thread_id_ == 0) {
          --counter;
          if (counter == 0 && my_core::thd_killed(current_thd)) {
            __XHANDLER_LOG(WARN, "query interrupted by user, %s", current_thd->query().str);
            return (static_cast<int>(Status::kCancelTask));
          }

          if (counter == 0) {
            counter = 1000;
          }
        }

        Counter::inc(n_recs, ctx->thread_id_);

        return ret;
      });

  if (ret) {
    XHANDLER_LOG(WARN, "prepare for parallel scan failed", K(ret));
  } else if ((ret = preader.run())) {
    XHANDLER_LOG(WARN, "do parallel scan failed", K(ret));
  } else {
    // get total count
    Counter::for_each(n_recs, [=](const Counter::Type n) {
      if (n > 0) {
        *num_rows += n;
      }
    });
  }

  //convert xengine err_code to mysql err_code
  if (ret == static_cast<int>(Status::kCancelTask)) {
    ret = HA_ERR_QUERY_INTERRUPTED; 
  }

  return ret;
}

/*
  Given a starting key and an ending key, estimate the number of rows that
  will exist between the two keys.
*/
ha_rows ha_xengine::records_in_range(uint inx, key_range *const min_key,
                                     key_range *const max_key) {
  DBUG_ENTER_FUNC();

  ha_rows ret = 0; // THDVAR(ha_thd(), records_in_range);
  if (ret) {
    DBUG_RETURN(ret);
  }
  if (table->force_index) {
    const ha_rows force_rows = 0/*THDVAR(ha_thd(), force_index_records_in_range)*/;
    if (force_rows) {
      DBUG_RETURN(force_rows);
    }
  }

  const Xdb_key_def &kd = *m_key_descr_arr[inx];

  uint size1 = 0;
  if (min_key) {
    size1 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple,
                                min_key->key, min_key->keypart_map);
    if (min_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        min_key->flag == HA_READ_PREFIX_LAST ||
        min_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple, size1);
    }
  } else {
    kd.get_infimum_key(m_sk_packed_tuple, &size1);
  }

  uint size2 = 0;
  if (max_key) {
    size2 = kd.pack_index_tuple(table, m_pack_buffer, m_sk_packed_tuple_old,
                                max_key->key, max_key->keypart_map);
    if (max_key->flag == HA_READ_PREFIX_LAST_OR_PREV ||
        max_key->flag == HA_READ_PREFIX_LAST ||
        max_key->flag == HA_READ_AFTER_KEY) {
      kd.successor(m_sk_packed_tuple_old, size2);
    }
    // pad the upper key with FFFFs to make sure it is more than the lower
    if (size1 > size2) {
      memset(m_sk_packed_tuple_old + size2, 0xff, size1 - size2);
      size2 = size1;
    }
  } else {
    kd.get_supremum_key(m_sk_packed_tuple_old, &size2);
  }

  const xengine::common::Slice slice1((const char *)m_sk_packed_tuple, size1);
  const xengine::common::Slice slice2((const char *)m_sk_packed_tuple_old, size2);

  // slice1 >= slice2 means no row will match
  if (slice1.compare(slice2) >= 0) {
    DBUG_RETURN(HA_EXIT_SUCCESS);
  }

  xengine::db::Range r(kd.m_is_reverse_cf ? slice2 : slice1,
                   kd.m_is_reverse_cf ? slice1 : slice2);

  uint64_t sz = 0;
  auto disk_size = kd.m_stats.m_actual_disk_size;
  if (disk_size == 0)
    disk_size = kd.m_stats.m_data_size;
  auto rows = kd.m_stats.m_rows;
  if (rows == 0 || disk_size == 0) {
    rows = 1;
    disk_size = XENGINE_ASSUMED_KEY_VALUE_DISK_SIZE;
  }

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wdeprecated-declarations"
  // Getting statistics, including from Memtables
  uint8_t include_flags = xengine::db::DB::INCLUDE_FILES;
  xdb->GetApproximateSizes(kd.get_cf(), &r, 1, &sz, include_flags);
  ret = rows * sz / disk_size;
  uint64_t memTableCount;
  xdb->GetApproximateMemTableStats(kd.get_cf(), r, &memTableCount, &sz);
  ret += memTableCount;

  /*
    GetApproximateSizes() gives estimates so ret might exceed stats.records.
    MySQL then decides to use full index scan rather than range scan, which
    is not efficient for most cases.
    To prevent this, changing estimated records slightly smaller than
    stats.records.
  */
  if (ret >= stats.records) {
    ret = stats.records * 0.99;
  }

  if (xengine_debug_optimizer_n_rows > 0) {
    ret = xengine_debug_optimizer_n_rows;
  } else if (ret == 0) {
    ret = 1;
  }

  DBUG_RETURN(ret);
}

void ha_xengine::update_create_info(HA_CREATE_INFO *const create_info) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(create_info != nullptr);

  if (!(create_info->used_fields & HA_CREATE_USED_AUTO)) {
    create_info->auto_increment_value = m_tbl_def->m_auto_incr_val;
  }

  DBUG_VOID_RETURN;
}

/**
  @brief
  Doing manual compaction on OPTIMIZE TABLE in XENGINE.
  Compaction itself is executed by background thread in XENGINE, but
  CompactRange() waits until compaction completes so this function
  may take a long time.
  Since XENGINE dataset is allocated per index id, OPTIMIZE TABLE
  triggers manual compaction for all indexes of the table.
  @details
  Compaction range is from the beginning of the index id to
  the first row of the next index id. When using reverse order
  column family, the first row of the next index id should be
  the last row of the previous index id.
*/
int ha_xengine::optimize(THD *const thd, HA_CHECK_OPT *const check_opt) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(thd != nullptr);
  DBUG_ASSERT(check_opt != nullptr);

  int rc = 0;
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    xdb->Flush(xengine::common::FlushOptions(), m_key_descr_arr[i]->get_cf());
    if (!xdb->CompactRange(xengine::common::CompactRangeOptions(),
                           m_key_descr_arr[i]->get_cf(),
                           nullptr,
                           nullptr,
                           db::TaskType::MANUAL_FULL_AMOUNT_TASK).ok()) {
      rc = 1;
      break;
    }
  }

  DBUG_RETURN(rc);
}

int ha_xengine::calculate_stats(const TABLE *const table_arg, THD *const thd,
                                HA_CHECK_OPT *const check_opt) {
  DBUG_ENTER_FUNC();

  // find per column family key ranges which need to be queried
  std::unordered_map<xengine::db::ColumnFamilyHandle *, std::vector<xengine::db::Range>>
      ranges;
  std::unordered_set<GL_INDEX_ID> ids_to_check;
  std::unordered_map<GL_INDEX_ID, uint> ids_to_keyparts;
  std::vector<uchar> buf(m_tbl_def->m_key_count * 2 *
                         Xdb_key_def::INDEX_NUMBER_SIZE);
  for (uint i = 0; i < m_tbl_def->m_key_count; i++) {
    const auto bufp = &buf[i * 2 * Xdb_key_def::INDEX_NUMBER_SIZE];
    const Xdb_key_def &kd = *m_key_descr_arr[i];
    ranges[kd.get_cf()].push_back(get_range(i, bufp));
    ids_to_check.insert(kd.get_gl_index_id());
    ids_to_keyparts[kd.get_gl_index_id()] = kd.get_key_parts();
  }

  // for analyze statements, force flush on memtable to get accurate cardinality
  Xdb_cf_manager &cf_manager = xdb_get_cf_manager();
  if (thd != nullptr && /*THDVAR(thd, flush_memtable_on_analyze) &&*/
      !xengine_pause_background_work) {
    for (auto it : ids_to_check) {
      xdb->Flush(xengine::common::FlushOptions(), cf_manager.get_cf(it.cf_id));
    }
  }

  // get XENGINE table properties for these ranges
  xengine::common::TablePropertiesCollection props;
  for (auto it : ranges) {
    const auto old_size MY_ATTRIBUTE((__unused__)) = props.size();
    const auto status = xdb->GetPropertiesOfTablesInRange(
        it.first, &it.second[0], it.second.size(), &props);
    DBUG_ASSERT(props.size() >= old_size);
    if (!status.ok())
      DBUG_RETURN(HA_ERR_INTERNAL_ERROR);
  }

  int num_sst = 0;
  // group stats per index id
  std::unordered_map<GL_INDEX_ID, Xdb_index_stats> stats;
  for (const auto &it : ids_to_check) {
    // Initialize the stats to 0. If there are no files that contain
    // this gl_index_id, then 0 should be stored for the cached stats.
    stats[it] = Xdb_index_stats(it);
    DBUG_ASSERT(ids_to_keyparts.count(it) > 0);
    stats[it].m_distinct_keys_per_prefix.resize(ids_to_keyparts[it]);
  }

// new extent stroagemanager doesn't support properties
// TODO
#if 0
  for (const auto &it : props) {
    std::vector<Xdb_index_stats> sst_stats;
    Xdb_tbl_prop_coll::read_stats_from_tbl_props(it.second, &sst_stats);
    /*
      sst_stats is a list of index statistics for indexes that have entries
      in the current SST file.
    */
    for (const auto &it1 : sst_stats) {
      /*
        Only update statistics for indexes that belong to this SQL table.

        The reason is: We are walking through all SST files that have
        entries from this table (and so can compute good statistics). For
        other SQL tables, it can be that we're only seeing a small fraction
        of table's entries (and so we can't update statistics based on that).
      */
      if (ids_to_check.find(it1.m_gl_index_id) == ids_to_check.end())
        continue;

      auto kd = ddl_manager.safe_find(it1.m_gl_index_id);
      DBUG_ASSERT(kd != nullptr);
      stats[it1.m_gl_index_id].merge(it1, true, kd->max_storage_fmt_length());
    }
    num_sst++;
  }
#endif

// for key info will be persisted when add extent, so we should not waste of doing persist_stats again.
#if 0
  // set and persist new stats
  ddl_manager.set_stats(stats);
  ddl_manager.persist_stats(true);
#endif

  DBUG_RETURN(HA_EXIT_SUCCESS);
}

int ha_xengine::analyze(THD *const thd, HA_CHECK_OPT *const check_opt) {
  DBUG_ENTER_FUNC();

  if (!table) {
    DBUG_RETURN(HA_EXIT_FAILURE);
  }

  DBUG_RETURN(calculate_stats(table, thd, check_opt));
}

void ha_xengine::get_auto_increment(ulonglong off, ulonglong inc,
                                    ulonglong nb_desired_values,
                                    ulonglong *const first_value,
                                    ulonglong *const nb_reserved_values) {
  /*
    MySQL has a somewhat complicated way of handling the auto-increment value.
    The first time get_auto_increment is called for a statement,
    nb_desired_values is the estimate for how many values will be needed.  The
    engine can then reserve some values, and those will be automatically used
    by MySQL, until a hard-coded value shows up in the insert statement, after
    which MySQL again calls this function to reset its starting value.
   *
    For simplicity we will just ignore nb_desired_values - we aren't going to
    reserve any extra values for a multi-insert statement.  Each row will
    simply acquire the next value as needed and we will always tell MySQL that
    we only reserved 1 value.  Since we are using an atomic value for
    m_auto_incr_val this should be safe - if we had to grab a mutex, doing
    an actual reserve of some values might be a better solution.
   */
  DEBUG_SYNC(ha_thd(), "xengine.autoinc_vars");

  if (off > inc) {
    off = 1;
  }

  Field *field;
  ulonglong new_val, max_val;
  field = table->key_info[table->s->next_number_index].key_part[0].field;
  max_val = xdb_get_int_col_max_value(field);

  // Local variable reference to simplify code below
  std::atomic<ulonglong> &auto_incr = m_tbl_def->m_auto_incr_val;

  if (inc == 1) {
    DBUG_ASSERT(off == 1);
    // Optimization for the standard case where we are always simply
    // incrementing from the last position

    new_val = auto_incr;
    while (new_val != std::numeric_limits<ulonglong>::max()) {
      if (auto_incr.compare_exchange_weak(new_val, std::min(new_val + 1, max_val))) {
        break;
      }
    }
  } else {
    // The next value can be more complicated if either 'inc' or 'off' is not 1
    ulonglong last_val = auto_incr;

    if (last_val > max_val) {
      new_val = std::numeric_limits<ulonglong>::max();
    } else {
      // Loop until we can correctly update the atomic value
      do {
        DBUG_ASSERT(last_val > 0);
        // Calculate the next value in the auto increment series: offset
        // + N * increment where N is 0, 1, 2, ...
        //
        // For further information please visit:
        // http://dev.mysql.com/doc/refman/5.7/en/replication-options-master.html
        //
        // The following is confusing so here is an explanation:
        // To get the next number in the sequence above you subtract out the
        // offset, calculate the next sequence (N * increment) and then add the
        // offset back in.
        //
        // The additions are rearranged to avoid overflow.  The following is
        // equivalent to (last_val - 1 + inc - off) / inc. This uses the fact
        // that (a+b)/c = a/c + b/c + (a%c + b%c)/c. To show why:
        //
        // (a+b)/c
        // = (a - a%c + a%c + b - b%c + b%c) / c
        // = (a - a%c) / c + (b - b%c) / c + (a%c + b%c) / c
        // = a/c + b/c + (a%c + b%c) / c
        //
        // Now, substitute a = last_val - 1, b = inc - off, c = inc to get the
        // following statement.
        ulonglong n =
            (last_val - 1) / inc + ((last_val - 1) % inc + inc - off) / inc;

        // Check if n * inc + off will overflow. This can only happen if we have
        // an UNSIGNED BIGINT field.
        if (n > (std::numeric_limits<ulonglong>::max() - off) / inc) {
          DBUG_ASSERT(max_val == std::numeric_limits<ulonglong>::max());
          // The 'last_val' value is already equal to or larger than the largest
          // value in the sequence.  Continuing would wrap around (technically
          // the behavior would be undefined).  What should we do?
          // We could:
          //   1) set the new value to the last possible number in our sequence
          //      as described above.  The problem with this is that this
          //      number could be smaller than a value in an existing row.
          //   2) set the new value to the largest possible number.  This number
          //      may not be in our sequence, but it is guaranteed to be equal
          //      to or larger than any other value already inserted.
          //
          //  For now I'm going to take option 2.
          //
          //  Returning ULLONG_MAX from get_auto_increment will cause the SQL
          //  layer to fail with ER_AUTOINC_READ_FAILED. This means that due to
          //  the SE API for get_auto_increment, inserts will fail with
          //  ER_AUTOINC_READ_FAILED if the column is UNSIGNED BIGINT, but
          //  inserts will fail with ER_DUP_ENTRY for other types (or no failure
          //  if the column is in a non-unique SK).
          new_val = std::numeric_limits<ulonglong>::max();
          auto_incr = new_val;  // Store the largest value into auto_incr
          break;
        }

        new_val = n * inc + off;

        // Attempt to store the new value (plus 1 since m_auto_incr_val contains
        // the next available value) into the atomic value.  If the current
        // value no longer matches what we have in 'last_val' this will fail and
        // we will repeat the loop (`last_val` will automatically get updated
        // with the current value).
        //
        // See above explanation for inc == 1 for why we use std::min.
      } while (!auto_incr.compare_exchange_weak(
          last_val, std::min(new_val + 1, max_val)));
    }
  }

  *first_value = new_val;
  *nb_reserved_values = 1;
}
#if 0
#ifndef DBUG_OFF

/* Debugger help function */
static char dbug_item_print_buf[512];

const char *dbug_print_item(Item *const item) {
  char *const buf = dbug_item_print_buf;
  String str(buf, sizeof(dbug_item_print_buf), &my_charset_bin);
  str.length(0);
  if (!item)
    return "(Item*)nullptr";
  item->print(&str, QT_ORDINARY);
  if (str.c_ptr() == buf)
    return buf;
  else
    return "Couldn't fit into buffer";
}

#endif /*DBUG_OFF*/
#endif

/**
  SQL layer calls this function to push an index condition.

  @details
    The condition is for index keyno (only one condition can be pushed at a
    time).
    The caller guarantees that condition refers only to index fields; besides
    that, fields must have

      $field->part_of_key.set_bit(keyno)

    which means that

       (handler->index_flags(keyno, $keypart, 0) & HA_KEYREAD_ONLY) == 1

    which means that field value can be restored from the index tuple.

  @return
    Part of condition we couldn't check (always nullptr).
*/

class Item *ha_xengine::idx_cond_push(uint keyno, class Item *const idx_cond) {
  DBUG_ENTER_FUNC();

  DBUG_ASSERT(keyno != MAX_KEY);
  DBUG_ASSERT(idx_cond != nullptr);

  const Xdb_key_def &kd = *m_key_descr_arr[keyno];
  if (kd.get_support_icp_flag()) {
    pushed_idx_cond = idx_cond;
    pushed_idx_cond_keyno = keyno;
    in_range_check_pushed_down = TRUE;

    /* We will check the whole condition */
    DBUG_RETURN(nullptr);
  } else {
    DBUG_RETURN(idx_cond);
  }
}

/*
  @brief
  Check the index condition.

  @detail
  Check the index condition. (The caller has unpacked all needed index
  columns into table->record[0])

  @return
    ICP_NO_MATCH - Condition not satisfied (caller should continue
                   scanning)
    OUT_OF_RANGE - We've left the range we're scanning (caller should
                   stop scanning and return HA_ERR_END_OF_FILE)

    ICP_MATCH    - Condition is satisfied (caller should fetch the record
                   and return it)
*/

enum icp_result ha_xengine::check_index_cond() const {
  DBUG_ASSERT(pushed_idx_cond);
  DBUG_ASSERT(pushed_idx_cond_keyno != MAX_KEY);

  if (end_range && compare_key_icp(end_range) > 0) {
    /* caller should return HA_ERR_END_OF_FILE already */
    return ICP_OUT_OF_RANGE;
  }

  return pushed_idx_cond->val_int() ? ICP_MATCH : ICP_NO_MATCH;
}

/**
  Checking if an index is used for ascending scan or not

  @detail
  Currently XENGINE does not support bloom filter for
  prefix lookup + descending scan, but supports bloom filter for
  prefix lookup + ascending scan. This function returns true if
  the scan pattern is absolutely ascending.
  @param kd
  @param find_flag
*/
bool ha_xengine::is_ascending(const Xdb_key_def &kd,
                              enum ha_rkey_function find_flag) const {
  bool is_ascending;
  switch (find_flag) {
  case HA_READ_KEY_EXACT: {
    is_ascending = !kd.m_is_reverse_cf;
    break;
  }
  case HA_READ_PREFIX: {
    is_ascending = true;
    break;
  }
  case HA_READ_KEY_OR_NEXT:
  case HA_READ_AFTER_KEY: {
    is_ascending = !kd.m_is_reverse_cf;
    break;
  }
  case HA_READ_KEY_OR_PREV:
  case HA_READ_BEFORE_KEY:
  case HA_READ_PREFIX_LAST:
  case HA_READ_PREFIX_LAST_OR_PREV: {
    is_ascending = kd.m_is_reverse_cf;
    break;
  }
  default:
    is_ascending = false;
  }
  return is_ascending;
}

#define SHOW_FNAME(name) xengine_show_##name

#define DEF_SHOW_FUNC(name, key)                                               \
  static int SHOW_FNAME(name)(MYSQL_THD thd, SHOW_VAR * var, char *buff) {     \
    xengine_status_counters.name =                                             \
        xengine::monitor::get_tls_query_perf_context()->                       \
            get_global_count(xengine::monitor::CountPoint::key);               \
    var->type = SHOW_LONGLONG;                                                 \
    var->value = (char *)&xengine_status_counters.name;                        \
    var->scope = SHOW_SCOPE_GLOBAL;                                            \
    return HA_EXIT_SUCCESS;                                                    \
  }

#define DEF_STATUS_VAR(name)                                                   \
  { "xengine_" #name, (char *)&SHOW_FNAME(name), SHOW_FUNC, SHOW_SCOPE_GLOBAL }

#define DEF_STATUS_VAR_PTR(name, ptr, option)                                  \
  { "xengine_" name, (char *)ptr, option, SHOW_SCOPE_GLOBAL }

#define DEF_STATUS_VAR_FUNC(name, ptr, option)                                 \
  { name, reinterpret_cast<char *>(ptr), option, SHOW_SCOPE_GLOBAL }

struct xengine_status_counters_t {
  uint64_t block_cache_miss;
  uint64_t block_cache_hit;
  uint64_t block_cache_add;
  uint64_t block_cache_index_miss;
  uint64_t block_cache_index_hit;
  uint64_t block_cache_filter_miss;
  uint64_t block_cache_filter_hit;
  uint64_t block_cache_data_miss;
  uint64_t block_cache_data_hit;
  uint64_t row_cache_add;
  uint64_t row_cache_hit;
  uint64_t row_cache_miss;
#if 0 // DEL-SYSSTAT
  uint64_t bloom_filter_useful;
#endif
  uint64_t memtable_hit;
  uint64_t memtable_miss;
  uint64_t compaction_key_drop_new;
  uint64_t compaction_key_drop_obsolete;
  uint64_t compaction_key_drop_user;
  uint64_t number_keys_written;
  uint64_t number_keys_read;
  uint64_t number_keys_updated;
  uint64_t bytes_written;
  uint64_t bytes_read;
  uint64_t no_file_closes;
  uint64_t no_file_opens;
  uint64_t no_file_errors;
  uint64_t l0_slowdown_micros;
  uint64_t l0_num_files_stall_micros;
  uint64_t rate_limit_delay_millis;
  uint64_t num_iterators;
  uint64_t number_multiget_get;
  uint64_t number_multiget_keys_read;
  uint64_t number_multiget_bytes_read;
  uint64_t number_deletes_filtered;
  uint64_t number_merge_failures;
#if 0 // DEL-SYSSTAT
  uint64_t bloom_filter_prefix_checked;
  uint64_t bloom_filter_prefix_useful;
  uint64_t number_reseeks_iteration;
  uint64_t getupdatessince_calls;
#endif
  uint64_t block_cachecompressed_miss;
  uint64_t block_cachecompressed_hit;
  uint64_t wal_synced;
  uint64_t wal_bytes;
  uint64_t write_self;
  uint64_t write_other;
  uint64_t write_timedout;
  uint64_t write_wal;
  uint64_t flush_write_bytes;
  uint64_t compact_read_bytes;
  uint64_t compact_write_bytes;
  uint64_t number_superversion_acquires;
  uint64_t number_superversion_releases;
  uint64_t number_superversion_cleanups;
  uint64_t number_block_not_compressed;
};

static xengine_status_counters_t xengine_status_counters;

DEF_SHOW_FUNC(block_cache_miss, BLOCK_CACHE_MISS)
DEF_SHOW_FUNC(block_cache_hit, BLOCK_CACHE_HIT)
DEF_SHOW_FUNC(block_cache_add, BLOCK_CACHE_ADD)
DEF_SHOW_FUNC(block_cache_index_miss, BLOCK_CACHE_INDEX_MISS)
DEF_SHOW_FUNC(block_cache_index_hit, BLOCK_CACHE_INDEX_HIT)
DEF_SHOW_FUNC(block_cache_filter_miss, BLOCK_CACHE_FILTER_MISS)
DEF_SHOW_FUNC(block_cache_filter_hit, BLOCK_CACHE_FILTER_HIT)
DEF_SHOW_FUNC(block_cache_data_miss, BLOCK_CACHE_DATA_MISS)
DEF_SHOW_FUNC(block_cache_data_hit, BLOCK_CACHE_DATA_HIT)
DEF_SHOW_FUNC(row_cache_add, ROW_CACHE_ADD)
DEF_SHOW_FUNC(row_cache_hit, ROW_CACHE_HIT)
DEF_SHOW_FUNC(row_cache_miss, ROW_CACHE_MISS)
#if 0 // DEL-SYSSTAT
DEF_SHOW_FUNC(bloom_filter_useful, BLOOM_FILTER_USEFUL)
#endif
DEF_SHOW_FUNC(memtable_hit, MEMTABLE_HIT)
DEF_SHOW_FUNC(memtable_miss, MEMTABLE_MISS)
DEF_SHOW_FUNC(number_keys_written, NUMBER_KEYS_WRITTEN)
DEF_SHOW_FUNC(number_keys_read, NUMBER_KEYS_READ)
DEF_SHOW_FUNC(number_keys_updated, NUMBER_KEYS_UPDATE)
DEF_SHOW_FUNC(bytes_written, BLOCK_CACHE_BYTES_WRITE)
DEF_SHOW_FUNC(bytes_read, ENGINE_DISK_READ)
#if 0 // DEL-SYSSTAT
DEF_SHOW_FUNC(bloom_filter_prefix_checked, BLOOM_FILTER_PREFIX_CHECKED)
DEF_SHOW_FUNC(bloom_filter_prefix_useful, BLOOM_FILTER_PREFIX_USEFUL)
DEF_SHOW_FUNC(number_reseeks_iteration, NUMBER_OF_RESEEKS_IN_ITERATION)
DEF_SHOW_FUNC(getupdatessince_calls, GET_UPDATES_SINCE_CALLS)
#endif
DEF_SHOW_FUNC(block_cachecompressed_miss, BLOCK_CACHE_COMPRESSED_MISS)
DEF_SHOW_FUNC(block_cachecompressed_hit, BLOCK_CACHE_COMPRESSED_HIT)
DEF_SHOW_FUNC(wal_synced, WAL_FILE_SYNCED)
DEF_SHOW_FUNC(wal_bytes, WAL_FILE_BYTES)
DEF_SHOW_FUNC(write_self, WRITE_DONE_BY_SELF)
DEF_SHOW_FUNC(write_other, WRITE_DONE_BY_OTHER)
DEF_SHOW_FUNC(write_wal, WRITE_WITH_WAL)
DEF_SHOW_FUNC(number_superversion_acquires, NUMBER_SUPERVERSION_ACQUIRES)
DEF_SHOW_FUNC(number_superversion_releases, NUMBER_SUPERVERSION_RELEASES)
DEF_SHOW_FUNC(number_superversion_cleanups, NUMBER_SUPERVERSION_CLEANUPS)
DEF_SHOW_FUNC(number_block_not_compressed, NUMBER_BLOCK_NOT_COMPRESSED)

static void myx_update_status() {
  export_stats.rows_deleted = global_stats.rows[ROWS_DELETED];
  export_stats.rows_inserted = global_stats.rows[ROWS_INSERTED];
  export_stats.rows_read = global_stats.rows[ROWS_READ];
  export_stats.rows_updated = global_stats.rows[ROWS_UPDATED];
#if 0 // DEL-SYSSTAT
  export_stats.rows_deleted_blind = global_stats.rows[ROWS_DELETED_BLIND];
#endif
  export_stats.system_rows_deleted = global_stats.system_rows[ROWS_DELETED];
  export_stats.system_rows_inserted = global_stats.system_rows[ROWS_INSERTED];
  export_stats.system_rows_read = global_stats.system_rows[ROWS_READ];
  export_stats.system_rows_updated = global_stats.system_rows[ROWS_UPDATED];
}

static SHOW_VAR myx_status_variables[] = {
    DEF_STATUS_VAR_FUNC("rows_deleted", &export_stats.rows_deleted,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_inserted", &export_stats.rows_inserted,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_read", &export_stats.rows_read, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("rows_updated", &export_stats.rows_updated,
                        SHOW_LONGLONG),
#if 0 // DEL-SYSSTAT
    DEF_STATUS_VAR_FUNC("rows_deleted_blind",
                        &export_stats.rows_deleted_blind, SHOW_LONGLONG),
#endif
    DEF_STATUS_VAR_FUNC("system_rows_deleted",
                        &export_stats.system_rows_deleted, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_inserted",
                        &export_stats.system_rows_inserted, SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_read", &export_stats.system_rows_read,
                        SHOW_LONGLONG),
    DEF_STATUS_VAR_FUNC("system_rows_updated",
                        &export_stats.system_rows_updated, SHOW_LONGLONG),

    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

static void show_myx_vars(THD *thd, SHOW_VAR *var, char *buff) {
  myx_update_status();
  var->type = SHOW_ARRAY;
  var->value = reinterpret_cast<char *>(&myx_status_variables);
  var->scope = SHOW_SCOPE_GLOBAL;
}

static SHOW_VAR xengine_status_vars[] = {
    DEF_STATUS_VAR(block_cache_miss),
    DEF_STATUS_VAR(block_cache_hit),
    DEF_STATUS_VAR(block_cache_add),
    DEF_STATUS_VAR(block_cache_index_miss),
    DEF_STATUS_VAR(block_cache_index_hit),
    DEF_STATUS_VAR(block_cache_filter_miss),
    DEF_STATUS_VAR(block_cache_filter_hit),
    DEF_STATUS_VAR(block_cache_data_miss),
    DEF_STATUS_VAR(block_cache_data_hit),
    DEF_STATUS_VAR(row_cache_add),
    DEF_STATUS_VAR(row_cache_hit),
    DEF_STATUS_VAR(row_cache_miss),
#if 0 // DEL-SYSSTAT
    DEF_STATUS_VAR(bloom_filter_useful),
#endif
    DEF_STATUS_VAR(memtable_hit),
    DEF_STATUS_VAR(memtable_miss),
    DEF_STATUS_VAR(number_keys_written),
    DEF_STATUS_VAR(number_keys_read),
    DEF_STATUS_VAR(number_keys_updated),
    DEF_STATUS_VAR(bytes_written),
    DEF_STATUS_VAR(bytes_read),
#if 0 // DEL-SYSSTAT
    DEF_STATUS_VAR(bloom_filter_prefix_checked),
    DEF_STATUS_VAR(bloom_filter_prefix_useful),
    DEF_STATUS_VAR(number_reseeks_iteration),
    DEF_STATUS_VAR(getupdatessince_calls),
#endif
    DEF_STATUS_VAR(block_cachecompressed_miss),
    DEF_STATUS_VAR(block_cachecompressed_hit),
    DEF_STATUS_VAR(wal_synced),
    DEF_STATUS_VAR(wal_bytes),
    DEF_STATUS_VAR(write_self),
    DEF_STATUS_VAR(write_other),
    DEF_STATUS_VAR(write_wal),
    DEF_STATUS_VAR(number_superversion_acquires),
    DEF_STATUS_VAR(number_superversion_releases),
    DEF_STATUS_VAR(number_superversion_cleanups),
    DEF_STATUS_VAR(number_block_not_compressed),
    DEF_STATUS_VAR_PTR("snapshot_conflict_errors",
                       &xengine_snapshot_conflict_errors, SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("wal_group_syncs", &xengine_wal_group_syncs,
                       SHOW_LONGLONG),
#if 0 // DEL-SYSSTAT
    DEF_STATUS_VAR_PTR("number_stat_computes", &xengine_number_stat_computes,
                       SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("number_sst_entry_put", &xengine_num_sst_entry_put,
                       SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("number_sst_entry_delete", &xengine_num_sst_entry_delete,
                       SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("number_sst_entry_singledelete",
                       &xengine_num_sst_entry_singledelete, SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("number_sst_entry_merge", &xengine_num_sst_entry_merge,
                       SHOW_LONGLONG),
    DEF_STATUS_VAR_PTR("number_sst_entry_other", &xengine_num_sst_entry_other,
                       SHOW_LONGLONG),
#endif
    {"xengine", reinterpret_cast<char *>(&show_myx_vars), SHOW_FUNC, SHOW_SCOPE_GLOBAL},
    {NullS, NullS, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

/*
  Background thread's main logic
*/

void Xdb_background_thread::run() {
  // How many seconds to wait till flushing the WAL next time.
  const int WAKE_UP_INTERVAL = 1;

  timespec ts_next_sync;
  clock_gettime(CLOCK_REALTIME, &ts_next_sync);
  ts_next_sync.tv_sec += WAKE_UP_INTERVAL;

  for (;;) {
    // Wait until the next timeout or until we receive a signal to stop the
    // thread. Request to stop the thread should only be triggered when the
    // storage engine is being unloaded.
    XDB_MUTEX_LOCK_CHECK(m_signal_mutex);
    const auto ret MY_ATTRIBUTE((__unused__)) =
        mysql_cond_timedwait(&m_signal_cond, &m_signal_mutex, &ts_next_sync);

    // Check that we receive only the expected error codes.
    DBUG_ASSERT(ret == 0 || ret == ETIMEDOUT);
    const bool local_stop = m_stop;
    const bool local_save_stats = m_save_stats;
    bool release = false;
    reset();
    // backup snapshot timer is set
    if (counter_ > 0) {
      counter_--;
      if (counter_ <= 0) {
        release = true;
        assert(0 == counter_);
      }
    }
    XDB_MUTEX_UNLOCK_CHECK(m_signal_mutex);

    if (local_stop) {
      // If we're here then that's because condition variable was signaled by
      // another thread and we're shutting down. Break out the loop to make
      // sure that shutdown thread can proceed.
      break;
    }

    // This path should be taken only when the timer expired.
    DBUG_ASSERT(ret == ETIMEDOUT);

    if (local_save_stats) {
      ddl_manager.persist_stats();
    }

    timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);

    // Flush the WAL.
    if (xdb && xengine_flush_log_at_trx_commit == 2) {
      DBUG_ASSERT(!xengine_db_options.allow_mmap_writes);
      const xengine::common::Status s = xdb->SyncWAL();
      if (!s.ok()) {
        xdb_handle_io_error(s, XDB_IO_ERROR_BG_THREAD);
      }
    }

    // release the backup snapshot
    if (release) {
      //xengine::common::Status s = xdb->release_backup_snapshot();
      //if (!s.ok()) {
        //// FIXME if failed
        //sql_print_error("ERROR: release the backup_snapshot failed\n");
      //} else {
        //if (unlink(backup_snapshot_file_.c_str()) != 0) {
           //sql_print_error("ERROR: delete the backup snapshot file failed %s\n", backup_snapshot_file_.c_str());
           //s = xengine::common::Status::IOError();
        //}
      //}
    }

    // Set the next timestamp for mysql_cond_timedwait() (which ends up calling
    // pthread_cond_timedwait()) to wait on.
    ts_next_sync.tv_sec = ts.tv_sec + WAKE_UP_INTERVAL;
  }

  // save remaining stats which might've left unsaved
  ddl_manager.persist_stats();
}

/**
  Deciding if it is possible to use bloom filter or not.

  @detail
   Even if bloom filter exists, it is not always possible
   to use bloom filter. If using bloom filter when you shouldn't,
   false negative may happen -- fewer rows than expected may be returned.
   It is users' responsibility to use bloom filter correctly.

   If bloom filter does not exist, return value does not matter because
   XENGINE does not use bloom filter internally.

  @param kd
  @param eq_cond      Equal condition part of the key. This always includes
                      system index id (4 bytes).
  @param use_all_keys True if all key parts are set with equal conditions.
                      This is aware of extended keys.
*/
bool can_use_bloom_filter(THD *thd, const Xdb_key_def &kd,
                          const xengine::common::Slice &eq_cond,
                          const bool use_all_keys, bool is_ascending) {
  bool can_use = false;

#if 0 // DEL-SYSVAR
  if (THDVAR(thd, skip_bloom_filter_on_read)) {
    return can_use;
  }
#endif

  const xengine::common::SliceTransform *prefix_extractor = kd.get_extractor();
  if (prefix_extractor) {
    /*
      This is an optimized use case for CappedPrefixTransform.
      If eq_cond length >= prefix extractor length and if
      all keys are used for equal lookup, it is
      always possible to use bloom filter.

      Prefix bloom filter can't be used on descending scan with
      prefix lookup (i.e. WHERE id1=1 ORDER BY id2 DESC), because of
      XENGINE's limitation. On ascending (or not sorting) scan,
      keys longer than the capped prefix length will be truncated down
      to the capped length and the resulting key is added to the bloom filter.

      Keys shorter than the capped prefix length will be added to
      the bloom filter. When keys are looked up, key conditionals
      longer than the capped length can be used; key conditionals
      shorter require all parts of the key to be available
      for the short key match.
    */
    if (use_all_keys && prefix_extractor->InRange(eq_cond))
      can_use = true;
    else if (!is_ascending)
      can_use = false;
    else if (prefix_extractor->SameResultWhenAppended(eq_cond))
      can_use = true;
    else
      can_use = false;
  } else {
    /*
      if prefix extractor is not defined, all key parts have to be
      used by eq_cond.
    */
    if (use_all_keys)
      can_use = true;
    else
      can_use = false;
  }

  return can_use;
}

/* For modules that need access to the global data structures */
bool xdb_is_initialized() { return xdb_initialized;  }

xengine::util::TransactionDB *xdb_get_xengine_db() { return xdb; }

Xdb_cf_manager &xdb_get_cf_manager() { return cf_manager; }

xengine::table::BlockBasedTableOptions &xdb_get_table_options() {
  return xengine_tbl_options;
}

const char *get_xdb_io_error_string(const XDB_IO_ERROR_TYPE err_type) {
  // If this assertion fails then this means that a member has been either added
  // to or removed from XDB_IO_ERROR_TYPE enum and this function needs to be
  // changed to return the appropriate value.
  static_assert(XDB_IO_ERROR_LAST == 4, "Please handle all the error types.");

  switch (err_type) {
  case XDB_IO_ERROR_TYPE::XDB_IO_ERROR_TX_COMMIT:
    return "XDB_IO_ERROR_TX_COMMIT";
  case XDB_IO_ERROR_TYPE::XDB_IO_ERROR_DICT_COMMIT:
    return "XDB_IO_ERROR_DICT_COMMIT";
  case XDB_IO_ERROR_TYPE::XDB_IO_ERROR_BG_THREAD:
    return "XDB_IO_ERROR_BG_THREAD";
  case XDB_IO_ERROR_TYPE::XDB_IO_ERROR_GENERAL:
    return "XDB_IO_ERROR_GENERAL";
  default:
    DBUG_ASSERT(false);
    return "(unknown)";
  }
}

// In case of core dump generation we want this function NOT to be optimized
// so that we can capture as much data as possible to debug the root cause
// more efficiently.
#pragma GCC push_options
#pragma GCC optimize("O0")

void xdb_handle_io_error(const xengine::common::Status status,
                         const XDB_IO_ERROR_TYPE err_type) {
  if (status.IsIOError()) {
    switch (err_type) {
    case XDB_IO_ERROR_TX_COMMIT:
    case XDB_IO_ERROR_DICT_COMMIT: {
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: failed to write to WAL. Error type = %s, "
                      "status code = %d, status = %s",
                      get_xdb_io_error_string(err_type), status.code(),
                      status.ToString().c_str());
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: aborting on WAL write error.");
      abort_with_stack_traces();
      break;
    }
    case XDB_IO_ERROR_BG_THREAD: {
      /* NO_LINT_DEBUG */
      sql_print_warning("XEngine: background thread failed to write to XEngine. "
                        "Error type = %s, status code = %d, status = %s",
                        get_xdb_io_error_string(err_type), status.code(),
                        status.ToString().c_str());
      break;
    }
    case XDB_IO_ERROR_GENERAL: {
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: failed on I/O. Error type = %s, "
                      "status code = %d, status = %s",
                      get_xdb_io_error_string(err_type), status.code(),
                      status.ToString().c_str());
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: aborting on I/O error.");
      abort_with_stack_traces();
      break;
    }
    default:
      DBUG_ASSERT(0);
      break;
    }
  } else if (status.IsCorruption()) {
    /* NO_LINT_DEBUG */
    sql_print_error("XEngine: data corruption detected! Error type = %s, "
                    "status code = %d, status = %s",
                    get_xdb_io_error_string(err_type), status.code(),
                    status.ToString().c_str());
    /* NO_LINT_DEBUG */
    sql_print_error("XEngine: aborting because of data corruption.");
    abort_with_stack_traces();
  } else if (!status.ok()) {
    switch (err_type) {
    case XDB_IO_ERROR_DICT_COMMIT: {
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: failed to write to WAL (dictionary). "
                      "Error type = %s, status code = %d, status = %s",
                      get_xdb_io_error_string(err_type), status.code(),
                      status.ToString().c_str());
      /* NO_LINT_DEBUG */
      sql_print_error("XEngine: aborting on WAL write error.");
      abort_with_stack_traces();
      break;
    }
    default:
      /* NO_LINT_DEBUG */
      sql_print_warning("XEngine: failed to read/write in XEngine. "
                        "Error type = %s, status code = %d, status = %s",
                        get_xdb_io_error_string(err_type), status.code(),
                        status.ToString().c_str());
      break;
    }
  }
}

#pragma GCC pop_options

Xdb_dict_manager *xdb_get_dict_manager(void) { return &dict_manager; }

Xdb_ddl_manager *xdb_get_ddl_manager(void) { return &ddl_manager; }

Xdb_binlog_manager *xdb_get_binlog_manager(void) { return &binlog_manager; }

void xengine_set_compaction_options(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    my_core::SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr, const void *const save) {
  if (var_ptr && save) {
    *(uint64_t *)var_ptr = *(const uint64_t *)save;
  }
  const Xdb_compact_params params = {
      (uint64_t)xengine_compaction_sequential_deletes,
      (uint64_t)xengine_compaction_sequential_deletes_window,
      (uint64_t)xengine_compaction_sequential_deletes_file_size};
  if (properties_collector_factory) {
    properties_collector_factory->SetCompactionParams(params);
  }
}

#if 0 // DEL-SYSVAR
static void xengine_set_query_trace_enable_count(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const bool enable_count = *static_cast<const bool*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetDBOptions({{
          "query_trace_enable_count",
          enable_count ? "true" : "false"}});
  xengine_db_options.query_trace_enable_count = enable_count;
  xengine::monitor::get_tls_query_perf_context()->opt_enable_count_ = enable_count;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_query_trace_print_stats(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const bool print_stats = *static_cast<const bool*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetDBOptions({{
          "query_trace_print_stats",
          print_stats ? "true" : "false"}});
  xengine_db_options.query_trace_print_stats = print_stats;
  xengine::monitor::get_tls_query_perf_context()->opt_print_stats_ = print_stats;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

}

#endif

static void xengine_set_query_trace_sum(
    THD *const thd, struct SYS_VAR *const var, void *const var_ptr,
     const void *save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  int64_t value = *static_cast<const int64_t*>(save);
  switch (value) {
  case 0:
    xengine_query_trace_sum = 0;
    xengine::monitor::QueryPerfContext::opt_trace_sum_ = false;
    break;
  case 1:
    xengine_query_trace_sum = 1;
    xengine::monitor::QueryPerfContext::opt_trace_sum_ = true;
    break;
  case 2:
    xengine::monitor::get_tls_query_perf_context()->clear_stats();
    break;
  default:
    break;
  }

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_query_trace_print_slow(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const bool print_slow = *static_cast<const bool*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xengine_query_trace_print_slow = print_slow;
  xengine::monitor::QueryPerfContext::opt_print_slow_ = print_slow;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_block_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  auto value = *static_cast<const long long*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xengine_block_cache_size = value;
  if (!xengine_tbl_options.no_block_cache) {
    DBUG_ASSERT(xengine_tbl_options.block_cache != nullptr);
    xengine_tbl_options.block_cache->SetCapacity(xengine_block_cache_size);
  }
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_row_cache_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  auto value = *static_cast<const long long*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  if (xengine_row_cache_size > 0) {
    DBUG_ASSERT(xengine_db_options.row_cache != nullptr);
    xengine_row_cache_size = value;
    xengine_db_options.row_cache->SetCapacity(xengine_row_cache_size);
  // ToDo: how about initialize a new row cache if not set at begin?
  }
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_db_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"db_write_buffer_size", std::to_string(value)}});
  xengine_db_options.db_write_buffer_size = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_db_total_write_buffer_size(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"db_total_write_buffer_size", std::to_string(value)}});
  xengine_db_options.db_total_write_buffer_size = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_write_buffer_size (
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  auto value = *static_cast<const unsigned long*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"write_buffer_size", std::to_string(value)}});
  xengine_default_cf_options.write_buffer_size = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

#if 0  // DEL-SYSVAR
static void xengine_set_arena_block_size (
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"arena_block_size", std::to_string(value)}});
  xengine_default_cf_options.arena_block_size = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
#endif

static void xengine_set_level0_file_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "level0_file_num_compaction_trigger",
          std::to_string(value)}});
  xengine_default_cf_options.level0_file_num_compaction_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_level0_layer_num_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "level0_layer_num_compaction_trigger",
          std::to_string(value)}});
  xengine_default_cf_options.level0_layer_num_compaction_trigger = value;
  // bug #21280399, avoid warning message in error log about:
  // level0_stop_writes_trigger >= level0_slowdown_writes_trigger >= level0_file_num_compaction_trigger
  if (xengine_default_cf_options.level0_slowdown_writes_trigger <
      xengine_default_cf_options.level0_file_num_compaction_trigger) {
    xengine_default_cf_options.level0_slowdown_writes_trigger =
        xengine_default_cf_options.level0_file_num_compaction_trigger;
  }
  if (xengine_default_cf_options.level0_stop_writes_trigger <
      xengine_default_cf_options.level0_slowdown_writes_trigger) {
    xengine_default_cf_options.level0_stop_writes_trigger =
      xengine_default_cf_options.level0_slowdown_writes_trigger;
  }
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_level1_extents_major_compaction_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "level1_extents_major_compaction_trigger",
          std::to_string(value)}});
  xengine_default_cf_options.level1_extents_major_compaction_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_disable_auto_compactions(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const bool value = *static_cast<const bool*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "disable_auto_compactions",
          value ? "true" : "false"}});
  xengine_default_cf_options.disable_auto_compactions = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_flush_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "flush_delete_percent",
          std::to_string(value)}});
  xengine_default_cf_options.flush_delete_percent = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_compaction_delete_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "compaction_delete_percent",
          std::to_string(value)}});
  xengine_default_cf_options.compaction_delete_percent = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_flush_delete_percent_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "flush_delete_percent_trigger",
          std::to_string(value)}});
  xengine_default_cf_options.flush_delete_percent_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_flush_delete_record_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "flush_delete_record_trigger",
          std::to_string(value)}});
  xengine_default_cf_options.flush_delete_record_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_compaction_task_extents_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
       "compaction_task_extents_limit",
          std::to_string(value)}});
  xengine_default_cf_options.compaction_task_extents_limit = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_scan_add_blocks_limit(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const uint64_t value = *static_cast<const uint64_t*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "scan_add_blocks_limit",
          std::to_string(value)}});
  xengine_default_cf_options.scan_add_blocks_limit = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_bottommost_level(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const uint64_t value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "bottommost_level",
          std::to_string(value)}});
  xengine_default_cf_options.bottommost_level = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

#if 0 // DEL-SYSVAR
static void xengine_set_max_bytes_for_level_base (
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const uint64_t value = *static_cast<const uint64_t*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"max_bytes_for_level_base", std::to_string(value)}});
  xengine_default_cf_options.max_bytes_for_level_base = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_max_bytes_for_level_multiplier(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const double value = *static_cast<const double*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"max_bytes_for_level_multiplier", std::to_string(value)}});
  xengine_default_cf_options.max_bytes_for_level_multiplier = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_target_file_size_base(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const uint64_t value = *static_cast<const uint64_t*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"target_file_size_base", std::to_string(value)}});
  xengine_default_cf_options.target_file_size_base = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_level0_slowdown_writes_trigger(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"level0_slowdown_writes_trigger", std::to_string(value)}});
  xengine_default_cf_options.level0_slowdown_writes_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_level0_stop_writes_trigger(
  THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"level0_stop_writes_trigger", std::to_string(value)}});
  xengine_default_cf_options.level0_stop_writes_trigger = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_target_file_size_multiplier(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int value = *static_cast<const int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{"target_file_size_multiplier", std::to_string(value)}});
  xengine_default_cf_options.target_file_size_multiplier = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
#endif

static void xengine_set_level2_usage_percent(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const int64_t value = *static_cast<const uint64_t*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetOptions({{
          "level2_usage_percent",
          std::to_string(value)}});
  xengine_default_cf_options.level2_usage_percent = value;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_mutex_backtrace_threshold_ns(
    THD *thd, struct SYS_VAR *var, void *var_ptr, const void *save) {
  DBUG_ASSERT(save != nullptr);
  const uint64_t mutex_backtrace_threshold_ns = *static_cast<const uint64_t*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetDBOptions({{
          "mutex_backtrace_threshold_ns",
          std::to_string(mutex_backtrace_threshold_ns)}});
  xengine_db_options.mutex_backtrace_threshold_ns = mutex_backtrace_threshold_ns;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

#if 0 // DEL-SYSVAR
void xengine_set_table_stats_sampling_pct(
    my_core::THD *const thd MY_ATTRIBUTE((__unused__)),
    my_core::SYS_VAR *const var MY_ATTRIBUTE((__unused__)),
    void *const var_ptr MY_ATTRIBUTE((__unused__)), const void *const save) {
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  const uint32_t new_val = *static_cast<const uint32_t *>(save);

  if (new_val != xengine_table_stats_sampling_pct) {
    xengine_table_stats_sampling_pct = new_val;

    if (properties_collector_factory) {
      properties_collector_factory->SetTableStatsSamplingPct(
          xengine_table_stats_sampling_pct);
    }
  }

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

void xengine_set_delayed_write_rate(THD *thd, struct SYS_VAR *var,
                                    void *var_ptr, const void *save) {
  const uint64_t new_val = *static_cast<const uint64_t *>(save);
  if (xengine_delayed_write_rate != new_val) {
    xengine_delayed_write_rate = new_val;
    xengine_db_options.delayed_write_rate = new_val;
  }
}
#endif

/*
  This function allows setting the rate limiter's bytes per second value
  but only if the rate limiter is turned on which has to be done at startup.
  If the rate is already 0 (turned off) or we are changing it to 0 (trying
  to turn it off) this function will push a warning to the client and do
  nothing.
  This is similar to the code in innodb_doublewrite_update (found in
  storage/innobase/handler/ha_innodb.cc).
*/
static void xengine_set_rate_limiter_bytes_per_sec(
    THD *thd,
    struct SYS_VAR *const var,
    void *const var_ptr,
    const void *const save) {
  const uint64_t new_val = *static_cast<const uint64_t *>(save);
  if (new_val == 0 || xengine_rate_limiter_bytes_per_sec == 0) {
    /*
      If a rate_limiter was not enabled at startup we can't change it nor
      can we disable it if one was created at startup
    */
    push_warning_printf(thd, Sql_condition::SL_WARNING, ER_WRONG_ARGUMENTS,
                        "XEngine: xengine_rate_limiter_bytes_per_sec cannot "
                        "be dynamically changed to or from 0.  Do a clean "
                        "shutdown if you want to change it from or to 0.");
  } else if (new_val != xengine_rate_limiter_bytes_per_sec) {
    /* Apply the new value to the rate limiter and store it locally */
    DBUG_ASSERT(xengine_rate_limiter != nullptr);
    xengine_rate_limiter_bytes_per_sec = new_val;
    xengine_rate_limiter->SetBytesPerSecond(new_val);
  }
}

void xdb_set_collation_exception_list(const char *const exception_list) {
  DBUG_ASSERT(xdb_collation_exceptions != nullptr);

  if (!xdb_collation_exceptions->set_patterns(exception_list)) {
    warn_about_bad_patterns(xdb_collation_exceptions,
                                     "strict_collation_exceptions");
  }
}

void xengine_set_collation_exception_list(THD *const thd,
                                          struct SYS_VAR *const var,
                                          void *const var_ptr,
                                          const void *const save) {
  const char *const val = *static_cast<const char *const *>(save);

  xdb_set_collation_exception_list(val == nullptr ? "" : val);

  *static_cast<const char **>(var_ptr) = val;
}

#if 0 // DEL-SYSVAR
void xengine_set_bulk_load(THD *const thd, struct SYS_VAR *const var
                                               MY_ATTRIBUTE((__unused__)),
                           void *const var_ptr, const void *const save) {
  Xdb_transaction *&tx = get_tx_from_thd(thd);

  if (tx != nullptr) {
    const int rc = tx->finish_bulk_load();
    if (rc != 0) {
      // NO_LINT_DEBUG
      sql_print_error("XEngine: Error %d finalizing last SST file while "
                      "setting bulk loading variable",
                      rc);
      abort_with_stack_traces();
    }
  }

  *static_cast<bool *>(var_ptr) = *static_cast<const bool *>(save);
}
#endif

static void xengine_set_stats_dump_period_sec(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);
  const unsigned int dump_period_sec = *static_cast<const unsigned int*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetDBOptions({{
          "stats_dump_period_sec",
          std::to_string(dump_period_sec)}});
  xengine_db_options.stats_dump_period_sec = dump_period_sec;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_dump_malloc_stats(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);
  const bool dump_malloc_stats = *static_cast<const bool*>(save);
  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xdb->SetDBOptions({{
      "dump_malloc_stats",
      dump_malloc_stats ? "true" : "false"}});
  xengine_db_options.dump_malloc_stats = dump_malloc_stats;
  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
/*
static void xengine_set_max_background_dumps(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xengine_db_options.max_background_dumps = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "max_background_dumps",
      std::to_string(xengine_db_options.max_background_dumps)}});

  xengine_db_options.env->SetBackgroundThreads(
      xengine_db_options.max_background_dumps,
      xengine::util::Env::Priority::DUMP);

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
*/

static void xengine_set_dump_memtable_limit_size(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.dump_memtable_limit_size = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "dump_memtable_limit_size",
      std::to_string(xengine_db_options.dump_memtable_limit_size)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_auto_shrink_enabled(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.auto_shrink_enabled = *static_cast<const bool *>(save);

  xdb->SetDBOptions({{
      "auto_shrink_enabled",
      std::to_string(xengine_db_options.auto_shrink_enabled)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_max_free_extent_percent(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.max_free_extent_percent = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "max_free_extent_percent",
      std::to_string(xengine_db_options.max_free_extent_percent)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_shrink_allocate_interval(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.shrink_allocate_interval = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "shrink_allocate_interval",
      std::to_string(xengine_db_options.shrink_allocate_interval)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.max_shrink_extent_count = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "max_shrink_extent_count",
      std::to_string(xengine_db_options.max_shrink_extent_count)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_total_max_shrink_extent_count(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.total_max_shrink_extent_count = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "total_max_shrink_extent_count",
      std::to_string(xengine_db_options.total_max_shrink_extent_count)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_auto_shrink_schedule_interval(THD *thd,
                                                      struct SYS_VAR *const var,
                                                      void *const var_ptr,
                                                      const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.auto_shrink_schedule_interval =
      *static_cast<const int *>(save);

  xdb->SetDBOptions(
      {{"auto_shrink_schedule_interval",
        std::to_string(xengine_db_options.auto_shrink_schedule_interval)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_estimate_cost_depth(THD *thd, struct SYS_VAR *const var,
                                            void *const var_ptr,
                                            const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.estimate_cost_depth = *static_cast<const int *>(save);

  xdb->SetDBOptions({{"estimate_cost_depth",
                      std::to_string(xengine_db_options.estimate_cost_depth)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_idle_tasks_schedule_time(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.idle_tasks_schedule_time = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "idle_tasks_schedule_time",
      std::to_string(xengine_db_options.idle_tasks_schedule_time)}});

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
#if 0 // DEL-SYSVAR
static void xengine_set_max_background_compactions(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);
  xengine_db_options.max_background_compactions = *static_cast<const int *>(save);

  xdb->SetDBOptions({{
      "max_background_compactions",
      std::to_string(xengine_db_options.max_background_compactions)}});

  xengine_db_options.env->SetBackgroundThreads(
      xengine_db_options.max_background_compactions,
      xengine::util::Env::Priority::LOW);

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}

static void xengine_set_background_flushes(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {
  DBUG_ASSERT(save != nullptr);

  XDB_MUTEX_LOCK_CHECK(xdb_sysvars_mutex);

  xengine_db_options.max_background_flushes = *static_cast<const int *>(save);
  xengine_db_options.env->SetBackgroundThreads(
      xengine_db_options.max_background_flushes, xengine::util::Env::Priority::HIGH);

  XDB_MUTEX_UNLOCK_CHECK(xdb_sysvars_mutex);
}
#endif

static void xengine_shrink_table_space_sub_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    const void *const save) {}

static int xengine_shrink_table_space_func(
    THD *thd, struct SYS_VAR *const var, void *const var_ptr,
    struct st_mysql_value *const value) {
  DBUG_ASSERT(value != nullptr);

  int ret = HA_EXIT_SUCCESS;
  char buf[FN_REFLEN];
  int len = sizeof(buf);
  const char *const value_raw = value->val_str(value, buf, &len);
  std::string value_str = std::string(value_raw);

  DBUG_EXECUTE_IF("before_shrink", sleep(2););
  xdb_drop_idx_thread.enter_race_condition();
  if (xengine_shrink_table_space >= 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Other shrink extent space is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    xdb_drop_idx_thread.exit_race_condition();
    return ret;
  } else if (xdb_drop_idx_thread.is_run()) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Background drop index is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    xdb_drop_idx_thread.exit_race_condition();
    return ret;
  } else if (xengine_inplace_populate_indexes > 0) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Create second index is running", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
    xdb_drop_idx_thread.exit_race_condition();
    return ret;
  }
  xengine_shrink_table_space = std::stoi(value_str);
  sql_print_information("SHRINK EXTENT SPACE set xengine_shrink_table_space = %d", xengine_shrink_table_space);
  xdb_drop_idx_thread.exit_race_condition();

  DBUG_EXECUTE_IF("sleep_in_shrink", sleep(5););
  if (-1 == xengine_shrink_table_space) {
    //do nothing
  } else if (xdb->shrink_table_space(xengine_shrink_table_space) != xengine::common::Status::kOk) {
    my_printf_error(ER_UNKNOWN_ERROR,
                        "Shrink table space not finished, please check the log of XEngine", MYF(0));
    ret = HA_ERR_INTERNAL_ERROR;
  }

  xdb_drop_idx_thread.enter_race_condition();
  xengine_shrink_table_space = -1;
  sql_print_information("SHRINK EXTENT SPACE set xengine_shrink_table_space = -1");
  xdb_drop_idx_thread.exit_race_condition();

  return ret;
}

void xdb_queue_save_stats_request() { xdb_bg_thread.request_save_stats(); }

void ha_xengine::rpl_before_delete_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = true;

  DBUG_VOID_RETURN;
}

void ha_xengine::rpl_after_delete_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_delete_rows = false;

  DBUG_VOID_RETURN;
}

void ha_xengine::rpl_before_update_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = true;

  DBUG_VOID_RETURN;
}

void ha_xengine::rpl_after_update_rows() {
  DBUG_ENTER_FUNC();

  m_in_rpl_update_rows = false;

  DBUG_VOID_RETURN;
}

/**
  @brief
  Read Free Replication can be used or not. Returning False means
  Read Free Replication can be used. Read Free Replication can be used
  on UPDATE or DELETE row events, and table must have user defined
  primary key.
*/
bool ha_xengine::use_read_free_rpl() {
  DBUG_ENTER_FUNC();
  DBUG_RETURN(false);
}

double ha_xengine::read_time(uint index, uint ranges, ha_rows rows) {
  DBUG_ENTER_FUNC();

  if (index != table->s->primary_key) {
    /* Non covering index range scan */
    DBUG_RETURN(handler::read_time(index, ranges, rows));
  }

  DBUG_RETURN((rows / 20.0) + 1);
}

ParallelScanCtx::ParallelScanCtx(ha_xengine* h)
    : pk_packed_tuple(nullptr), sk_packed_tuple(nullptr), pack_buffer(), ha(h),
      table_name(h->table->s->table_name.str), db_name(h->table->s->db.str),
      checksums(0), rows(0) {}

ParallelScanCtx::~ParallelScanCtx() {
  destroy();
}

int ParallelScanCtx::init() {
  if (!(pk_packed_tuple = reinterpret_cast<uchar *>(my_malloc(
            PSI_NOT_INSTRUMENTED, ha->m_pack_key_len, MYF(0)))) ||
      !(sk_packed_tuple = reinterpret_cast<uchar *>(my_malloc(
            PSI_NOT_INSTRUMENTED, ha->m_max_packed_sk_len, MYF(0)))) ||
      !(pack_buffer = reinterpret_cast<uchar *>(my_malloc(
            PSI_NOT_INSTRUMENTED, ha->m_max_packed_sk_len, MYF(0))))) {
    DBUG_ASSERT(0);
    return 1;
  }

  if (open_table_from_share(current_thd, ha->table->s, table_name, 0,
                            EXTRA_RECORD | DELAYED_OPEN | SKIP_NEW_HANDLER, 0,
                            &thd_table, false, nullptr)) {
    DBUG_ASSERT(0);
    return 1;
  }
  return 0;
}

void ParallelScanCtx::destroy() {
  my_free(pk_packed_tuple);
  pk_packed_tuple = nullptr;

  my_free(sk_packed_tuple);
  sk_packed_tuple = nullptr;

  my_free(pack_buffer);
  pack_buffer = nullptr;

  closefrm(&thd_table, false);
}

int ParallelDDLScanCtx::init() {
  int res = HA_EXIT_SUCCESS;
  if ((res = ParallelScanCtx::init())) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: ParallelScanCtx init failed, errcode=%d, table_name: %s", res, table_name);
    DBUG_ASSERT(0);
  } else if ((res = xdb_merge->init())) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: xdb_merge init failed, errcode=%d, table_name: %s", res, table_name);
    DBUG_ASSERT(0);
  } else if ((res = bg_merge.init())) {
    __XHANDLER_LOG(ERROR, "XEngineDDL: bg_merge init failed, errcode=%d, table_name: %s", res, table_name);
    DBUG_ASSERT(0);
  }
  return res;
}

ParallelDDLMergeCtx::~ParallelDDLMergeCtx() {
  for (auto& t : m_merges) {
    if (t.valid()) {
      t.wait();
    }
  }
}

void ParallelDDLMergeCtx::start(size_t max_threads, bool inject_err) {
  auto f = [this, inject_err] (size_t thread_id) {
    int res = (inject_err && thread_id == this->m_ddl_ctx_set.size() - 1) ? HA_ERR_INTERNAL_ERROR
                                                                          : HA_EXIT_SUCCESS;
    if (res || (res = this->m_merge_func(thread_id))) {
      // if one global merge fail, interrupt all the others
      for (auto &ctx : this->m_ddl_ctx_set) {
        ctx->bg_merge.set_interrupt();
      }
    }

    return res;
  };
  for (size_t i = 0; i < max_threads; i++) {
    m_merges.push_back(std::async(std::launch::async, f, i));
  }
}

int ParallelDDLMergeCtx::finish() {
  int res = HA_EXIT_SUCCESS;
  for (size_t i = 0; i < m_merges.size(); i++) {
    if ((res = m_merges[i].get())) {
      return res;
    }
  }
  return res;
}

}; // namespace myx

void mysql_set_xengine_info_log_level(ulong lvl) {
  xengine::logger::Logger::get_log().set_log_level(myx::get_xengine_log_level(lvl));
}

bool mysql_reinit_xengine_log()
{
  int log_init_ret = 0;
  auto log_level = myx::get_xengine_log_level(log_error_verbosity);
  if ((NULL != log_error_dest) && (0 != strcmp(log_error_dest, "stderr")) &&
      xengine::logger::Logger::get_log().is_inited()) {
    fsync(STDERR_FILENO);
    log_init_ret = xengine::logger::Logger::get_log().reinit(STDERR_FILENO, log_level);
  }

  return (0 != log_init_ret);
}

/*
  Register the storage engine plugin outside of myx namespace
  so that mysql_declare_plugin does not get confused when it does
  its name generation.
*/

struct st_mysql_storage_engine xengine_storage_engine = {
    MYSQL_HANDLERTON_INTERFACE_VERSION};

mysql_declare_plugin(xengine_se){
    MYSQL_STORAGE_ENGINE_PLUGIN,       /* Plugin Type */
    &xengine_storage_engine,           /* Plugin Descriptor */
    "XENGINE",                         /* Plugin Name */
    "Alibaba  X-Engine Team",          /* Plugin Author */
    "X-Engine storage engine",         /* Plugin Description */
    PLUGIN_LICENSE_GPL,                /* Plugin Licence */
    myx::xengine_init_func,            /* Plugin Entry Point */
    nullptr,                           /* Plugin Check Uninstall */
    myx::xengine_done_func,            /* Plugin Deinitializer */
    0x0001,                            /* version number (0.1) */
    myx::xengine_status_vars,          /* status variables */
    myx::xengine_system_variables,     /* system variables */
    nullptr,                           /* config options */
    0,                                 /* flags */
},
    myx::xdb_i_s_cfstats, myx::xdb_i_s_dbstats,
    myx::xdb_i_s_perf_context, myx::xdb_i_s_perf_context_global,
    /*myx::xdb_i_s_cfoptions,*/ myx::xdb_i_s_compact_stats,
    myx::xdb_i_s_global_info, myx::xdb_i_s_ddl,
    myx::xdb_i_s_index_file_map, myx::xdb_i_s_lock_info,
    myx::xdb_i_s_trx_info, myx::i_s_xengine_tables, myx::i_s_xengine_columns,
    myx::xdb_i_s_xengine_compaction_task, myx::xdb_i_s_xengine_compaction_history,
    myx::xdb_i_s_xengine_mem_alloc,
    myx::xdb_i_s_xengine_subtable,
    myx::xdb_i_s_xengine_table_space,
    myx::xdb_i_s_query_trace,
    myx::xdb_i_s_xengine_debug_info
    mysql_declare_plugin_end;
