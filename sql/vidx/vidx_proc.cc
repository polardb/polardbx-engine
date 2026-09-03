/* Copyright (c) 2026, Alibaba and/or its affiliates. All rights reserved.

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

#include "sql/vidx/vidx_proc.h"

#include <algorithm>  // std::sort (prebuild_check report order)
#include <cstring>    // strlen, strcmp
#include <sstream>
#include <string>
#include <vector>

#include "my_base.h"
#include "mysqld_error.h"  // ER_VECTOR_INDEX_USAGE, ER_*
#include "sql/auth/auth_acls.h"    // SELECT_ACL
#include "sql/auth/auth_common.h"  // check_table_access
#include "sql/item.h"
#include "sql/protocol.h"
#include "sql/sql_base.h"   // open_and_lock_tables, close_thread_tables
#include "sql/sql_class.h"  // THD
#include "sql/table.h"      // TABLE, Table_ref
#include "thr_lock.h"       // TL_READ

#include "vidx/vidx_hnsw.h"   // vidx::hnsw::mhnsw_preload
#include "vidx/vidx_index.h"  // vidx::hnsw::copy_index_option_m

namespace im {

/* Schema: dbms_vidx */
LEX_CSTRING VIDX_PROC_SCHEMA = {C_STRING_WITH_LEN("dbms_vidx")};

Proc *Vidx_proc_preload::instance() {
  static Proc *proc = new Vidx_proc_preload(key_memory_package);
  return proc;
}

Sql_cmd *Vidx_proc_preload::invoke_cmd(THD *thd,
                                       mem_root_deque<Item *> *list) const {
  return new (thd->mem_root)
      Sql_cmd_vidx_proc_preload(thd, list, this, /*check_only=*/false);
}

Proc *Vidx_proc_preload_check::instance() {
  static Proc *proc = new Vidx_proc_preload_check(key_memory_package);
  return proc;
}

Sql_cmd *Vidx_proc_preload_check::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root)
      Sql_cmd_vidx_proc_preload(thd, list, this, /*check_only=*/true);
}

bool Sql_cmd_vidx_proc_preload::pc_execute(THD *thd) {
  DBUG_TRACE;

  /* Signature (validated by the framework against the registered parameter
     list): preload(db, table, column) / preload_check(db, table, column).
     LOAD vs CHECK is encoded by the procedure name, not an argument. */
  const size_t argc = (m_list == nullptr ? 0 : m_list->size());
  if (argc != 3) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_vidx.preload, expected (db, table, vector_column)");
    return true;
  }

  auto it = m_list->begin();
  String db_buf, tbl_buf, col_buf;
  String *db_str = (*it)->val_str(&db_buf);
  ++it;
  String *tbl_str = (*it)->val_str(&tbl_buf);
  ++it;
  String *col_str = (*it)->val_str(&col_buf);

  const bool check_only = m_check_only;

  if (db_str == nullptr || db_str->length() == 0 || tbl_str == nullptr ||
      tbl_str->length() == 0 || col_str == nullptr || col_str->length() == 0) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_vidx.preload, db/table/vector_column cannot be empty");
    return true;
  }

  /* Copy onto thd->mem_root so the C strings stay valid past the Items. */
  const char *db_name = thd->strmake(db_str->ptr(), db_str->length());
  const char *table_name = thd->strmake(tbl_str->ptr(), tbl_str->length());
  const char *vec_column = thd->strmake(col_str->ptr(), col_str->length());
  if (db_name == nullptr || table_name == nullptr || vec_column == nullptr) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), 0);
    return true;
  }

  /* Open the base table (read lock); the auxiliary HNSW graph table is
     opened transparently as table->hlindex_active by the SQL layer. */
  Table_ref table_ref(db_name, table_name, TL_READ, MDL_SHARED_READ);
  if (open_and_lock_tables(thd, &table_ref, 0)) {
    return true;  // error already set
  }

  TABLE *table = table_ref.table;
  if (table == nullptr) {
    close_thread_tables(thd);
    my_error(ER_NO_SUCH_TABLE, MYF(0), db_name, table_name);
    return true;
  }

  vidx::hnsw::Preload_stats stats;
  std::string reason;
  int ret = vidx::hnsw::mhnsw_preload(table, vec_column, check_only, stats,
                                      reason);

  close_thread_tables(thd);

  if (ret != 0) {
    /* Hard-fail with the precise reason (memory shortfall, etc.). */
    my_error(ER_VECTOR_INDEX_USAGE, MYF(0),
             reason.empty() ? "preload failed" : reason.c_str());
    return true;
  }

  /* Stash the result row on thd->mem_root for send_result().  Display name
     is db.table.column so multi-index preloads are distinguishable. */
  std::string disp(db_name);
  disp.append(".").append(table_name).append(".").append(vec_column);
  m_table_name_len = disp.length();
  m_table_name = static_cast<char *>(thd->alloc(m_table_name_len + 1));
  if (m_table_name != nullptr)
    memcpy(m_table_name, disp.c_str(), m_table_name_len + 1);
  else
    m_table_name_len = 0;

  m_total_rows = stats.total_rows;
  m_estimated_bytes = stats.est_bytes;
  m_cache_used_bytes = stats.used_bytes;
  m_cache_limit_bytes = vidx::hnsw::max_cache_size;

  return false;  // success
}

void Sql_cmd_vidx_proc_preload::send_result(THD *thd, bool error) {
  DBUG_TRACE;
  if (error) return;  // error already sent by pc_execute

  Protocol *protocol = thd->get_protocol();
  if (m_proc->send_result_metadata(thd)) return;

  protocol->start_row();
  protocol->store_string(m_table_name ? m_table_name : "", m_table_name_len,
                         system_charset_info);
  protocol->store(static_cast<longlong>(m_total_rows));
  protocol->store(static_cast<longlong>(m_estimated_bytes));
  protocol->store(static_cast<longlong>(m_cache_used_bytes));
  protocol->store(static_cast<longlong>(m_cache_limit_bytes));
  if (protocol->end_row()) return;

  my_eof(thd);
}

Proc *Vidx_proc_prebuild_check::instance() {
  static Proc *proc = new Vidx_proc_prebuild_check(key_memory_package);
  return proc;
}

Sql_cmd *Vidx_proc_prebuild_check::invoke_cmd(
    THD *thd, mem_root_deque<Item *> *list) const {
  return new (thd->mem_root)
      Sql_cmd_vidx_proc_prebuild_check(thd, list, this);
}

/* Shared db/table extraction for prebuild_check's two entry points:
   check_access() gates on the same names pc_execute() later consumes, so a
   single parser keeps the privilege target and the execution target in
   lockstep.  Returns false when the two leading arguments are missing or
   empty (callers decide whether that is an error) and on strmake OOM,
   which raises ER_OUTOFMEMORY itself -- callers tell the two apart via
   thd->is_error(). */
static bool parse_prebuild_db_table_args(THD *thd,
                                         mem_root_deque<Item *> *list,
                                         const char **db_name,
                                         const char **table_name) {
  *db_name = nullptr;
  *table_name = nullptr;
  const size_t argc = (list == nullptr ? 0 : list->size());
  if (argc < 2) return false;

  auto it = list->begin();
  String db_buf, tbl_buf;
  String *db_str = (*it)->val_str(&db_buf);
  ++it;
  String *tbl_str = (*it)->val_str(&tbl_buf);
  if (db_str == nullptr || db_str->length() == 0 || tbl_str == nullptr ||
      tbl_str->length() == 0)
    return false;

  /* Copy onto thd->mem_root so the C strings stay valid past the Items. */
  *db_name = thd->strmake(db_str->ptr(), db_str->length());
  *table_name = thd->strmake(tbl_str->ptr(), tbl_str->length());
  if (*db_name == nullptr || *table_name == nullptr) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), 0);
    return false;
  }
  return true;
}

bool Sql_cmd_vidx_proc_prebuild_check::check_access(THD *thd) {
  /* Table-level SELECT on the target gates the report (it reads only
     what the user could SELECT anyway); deliberately NOT the admin-proc
     SUPER default.  Unusable name arguments fall through: arity and
     emptiness are reported by check_parameter()/pc_execute(). */
  const char *db_name = nullptr;
  const char *table_name = nullptr;
  if (!parse_prebuild_db_table_args(thd, m_list, &db_name, &table_name))
    return thd->is_error();

  /* Full-table SELECT (no column-grant combination): the scan reads the
     null bits of every VECTOR column, not one named column. */
  Table_ref table_ref(db_name, table_name, TL_READ, MDL_SHARED_READ);
  return check_table_access(thd, SELECT_ACL, &table_ref,
                            /*any_combination_of_privileges_will_do=*/false,
                            /*number=*/1, /*no_errors=*/false);
}

bool Sql_cmd_vidx_proc_prebuild_check::pc_execute(THD *thd) {
  DBUG_TRACE;

  /* prebuild_check(db, table[, m]): the framework matched one of the two
     registered signatures; re-derive which one arrived. */
  const size_t argc = (m_list == nullptr ? 0 : m_list->size());
  if (argc != 2 && argc != 3) {
    my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
             "dbms_vidx.prebuild_check, expected (db, table[, m])");
    return true;
  }

  const char *db_name = nullptr;
  const char *table_name = nullptr;
  if (!parse_prebuild_db_table_args(thd, m_list, &db_name, &table_name)) {
    if (!thd->is_error())
      my_error(ER_WRONG_PARAMETERS_TO_PROCEDURE, MYF(0),
               "dbms_vidx.prebuild_check, db/table cannot be empty");
    return true;
  }

  uint m_requested = UINT_MAX; /* UINT_MAX resolves the session default */
  if (argc == 3) {
    auto it = m_list->begin();
    ++it;
    ++it;
    const longlong m_arg = (*it)->val_int();
    /* Fold out-of-range values to 0: copy_index_option_m rejects them
       with the same range error CREATE INDEX would raise. */
    m_requested =
        (m_arg > 0 && m_arg < UINT_MAX) ? static_cast<uint>(m_arg) : 0;
  }

  /* Same resolution/validation CREATE INDEX uses: UINT_MAX takes the
     session vidx_hnsw_default_m, anything else is range-checked
     (my_error raised inside on violation). */
  uint default_m = 0;
  if (vidx::hnsw::copy_index_option_m(thd, &default_m, m_requested))
    return true;

  Table_ref table_ref(db_name, table_name, TL_READ, MDL_SHARED_READ);
  if (open_and_lock_tables(thd, &table_ref, 0)) {
    return true;  // error already set
  }

  TABLE *table = table_ref.table;
  if (table == nullptr) {
    close_thread_tables(thd);
    my_error(ER_NO_SUCH_TABLE, MYF(0), db_name, table_name);
    return true;
  }

  std::vector<vidx::hnsw::Prebuild_estimate> estimates;
  std::string reason;
  const int ret =
      vidx::hnsw::mhnsw_prebuild_check(table, default_m, &estimates, reason);

  /* Copy the report onto thd->mem_root BEFORE closing the table: the
     estimate rows point into TABLE_SHARE memory. */
  bool oom = false;
  if (ret == 0) {
    m_nrows = estimates.size();
    m_rows = static_cast<Row *>(thd->alloc(sizeof(Row) * m_nrows));
    oom = m_rows == nullptr;
    for (size_t i = 0; !oom && i < m_nrows; i++) {
      const vidx::hnsw::Prebuild_estimate &est = estimates[i];
      Row &row = m_rows[i];

      row.column_len = strlen(est.column);
      row.column = thd->strmake(est.column, row.column_len);
      row.index_name_len =
          est.index_name != nullptr ? strlen(est.index_name) : 0;
      row.index_name = est.index_name != nullptr
                           ? thd->strmake(est.index_name, row.index_name_len)
                           : nullptr;
      oom = row.column == nullptr ||
            (est.index_name != nullptr && row.index_name == nullptr);

      row.dimensions = est.dimensions;
      row.m = est.m;
      row.rows = est.rows;
      row.est_bytes = est.est_bytes;
      /* What to SET vidx_hnsw_cache_size to for a single-pass build of
         this column, rounded up to a whole MiB. */
      constexpr ulonglong mib = 1ULL << 20;
      row.suggested_bytes = (est.est_bytes + mib - 1) & ~(mib - 1);
    }
  }

  close_thread_tables(thd);

  if (ret != 0) {
    /* A KILL keeps its own error code instead of the usage wrap, same as
       finish_ddl_direct_build. */
    if (thd->killed) {
      thd->send_kill_message();
      return true;
    }
    my_error(ER_VECTOR_INDEX_USAGE, MYF(0),
             reason.empty() ? "prebuild check failed" : reason.c_str());
    return true;
  }
  if (oom) {
    my_error(ER_OUTOFMEMORY, MYF(ME_FATALERROR), 0);
    return true;
  }

  m_cache_limit_bytes = vidx::hnsw::max_cache_size;

  /* Most demanding column first: slots build sequentially, so the top
     row alone is the single-pass verdict for the whole ALTER.  The sort
     key is the RAW estimate: Suggested_cache_bytes is its monotone MiB
     rounding, so the report stays descending by Suggested too, while
     columns inside one MiB bucket still order by real cost.  The name
     tie-break keeps the order deterministic for tests. */
  std::sort(m_rows, m_rows + m_nrows, [](const Row &a, const Row &b) {
    if (a.est_bytes != b.est_bytes) return a.est_bytes > b.est_bytes;
    return strcmp(a.column, b.column) < 0;
  });

  return false;  // success
}

void Sql_cmd_vidx_proc_prebuild_check::send_result(THD *thd, bool error) {
  DBUG_TRACE;
  if (error) return;  // error already sent by pc_execute

  Protocol *protocol = thd->get_protocol();
  if (m_proc->send_result_metadata(thd)) return;

  for (size_t i = 0; i < m_nrows; i++) {
    const Row &row = m_rows[i];

    protocol->start_row();
    protocol->store_string(row.column, row.column_len, system_charset_info);
    if (row.index_name != nullptr)
      protocol->store_string(row.index_name, row.index_name_len,
                             system_charset_info);
    else
      protocol->store_null();
    protocol->store(static_cast<longlong>(row.dimensions));
    protocol->store(static_cast<longlong>(row.m));
    protocol->store(static_cast<longlong>(row.rows));
    protocol->store(static_cast<longlong>(row.est_bytes));
    protocol->store(static_cast<longlong>(m_cache_limit_bytes));

    /* windowed(~K): K bounded build windows, each bulk-persisted and
       handed over (window 2+ pays on-demand reloads). */
    char mode[48];
    size_t mode_len;
    if (row.est_bytes <= m_cache_limit_bytes) {
      mode_len = static_cast<size_t>(
          snprintf(mode, sizeof(mode), "single-pass"));
    } else {
      const ulonglong limit = std::max<ulonglong>(m_cache_limit_bytes, 1);
      const ulonglong windows = (row.est_bytes + limit - 1) / limit;
      mode_len = static_cast<size_t>(
          snprintf(mode, sizeof(mode), "windowed(~%llu)", windows));
    }
    protocol->store_string(mode, mode_len, system_charset_info);
    protocol->store(static_cast<longlong>(row.suggested_bytes));
    if (protocol->end_row()) return;
  }

  my_eof(thd);
}

}  // namespace im
