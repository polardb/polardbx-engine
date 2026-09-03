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

#ifndef SQL_VIDX_VIDX_PROC_INCLUDED
#define SQL_VIDX_VIDX_PROC_INCLUDED

#include <string>
#include "my_base.h"  // ha_rows
#include "sql/package/proc.h"

/**
  Vector-index admin procedures (dbms_vidx package).

  CALL dbms_vidx.preload(db_name, table_name, vector_column);
  CALL dbms_vidx.preload_check(db_name, table_name, vector_column);
  CALL dbms_vidx.prebuild_check(db_name, table_name[, m]);

  Two sibling procedures share one engine (mhnsw_preload) but read
  unambiguously at the call site -- the procedure name IS the intent, so
  there is no boolean "check_only" flag to remember the polarity of:

    - preload()        fully loads the HNSW auxiliary-table graph of the
                       named vector index into the in-memory shared cache so
                       a subsequent benchmark hits a warm cache (no lazy
                       per-node vector decode).
    - preload_check()  estimate + capacity-check only: reports whether the
                       graph WOULD fit within vidx_hnsw_cache_size without
                       materialising the full graph.

  Both pre-flight estimate the footprint and hard-fail if it would not fit.

  @param db_name        schema of the base table
  @param table_name     base table carrying the vector index
  @param vector_column  the indexed vector column; selects which vector
                        index to preload (lets this work once a table can
                        carry multiple vector indexes)

  Returns a single-row result set describing the outcome.  The Mode column
  is "LOAD" for preload() and "CHECK" for preload_check(); in CHECK mode
  Nodes_loaded is 0 and the full graph is not materialised (the entry point
  that opening the share needs may still be loaded).
*/

namespace im {

/* Schema: dbms_vidx */
extern LEX_CSTRING VIDX_PROC_SCHEMA;

/**
  Shared base for the two preload procedures: identical 3-arg signature
  (db, table, vector_column) and identical 5-column result set.  The two
  leaves differ only by name and by the check_only flag they thread into
  the Sql_cmd.

  No "Mode" column: the procedure name already says LOAD vs CHECK.  No
  "Nodes_loaded" column: on a successful preload it always equals
  Total_rows (the load pass re-scans the same locked graph the estimate
  pass counted), and preload_check loads nothing -- so it carries no
  information either way.  Cache_used_bytes already distinguishes the two
  at a glance (full-graph footprint vs just the entry point).
*/
class Vidx_proc_preload_base : public Proc {
  enum enum_column {
    COLUMN_INDEX = 0,
    COLUMN_TOTAL_ROWS,
    COLUMN_ESTIMATED_BYTES,
    COLUMN_CACHE_USED_BYTES,
    COLUMN_CACHE_LIMIT_BYTES,
    COLUMN_LAST
  };

 public:
  explicit Vidx_proc_preload_base(PSI_memory_key key) : Proc(key) {
    m_result_type = Result_type::RESULT_SET;

    m_parameters.push_back(MYSQL_TYPE_VARCHAR);  // db
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);  // table
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);  // column

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Index"), 512},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Total_rows"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Estimated_bytes"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Cache_used_bytes"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Cache_limit_bytes"), 0},
    };
    for (size_t i = 0; i < COLUMN_LAST; i++) m_columns.push_back(elements[i]);
  }

  const std::string qname() const override {
    std::stringstream ss;
    ss << VIDX_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }
};

/**
  Proc of dbms_vidx.preload (full load).
*/
class Vidx_proc_preload : public Vidx_proc_preload_base {
 public:
  explicit Vidx_proc_preload(PSI_memory_key key)
      : Vidx_proc_preload_base(key) {}

  static Proc *instance();

  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override { return std::string("preload"); }
};

/**
  Proc of dbms_vidx.preload_check (estimate + capacity check only).
*/
class Vidx_proc_preload_check : public Vidx_proc_preload_base {
 public:
  explicit Vidx_proc_preload_check(PSI_memory_key key)
      : Vidx_proc_preload_base(key) {}

  static Proc *instance();

  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("preload_check");
  }
};

/**
  Cmd of dbms_vidx.preload / preload_check (admin proc: requires SUPER).
  @p check_only selects LOAD vs CHECK behaviour; set by the owning Proc.
*/
class Sql_cmd_vidx_proc_preload : public Sql_cmd_admin_proc {
 public:
  Sql_cmd_vidx_proc_preload(THD *thd, mem_root_deque<Item *> *list,
                            const Proc *proc, bool check_only)
      : Sql_cmd_admin_proc(thd, list, proc), m_check_only(check_only) {}

  bool pc_execute(THD *thd) override;
  void send_result(THD *thd, bool error) override;

 private:
  const bool m_check_only;

  /* Result row, filled by pc_execute, consumed by send_result. */
  char *m_table_name{nullptr};
  size_t m_table_name_len{0};
  ha_rows m_total_rows{0};
  ulonglong m_estimated_bytes{0};
  ulonglong m_cache_used_bytes{0};
  ulonglong m_cache_limit_bytes{0};
};

/**
  Proc of dbms_vidx.prebuild_check: pre-DDL sizing report for the vector
  indexes a COPY ALTER on the table would (re)build.

  One result row per VECTOR column, ordered by Suggested_cache_bytes
  descending -- index slots build sequentially and each slot's private
  ctx is freed before the next starts, so the TOP row alone decides
  whether the whole ALTER builds single-pass.  Indexed columns report
  their index's real M; unindexed columns (the index being planned) use
  the optional m argument, defaulting to the session vidx_hnsw_default_m
  exactly like CREATE INDEX without M=.

  Report-only by design: exceeding vidx_hnsw_cache_size does not fail a
  build, it degrades it to windowed construction, so the verdict is the
  Build_mode column plus a suggested cache value, never an error.
*/
class Vidx_proc_prebuild_check : public Proc {
  enum enum_column {
    COLUMN_COLUMN = 0,
    COLUMN_INDEX_NAME,
    COLUMN_DIMENSIONS,
    COLUMN_M,
    COLUMN_ROWS,
    COLUMN_ESTIMATED_BYTES,
    COLUMN_CACHE_LIMIT_BYTES,
    COLUMN_BUILD_MODE,
    COLUMN_SUGGESTED_CACHE_BYTES,
    COLUMN_LAST
  };

 public:
  explicit Vidx_proc_prebuild_check(PSI_memory_key key)
      : Proc(key), m_parameters_short(key) {
    m_result_type = Result_type::RESULT_SET;

    /* Default (longest) signature (db, table, m): check_parameter()
       type-checks the actual arguments against THIS list, so it must be
       the superset; the 2-arg form is the registered short variant. */
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);   // db
    m_parameters.push_back(MYSQL_TYPE_VARCHAR);   // table
    m_parameters.push_back(MYSQL_TYPE_LONGLONG);  // m (optional)
    m_parameters_short.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters_short.push_back(MYSQL_TYPE_VARCHAR);
    m_parameters_list.push_back(&m_parameters);
    m_parameters_list.push_back(&m_parameters_short);

    Column_element elements[COLUMN_LAST] = {
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Column"), 192},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Index_name"), 192},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Dimensions"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("M"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Rows"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Estimated_bytes"), 0},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Cache_limit_bytes"), 0},
        {MYSQL_TYPE_VARCHAR, C_STRING_WITH_LEN("Build_mode"), 64},
        {MYSQL_TYPE_LONGLONG, C_STRING_WITH_LEN("Suggested_cache_bytes"), 0},
    };
    for (size_t i = 0; i < COLUMN_LAST; i++) m_columns.push_back(elements[i]);
  }

  static Proc *instance();

  Sql_cmd *invoke_cmd(THD *thd, mem_root_deque<Item *> *list) const override;

  const std::string str() const override {
    return std::string("prebuild_check");
  }

  const std::string qname() const override {
    std::stringstream ss;
    ss << VIDX_PROC_SCHEMA.str << "." << str();
    return ss.str();
  }

 protected:
  Parameters m_parameters_short; /* (db, table): m falls back to session */
};

/**
  Cmd of dbms_vidx.prebuild_check.  Read-only sizing report; access is
  table-level SELECT on the target (check_access override), not SUPER:
  the proc reads nothing the user could not SELECT anyway.
*/
class Sql_cmd_vidx_proc_prebuild_check : public Sql_cmd_admin_proc {
 public:
  Sql_cmd_vidx_proc_prebuild_check(THD *thd, mem_root_deque<Item *> *list,
                                   const Proc *proc)
      : Sql_cmd_admin_proc(thd, list, proc) {}

  bool check_access(THD *thd) override;
  bool pc_execute(THD *thd) override;
  void send_result(THD *thd, bool error) override;

 private:
  /* One report row; mem_root lifetime (Sql_cmd is never destructed, so
     no heap-owning members). */
  struct Row {
    const char *column;     /* VECTOR column name */
    size_t column_len;
    const char *index_name; /* existing vector index; nullptr renders as
                               SQL NULL (column not indexed yet) */
    size_t index_name_len;
    ulonglong dimensions;   /* VECTOR(N) of the column */
    ulonglong m;            /* the index's real M, or the resolved default */
    ulonglong rows;         /* exact non-NULL vector rows (scanned) */
    ulonglong est_bytes;    /* raw expected build footprint; the sort key */
    /* est_bytes rounded up to a whole MiB: the value to SET
       vidx_hnsw_cache_size to for a single-pass build of this column. */
    ulonglong suggested_bytes;
  };

  /* Report rows, raw-estimate descending (== Suggested descending). */
  Row *m_rows{nullptr};
  size_t m_nrows{0};
  ulonglong m_cache_limit_bytes{0};
};

}  // namespace im

#endif  // SQL_VIDX_VIDX_PROC_INCLUDED
