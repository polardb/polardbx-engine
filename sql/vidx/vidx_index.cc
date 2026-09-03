/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

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

#include "mysql/plugin.h"                    // MYSQL_DAEMON_PLUGIN, ...
#include "mysql_com.h"                       // MYSQL_ERRMSG_SIZE
#include <memory>                            // std::unique_ptr
#include <vector>                            // std::vector
#include "scope_guard.h"                     // Scope_guard
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/dd_table.h"                 // dd::table_exists, ...
#include "sql/dd/dictionary.h"               // dd::release_mdl
#include "sql/dd/impl/properties_impl.h"   // dd::Properties_impl
#include "sql/dd/string_type.h"              // dd::String_type
#include "sql/dd/types/index.h"              // dd::Index
#include "sql/dd/types/index_element.h"      // dd::Index_element
#include "sql/dd_table_share.h"              // open_table_def
#include "sql/field.h"                       // Field
#include "sql/handler.h"                     // setup_transaction_participant
#include "sql/item_strfunc.h"                // vidx::Item_func_vec_distance
#include "sql/join_optimizer/access_path.h"  // AccessPath
#include "vidx/vidx_log.h"                    // vidx::log_slave_dml_err
#include "sql/mysqld.h"                      // reg_ext_length
#include "sql/sql_base.h"                    // EXTRA_RECORD
#include "sql/sql_class.h"                   // THD
#include "sql/sql_plugin_ref.h"              // st_plugin_int
#include "sql/sql_show.h"                    // add_status_vars
#include "sql/sql_select.h"                  // JOIN_TAB
#include "sql/sql_table.h"                   // mysql_rename_table
#include "storage/innobase/include/ut0dbg.h" // ut_a

#include "vidx/vidx_aux_dml.h"
#include "vidx/vidx_hnsw.h"
#include "vidx/vidx_index.h"
#include "vidx/vidx_stats.h"   // vidx_status_variables

namespace vidx {
/* -------------------- Macros -------------------- */
#define VIDX_NAME "vidx_%lu_%u"
static constexpr uint VIDX_NAME_LEN = 5 + 20 + 1 + 10 + 1;  /* vidx_ + %lu + _ + %u + \0 */
static constexpr uint vidx_num = 0;
static constexpr uint VIDX_DDL_THREADS_DEF = 4;
static constexpr uint VIDX_DDL_THREADS_MAX = 64;

#define TL_FIRST_WRITE TL_WRITE_ALLOW_WRITE

static constexpr uint32_t SCAN_COST = 4;

/* -------------------- External Vars Definition -------------------- */
st_plugin_int *vidx_plugin;
bool feature_disabled = false;
bool use_fast_float = true;

/* -------------------- Static Vars Definition -------------------- */
static TYPELIB distances = {array_elements(distance_names) - 1, "",
                            distance_names, nullptr};

static MYSQL_SYSVAR_BOOL(disabled, feature_disabled, PLUGIN_VAR_RQCMDARG,
                         "Whether to enable vector index feature", nullptr,
                         nullptr, true);

/* Sysvar registration for the force_non_indep escape hatch.  Storage
   is defined in sql/vidx/vidx_aux_dml.cc (vidx::aux_dml namespace);
   sysvar registration stays here so all vidx plugin sysvars are
   declared in one place.

   READ-ONLY (config-file + restart): this is an emergency revert
   knob, not a runtime tuning lever.  Making it READONLY removes the
   only race window where two vidx primitives (e.g. mhnsw_invalidate
   then mhnsw_insert in the same hlindexes_on_update) could disagree
   on the routing decision and leave a half-state orphan.  With
   READONLY, every primitive can re-call need_independent_trx() and
   be guaranteed to see the same value through the whole user statement
   — no need to plumb the decision through call arguments. */
static MYSQL_SYSVAR_BOOL(
    force_non_indep, vidx::aux_dml::force_non_indep,
    PLUGIN_VAR_OPCMDARG | PLUGIN_VAR_READONLY,
    "Emergency escape hatch: when ON, force vector-index DML onto the "
    "legacy non-independent path (aux writes commit with the user "
    "trx).  Read-only — set in my.cnf and restart.  See "
    "vidx::aux_dml::need_independent_trx() for full routing semantics.",
    nullptr, nullptr, false);

static MYSQL_THDVAR_ENUM(default_distance, PLUGIN_VAR_RQCMDARG,
                         "Distance function to build the vector index for",
                         nullptr, nullptr, EUCLIDEAN, &distances);

static MYSQL_THDVAR_UINT(
    hnsw_default_m, PLUGIN_VAR_RQCMDARG,
    "Larger values mean slower SELECTs and INSERTs, larger index size "
    "and higher memory consumption but more accurate results",
    nullptr, nullptr, hnsw::M_DEF, hnsw::M_MIN, hnsw::M_MAX, 1);

static MYSQL_THDVAR_UINT(
    hnsw_default_ef_construction, PLUGIN_VAR_RQCMDARG,
    "ef_construction for HNSW graph building. Larger values produce "
    "higher quality graphs but slower index creation. Only affects "
    "CREATE INDEX; runtime INSERT uses the persisted value.",
    nullptr, nullptr, hnsw::EF_CONSTRUCTION_DEF,
    hnsw::EF_CONSTRUCTION_MIN, hnsw::EF_CONSTRUCTION_MAX, 1);

static MYSQL_THDVAR_UINT(
    hnsw_ef_search, PLUGIN_VAR_RQCMDARG,
    "Larger values mean slower SELECTs but more accurate results. "
    "Defines the minimal number of result candidates to look for in the "
    "vector index for ORDER BY ... LIMIT N queries. The search will never "
    "search for less rows than that, even if LIMIT is smaller",
    nullptr, nullptr, 20, 1, hnsw::max_ef, 1);

static MYSQL_THDVAR_UINT(
    ddl_threads, PLUGIN_VAR_RQCMDARG,
    "Number of worker threads for every in-memory window of a synchronous "
    "vector-index COPY DDL build. 1 selects the serial path",
    nullptr, nullptr, VIDX_DDL_THREADS_DEF, 1, VIDX_DDL_THREADS_MAX, 1);

static MYSQL_SYSVAR_ULONGLONG(hnsw_cache_size, hnsw::max_cache_size,
                              PLUGIN_VAR_RQCMDARG,
                              "Upper limit for one HNSW vector index runtime "
                              "cache; synchronous COPY DDL reuses it as a "
                              "soft window hint checked between micro-batches",
                              nullptr, nullptr, hnsw::DEF_CACHE_SIZE,
                              1024 * 1024, ULONG_LONG_MAX, 1);

static MYSQL_SYSVAR_ULONG(
    hnsw_lock_timeout_ms, hnsw::lock_timeout_ms, PLUGIN_VAR_RQCMDARG,
    "Lock wait timeout in milliseconds for HNSW graph table operations "
    "on replica apply threads. Caps how long a write blocks on a "
    "concurrent writer (e.g. another attachable inner-trx) before being "
    "skipped, so a single hot graph row cannot stall apply progress. "
    "Effective granularity is 1 second (values below 1000 are rounded "
    "up to 1 s).",
    nullptr, nullptr, hnsw::DEF_LOCK_TIMEOUT_MS, 1,
    hnsw::MAX_LOCK_TIMEOUT_MS, 1);

static MYSQL_SYSVAR_BOOL(
    use_fast_float, use_fast_float, PLUGIN_VAR_RQCMDARG,
    "When ON (default), use the fast_float library for VEC_FROMTEXT() "
    "float parsing (~5x faster than strtof). When OFF, fall back to the "
    "standard glibc strtof. Useful for performance comparison benchmarks.",
    nullptr, nullptr, true);

static SYS_VAR *sys_vars[] = {
    MYSQL_SYSVAR(disabled),
    MYSQL_SYSVAR(force_non_indep),
    MYSQL_SYSVAR(default_distance),
    MYSQL_SYSVAR(hnsw_default_m),
    MYSQL_SYSVAR(hnsw_default_ef_construction),
    MYSQL_SYSVAR(hnsw_ef_search),
    MYSQL_SYSVAR(ddl_threads),
    MYSQL_SYSVAR(hnsw_cache_size),
    MYSQL_SYSVAR(hnsw_lock_timeout_ms),
    MYSQL_SYSVAR(use_fast_float),
    nullptr};

/* vidx_status_variables[] (SHOW GLOBAL STATUS LIKE 'Vidx_%') and the
   three show_* callbacks moved to sql/vidx/vidx_stats.cc; registered
   below in plugin_init() via the extern array. */

static struct st_mysql_storage_engine daemon = {MYSQL_DAEMON_INTERFACE_VERSION};

/* -------------------- Static Functions Definition -------------------- */
static int plugin_init(void *p) {
  vidx_plugin = (st_plugin_int *)p;
  vidx_plugin->data = hnsw::trx_handler;

  if (setup_transaction_participant(vidx_plugin)) return 1;

  hnsw::init_ddl_build_instrumentation();

  // Register SHOW STATUS variables
  if (add_status_vars(vidx_status_variables)) {
    return 1;
  }

  return 0;
}

static int plugin_deinit(void *) { return 0; }

/* Get the string value in the dd table's option "__hlindexes__".
@param[in]  dd_table   the dd table
@param[out] hlindexes      the value of the option "__hlindexes__" */
static inline void dd_table_get_hlindexes(const dd::Table *dd_table,
                                          dd::String_type *hlindexes) {
  assert(hlindexes != nullptr);
  assert(dd_table_has_hlindexes(dd_table));

  dd_table->options().get("__hlindexes__", hlindexes);
  assert(!hlindexes->empty());
}

/* Set the string value in the dd table's option "__hlindexes__".
@param[in]  dd_table   the dd table
@param[in]  hlindexes      the value of the option "__hlindexes__" */
static inline void dd_table_set_hlindexes(dd::Table *dd_table,
                                          dd::String_type hlindexes) {
  dd_table->options().set("__hlindexes__", hlindexes);
}

/* Length of the base-table row reference (tref) that the auxiliary table's
   FIELD_TREF column must be able to hold.

   This mirrors ha_innobase::open() exactly:

     ref_length = table->key_info[table_share->primary_key].key_length;

   The primary key must be identified by TABLE_SHARE::primary_key, never by
   the key name: MySQL promotes a qualifying UNIQUE NOT NULL key to be the
   semantic primary key while keeping its original name (see prepare_share()
   in sql/dd_table_share.cc).  A name-based check would classify such a table
   as "no primary key" and size tref as a 6-byte hidden row id, two bytes
   short of the row reference InnoDB actually produces at run time.

   Note that table->file->ref_length cannot be used here: on the CREATE path
   ha_create_table() builds the TABLE with db_stat=0, so the handler is never
   opened and ref_length still holds its constructor default sizeof(my_off_t).

   @param[in]  table  the base table
   @return the row reference length in bytes */
static inline uint get_tref_len(TABLE *table) {
  if (table->s->is_missing_primary_key()) return DATA_ROW_ID_LEN;

  return table->key_info[table->s->primary_key].key_length;
}

int check_tref_len(const uint tref_len, const uint field_tref_len) {
  if (likely(tref_len <= field_tref_len)) return 0;

  char msg[MYSQL_ERRMSG_SIZE];
  snprintf(msg, sizeof(msg),
           "auxiliary table column `tref` is %u bytes but the table's row "
           "reference is %u bytes; the vector index was built with an "
           "incorrect reference length and must be rebuilt (ALTER TABLE "
           "... DROP INDEX, then ADD VECTOR INDEX; or OPTIMIZE TABLE on the "
           "base table)",
           field_tref_len, tref_len);
  my_error(ER_VECTOR_INDEX_USAGE, MYF(0), msg);
  return HA_ERR_GENERIC;
}

static const char *build_name(THD *thd, const uint64_t base, const uint num,
                              std::string &error_message) {
  /* The length of vector index table name should be shorter than 64 because
  the `name` of `tables` is varchar(64). See also Tables::Tables() in
  sql/dd/impl/tables/tables.cc */
  static_assert(VIDX_NAME_LEN <= 64);

  char *name = reinterpret_cast<char *>(thd->mem_root->Alloc(VIDX_NAME_LEN));

  if (name == nullptr) {
    error_message = "Failed to allocate memory for table name.";
  } else {
    snprintf(name, VIDX_NAME_LEN, VIDX_NAME, base, num);
  }

  return reinterpret_cast<const char *>(name);
}

static bool request_mdl_lock(THD *thd, const char *db_name,
                             const char *table_name, enum_mdl_type mdl_type,
                             enum_mdl_duration lock_duration,
                             std::string &error_message,
                             MDL_request *mdl_request = nullptr) {
  if (mdl_request == nullptr) {
    mdl_request = new (thd->mem_root) MDL_request;

    if (mdl_request == nullptr) {
      error_message = "Failed to allocate memory for mdl_request.";
      return true;
    }
  }

  MDL_REQUEST_INIT(mdl_request, MDL_key::TABLE, db_name, table_name, mdl_type,
                   lock_duration);
  if (thd->mdl_context.acquire_lock(mdl_request,
                                    thd->variables.lock_wait_timeout)) {
    error_message = "Failed to acquire DML lock.";
    return true;
  }

  return false;
}

static const dd::Table *open_hlindex_dd(THD *thd, const char *hlindex_name,
                                        const char *db_name,
                                        std::string &error_message) {
  /* Acquire the dd table.

     CONTRACT: the caller must hold a Dictionary_client::Auto_releaser
     covering the whole lifetime of the returned pointer.  Creating a
     releaser here would release the object on return, after which the
     shared DD cache may evict and free it (MDL does not pin cache
     entries) — leaving the caller with a dangling pointer. */
  const dd::Table *hlindex_dd = nullptr;

  if (thd->dd_client()->acquire(db_name, hlindex_name, &hlindex_dd)) {
    error_message = "Failed to acquire vector dd table.";
    return nullptr;
  }

  if (hlindex_dd == nullptr || !dd_table_is_hlindex(hlindex_dd)) {
    assert(false);

    error_message = "Can't find vector table in dd.";
    return nullptr;
  }

  assert(hlindex_dd->hidden() == dd::Abstract_table::HT_HIDDEN_HLINDEX ||
         hlindex_dd->hidden() == dd::Abstract_table::HT_HIDDEN_DDL);
  assert(dd_table_is_hlindex(hlindex_dd));

  return hlindex_dd;
}

/* -------------------- External Functions Definition -------------------- */
bool check_vector_ddl(THD *thd [[maybe_unused]], Alter_info *alter_info,
                      KEY *key_info, const uint key_count, TABLE *table) {
  /* Count vector keys in old table and new key array. */
  const uint old_nvec = table->s->hlindexes();
  uint new_nvec = 0;
  for (uint i = 0; i < key_count; i++) {
    if (key_info[i].flags & HA_VECTOR) new_nvec++;
  }

  if (key_count + new_nvec > MAX_INDEXES) {
    my_error(ER_TOO_MANY_KEYS, MYF(0), MAX_INDEXES);
    return true;
  }

  if (old_nvec == 0 && new_nvec == 0) {
    /* No vector index involved, nothing to check. */
    return false;
  }

  /*
    Vector index on temporary tables is not supported.  CREATE TABLE checks
    HA_LEX_CREATE_TMP_TABLE in sql_table.cc, but ALTER TABLE bypasses that
    path — guard it here.
  */
  if (table->s->tmp_table != NO_TMP_TABLE) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "the VECTOR index in partitioned or temp tables");
    return true;
  }

  /* A vector-index DDL (add / drop / rename) must be the sole operation
     in the ALTER TABLE — no mixing with column changes, other indexes, etc. */
  const auto flags = alter_info->flags;
  bool mixed = false;

  if (new_nvec > old_nvec)
    mixed = (flags != Alter_info::ALTER_ADD_INDEX);
  else if (new_nvec < old_nvec)
    mixed = (flags != Alter_info::ALTER_DROP_INDEX);
  else if (old_nvec > 0 && (flags & Alter_info::ALTER_RENAME_INDEX))
    mixed = (flags != Alter_info::ALTER_RENAME_INDEX ||
             alter_info->alter_rename_key_list.size() != 1);

  if (mixed) {
    my_error(ER_NOT_SUPPORTED_YET, MYF(0),
             "perform other operations while alter a vector index");
    return true;
  }

  return false;
}

/* Return true if the vector field in the rec is NULL.
Otherwise, return false. */
static bool check_vector_is_null(TABLE *table, const uchar *rec, KEY *vec_key) {
  const ptrdiff_t offset = rec - table->record[0];
  Field *field = vec_key->key_part->field;

  assert(field->is_vector());

  return field->is_real_null(offset);
}

namespace hnsw {
uint get_ddl_threads(const THD *thd) {
  /* THDVAR() requires a non-const THD*; this is a read-only snapshot used
     for the duration of one vector-index slot build. */
  return THDVAR(const_cast<THD *>(thd), ddl_threads);
}

uint get_ef_search(const THD *thd) {
  /* THDVAR() requires a non-const THD* (server framework limitation);
     reading a session variable does not modify the THD. */
  return THDVAR(const_cast<THD *>(thd), hnsw_ef_search);
}

uint index_options_print(const uint distance, const uint m,
                         const uint ef_construction, char *buf, uint buf_len) {
  assert(validate_index_option_distance(distance));
  assert(validate_index_option_m(m));
  assert(validate_index_option_ef_construction(ef_construction));

  uint len = snprintf(buf, buf_len, " M=%u DISTANCE=%s EF_CONSTRUCTION=%u", m,
                      distance_names[distance], ef_construction);

  if (len >= buf_len) {
    return buf_len - 1;
  }

  return len;
}

bool copy_index_option_m(THD *thd, uint *to, const uint from) {
  if (from == UINT_MAX) {
    /* distance is not set. */
    *to = THDVAR(thd, hnsw_default_m);
    return false;
  }

  if (!validate_index_option_m(from)) {
    my_printf_error(ER_VECTOR_INDEX_USAGE,
                    "M=%u is out of valid range [%u, %u].", MYF(0), from,
                    M_MIN, M_MAX);
    return true;
  }

  *to = from;
  return false;
}

bool copy_index_option_ef_construction(THD *thd, uint *to, const uint from) {
  if (from == UINT_MAX) {
    *to = THDVAR(thd, hnsw_default_ef_construction);
    return false;
  }
  if (!validate_index_option_ef_construction(from)) {
    my_printf_error(ER_VECTOR_INDEX_USAGE,
                    "EF_CONSTRUCTION=%u is out of valid range [%u, %u].",
                    MYF(0), from, EF_CONSTRUCTION_MIN, EF_CONSTRUCTION_MAX);
    return true;
  }
  *to = from;
  return false;
}
}  // namespace hnsw

bool copy_index_option_distance(THD *thd, uint *to, const uint from) {
  if (from == UINT_MAX) {
    /* distance is not set. */
    *to = THDVAR(thd, default_distance);
    return false;
  }

  assert(validate_index_option_distance(from));

  *to = from;
  return false;
}

bool create_table(THD *thd, KEY *key, dd::Table *dd_table, TABLE *table,
                  const char *db_name, const uint64_t old_table_id) {
  assert(key_is_vector(key));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;

  /* 1. Build table name and path using this key's stable index id. */
  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), key->vector_index_id, error_message);
  if (hlindex_name == nullptr) {
  error_end:
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0),
             thd_sql_command(thd) == SQLCOM_TRUNCATE ? "Truncate" : "Create",
             key->name, db_name, dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  char path[FN_REFLEN + 1];
  bool was_truncated;
  build_table_filename(path, sizeof(path) - 1 - reg_ext_length, db_name,
                       hlindex_name, "", 0, &was_truncated);
  // Check truncation, will lead to overflow when adding extension
  if (was_truncated) {
    my_error(ER_IDENT_CAUSES_TOO_LONG_PATH, MYF(0), sizeof(path) - 1, path);
    return true;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););
  DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
    error_message = "debug failed before vidx ddl.";
    goto error_end;
  });

  /* 2. Request MDL X lock */
  if (request_mdl_lock(thd, db_name, hlindex_name, MDL_EXCLUSIVE,
                       MDL_TRANSACTION, error_message)) {
    goto error_end;
  }

  /* 3. Check if the hlindex name exists. */
  bool exists;
  if (dd::table_exists(thd->dd_client(), db_name, hlindex_name, &exists)) {
    return true;  // Error is already reported.
  }

  if (!exists &&
      ha_check_if_table_exists(thd, db_name, hlindex_name, &exists)) {
    /* Table doesn't exist. Check if some engine can provide it. */
    my_printf_error(ER_OUT_OF_RESOURCES,
                    "Failed to open '%-.64s', error while "
                    "unpacking from engine",
                    MYF(0), hlindex_name);
    return true;
  }

  if (exists) {
    error_message = "Vector table name exists.";
    goto error_end;
  }

  if (old_table_id == dd::INVALID_OBJECT_ID) {
    /* CREATE TABLE */
    /*
     * MDEV-36188: validate primary key (tref) length <= 256 bytes.
     *
     * tref is the varbinary column of the IDX_TREF index in the auxiliary
     * table, storing the main table's primary key value. If too long, it
     * exceeds InnoDB's index column length limit, triggering an assert.
     *
     * MariaDB checks in mhnsw_hlindex_table_def() when generating the
     * auxiliary table DDL; MySQL 8.0 checks before create_dd_table():
     *   - gives a clear ER_TOO_LONG_KEY error instead of hitting an
     *     InnoDB internal assert
     *   - reuses get_tref_len result, avoiding redundant calls
     *
     * 256 bytes is an empirical upper bound (< half of InnoDB's max
     * inline key length); MariaDB uses the same limit.
     */
    static constexpr uint max_tref_length = 256;
    const uint tref_len = get_tref_len(table);
    if (tref_len > max_tref_length) {
      my_printf_error(ER_TOO_LONG_KEY,
                      "Primary key is too long for vector index, "
                      "max length is %d bytes",
                      MYF(0), max_tref_length);
      return true;
    }

    /* 4. Create dd table and store it */
    std::unique_ptr<dd::Table> hlindex_dd_ptr = hnsw::create_dd_table(
        thd, hlindex_name, key, dd_table, table, db_name, tref_len);

    if (!hlindex_dd_ptr) {
      return true;  // Error is already reported.
    }

    if (thd->dd_client()->store(hlindex_dd_ptr.get())) {
      error_message = "Failed to store vector dd table.";
      goto error_end;
    }
  } else {
    /* TRUNCATE TABLE */
    assert(old_table_id != dd_table->se_private_id());

    /* 4. Rename old table. */
    dd::cache::Dictionary_client::Auto_releaser releaser(thd->dd_client());
    const dd::Schema *schema = nullptr;
    if (thd->dd_client()->acquire(db_name, &schema) || schema == nullptr) {
      error_message = "Failed to acquire schema.";
      goto error_end;
    }

    const char *old_name =
        build_name(thd, old_table_id, key->vector_index_id, error_message);
    if (old_name == nullptr ||
        request_mdl_lock(thd, db_name, old_name, MDL_EXCLUSIVE, MDL_TRANSACTION,
                         error_message)) {
      goto error_end;
    }

    if (mysql_rename_table(thd, table->file->ht, db_name, old_name, db_name,
                           old_name, *schema, db_name, hlindex_name,
                           NO_DD_COMMIT | VIDX_RENAME)) {
      return true;
    }
  }

  /* 5. Get dd table. */
  dd::Table *hlindex_dd = nullptr;

  if (thd->dd_client()->acquire_for_modification(db_name, hlindex_name,
                                                 &hlindex_dd) ||
      hlindex_dd == nullptr) {
    error_message = "Failed to acquire_for_modification vector dd table.";
    goto error_end;
  }

  if (!dd_table_is_hlindex(hlindex_dd)) {
    error_message = "hlindex's name may be used by other tables.";
    goto error_end;
  }

  /* 6. Create table */
  HA_CREATE_INFO unused;
  if (ha_create_table(thd, path, db_name, hlindex_name, &unused, true, false,
                      hlindex_dd) != 0) {
    return true;  // Error is already reported.
  }

  /* 7. Update dd table */
  if (thd->dd_client()->update<dd::Table>(hlindex_dd)) {
    error_message = "Failed to update vector dd table.";
    goto error_end;
  }

  /* 8. Set __hlindexes__ to this key name as intermediate state.
     The caller (ha_create_table) will call finalize_hlindex_meta() after
     all hlindex tables are created, which overwrites with the final format
     (bare string for single-index, structured for multi-index). */
  vidx::dd_table_set_hlindexes(dd_table, key->name);

  return false;
}

bool delete_table(THD *thd, const dd::Table *dd_table, const char *db_name) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;
  const char *hlindex_name = nullptr;

  /* 1. Decode all vector index entries (legacy single-index falls back to
     one entry with id 0). */
  std::vector<hlindex_meta_entry> entries;
  if (decode_hlindex_meta(dd_table, &entries, nullptr)) {
    error_message = "Failed to decode vector index metadata.";
    goto error_end;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););

  /* 2. Drop each hlindex auxiliary table. */
  for (const auto &entry : entries) {
    /* 2a. Build name and path. */
    hlindex_name =
        build_name(thd, dd_table->se_private_id(), entry.id, error_message);
    if (hlindex_name == nullptr) goto error_end;

    DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
      error_message = "debug failed before vidx ddl.";
      goto error_end;
    });

    char path[FN_REFLEN + 1];
    bool was_truncated;
    build_table_filename(path, sizeof(path) - 1 - reg_ext_length, db_name,
                         hlindex_name, "", 0, &was_truncated);
    if (was_truncated) {
      error_message = "Path too long after adding extension.";
      goto error_end;
    }

    if (request_mdl_lock(thd, db_name, hlindex_name, MDL_EXCLUSIVE,
                         MDL_TRANSACTION, error_message)) {
      goto error_end;
    }

    /* 2b. Flush all open TABLE instances for the hlindex table from the table
       cache. This is necessary because when show_vidx_hlindex_tables is
       enabled users can SELECT directly from the hlindex table, creating
       standalone TABLE instances with open InnoDB handlers (n_ref_count=1).
       Without this flush, row_drop_table_for_mysql() would find n_ref_count>0
       and abort. MDL_X above ensures no new instances can be opened. */
    tdc_remove_table(thd, TDC_RT_REMOVE_ALL, db_name, hlindex_name, false);

    /* Keep the acquired dd::Table alive until ha_delete_table /
       dd::drop_table below are done (see open_hlindex_dd CONTRACT). */
    dd::cache::Dictionary_client::Auto_releaser hlindex_releaser(
        thd->dd_client());
    const dd::Table *hlindex_dd =
        open_hlindex_dd(thd, hlindex_name, db_name, error_message);
    if (hlindex_dd == nullptr) goto error_end;

    /* 2c. Drop table and dd entry. */
    handlerton *hton{nullptr};
    if (dd::table_storage_engine(thd, hlindex_dd, &hton)) {
      error_message = "Failed to resolve storage engine.";
      goto error_end;
    }

    if (ha_delete_table(thd, hton, path, db_name, hlindex_name, hlindex_dd,
                        false)) {
      error_message = "Failed to delete hlindex data file.";
      goto error_end;
    }

    if (dd::drop_table(thd, db_name, hlindex_name, *hlindex_dd)) {
      error_message = "Failed to drop hlindex dd entry.";
      goto error_end;
    }
  }

  /* 3. Remove the "__hlindexes__" option in base dd table. The base dd
     table may be used later in acquire_uncached_table() to build the base
     table share. */
  (const_cast<dd::Table *>(dd_table))->options().remove("__hlindexes__");

  return false;

error_end:
  {
    dd::String_type key_name;
    dd_table_get_hlindexes(dd_table, &key_name);
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Drop", key_name.c_str(), db_name,
             dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }
}

bool rename_table(THD *thd, dd::Table *dd_table, handlerton *base,
                  const dd::Schema &new_schema, const char *old_db,
                  const char *new_db, uint flags) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(dd_table->engine() == "InnoDB");

  std::string error_message;
  const char *hlindex_name = nullptr;

  /* 1. Decode all vector index entries (legacy single-index falls back to
     one entry with id 0). */
  std::vector<hlindex_meta_entry> entries;
  if (decode_hlindex_meta(dd_table, &entries, nullptr)) {
    error_message = "Failed to decode vector index metadata.";
    goto error_end;
  }

  DBUG_EXECUTE_IF("crash_before_vidx_ddl", DBUG_SUICIDE(););

  /* 2. Rename each hlindex auxiliary table. */
  for (const auto &entry : entries) {
    /* 2a. Build table name. */
    hlindex_name =
        build_name(thd, dd_table->se_private_id(), entry.id, error_message);
    if (hlindex_name == nullptr) goto error_end;

    DBUG_EXECUTE_IF("failed_before_vidx_ddl", {
      error_message = "debug failed before vidx ddl.";
      goto error_end;
    });

    /* 2b. Request MDL X lock on both old and new schema. */
    if (request_mdl_lock(thd, new_db, hlindex_name, MDL_EXCLUSIVE,
                         MDL_TRANSACTION, error_message) ||
        request_mdl_lock(thd, old_db, hlindex_name, MDL_EXCLUSIVE,
                         MDL_TRANSACTION, error_message)) {
      goto error_end;
    }

    /* 2c. Rename table. */
    if (mysql_rename_table(thd, base, old_db, hlindex_name, old_db,
                           hlindex_name, new_schema, new_db, hlindex_name,
                           flags | VIDX_RENAME)) {
      error_message = "Failed to rename hlindex table.";
      goto error_end;
    }
  }

  return false;

error_end:
  {
    dd::String_type key_name;
    dd_table_get_hlindexes(dd_table, &key_name);
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Rename", key_name.c_str(), new_db,
             dd_table->name().c_str(),
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }
}

bool build_hlindex_key(THD *thd, TABLE_SHARE *table_share,
                       const dd::Table *dd_table, const uint nr) {
  assert(dd_table_has_hlindexes(dd_table));
  assert(nr >= table_share->keys && nr < table_share->total_keys);

  std::string error_message;

  /* 1a. Resolve this vector index's stable index id and name from the
  structured metadata (the slot-th entry).  Legacy single-index tables
  decode to a single entry with id 0, preserving the old vidx_<id>_0 name. */
  const uint slot = nr - table_share->keys;
  std::vector<hlindex_meta_entry> entries;
  if (decode_hlindex_meta(dd_table, &entries, nullptr) ||
      slot >= entries.size()) {
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Show", "?", table_share->db.str,
             table_share->table_name.str, "?",
             "Failed to decode vector index metadata.");
    return true;
  }
  const dd::String_type key_name = entries[slot].name;
  const uint index_id = entries[slot].id;
  if (nr >= MAX_INDEXES) {
    my_error(ER_TOO_MANY_KEYS, MYF(0), MAX_INDEXES);
    return true;
  }

  /* Enriched format: creation context, hlindex DD table does not exist yet.
     Read vector params directly from __hlindexes__ and skip open_hlindex_dd. */
  if (entries[slot].enriched) {
    fill_key_from_enriched(table_share, nr, entries[slot], key_name);
    return false;
  }

  /* 1b. Build name. */
  const char *hlindex_name =
      build_name(thd, dd_table->se_private_id(), index_id, error_message);
  if (hlindex_name == nullptr) {
  error_end:
    my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Show", key_name.c_str(),
             table_share->db.str, table_share->table_name.str,
             (hlindex_name == nullptr ? "?" : hlindex_name),
             error_message.c_str());
    return true;
  }

  /* 2. Acquire the dd table with S mdl. */
  MDL_request mdl_request;
  if (request_mdl_lock(thd, table_share->db.str, hlindex_name, MDL_SHARED,
                       MDL_EXPLICIT, error_message, &mdl_request)) {
    goto error_end;
  }

  Scope_guard guard{[thd, &mdl_request]() {
    if (mdl_request.ticket != nullptr) dd::release_mdl(thd, mdl_request.ticket);
  }};

  /* Keep the DD object alive for key building, and release it before
      the explicit MDL guard below releases the MDL ticket. */
  dd::cache::Dictionary_client::Auto_releaser hlindex_releaser(
      thd->dd_client());
  const dd::Table *hlindex_dd = open_hlindex_dd(
      thd, hlindex_name, table_share->db.str, error_message);

  if (hlindex_dd == nullptr) {
    goto error_end;
  }

  /* 3. Build the key info. Do fill_index_from_dd() and
  fill_index_elements_from_dd(). */
  KEY *vec_key = &(table_share->key_info[nr]);
  KEY_PART_INFO *key_part = vec_key->key_part;
  MEM_ROOT *mem_root = &(table_share->mem_root);

  /* Don't assert table_share is not temp table, because the vector index may be
  in a temp table during the copy ddl. */

  vec_key->flags = HA_VECTOR;
  vec_key->name =
      strmake_root(mem_root, key_name.c_str(), key_name.length() + 1);
  vec_key->vector_index_id = index_id;
  vec_key->algorithm = HA_KEY_ALG_BTREE;
  vec_key->is_algorithm_explicit = false;
  vec_key->is_visible = true;
  vec_key->user_defined_key_parts = 1;
  vec_key->parser = nullptr;
  vec_key->engine_attribute.length = 0;
  vec_key->engine_attribute.str = nullptr;
  vec_key->secondary_engine_attribute.length = 0;
  vec_key->secondary_engine_attribute.str = nullptr;

  dd::String_type comment = hlindex_dd->comment();
  if (comment.length() > 0) {
    vec_key->comment.length = comment.length();
    vec_key->comment.str =
        strmake_root(mem_root, comment.c_str(), comment.length() + 1);
    vec_key->flags |= HA_USES_COMMENT;
  } else {
    vec_key->comment.length = 0;
  }

  hlindex_dd->options().get("__vector_m__", &(vec_key->vector_m));
  hlindex_dd->options().get("__vector_distance__", &(vec_key->vector_distance));
  if (hlindex_dd->options().exists("__vector_ef_construction__"))
    hlindex_dd->options().get("__vector_ef_construction__",
                              &(vec_key->vector_ef_construction));
  else
    vec_key->vector_ef_construction = hnsw::EF_CONSTRUCTION_DEF;
  hlindex_dd->options().get("__vector_column__", &(key_part->fieldnr));
  Field *field = key_part->field = table_share->field[key_part->fieldnr - 1];
  key_part->key_part_flag = 0;
  key_part->length = field->key_length();
  key_part->offset = field->offset(table_share->default_values);
  key_part->type = field->key_type();
  key_part->bin_cmp = ((field->real_type() != MYSQL_TYPE_VARCHAR &&
                        field->real_type() != MYSQL_TYPE_STRING) ||
                       (field->charset()->state & MY_CS_BINSORT));

  vec_key->key_length = key_part->length;
  table_share->keynames.type_names[nr] = vec_key->name;
  table_share->keys_in_use.set_bit(nr);
  table_share->visible_indexes.set_bit(nr);

  return false;
}

bool test_if_cheaper_vector_ordering(JOIN_TAB *tab, ORDER *order, ha_rows limit,
                                     bool no_changes, int *order_idx) {
  if (order == nullptr || order->next != nullptr ||
      order->direction != ORDER_ASC ||
      !is_function_of_type(*order->item, Item_func::VECTOR_DISTANCE_FUNC)) {
    return false;
  }

  Item_func_vec_distance *item =
      down_cast<Item_func_vec_distance *>(*order->item);
  int item_idx = item->get_key();
  ha_rows rows;

  if (item_idx == -1) {
    /* args in function are not one vector column and one const value. */
    return false;
  }

  assert(item_idx >= 0);
  assert((uint)item_idx >= tab->table()->s->keys);
  assert((uint)item_idx < tab->table()->s->total_keys);
  if ((uint)item_idx >= MAX_INDEXES) return false;

  if (tab->table()->force_index_order) {
    /* Handle the hint about force index. */
    if (tab->table()->keys_in_use_for_order_by.is_set(item_idx)) {
      if (limit == HA_POS_ERROR || limit > tab->table()->file->stats.records) {
        limit = tab->table()->file->stats.records;
      }

      goto use_vector_index;
    } else {
      return false;
    }
  }

  /*
    Bug fix: respect IGNORE INDEX / USE INDEX hints for ORDER BY.

    MySQL index hints affect table->keys_in_use_for_order_by (a bitmap of
    indexes eligible for ORDER BY) as follows:

      Hint                             force_index_order   bitmap effect
      -------------------------------- ------------------- ----------------
      FORCE INDEX FOR ORDER BY (vi)    true                bitmap = {vi}
      FORCE INDEX (vi)                 true                bitmap = {vi}
      IGNORE INDEX FOR ORDER BY (vi)   false               bitmap removes vi
      IGNORE INDEX (vi)                false               bitmap removes vi
      USE INDEX FOR ORDER BY (vi)      false               bitmap = {vi}
      USE INDEX FOR ORDER BY ()        false               bitmap = {}
      (no hint)                        false               bitmap = all keys

    The FORCE case is already handled above (line 820): when force_index_order
    is true, we check the bitmap and either force the vector index or reject
    it.

    However, IGNORE / USE hints do NOT set force_index_order, so they skip
    the FORCE branch and fall through to the cost-based path below.  Without
    this guard, the cost-based path would still pick the vector index even
    though the user explicitly excluded it via IGNORE INDEX or constrained
    the eligible set via USE INDEX.

    Fix: before entering cost-based evaluation, verify that the vector index
    is present in keys_in_use_for_order_by.  When no hint is given, all
    usable indexes are in the bitmap by default, so this is a no-op for the
    common case.
  */
  if (!tab->table()->keys_in_use_for_order_by.is_set(item_idx)) {
    return false;
  }

  if (limit == HA_POS_ERROR && limit >= tab->table()->file->stats.records) {
    return false;
  }

  switch (tab->type()) {
    case JT_RANGE:
      rows = tab->range_scan()->num_output_rows();
      break;

    case JT_ALL:
    case JT_INDEX_SCAN:
      rows = tab->table()->file->stats.records;
      break;

    default:
      return false;
  }

  static_assert(SCAN_COST > 1);

  if (tab->index() == 0) {
    /* PRIMARY index scanning vs vector index scanning */
    if (limit > rows / SCAN_COST) {
      return false;
    }
  } else if (limit >= rows) {
    /* Secondary index scanning vs vector index scanning */
    return false;
  }

use_vector_index:
  assert(limit <= tab->table()->file->stats.records);
  assert(limit != HA_POS_ERROR);

  *order_idx = item_idx;

  /*
    In probe mode (no_changes == true), only report that vector index
    ordering is possible.  Do NOT touch the access path -- the caller
    (Plan_change_watchdog) will assert if we do.
  */
  if (no_changes) return true;

  item->set_limit(limit);

  tab->set_type(JT_INDEX_SCAN);
  tab->ref().key = item_idx;
  tab->ref().key_parts = 0;
  tab->set_index(item_idx);
  tab->set_vec_func(item);

  return true;
}
}  // namespace vidx

using namespace vidx;
using namespace vidx::hnsw;

int TABLE::hlindex_open(uint nr) {
  assert(nr >= s->keys && nr < s->total_keys);
  const uint nvec = s->hlindexes();
  const uint slot = nr - s->keys;
  hlindex_active_slot = slot;

  /* Allocate the per-TABLE owning array on first use (freed alongside the
  hlindex tables in the table cleanup paths in sql_base.cc). */
  if (hlindex_tables == nullptr) {
    hlindex_tables = static_cast<TABLE **>(
        my_malloc(key_memory_TABLE, sizeof(TABLE *) * nvec, MYF(MY_WME | MY_ZEROFILL)));
    if (hlindex_tables == nullptr) return 1;
  }

  if (hlindex_tables[slot] == nullptr) {
    std::string error_message;
    KEY *vec_key = s->key_info + nr;
    char path[FN_REFLEN + 1];

    /* 1.Build name using this index's stable per-index id. */
    const char *hlindex_name = build_name(in_use, s->m_se_private_id,
                                          vec_key->vector_index_id, error_message);

    if (hlindex_name == nullptr) {
    error_end:
      my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Open", vec_key->name, s->db.str,
               s->table_name.str,
               (hlindex_name == nullptr ? "?" : hlindex_name),
               error_message.c_str());
      return 1;
    }

    /* 2. Acquire the dd table with S mdl. */
    if (request_mdl_lock(in_use, s->db.str, hlindex_name, MDL_SHARED_READ,
                         MDL_TRANSACTION, error_message)) {
      goto error_end;
    }

    /* Keep the acquired dd::Table alive until open_table_from_share at
       the bottom is done (see open_hlindex_dd CONTRACT). */
    dd::cache::Dictionary_client::Auto_releaser hlindex_releaser(
        in_use->dd_client());
    const dd::Table *hlindex_dd =
        open_hlindex_dd(in_use, hlindex_name, s->db.str, error_message);

    if (hlindex_dd == nullptr) {
      goto error_end;
    }

    /* 3. Open the shared hlindex */
    s->lock_share();
    Scope_guard guard{[this]() { s->unlock_share(); }};

    /* Share allocated and published into hlindex_shares[slot] by THIS call.
       If a later step fails before a TABLE is bound to it, roll the slot
       back so the share does not leak its MEM_ROOT. */
    TABLE_SHARE *new_share = nullptr;

    /* Allocate the shared hlindex-share array on first use (under lock_share). */
    if (s->hlindex_shares == nullptr) {
      s->hlindex_shares = static_cast<TABLE_SHARE **>(
          s->mem_root.Alloc(sizeof(TABLE_SHARE *) * nvec));
      if (s->hlindex_shares == nullptr) {
        error_message = "Failed to alloc hlindex share array.";
        goto error_end;
      }
      memset(s->hlindex_shares, 0, sizeof(TABLE_SHARE *) * nvec);
    }

    if (s->hlindex_shares[slot] == nullptr) {
      /* Build the table cache key: "db\0name\0".
         Same format as create_table_def_key() in sql_base.cc. */
      char cache_key[NAME_LEN + 1 + NAME_LEN + 1];
      size_t key_length =
          strmake(strmake(cache_key, s->db.str, NAME_LEN) + 1,
                  hlindex_name, NAME_LEN) - cache_key + 1;
      bool normal_share = false;

      /* Open the table hlindex */
      if (s->tmp_table != NO_TMP_TABLE) {
        /* Base table is temp. */
        bool was_truncated;
        build_table_filename(path, sizeof(path) - 1 - reg_ext_length, s->db.str,
                             hlindex_name, "", 0, &was_truncated);

        /* Allocate TABLE_SHARE + key + path in one block so the strings
           share the lifetime of the share.  Pattern from
           open_table_uncached() in sql_base.cc. */
        size_t plen = strlen(path);
        new_share = reinterpret_cast<TABLE_SHARE *>(
            in_use->mem_root->Alloc(sizeof(TABLE_SHARE) + key_length +
                                    plen + 1));
        if (new_share == nullptr) {
          error_message = "Failed to alloc temporary hlindex share.";
          goto error_end;
        }
        char *saved_key = reinterpret_cast<char *>(new_share + 1);
        memcpy(saved_key, cache_key, key_length);
        char *saved_path = saved_key + key_length;
        memcpy(saved_path, path, plen + 1);

        init_tmp_table_share(in_use, new_share, saved_key, key_length,
                             strend(saved_key) + 1, saved_path, nullptr);
      } else if ((new_share = alloc_table_share(s->db.str, hlindex_name,
                                                cache_key, key_length,
                                                false)) == nullptr) {
        /* Base table is normal. */
        error_message = "Failed to alloc_table_share.";
        goto error_end;
      } else {
        normal_share = true;
      }

      if (open_table_def(in_use, new_share, *hlindex_dd)) {
        if (normal_share) free_table_share(new_share);
        error_message = "Failed to open_table_def.";
        goto error_end;
      }

      new_share->is_hlindex = true;

      assert(new_share->hlindex_data == nullptr);
      assert(new_share->hlindex_shares == nullptr);
      s->hlindex_shares[slot] = new_share;
    }

    /* 4. Open a new hlindex TABLE for this slot. */
    TABLE *hlidx =
        (TABLE *)my_malloc(key_memory_TABLE, sizeof(TABLE), MYF(MY_WME));

    if (hlidx == nullptr) {
      if (new_share != nullptr) {
        s->hlindex_shares[slot] = nullptr;
        free_table_share(new_share);
      }
      error_message = "Failed to my_malloc hlindex table.";
      goto error_end;
    }

    /* No increment_ref_count / LOCK_open needed.

       TABLE_SHARE::hlindex is a private share whose lifetime is bound to
       the parent TABLE_SHARE::destroy(), not TDC ref_count-driven eviction.
       The parent share is only destroyed after all TABLE instances (and
       their hlindex) are closed via intern_close_table, so no dangling
       reference is possible.

       With hlindex persisting across statements, multiple TABLE instances
       may call hlindex_open concurrently; concurrent ++m_ref_count (plain
       unsigned int) would be a data race.  Since ref_count serves no
       functional purpose here, we remove both increment (here) and the
       matching decrement (closefrm is_hlindex path) entirely. */

    int error = open_table_from_share(in_use, s->hlindex_shares[slot], hlindex_name,
                                      (uint)(HA_OPEN_KEYFILE | HA_OPEN_RNDFILE |
                                             HA_GET_INDEX | HA_TRY_READ_ONLY),
                                      EXTRA_RECORD, in_use->open_options,
                                      hlidx, false, hlindex_dd);

    if (error != 0) {
      destroy(hlidx);
      my_free(hlidx);
      if (new_share != nullptr) {
        s->hlindex_shares[slot] = nullptr;
        free_table_share(new_share);
      }
      error_message = "Failed to open_table_from_share.";
      goto error_end;
    }

    hlidx->in_use = nullptr;
    hlindex_tables[slot] = hlidx;
  }

  /* Bind the active hlindex table for this slot. */
  hlindex_active = hlindex_tables[slot];
  return 0;
}

/**
  Lock (rebind) hlindex for the current statement.

  Locks: MDL (once in hlindex_open) / store_lock / ha_external_lock (each rebind).
  Rebind triggers when hlindex->in_use != current THD.
  No-op if already bound to current THD (idempotent).

  @note  hlindex skips full ha_reset() at statement end; blob_heap persists
         (~KB, bounded).  See Docs/vidx_hlindex_perf_design.md section 8.
*/
int TABLE::hlindex_lock(uint nr [[maybe_unused]]) {
  assert(nr >= s->keys && nr < s->total_keys);
  assert(nr - s->keys == hlindex_active_slot);
  assert(hlindex_active);
  assert(hlindex_active->file);

  /* Caller must hold base-table MDL (DDL/FTWRL safety). */
  ut_a(in_use->mdl_context.owns_equal_or_stronger_lock(
      MDL_key::TABLE, s->db.str, s->table_name.str, MDL_SHARED));

  if (hlindex_active->in_use != in_use) {
    hlindex_active->file->rebind_psi();
    /* Resets prebuilt template; does NOT free blob_heap. */
    hlindex_active->file->ha_extra(HA_EXTRA_RESET_STATE);

    hlindex_active->reset();
    hlindex_active->set_created();
    hlindex_active->use_all_columns();

    hlindex_active->in_use = in_use;
    hlindex_active->pos_in_table_list = pos_in_table_list;

    assert(hlindex_active->file->lock_count() <= 1);

    /*
      The hlindex handler is cached across statements (release_table no
      longer closes it), so a prior DML's LOCK_X may still live in the
      engine's per-statement state.  store_lock() is what mysql_lock_tables
      uses to clear that for ordinary tables; call it manually here for the
      hlindex.  Without it, a SELECT following an UPDATE on the same TABLE
      would trip the F_RDLCK/LOCK_X assertion in ha_innobase::external_lock().

      Skip when pos_in_table_list is null: that only happens in internal /
      DDL paths (e.g. OPTIMIZE TABLE rebuild) where the hlindex is freshly
      opened and has no stale state to clear, and where store_lock() would
      crash dereferencing pos_in_table_list->lock_descriptor().
    */
    if (pos_in_table_list != nullptr) {
      const thr_lock_type new_lock_type =
          reginfo.lock_type < TL_WRITE_ALLOW_WRITE ? TL_READ : TL_WRITE;
      THR_LOCK_DATA *dummy_data = nullptr;
      THR_LOCK_DATA **dummy_to = &dummy_data;
      hlindex_active->file->store_lock(in_use, dummy_to, new_lock_type);
    }

    return hlindex_active->file->ha_external_lock(
        in_use, reginfo.lock_type < TL_WRITE_ALLOW_WRITE ? F_RDLCK : F_WRLCK);
  }

  return 0;
}

/**
  Statement-end cleanup for hlindex (called from base table's ha_reset
  AFTER cascade F_UNLCK).  Clears in_use so next hlindex_lock() rebinds.
  Does NOT call handler::reset() — see design doc section 8.
*/
int TABLE::reset_hlindexes() {
  if (hlindex_tables != nullptr) {
    auto [p, end] = hlindex_tables_range();
    for (; p < end; ++p) {
      if (*p && (*p)->in_use) {
        (*p)->in_use = nullptr;
        (*p)->pos_in_table_list = nullptr;
      }
    }
  }
  return 0;
}

/**
  Open one additional TABLE instance over the hlindex share that
  TABLE::hlindex_open() already published for index @p nr.

  DDL-build worker threads need private graph TABLE instances for
  read-through loading (handler, record buffers and rnd position are
  per-TABLE state), while the share, MDL and dd::Table acquisition stay
  exactly the hlindex_open() recipe above.  The instance is returned
  detached (in_use == nullptr); the consumer rebinds it to its own THD
  the same way hlindex_lock() rebinds across statements, and the opener
  closes it with closefrm(false) + my_free() after the consumer
  detached again.  The share needs no extra reference: its lifetime is
  bound to the parent TABLE_SHARE::destroy() (see hlindex_open()),
  which outlives every DDL-build worker by construction.
*/
int vidx::hlindex_open_worker_instance(TABLE *base, uint nr, TABLE **out) {
  *out = nullptr;
  TABLE_SHARE *s = base->s;
  assert(nr >= s->keys && nr < s->total_keys);
  const uint slot = nr - s->keys;
  if (s->hlindex_shares == nullptr || s->hlindex_shares[slot] == nullptr)
    return 1; /* hlindex_open() must have published the share already */

  std::string error_message;
  KEY *vec_key = s->key_info + nr;
  THD *thd = base->in_use;

  const char *hlindex_name = build_name(
      thd, s->m_se_private_id, vec_key->vector_index_id, error_message);
  if (hlindex_name == nullptr) return 1;

  if (request_mdl_lock(thd, s->db.str, hlindex_name, MDL_SHARED_READ,
                       MDL_TRANSACTION, error_message))
    return 1;

  /* Keep the acquired dd::Table alive until open_table_from_share is
     done (see open_hlindex_dd CONTRACT). */
  dd::cache::Dictionary_client::Auto_releaser hlindex_releaser(
      thd->dd_client());
  const dd::Table *hlindex_dd =
      open_hlindex_dd(thd, hlindex_name, s->db.str, error_message);
  if (hlindex_dd == nullptr) return 1;

  TABLE *hlidx =
      (TABLE *)my_malloc(key_memory_TABLE, sizeof(TABLE), MYF(MY_WME));
  if (hlidx == nullptr) return 1;

  if (open_table_from_share(thd, s->hlindex_shares[slot], hlindex_name,
                            (uint)(HA_OPEN_KEYFILE | HA_OPEN_RNDFILE |
                                   HA_GET_INDEX | HA_TRY_READ_ONLY),
                            EXTRA_RECORD, thd->open_options, hlidx, false,
                            hlindex_dd) != 0) {
    destroy(hlidx);
    my_free(hlidx);
    return 1;
  }

  hlidx->in_use = nullptr;
  *out = hlidx;
  return 0;
}

int TABLE::hlindexes_on_insert() {
  if (vidx_ddl_direct_build_target) return 0;

  for (uint key = s->keys; key < s->total_keys; key++) {
    if (vidx::check_vector_is_null(this, record[0], key_info + key)) {
      continue;
    }

    int err;
    if ((err = hlindex_open(key))) {
      vidx::log_slave_dml_err(this, __func__, "hlindex_open", err);
      return err;
    }
    /* Populate base handler's row reference BEFORE the Attachable_trx_rw
       scope.  Inside the attachable, base-handler methods are forbidden
       because they would silently rebind m_prebuilt->trx to the
       attachable trx.  See Hlindex_dml_scope CONTRACT. */
    file->position(record[0]);
    const bool indep = vidx::aux_dml::need_independent_trx(in_use);
    vidx::aux_dml::Hlindex_dml_scope scope(this, key, indep);
    if ((err = scope.init_err())) return scope.commit(err);
    err = mhnsw_insert(this, key_info + key, /*from_update=*/false, indep);
    if ((err = scope.commit(err))) {
      /* Multi-index indep INSERT: earlier keys' graph rows are already
         committed by their own attachables; failing here leaves them
         orphaned after the user statement rolls back.  Account the
         pressure so operators can spot it (self-heals on next DML). */
      if (indep && key > s->keys) vidx_stats.insert_partial_orphan++;
      vidx::log_slave_dml_err(this, __func__, "mhnsw_insert", err);
      return err;
    }
  }

  return 0;
}

int TABLE::hlindexes_on_update() {
  // Increment update counter
  vidx_stats.update_count++;

  for (uint key = s->keys; key < s->total_keys; key++) {
    const bool old_is_null =
        vidx::check_vector_is_null(this, record[1], key_info + key);
    const bool new_is_null =
        vidx::check_vector_is_null(this, record[0], key_info + key);

    if (old_is_null && new_is_null) {
      continue;
    }

    /* DML update contract — only update the auxiliary table when the
       vector content or the primary key (TREF) actually changed.
       The non-NULL → non-NULL skip predicate lives in
       vidx::aux_dml::update_can_skip (see vidx_aux_dml.h docblock for
       the rationale and the full case matrix). */
    if (!old_is_null && !new_is_null &&
        aux_dml::update_can_skip(this, key)) {
      vidx_stats.update_skipped++;
      continue;
    }

    int err;
    if ((err = hlindex_open(key))) {
      vidx::log_slave_dml_err(this, __func__, "hlindex_open", err);
      return err;
    }

    /* Indep DML path runs invalidate and insert as TWO separate Attachable_trx_rw
       scopes (mhnsw_invalidate / mhnsw_insert each open their own attachable).
       Reason: position() must happen OUTSIDE attachable (base handler's
       prebuilt->trx is stale inside), so we cannot re-position between
       invalidate and insert if they shared one attachable.

       Known intermediate-state caveat: if invalidate's inner trx commits
       and then insert fails, the graph has an old node soft-deleted but
       no new node yet — same "rollback leaves orphan" family as XA
       ROLLBACK (see vidx_xa_advanced.test §4).  Lazy invalidate / orphan-
       row treatment in mhnsw_invalidate cleans up on subsequent DML.
       update_partial_orphan is accounted inside mhnsw_insert (search for
       from_update branches) so the caller only deals with the
       error-propagation side. */
    if (!old_is_null) {
      /* Pre-position with OLD image so mhnsw_invalidate reads the right
         TREF off file->ref.  Must happen OUTSIDE the attachable scope
         (see Hlindex_dml_scope CONTRACT). */
      file->position(record[1]);
      const bool indep = vidx::aux_dml::need_independent_trx(in_use);
      vidx::aux_dml::Hlindex_dml_scope scope(this, key, indep);
      if ((err = scope.init_err())) return scope.commit(err);
      err = mhnsw_invalidate(this, key_info + key);
      if ((err = scope.commit(err))) {
        vidx::log_slave_dml_err(this, __func__, "mhnsw_invalidate", err);
        return err;
      }
    }

    DBUG_EXECUTE_IF("vidx_indep_update_fail_after_invalidate", {
      return HA_ERR_GENERIC;
    });

    if (!new_is_null) {
      /* from_update=true when old_is_null is false: invalidate already
         ran in its own attachable and the orphan window is real.
         When old_is_null is true (NULL → non-NULL = pure insert leg)
         there was no invalidate, so no orphan window either. */
      const bool from_update = !old_is_null;
      /* Pre-position with NEW image (overwrites file->ref from any prior
         position(record[1]) above). */
      file->position(record[0]);
      const bool indep = vidx::aux_dml::need_independent_trx(in_use);
      vidx::aux_dml::Hlindex_dml_scope scope(this, key, indep);
      if ((err = scope.init_err())) return scope.commit(err);
      err = mhnsw_insert(this, key_info + key, from_update, indep);
      if ((err = scope.commit(err))) {
        vidx::log_slave_dml_err(this, __func__, "mhnsw_insert", err);
        return err;
      }
    }
  }

  return 0;
}

int TABLE::hlindexes_on_delete(const uchar *buf) {
  assert(buf == record[0] || buf == record[1]);  // note: REPLACE

  for (uint key = s->keys; key < s->total_keys; key++) {
    if (vidx::check_vector_is_null(this, buf, key_info + key)) {
      continue;
    }

    int err;
    if ((err = hlindex_open(key))) {
      vidx::log_slave_dml_err(this, __func__, "hlindex_open", err);
      return err;
    }
    /* Pre-position with buf so mhnsw_invalidate reads the TREF off
       file->ref.  Must happen OUTSIDE the attachable scope (see
       Hlindex_dml_scope CONTRACT). */
    file->position(buf);
    const bool indep = vidx::aux_dml::need_independent_trx(in_use);
    vidx::aux_dml::Hlindex_dml_scope scope(this, key, indep);
    if ((err = scope.init_err())) return scope.commit(err);
    err = mhnsw_invalidate(this, key_info + key);
    if ((err = scope.commit(err))) {
      vidx::log_slave_dml_err(this, __func__, "mhnsw_invalidate", err);
      return err;
    }
  }

  return 0;
}

int TABLE::hlindexes_on_delete_all() {
  for (uint key = s->keys; key < s->total_keys; key++) {
    int err;
    if ((err = hlindex_open(key))) {
      vidx::log_slave_dml_err(this, __func__, "hlindex_open", err);
      return err;
    }
    /* TRUNCATE doesn't need pre-position (no specific row); scope still
       takes the hlindex_lock and (on indep path) opens the attachable. */
    const bool indep = vidx::aux_dml::need_independent_trx(in_use);
    vidx::aux_dml::Hlindex_dml_scope scope(this, key, indep);
    if ((err = scope.init_err())) return scope.commit(err);
    err = mhnsw_delete_all(this, key_info + key);
    if ((err = scope.commit(err))) {
      vidx::log_slave_dml_err(this, __func__, "mhnsw_delete_all", err);
      return err;
    }
  }

  return 0;
}

int TABLE::hlindex_read_first(uint key, void *item) {
  assert(key >= s->keys && key < s->total_keys);

  int err;

  if ((err = hlindex_open(key)) || (err = hlindex_lock(key)) ||
      (err = mhnsw_read_first(this, key_info + key, (Item *)item))) {
    return err;
  }

  return 0;
}

int TABLE::hlindex_read_next() { return mhnsw_read_next(this); }

int TABLE::hlindex_read_end() { return mhnsw_read_end(this); }

mysql_declare_plugin(vidx){
    MYSQL_DAEMON_PLUGIN,
    &vidx::daemon,
    "vidx",
    "AliCloud",
    "A plugin for vector index algorithm", /* Plugin name */
    PLUGIN_LICENSE_GPL,
    vidx::plugin_init, /* Plugin Init */
    nullptr,
    vidx::plugin_deinit, /* Plugin Deinit */
    0x0100,              /* Plugin Version: major.minor */
    nullptr,             /* status variables */
    vidx::sys_vars,      /* system variables */
    nullptr,             /* config options */
    0,                   /* flags */
} mysql_declare_plugin_end;
