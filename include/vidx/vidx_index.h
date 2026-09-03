#ifndef VIDX_INDEX_INCLUDED
#define VIDX_INDEX_INCLUDED

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

#include "my_base.h"             // ha_rows
#include "my_inttypes.h"         // uintxx_t
#include "sql/dd/properties.h"   // dd::Properties
#include "sql/dd/string_type.h"  // dd::String_type
#include "sql/dd/types/table.h"  // dd::Table
#include "sql/key.h"             // KEY

#include <vector>                // std::vector
#include "vidx/vidx_common.h"

class THD;
struct TABLE;
struct handlerton;
struct TABLE_SHARE;
struct st_plugin_int;
class JOIN_TAB;
class ORDER;
class Alter_info;

namespace dd {
class Schema;
}  // namespace dd

namespace vidx {
constexpr uint32_t DATA_ROW_ID_LEN = 6;

class Item_func_vec_distance;

extern st_plugin_int *vidx_plugin;
extern bool feature_disabled;

/* Validate that an auxiliary table's tref column can store the base-table
   row reference used by this vector index.
   @param[in] tref_len        Required base-table row reference length.
   @param[in] field_tref_len  Actual auxiliary-table tref column length.
   @return 0 if valid; otherwise HA_ERR_GENERIC after reporting
           ER_VECTOR_INDEX_USAGE. */
int check_tref_len(const uint tref_len, const uint field_tref_len);

bool check_vector_ddl(THD *thd, Alter_info *alter_info, KEY *key_info,
                      const uint key_count, TABLE *table);

/* Open one additional detached TABLE instance (in_use == nullptr) over the
   hlindex share that TABLE::hlindex_open() already published for index
   @p nr.  DDL-build worker threads rebind such instances to their own
   internal THDs for read-through graph loading; the caller closes them
   with closefrm(false) + my_free() after the workers detached.  Returns
   0 on success, 1 on failure (callers degrade to leader-mediated
   loading; never a correctness requirement). */
int hlindex_open_worker_instance(TABLE *base, uint nr, TABLE **out);

namespace hnsw {
uint index_options_print(const uint distance, const uint m,
                         const uint ef_construction, char *buf, uint buf_len);

bool copy_index_option_m(THD *thd, uint *to, const uint from);
bool copy_index_option_ef_construction(THD *thd, uint *to, const uint from);

/* Synchronous COPY-DDL graph construction. */
void init_ddl_build_instrumentation();
void prepare_ddl_direct_build(TABLE *new_table, bool is_noop);
bool finish_ddl_direct_build(THD *thd, TABLE *new_table, ha_rows copied);
}  // namespace hnsw

bool copy_index_option_distance(THD *thd, uint *to, const uint from);

/* Create the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] key                 The vector key.
@param[in] dd_table            The dd table object describing the base table.
@param[in] table               The base table
@param[in] db_name             The DB name.
@param[in] old_table_id        The old table id before truncate, or
                               dd::INVALID_OBJECT_ID if new create.
@return true if failed. */
bool create_table(THD *thd, KEY *key, dd::Table *dd_table, TABLE *table,
                  const char *db_name, const uint64_t old_table_id);

/* Create all hlindex auxiliary tables for a base table and finalize metadata.
@param[in] thd             Thread context.
@param[in] share           The base TABLE_SHARE (provides vec key array).
@param[in,out] table       The base TABLE (hlindex_active_slot is set).
@param[in,out] table_def   The base dd::Table.
@param[in] db_name         The DB name.
@param[in] old_table_id    The old table id before truncate, or
                           dd::INVALID_OBJECT_ID if new create.
@return non-zero on failure. */
int create_all_hlindexes(THD *thd, TABLE_SHARE *share, TABLE *table,
                         dd::Table *table_def, const char *db_name,
                         uint64_t old_table_id);

/* Prepare enriched __hlindexes__ metadata for ha_create_table.
  No-op if key_info contains no vector keys. */
void prepare_hlindex_meta(dd::Table *table_def,
                          const KEY *key_info, uint key_count);

/* Finalize __hlindexes__ on the base dd table after all hlindex tables created.
   Writes the structured metadata (count/next/name_N/id_N) for multi-index;
   for single-index tables writes the bare key name (legacy compat).
   @param[in]  dd_table        The base dd table.
   @param[in]  key_info        The key array (key_info[0..total_keys-1]).
   @param[in]  keys            Number of non-vector keys.
   @param[in]  total_keys      Total keys including vector keys.
   @return true if failed. */
bool finalize_hlindex_meta(dd::Table *dd_table, const KEY *key_info,
                           uint keys, uint total_keys);
/* Drop the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] dd_table            The dd table object describing the base table.
@param[in] db_name             The DB name.
@return true if failed. */
bool delete_table(THD *thd, const dd::Table *dd_table, const char *db_name);

/* Rename the auxiliary table for the vector index.
@param[in] thd                 Thread context.
@param[in] dd_table            The dd table object describing the base table.
@param[in] base                The base handlerton.
@param[in] new_schema          The dd schema object.
@param[in] old_db              The old DB name before rename.
@param[in] new_db              The new DB name after rename.
@param[in] flags               flags. See also mysql_rename_table().
@return true if failed. */
bool rename_table(THD *thd, dd::Table *dd_table, handlerton *base,
                  const dd::Schema &new_schema, const char *old_db,
                  const char *new_db, uint flags);

/* Build the info of vector key.
@param[in] thd                 Thread context.
@param[in] share               The table share.
@param[in] dd_table            The dd table object describing the base table.
@param[in] nr                  The number of the vector key.
@return true if failed. */
bool build_hlindex_key(THD *thd, TABLE_SHARE *share, const dd::Table *dd_table,
                       const uint nr);

/* Number of vector (hlindex) indexes recorded in __hlindexes__.
   Legacy single-index tables (bare key name in __hlindexes__) return 1;
   multi-index tables (structured format) return the recorded count;
   tables without any vector index return 0.
   @param[in] dd_table  The base dd table.
   @return the number of vector indexes. */
uint dd_table_get_hlindex_count(const dd::Table *dd_table);

/* Decoded entry from __hlindexes__ structured metadata. */
struct hlindex_meta_entry {
  dd::String_type name;
  uint id;
  /* Enriched fields (CREATE-time only, not persisted in final format). */
  uint m{0};
  uint dist{0};
  uint ef{0};
  uint col{0};         /* 1-based fieldnr */
  bool enriched{false}; /* true if m/dist/ef/col are populated */
};

/* Fill KEY/KEY_PART_INFO from enriched __hlindexes__ metadata.
   Used during CREATE/ALTER when the hlindex DD table does not yet exist. */
void fill_key_from_enriched(TABLE_SHARE *share, uint nr,
                            const hlindex_meta_entry &entry,
                            const dd::String_type &key_name);

/* Decode vector index entries from __hlindexes__ DD option.
   Detects format automatically: structured (has "count" key) vs bare string.
@param[in]  dd_table   the dd table
@param[out] entries    the decoded entries, in slot order
@param[out] next_id    the next-id allocator value (may be nullptr)
@return true on parse error. */
bool decode_hlindex_meta(
    const dd::Table *dd_table, std::vector<hlindex_meta_entry> *entries,
    uint *next_id);

/* Test if ORDER BY is a single distance function(ORDER BY VEC_DISTANCE),
sort order is descending, and vector index is more efficient than original
access path.
@param[in]  tab           JOIN_TAB to check
@param[in]  order         pointer to ORDER struct.
@param[in]  limit         maximum number of rows to select.
@param[in]  no_changes    if true, only probe whether vector index ordering is
                           possible; do NOT modify the access path of @a tab.
@param[out] order_idx     idx choosen.
@return True if vector index is enabled and efficient, otherwise False. */
bool test_if_cheaper_vector_ordering(JOIN_TAB *tab, ORDER *order, ha_rows limit,
                                     bool no_changes, int *order_idx);

/* Check if the key is a vector key.
@param[in] key                 The vector key.
@return true if the key is a vector key. */
static inline bool key_is_vector(KEY *key) {
  return key != nullptr && (key->flags & HA_VECTOR);
}

/* Assign sequential vector_index_id (0, 1, 2, ...) to each vector key. */
static inline void assign_vector_slots(KEY *keys, uint count) {
  uint slot = 0;
  for (KEY *k = keys, *end = k + count; k < end; ++k) {
    if (k->flags & HA_VECTOR) k->vector_index_id = slot++;
  }
}

/* Check the option "__hlindexes__" of the dd table exists and is not empty.
@param[in]  dd_table   The dd table.
@return true if "__hlindexes__" exists and is not empty, otherwise false. */
static inline bool dd_table_has_hlindexes(const dd::Table *dd_table) {
  return dd_table->options().exists("__hlindexes__");
}

/* Check if the dd table is a vector table.
@param[in]  dd_table   The dd table.
@return true if the dd table is a vector table, otherwise false. */
static inline bool dd_table_is_hlindex(const dd::Table *dd_table) {
  assert(dd_table->options().exists("__vector_column__") ==
         dd_table->options().exists("__vector_m__"));
  assert(dd_table->options().exists("__vector_column__") ==
         dd_table->options().exists("__vector_distance__"));
  return dd_table->options().exists("__vector_column__");
}
}  // namespace vidx

#endif /* VIDX_INDEX_INCLUDED */
