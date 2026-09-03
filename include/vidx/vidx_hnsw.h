#ifndef VIDX_HNSW_INCLUDED
#define VIDX_HNSW_INCLUDED

#include <cstddef>
#include <string>
#include <vector>

#include "my_base.h"  // ha_rows

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

class THD;
class KEY;
struct TABLE;

namespace dd {
class Table;
}  // namespace dd

namespace vidx {
namespace hnsw {
static constexpr uint32_t DEF_CACHE_SIZE = 16 * 1024 * 1024;
static constexpr uint max_ef = 10000;
static constexpr ulong DEF_LOCK_TIMEOUT_MS = 1000;
static constexpr ulong MAX_LOCK_TIMEOUT_MS = 50000;

extern ulonglong max_cache_size;
extern ulong lock_timeout_ms;
extern void *trx_handler;

std::unique_ptr<dd::Table> create_dd_table(THD *thd, const char *table_name,
                                           KEY *key, dd::Table *dd_table,
                                           TABLE *table, const char *db_name,
                                           const uint tref_len);
class MHNSW_Share;

/*
  DML primitives — self-contained.  Each function internally obtains
  the MHNSW_Trx context via get_from_thd.  When called inside an
  Hlindex_dml_scope (attachable rw trx), get_from_thd transparently
  binds MHNSW_Trx to the OUTER (main-table) transaction so that:
    - The graph cache persists across multiple DML ops in one user trx.
    - do_commit fires at outer trx commit (single share-cache invalidation).
    - ha_data survives the attachable roundtrip via backup/restore.

  Caller contract:
    - hlindex_open() has succeeded.
    - The base handler's row reference (table->file->ref) has been
      populated via table->file->position(...) BEFORE entry.  The
      primitive reads file->ref to identify the row but never calls
      any base-handler method.

  mhnsw_read_first / mhnsw_read_next / mhnsw_read_end form an iterator
  triple: read_end is required iff read_first returned 0; on read_first
  failure no read_end is needed (no scan state was set up).
*/
/*
  @p from_update is true when this insert is the second leg of an
  invalidate+insert pair (called from hlindexes_on_update); false when
  it is a stand-alone insert (called from hlindexes_on_insert).  Used
  for orphan accounting in two places inside mhnsw_insert:
    - save_new_row swallows a slave-side LOCK_WAIT_TIMEOUT and returns
      0 — caller cannot tell, so we bump update_partial_orphan here.
    - mhnsw_insert is about to return a real error — the caller's prior
      mhnsw_invalidate already committed a soft-delete in its own
      attachable, so the failure leaves an orphan.  Accounted at the
      func_end: tail so every error exit goes through one place.
  Both branches are gated by @p indep because non-indep / DDL paths roll
  back invalidate and insert together with the user trx and never produce
  an orphan.  @p indep is the same routing flag the caller passed to
  Hlindex_dml_scope; threading it in avoids re-evaluating
  need_independent_trx() inside the primitive.
*/
int mhnsw_insert(TABLE *table, KEY *keyinfo, bool from_update, bool indep);
int mhnsw_read_first(TABLE *table, KEY *keyinfo, Item *dist);
int mhnsw_read_next(TABLE *table);
int mhnsw_read_end(TABLE *table);
int mhnsw_invalidate(TABLE *table, KEY *keyinfo);
int mhnsw_delete_all(TABLE *table, KEY *keyinfo);
void mhnsw_free(TABLE_SHARE *share);

/*
  Output statistics produced by mhnsw_preload().  Every field defaults to
  zero so callers reading them after an early failure see well-defined
  values; the fields are only fully meaningful on success.
*/
struct Preload_stats {
  size_t est_bytes{0};      // scaled upper-bound footprint estimate
  size_t used_bytes{0};     // ctx->cache_allocated_size() after the call
  ha_rows total_rows{0};    // graph rows seen
};

/*
  Full-graph cache preload backing dbms_vidx.preload(db, table, column, check).

  Estimates the in-memory footprint of the whole HNSW graph, hard-fails
  (HA_ERR_OUT_OF_MEM) with a filled-in @p err_reason if it would not fit
  within vidx_hnsw_cache_size, otherwise scans the auxiliary graph table
  and materialises every node into the shared cache so the vectors stay
  resident.  See definition in vidx_hnsw_ops.cc for the full contract.

  @p vec_column names the indexed column of the vector index to preload;
  the vector index whose first key part matches it case-insensitively is
  chosen.  It must be non-empty (the SQL layer rejects an empty column).

  When @p check_only is true, only the estimate pass and the capacity
  gate run: the function reports whether the graph WOULD fit and returns
  without materialising the full graph.  In that mode stats.used_bytes
  reflects the current cache usage.  This is not a zero-allocation probe:
  acquiring a cold share still loads the single entry-point node;
  check_only only suppresses the full-graph load.

  @p stats      receives the preload statistics; reset to defaults on entry
                so an early failure still leaves well-defined values.
  @p err_reason on failure receives a human-readable cause.
*/
int mhnsw_preload(TABLE *table, const char *vec_column, bool check_only,
                  Preload_stats &stats, std::string &err_reason);

/*
  One row of the dbms_vidx.prebuild_check(db, table[, m]) report: the
  expected in-memory build footprint of the vector index a COPY ALTER
  would (re)build on one VECTOR column of the table.

  A COPY ALTER rebuilds EVERY vector index of the table, one slot at a
  time, and frees each slot's private build ctx before the next slot
  starts (ddl_direct_build_impl), so the ALTER stays single-pass iff
  every per-column estimate fits vidx_hnsw_cache_size -- the verdict
  compares the MAX row, never the sum.

  column/index_name point into TABLE_SHARE memory: consume or copy them
  while the table stays open.
*/
struct Prebuild_estimate {
  const char *column{nullptr};      /* VECTOR column name */
  const char *index_name{nullptr};  /* existing vector index, or nullptr */
  uint dimensions{0};
  uint m{0};              /* existing KEY's M, or the caller's default */
  ha_rows rows{0};        /* exact non-NULL vector rows (full scan) */
  ulonglong est_bytes{0}; /* scaled expected peak build footprint */
};

/*
  Fill one Prebuild_estimate per VECTOR column of @p table, in field
  order.  @p default_m sizes the columns that carry no vector index yet
  (the planned index); indexed columns use their KEY's real M.  Row
  counts come from one full base-table scan -- exact and
  statistics-free, because NULL vectors produce no graph node and
  persistent statistics lag behind DML.  The per-node math mirrors the
  preload estimator with the measured layer replaced by its geometric
  expectation.  Returns a handler error and fills @p err_reason on
  failure (no VECTOR column, scan error, KILL).
*/
int mhnsw_prebuild_check(TABLE *table, uint default_m,
                         std::vector<Prebuild_estimate> *out,
                         std::string &err_reason);

/*
  need_independent_trx() and force_non_indep have moved to
  include/vidx/vidx_aux_dml.h (namespace vidx::aux_dml) as part of
  the aux-DML runtime extraction.  Include that header directly if
  you need them.
*/

/*
  Forwarder invoked by THD::cleanup() after xa::Transaction_cache::detach
  when a connection drops while holding an XA_PREPARED transaction
  without xa_detach_on_prepare.  set_prepared_in_tc already returned 0
  at PREPARE time (non-detach branch), so this thd's MHNSW_Trx list is
  about to be torn down with the connection and no later do_commit will
  ever run on this thd.  The forwarder routes a final flush through
  do_commit equivalent so share-cache invalidation is not lost.
*/
void flush_share_cache_on_xa_disconnect(THD *thd);

}  // namespace hnsw
}  // namespace vidx

#endif /* VIDX_HNSW_INCLUDED */
