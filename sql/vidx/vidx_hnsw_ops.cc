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

/*
  vidx HNSW runtime helpers extracted out of vidx_hnsw.cc.

  This file holds helpers that were file-static inside vidx_hnsw.cc but
  whose responsibility (XA disconnect flush, entry-point loading,
  cross-trx neighbor prefetch, invalidate cache alignment) is logically
  separable from the HNSW graph algorithms.  Moving them here keeps
  vidx_hnsw.cc focused on the algorithm core and shrinks its diff
  surface against the pre-stage-3 baseline.

  All helpers operate on the file-static class hierarchy defined in
  vidx/vidx_hnsw_ctx.h (FVector / FVectorNode / MHNSW_Share /
  MHNSW_Trx / Neighborhood / ScopedLockWaitTimeout).
*/

#include "vidx/vidx_hnsw_ctx.h"

#include <sstream>                        // std::ostringstream (set_reason)
#include <string>                         // std::string

#include <algorithm>                      // std::max (prebuild estimator)
#include <cmath>                          // std::ceil (prebuild estimator)

#include "m_ctype.h"                       // my_strcasecmp, system_charset_info
#include "sql/debug_sync.h"               // DEBUG_SYNC (prebuild-scan KILL test)
#include "sql/field.h"                    // Field::data_length()
#include "sql/handler.h"                  // ha_rnd_pos, set_lock_wait_timeout
#include "sql/key.h"                      // key_copy
#include "scope_guard.h"                  // Scope_guard
#include "template_utils.h"               // down_cast

#include "vidx/vidx_field.h"              // Field_vector::get_dimensions

#include "vidx/vidx_hnsw.h"               // vidx::hnsw::mhnsw_preload decl
#include "vidx/vidx_index.h"              // vidx::check_tref_len
#include "vidx/vidx_log.h"                // vidx::log_slave_dml_warn / log_dml_warn

namespace vidx {
namespace hnsw {

/*
  Disconnect-time XA degradation forwarder.

  Called from THD::cleanup() right after xa::Transaction_cache::detach()
  when a client closes its connection while still holding an XA_PREPARED
  transaction without xa_detach_on_prepare.  At that point:

   - set_prepared_in_tc already ran at PREPARE time and returned 0
     (case 2 in the comment on hnsw_hton).
   - The Transaction_ctx has just been moved to the global cache; this
     thd's ha_data, including the MHNSW_Trx list, is about to be torn
     down with the connection.
   - No later do_commit / do_rollback will fire on THIS thd — the
     eventual XA COMMIT / XA ROLLBACK will run on a recovery thd via
     commit_by_xid / rollback_by_xid, which has no MHNSW_Trx attached.

  Force a flush here so share-cache invalidation isn't lost.  This is
  the disconnect counterpart of the eager flush that set_prepared_in_tc
  performs for xa_detach_on_prepare = ON.
*/
void flush_share_cache_on_xa_disconnect(THD *thd) {
  /* Unconditional idempotent fallback for the non-detached XA-PREPARED
     disconnect edge.  If the natural do_commit / do_rollback path
     already fired for this thd, the MHNSW_Trx list is nullptr and the
     call is a no-op; otherwise it reaps the pending share-cache
     invalidations before ha_data is torn down with the connection.
     Cheap enough to always call — no per-trx bookkeeping needed. */
  MHNSW_Trx::MHNSW_hton::do_commit(&MHNSW_Trx::hnsw_hton, thd, true);
}

/*
  Load the HNSW entry-point node into ctx->start by reading the last
  row in IDX_LAYER (the highest-layer node) under MVCC and decoding it.

  Idempotent: returns 0 immediately when ctx->start is already cached.
  Bumps acquire_start_hits / acquire_start_misses so the SHOW STATUS
  cache-hit ratio reflects entry-point loads only.

  MVCC read on the graph table avoids LOCK_X conflicts with:
    - Concurrent Attachable_trx_rw writers (each holds row locks
      until its per-row commit).
    - User SELECT / SHOW statements directly querying the auxiliary
      graph table.

  Returns:
    - 0                  : ctx->start populated (or already cached).
    - HA_ERR_END_OF_FILE : graph table is empty — caller may treat as
                           first-insert; ctx->start stays nullptr.
    - other              : real handler error.

  Used by MHNSW_Share::acquire (read path / share-cache routing) and
  by the three mhnsw_* DML wrappers inside Hlindex_dml_scope.  All
  callers funnel here so the entry-point loading invariants live in
  one place.
*/
int load_entry_point_node(MHNSW_Share *ctx, TABLE *graph) {
  if (int err = vidx::check_tref_len(
          ctx->tref_len, graph->field[FIELD_TREF]->field_length))
    return err;

  if (ctx->start) {
    vidx_stats.acquire_start_hits++;
    return 0;
  }

  ctx->lock_start();
  Scope_guard start_guard{[ctx]() { ctx->unlock_start(); }};
  if (ctx->start) {
    vidx_stats.acquire_start_hits++;
    return 0;
  }

  if (int err = graph->file->ha_index_init(IDX_LAYER, 1)) return err;

  graph->file->set_consistent_read(true);
  int err = graph->file->ha_index_last(graph->record[0]);
  graph->file->set_consistent_read(false);
  graph->file->ha_index_end();
  if (err) return err;  /* HA_ERR_END_OF_FILE = empty graph; not a miss. */

  vidx_stats.acquire_start_misses++;
  graph->file->position(graph->record[0]);
  ctx->set_lengths(
      FVector::data_to_value_size(graph->field[FIELD_VEC]->data_length()));

  /* MDEV-35834 Server crash in FVector::distance_to upon concurrent SELECT */
  auto node = ctx->get_node(graph->file->ref);
  if (unlikely(!node)) return HA_ERR_OUT_OF_MEM;
  if ((err = node->load_from_record(graph))) {
    /* get_node already inserted the stub into node_cache; remove it
       so a subsequent get_node(same_gref) does not return a half-
       initialised node with stored=true but vec/neighbors null.
       Otherwise readers walking the graph could deref node->vec and
       crash (the original MDEV-35834 family). */
    ctx->uncache_node(node);
    return err;
  }
  vidx_stats.decode_start_node_count++;

  ctx->start = node;  // set the shared start only when node is fully loaded
  return 0;
}

/*
  Prefetch X-locks on every neighbor of @p target in gref-ascending
  order before update_second_degree_neighbors runs.

  Why: update_second_degree_neighbors's per-layer loop locks rows in
  whatever order each layer happens to iterate.  Two concurrent writers
  whose target sets overlap can therefore form an ABBA wait-for cycle
  on neighbor rows that live across layers.  By pre-locking the UNION
  of all layers under a single sorted pass, every concurrent writer
  agrees on the same total order over gref-space, eliminating Coffman
  condition #4.

  How: the function takes target->neighbors[L].links[*]->gref() across
  all layers, sorts them ascending, dedups adjacent duplicates (a node
  appearing in multiple layers shows up multiple times — InnoDB row
  locks are reentrant so we only need to acquire each once), then
  walks the sorted list invoking ha_rnd_pos under F_WRLCK on each.
  ScopedLockWaitTimeout shortens the per-call timeout on slave apply
  threads so a stuck row does not wedge replication; user threads get
  the global innodb_lock_wait_timeout.

  Once prefetch returns, update_second_degree_neighbors's later
  ha_rnd_pos on the same row is reentrant on the trx_t's lock list
  and does not re-wait (see step (e) below for details).

  Error handling:
   - HA_ERR_KEY_NOT_FOUND: benign TOCTOU window (neighbor concurrently
     removed); skipped.  update_second_degree_neighbors /
     save_neighbors_only handles the stale node via its existing
     TOCTOU path.
   - HA_ERR_LOCK_WAIT_TIMEOUT: MUST NOT be silently skipped — proceeding
     into update_second_degree_neighbors without that row's LOCK_X
     would re-open the very deadlock window prefetch exists to close.
     Slave path swallows the error so apply progresses (in-memory
     graph stays slightly stale, fixed by the next touch); user path
     bubbles the error so the attachable trx rolls back and SQL
     retries.

  Returns 0 on success and sets @p out_result accordingly:
    - PrefetchResult::Ok               : every neighbor X-locked, caller
                                         must run update_second_degree_neighbors.
    - PrefetchResult::OkSkipUpdate2nd  : slave-tolerated lock-wait
                                         timeout, caller must skip
                                         update_second_degree_neighbors
                                         (graph stays slightly stale,
                                         self-heals on next touch).

  Positive return value means user-path failure; @p out_result is
  unspecified in that case.
*/
int prefetch_neighbor_locks_in_gref_order(TABLE *table, TABLE *graph,
                                          FVectorNode *target,
                                          int target_layer,
                                          const MHNSW_Share *ctx,
                                          THD *thd,
                                          PrefetchResult *out_result) {
  *out_result = PrefetchResult::Ok;

  /* (a) Size the write set: count of all neighbor entries across every
         layer the target participates in (a node may appear in several
         layers).  Empty target -> nothing to lock, skip out. */
  size_t cap = 0;
  for (int L = target_layer; L >= 0; L--) cap += target->neighbors[L].num;
  if (cap == 0) return 0;

  const uint gref_len_local = ctx->gref_len;
  const size_t buf_sz = cap * sizeof(const uchar *);
  const uchar **write_set =
      static_cast<const uchar **>(my_safe_alloca(buf_sz, MAX_ALLOCA_SZ));
  Scope_guard ws_guard{[write_set, buf_sz]() {
    my_safe_afree(const_cast<const uchar **>(write_set), buf_sz, MAX_ALLOCA_SZ);
  }};

  /* (b) Collect every neighbor's gref (clustered row reference) into
         the flat write_set buffer — the union across all layers. */
  size_t n = 0;
  for (int L = target_layer; L >= 0; L--) {
    Neighborhood &nb = target->neighbors[L];
    for (size_t i = 0; i < nb.num; i++) write_set[n++] = nb.links[i]->gref();
  }

  /* (c) Sort by gref ascending — this is THE step that gives us the
         total order on locked rows.  Every concurrent writer sorts
         against the same gref space, so the wait-for graph stays a
         DAG (Coffman #4: no circular wait). */
  std::sort(write_set, write_set + n,
            [gref_len_local](const uchar *a, const uchar *b) {
              return memcmp(a, b, gref_len_local) < 0;
            });

  /* (d) Dedup adjacent equal grefs.  Adjacent dedup is sufficient because
         step (c) sorted the array, so all copies of the same gref are
         already contiguous.  A node living in multiple layers shows up
         multiple times in the union; LOCK_X is per-row so we only need
         to lock it once. */
  size_t m = 0;
  for (size_t i = 0; i < n; i++) {
    if (m == 0 ||
        memcmp(write_set[m - 1], write_set[i], gref_len_local) != 0)
      write_set[m++] = write_set[i];
  }

  /* (e) Acquire LOCK_X on every neighbor in sorted order.
         ScopedLockWaitTimeout shortens the per-call timeout on slave
         apply so a stuck row does not wedge replication; user threads
         keep the global innodb_lock_wait_timeout. */
  ScopedLockWaitTimeout scoped_timeout_pf(graph->file, thd->slave_thread);
  for (size_t i = 0; i < m; i++) {
    /* ha_rnd_pos under F_WRLCK takes LOCK_X on the clustered row.
       InnoDB row locks are reentrant on the same trx_t, so the later
       ha_rnd_pos inside save_neighbors_only on this same row will not
       wait — the second call is effectively free. */
    int e = graph->file->ha_rnd_pos(graph->record[1],
                                    const_cast<uchar *>(write_set[i]),
                                    komodo::GM_NONE);
    if (e == HA_ERR_KEY_NOT_FOUND) {
      /* Benign TOCTOU: neighbor concurrently removed between graph
         traversal and now.  Skipping is safe — save_neighbors_only's
         own TOCTOU path (neighbor_save_skip_not_found) handles the
         stale entry correctly. */
      continue;
    }
    if (e == HA_ERR_LOCK_WAIT_TIMEOUT) {
      vidx_stats.prefetch_lock_timeout++;
      if (thd->slave_thread) {
        /* Slave path: swallow and tell caller to SKIP update_2nd
           ENTIRELY.  We must NOT silently skip just this one neighbor
           and continue: update_2nd would later try to lock that same
           row without the prefetch ordering guarantee, re-opening the
           ABBA window prefetch exists to close.  Skipping update_2nd
           leaves the in-memory graph slightly stale; the next touch
           (insert/update) on these neighbors will heal it. */
        vidx::log_slave_dml_warn(table, "prefetch_neighbor_locks",
                                 "prefetch_lock", e);
        *out_result = PrefetchResult::OkSkipUpdate2nd;
        return 0;
      }
      /* User path: bubble the timeout up so the attachable trx rolls
         back and the user's SQL retries cleanly. */
      return e;
    }
    if (e) return e;
  }
  return 0;
}

/* Cache-align helper for the three mhnsw_invalidate soft-skip exit
   branches (NOT_FOUND / slave LOCK_TIMEOUT / tref_mismatch).  Each
   branch decides the on-disk update is moot or not feasible, sets
   err=0, and bails — but the in-memory FVectorNode in the per-trx
   MHNSW_Share cache still has deleted=false, so subsequent ANN search
   in the same trx would rank the just-invalidated row as live.  Mark
   it deleted to align cache with the intended end-state.  OOM in
   ctx->get_node() is silently degraded (cache will refresh on next
   access); we cannot escalate because the caller has already decided
   to return err=0 from this path. */
void invalidate_align_cache(MHNSW_Share *ctx, const uchar *gref,
                            ulonglong &skip_counter) {
  skip_counter++;
  FVectorNode *node = ctx->get_node(gref);
  if (likely(node)) node->deleted = true;
}

/*
  Two-step lock-order unification + TOCTOU + soft-delete.  Pulled out of
  mhnsw_invalidate so its wrapper keeps the baseline (m83) shape.

    Step 1: MVCC read of IDX_TREF -> get gref, NO row lock.
    Step 2: ha_rnd_pos(gref) -> LOCK_X on GEN_CLUST only, TOCTOU
            check FIELD_TREF, ha_update_row to NULL FIELD_TREF, then
            mark the in-memory FVectorNode deleted.

  GEN_CLUST is the single lock dimension for all writes in the indep-DML
  attachable; combined with gref-ascending neighbor locking in
  update_second_degree_neighbors, every cross-trx wait-for edge through
  this code path is monotonic in gref order.  Residual cycles remain
  theoretically possible (clear_stale_tref_node, invoked from
  save_new_row on a dup-key collision, locks through IDX_TREF before
  reaching GEN_CLUST).  InnoDB's deadlock detector resolves those by
  picking a victim — non-zero Vidx_indep_dml_deadlock under load is the
  in-band signal.

  Replica-apply timeout tolerance is preserved on Step 2 only — the
  MVCC read in Step 1 cannot LOCK_WAIT.

  Inited bookkeeping: caller's scope.commit() asserts inited == NONE.
  The Scope_guard below collapses RND/INDEX -> NONE before any return.

  Pre-conditions:
    - graph->record[0]'s FIELD_TREF buffers writable (we set them
      from table->file->ref).
    - graph->file->inited == NONE.

  Returns 0 on success or skipped (NOT_FOUND / slave-tolerated timeout
  / TOCTOU mismatch); >0 on hard error (caller propagates).
*/
int invalidate_locate_and_softdelete(TABLE *table, TABLE *graph,
                                     MHNSW_Share *ctx) {
  THD *thd = table->in_use;
  handler *h = table->file;

  graph->field[FIELD_TREF]->set_notnull();
  graph->field[FIELD_TREF]->store((const char *)h->ref, h->ref_length,
                                  &my_charset_bin);

  /* Snapshot tref bytes for the TOCTOU consistency check at Step 2.
     record[0]'s FIELD_TREF was just populated above with h->ref, but
     subsequent ha_index_read_idx_map / ha_rnd_pos may overwrite the
     record buffers, so we keep an independent copy. */
  const uint tref_len = h->ref_length;
  uchar *saved_tref = (uchar *)alloca(tref_len);
  memcpy(saved_tref, h->ref, tref_len);

  uchar *key = (uchar *)alloca(graph->key_info[IDX_TREF].key_length);
  key_copy(key, graph->record[0], &graph->key_info[IDX_TREF],
           graph->key_info[IDX_TREF].key_length);

  /* Restore handler::inited to entry state on every return path so the
     caller's scope.commit() (which runs ha_external_lock asserting
     inited == NONE) stays happy.  Plain re-check is cheap; the call is
     a no-op when state is already aligned. */
  const auto saved_inited = graph->file->inited;
  Scope_guard restore_inited{[graph, saved_inited]() {
    if (graph->file->inited != saved_inited &&
        saved_inited == handler::NONE) {
      graph->file->ha_index_or_rnd_end();
    }
  }};

  /* Step 1: MVCC read of IDX_TREF -> get gref, NO row lock. */
  graph->file->set_consistent_read(true);
  int err = graph->file->ha_index_read_idx_map(
      graph->record[1], IDX_TREF, key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
  graph->file->set_consistent_read(false);
  if (err == HA_ERR_KEY_NOT_FOUND) {
    /* The node may legitimately not exist — skip the invalidate. */
    vidx_stats.invalidate_skip_not_found++;
    return 0;
  }
  if (err) {
    vidx::log_slave_dml_err(table, "mhnsw_invalidate",
                            "ha_index_read_idx_map(MVCC)", err);
    return err;
  }

  /* Capture gref of the row located via IDX_TREF.  position() writes
     ref into graph->file->ref; we keep a stable copy because the
     subsequent ha_rnd_pos() will overwrite ref. */
  graph->file->position(graph->record[1]);
  const uint gref_len = graph->file->ref_length;
  uchar *saved_gref = (uchar *)alloca(gref_len);
  memcpy(saved_gref, graph->file->ref, gref_len);

  /* Switch m_prebuilt->index back to clustered for the upcoming
     ha_rnd_pos.  restore_inited collapses inited from RND back to
     NONE on return. */
  if ((err = graph->file->ha_rnd_init(true))) {
    vidx::log_slave_dml_err(table, "mhnsw_invalidate", "ha_rnd_init", err);
    return err;
  }

  /* Step 2: ha_rnd_pos via captured gref.  LOCK_X on GEN_CLUST only. */
  const bool tolerate_lock_timeout = thd->slave_thread;
  /*
    Shorten innodb_lock_wait_timeout to hnsw_lock_timeout_ms (default
    1 s) when tolerating timeout, so collisions with concurrent
    attachable inner transactions fail fast on replica.
  */
  {
    ScopedLockWaitTimeout scoped_timeout(graph->file, tolerate_lock_timeout);
    err = graph->file->ha_rnd_pos(graph->record[1], saved_gref,
                                  komodo::GM_NONE);
  }
  if (err == HA_ERR_KEY_NOT_FOUND) {
    /* InnoDB does not reuse ROW_IDs (dict0boot.cc:225), so the gref
       cannot point at a different row; the only way ha_rnd_pos can
       miss is that the row was hard-deleted between Step 1 and Step
       2 — on-disk invalidate is moot. */
    invalidate_align_cache(ctx, saved_gref,
                           vidx_stats.invalidate_skip_not_found);
    return 0;
  }
  if (err == HA_ERR_LOCK_WAIT_TIMEOUT && tolerate_lock_timeout) {
    /* Slave-only soft skip: on-disk row left untouched (orphan
       tolerance handles the divergence on the next read/insert that
       reaches it); cache aligned to the intended end-state. */
    invalidate_align_cache(ctx, saved_gref,
                           vidx_stats.invalidate_skip_read_timeout);
    vidx::log_slave_dml_warn(table, "mhnsw_invalidate", "ha_rnd_pos", err);
    return 0;
  }
  if (err) {
    vidx::log_slave_dml_err(table, "mhnsw_invalidate", "ha_rnd_pos", err);
    return err;
  }

  /* TOCTOU consistency check: FIELD_TREF in the locked row must still
     match saved_tref.  A concurrent invalidate may have NULLified it
     (or rewritten it) between Step 1 and Step 2; in that case the
     on-disk state already reflects the intended invalidate, and we
     just align the in-memory cache. */
  Field *tref_field = graph->field[FIELD_TREF];
  const ptrdiff_t row1_diff = graph->record[1] - graph->record[0];
  tref_field->move_field_offset(row1_diff);
  bool tref_mismatch = false;
  if (tref_field->is_null()) {
    tref_mismatch = true;
  } else {
    String tmp;
    tref_field->val_str(&tmp);
    if (tmp.length() != tref_len ||
        memcmp(tmp.ptr(), saved_tref, tref_len) != 0) {
      tref_mismatch = true;
    }
  }
  tref_field->move_field_offset(-row1_diff);

  if (tref_mismatch) {
    /* Concurrent invalidate already NULLed (or rewrote) FIELD_TREF
       between Step 1 and Step 2; on-disk state already reflects the
       intended invalidate. */
    invalidate_align_cache(ctx, saved_gref,
                           vidx_stats.invalidate_skip_tref_changed);
    return 0;
  }

  restore_record(graph, record[1]);
  graph->field[FIELD_TREF]->set_null();
  if ((err = graph->file->ha_update_row(graph->record[1],
                                        graph->record[0])) > 0) {
    /*
      Tolerate lock wait timeout on the soft-delete update — ONLY for
      replica apply threads.  The on-disk TREF stays non-NULL (orphan
      row), handled by orphan-row treatment.  We still fall through to
      update the in-memory cache since we already have the record data
      from the successful read above.
    */
    if (err == HA_ERR_LOCK_WAIT_TIMEOUT && tolerate_lock_timeout) {
      vidx_stats.invalidate_skip_update_timeout++;
      vidx::log_slave_dml_warn(table, "mhnsw_invalidate", "ha_update_row",
                               err);
      err = 0;
      /* fall through to update in-memory cache */
    } else {
      vidx::log_slave_dml_err(table, "mhnsw_invalidate", "ha_update_row",
                              err);
      return err;
    }
  }

  /* InnoDB ROW_ID is stable across reads, so saved_gref is the same
     ref the cache uses to key the node. */
  FVectorNode *node = ctx->get_node(saved_gref);
  if (unlikely(!node)) {
    vidx::log_slave_dml_err(table, "mhnsw_invalidate", "ctx->get_node",
                            HA_ERR_OUT_OF_MEM);
    return HA_ERR_OUT_OF_MEM;
  }
  node->deleted = true;
  return 0;
}

/*
  Drain Search_context::found from pos forward, skipping orphan refs
  produced by indep DML's atomicity contract: the aux-graph entry can
  outlive the base row (e.g. invalidate committed independently, then
  the user statement fails and rolls back; or TREF soft-deleted nodes
  whose base row is still visible to the search snapshot).
  ha_rnd_pos returns HA_ERR_KEY_NOT_FOUND / HA_ERR_RECORD_DELETED for
  those — without this drain a single orphan would bubble up to
  IndexScanIterator::HandleError, which treats it as end-of-scan and
  truncates the LIMIT prematurely (see Docs/ann_recall_shrinkage_
  investigation.md §3).  Each skip is accounted in
  Vidx_read_skip_orphan so operators can spot sustained orphan
  pressure.

  Returns:
    0                   - real row positioned (table->record[0] ready).
    HA_ERR_END_OF_FILE  - drain reached pos == found.num without
                          finding a real row; caller decides whether
                          to re-search or terminate.
    <other err>         - hard error from ha_rnd_pos; propagate.
*/
int drain_skipping_orphans(TABLE *table, Search_context *result) {
  while (result->pos < result->found.num) {
    uchar *ref = result->found.links[result->pos++]->tref();
    int err = table->file->ha_rnd_pos(table->record[0], ref, komodo::GM_NONE);
    if (err == HA_ERR_KEY_NOT_FOUND || err == HA_ERR_RECORD_DELETED) {
      vidx_stats.read_skip_orphan++;
      continue;
    }
    return err;
  }
  return HA_ERR_END_OF_FILE;
}

/**
  Soft-delete the auxiliary-table node whose TREF matches @p record[0].

  Two-step lock-order unification:

    Step 1: MVCC read of IDX_TREF -> get stale_gref, NO row lock.
    Step 2: ha_rnd_pos(stale_gref) -> LOCK_X on GEN_CLUST only,
            then TOCTOU check + ha_update_row to NULL out FIELD_TREF.

  Eliminates the legacy "IDX_TREF -> GEN_CLUST" lock order on the
  dup-key recovery path, aligning it with the rest of indep-DML
  writes that all start from GEN_CLUST and avoid cross-index cycles.

  After the secondary-index lookup we still call ha_rnd_init(true)
  to restore m_prebuilt->index to the clustered index for the
  subsequent ha_rnd_pos() and any continuing RND scan in the caller
  (mhnsw_insert).  scan=true satisfies the handler assertion:
    assert(inited == NONE || (inited == RND && scan))

  TOCTOU outcomes that take the silent-skip path (return 0):
    - HA_ERR_KEY_NOT_FOUND on Step 1 lookup: stale already cleaned.
    - HA_ERR_KEY_NOT_FOUND on Step 2 ha_rnd_pos: stale row gone.
    - FIELD_TREF in the locked row no longer matches saved value:
      another concurrent trx already cleared/rewrote it.

  @param graph  auxiliary TABLE
  @return 0 on success or skipped, non-zero on error
*/
static int clear_stale_tref_node(TABLE *graph) {
  /* Called from save_new_row when ha_write_row hit IDX_TREF dup-key:
     a stale node still carries the TREF we want to (re-)assign to the
     freshly-inserted node.  Clear that stale node's FIELD_TREF (soft
     delete) so our INSERT can proceed. */

  /* (a) Build the IDX_TREF lookup key from record[0]. */
  uchar *tref_key = (uchar *)alloca(graph->key_info[IDX_TREF].key_length);
  key_copy(tref_key, graph->record[0], &graph->key_info[IDX_TREF],
           graph->key_info[IDX_TREF].key_length);

  /* (b) Snapshot record[0] before any handler call clobbers it, then
         install a guard that restores record[0] from the snapshot on
         EVERY exit path (success / error / TOCTOU skip) and frees the
         alloca buffer.  Without this guard the function would carry
         eight identical "memcpy(record[0], saved, ...)" lines scattered
         across early-return sites.  The snapshot is filled BEFORE the
         guard is installed so the dtor never reads uninitialised bytes
         even on a hypothetical exception between alloca and snapshot
         (memcpy itself is noexcept). */
  const size_t saved_sz = graph->s->reclength;
  auto *saved = static_cast<uchar *>(my_safe_alloca(saved_sz, MAX_ALLOCA_SZ));
  memcpy(saved, graph->record[0], saved_sz);
  Scope_guard saved_guard{[graph, saved, saved_sz]() {
    memcpy(graph->record[0], saved, saved_sz);
    my_safe_afree(saved, saved_sz, MAX_ALLOCA_SZ);
  }};

  /* (c) Snapshot the TREF bytes we are about to clear, for the
         post-lock TOCTOU check (Step 2 below).  If a concurrent trx
         already cleared/rewrote this TREF before we got LOCK_X, the
         locked row's FIELD_TREF won't match what we saved here and we
         skip the soft-delete. */
  Field *tref_field = graph->field[FIELD_TREF];
  const bool saved_tref_was_null = tref_field->is_null();
  uint saved_tref_len = 0;
  uchar *saved_tref = nullptr;
  if (!saved_tref_was_null) {
    String tmp;
    tref_field->val_str(&tmp);
    saved_tref_len = tmp.length();
    saved_tref = (uchar *)alloca(saved_tref_len);
    memcpy(saved_tref, tmp.ptr(), saved_tref_len);
  }

  /* (d) End any active RND scan owned by mhnsw_insert.  ha_index_read_
         idx_map below asserts inited == NONE; the matching ha_rnd_init
         (true) calls farther down restore the scan state. */
  graph->file->ha_index_or_rnd_end();

  /* ============== Step 1 ==============
     MVCC read of IDX_TREF — finds the stale row, takes NO lock.
     Reads into record[1] to keep record[0] untouched. */
  graph->file->set_consistent_read(true);
  int lookup = graph->file->ha_index_read_idx_map(
      graph->record[1], IDX_TREF, tref_key, HA_WHOLE_KEY, HA_READ_KEY_EXACT);
  graph->file->set_consistent_read(false);

  if (lookup == HA_ERR_KEY_NOT_FOUND) {
    /* TOCTOU outcome 1: another concurrent trx already cleared this
       TREF entry.  Restore the cursor; saved_guard restores record[0].
       Tell caller "no work to do" so it retries ha_write_row. */
    int re = graph->file->ha_rnd_init(true);
    if (re) {
      vidx::log_dml_warn(graph, "clear_stale_tref_node", "ha_rnd_init", re);
      return re;
    }
    return 0;
  }
  if (lookup) {
    /* Lookup error other than NOT_FOUND (lock_wait_timeout etc.).
       Restore cursor before bubbling up so caller's epilogue sees a
       valid handler state; saved_guard handles record[0]. */
    int re = graph->file->ha_rnd_init(true);
    if (re) {
      vidx::log_dml_warn(graph, "clear_stale_tref_node", "ha_rnd_init", re);
      return re;
    }
    vidx::log_dml_warn(graph, "clear_stale_tref_node", "stale_node_lookup",
                       lookup);
    return lookup;
  }

  /* (e) Capture the stale row's gref (clustered ref) before any later
         handler call overwrites graph->file->ref.  This gref is what
         Step 2 uses to take LOCK_X directly on the clustered index. */
  graph->file->position(graph->record[1]);
  const uint gref_len = graph->file->ref_length;
  uchar *stale_gref = (uchar *)alloca(gref_len);
  memcpy(stale_gref, graph->file->ref, gref_len);

  /* (f) Switch m_prebuilt->index back to clustered so ha_rnd_pos sees
         the right index, and so any continuing RND scan in the caller
         resumes correctly.  scan=true satisfies the handler invariant
         "inited == NONE || (inited == RND && scan)". */
  int err = graph->file->ha_rnd_init(true);
  if (err) {
    vidx::log_dml_warn(graph, "clear_stale_tref_node", "ha_rnd_init", err);
    return err;
  }

  /* ============== Step 2 ==============
     ha_rnd_pos via stale_gref — takes LOCK_X on GEN_CLUST only (no
     IDX_TREF lock involved).  This is the lock-order unification: the
     entire indep-DML write side now starts locking from the clustered
     index, eliminating the legacy "IDX_TREF -> GEN_CLUST" cross-index
     wait that produced ABBA cycles. */
  err = graph->file->ha_rnd_pos(graph->record[1], stale_gref, komodo::GM_NONE);
  if (err == HA_ERR_KEY_NOT_FOUND) {
    /* TOCTOU outcome 2: between Step 1 and Step 2 the stale row was
       physically deleted (purge or hard-delete).  Nothing to clear;
       saved_guard restores record[0]. */
    return 0;
  }
  if (err) {
    vidx::log_dml_warn(graph, "clear_stale_tref_node", "ha_rnd_pos", err);
    return err;
  }

  /* (g) TOCTOU check 3: even though we now hold LOCK_X on the clustered
         row, the FIELD_TREF in it may have been rewritten by another
         trx between our MVCC read in Step 1 and our X-lock acquisition
         in Step 2.  Compare against the snapshot we took at (c).
         move_field_offset temporarily re-points the Field object at
         record[1] for the read; we restore it before returning. */
  const ptrdiff_t row1_diff = graph->record[1] - graph->record[0];
  tref_field->move_field_offset(row1_diff);
  bool tref_mismatch = false;
  if (tref_field->is_null()) {
    tref_mismatch = !saved_tref_was_null;
  } else if (saved_tref_was_null) {
    tref_mismatch = true;
  } else {
    String tmp;
    tref_field->val_str(&tmp);
    if (tmp.length() != saved_tref_len ||
        memcmp(tmp.ptr(), saved_tref, saved_tref_len) != 0) {
      tref_mismatch = true;
    }
  }
  tref_field->move_field_offset(-row1_diff);

  if (tref_mismatch) {
    /* TOCTOU outcome 3: another trx already did the equivalent work
       (cleared this TREF or moved it to a different node).  We have
       nothing left to clear here; saved_guard restores record[0]. */
    return 0;
  }

  /* (h) Soft-delete: build "after-image" in record[0] = before-image
         (record[1]) with FIELD_TREF set to NULL, then ha_update_row.
         restore_record is the standard MySQL idiom for "copy row[1]
         into row[0]".  HA_ERR_RECORD_IS_THE_SAME means the engine
         optimised the no-op away — treat as success. */
  restore_record(graph, record[1]);
  graph->field[FIELD_TREF]->set_null();
  err = graph->file->ha_update_row(graph->record[1], graph->record[0]);
  if (err == HA_ERR_RECORD_IS_THE_SAME) err = 0;
  if (err) {
    vidx::log_dml_warn(graph, "clear_stale_tref_node", "soft-delete", err);
    return err;
  }

  /* saved_guard restores caller's record[0] on return; the soft-delete
     itself is committed via the enclosing attachable trx. */
  return 0;
}

/*
  Insert a brand-new target row into the auxiliary graph table.  Pairs
  with save_neighbors_only: this one creates rows (only used by
  mhnsw_insert), the other rewrites just the NEIGHBORS column on
  existing rows (used by update_second_degree_neighbors).

  Pre-condition: stored == false (this is a fresh target whose gref will
  be allocated by InnoDB's ha_write_row).  On success: stored becomes
  true, gref is captured via position(), and the node is registered in
  the trx-local cache.
*/
int FVectorNode::save_new_row(TABLE *graph) {
  DBUG_TRACE;
  assert(vec);
  assert(neighbors);
  assert(!stored);

  /* Snapshot inited state on entry — callers may enter with inited ==
     NONE (e.g. the first-insert path in mhnsw_insert when the graph is
     empty) and expect that on return; the pre-check and
     clear_stale_tref_node detours below switch the cursor between RND
     and INDEX scans, so we must restore before unwinding to avoid the
     `inited == NONE || open_by_handler` assert in ha_external_lock. */
  const auto saved_inited = graph->file->inited;

  restore_record(graph, s->default_values);
  graph->field[FIELD_LAYER]->store(max_layer, false);
  if (deleted)
    graph->field[FIELD_TREF]->set_null();
  else {
    graph->field[FIELD_TREF]->set_notnull();
    graph->field[FIELD_TREF]->store((const char *)tref(), tref_len(),
                                    &my_charset_bin);
  }
  graph->field[FIELD_VEC]->store((const char *)vec->data(),
                                 FVector::data_size(ctx->vec_len),
                                 &my_charset_bin);

  size_t total_size = 0;
  for (size_t i = 0; i <= max_layer; i++) {
    if (unlikely(neighbors[i].num > 255)) return HA_ERR_CRASHED;
    total_size += 1 + gref_len() * neighbors[i].num;
  }

  auto *neighbor_blob =
      static_cast<uchar *>(my_safe_alloca(total_size, MAX_ALLOCA_SZ));
  Scope_guard neighbor_blob_guard{
      [neighbor_blob, total_size]() { my_safe_afree(neighbor_blob, total_size, MAX_ALLOCA_SZ); }};
  uchar *ptr = neighbor_blob;
  for (size_t i = 0; i <= max_layer; i++) {
    *ptr++ = (uchar)(neighbors[i].num);
    for (size_t j = 0; j < neighbors[i].num; j++, ptr += gref_len())
      memcpy(ptr, neighbors[i].links[j]->gref(), gref_len());
  }
  graph->field[FIELD_NEIGHBORS]->store((const char *)neighbor_blob, total_size,
                                       &my_charset_bin);

  int err;
  /*
    IDX_TREF pre-check: MVCC-read IDX_TREF to detect tref collisions
    before ha_write_row.  If a stale row already occupies our tref,
    clear it via the GEN_CLUST-first clear_stale_tref_node so the
    subsequent ha_write_row no longer triggers InnoDB's internal
    dup-check — which under RC + skip_gap_locks would acquire LOCK_S
    NOT_GAP on the stale row's IDX_TREF entry, the only residual
    cross-index lock left after the lock-order unification.  TOCTOU
    between this pre-check and ha_write_row is still covered by the
    original dup-key fallback below.

    Skip pre-check when FIELD_TREF is NULL: NULL entries in a UNIQUE
    secondary index are not enforced as unique, so dup-check cannot
    fire on a NULL insert.
  */
  bool has_tref = !graph->field[FIELD_TREF]->is_null();
  if (has_tref) {
    uchar *pre_key = (uchar *)alloca(graph->key_info[IDX_TREF].key_length);
    key_copy(pre_key, graph->record[0], &graph->key_info[IDX_TREF],
             graph->key_info[IDX_TREF].key_length);

    /* ha_index_read_idx_map asserts inited == NONE.  The function
       epilogue below restores saved_inited so any side-effect on
       scan state is invisible to the caller. */
    graph->file->ha_index_or_rnd_end();

    graph->file->set_consistent_read(true);
    int found = graph->file->ha_index_read_idx_map(
        graph->record[1], IDX_TREF, pre_key, HA_WHOLE_KEY,
        HA_READ_KEY_EXACT);
    graph->file->set_consistent_read(false);

    /* If the lookup didn't hit dup, we still need m_prebuilt->index
       restored to clustered for any subsequent ha_write_row /
       ha_rnd_pos.  ha_rnd_init(true) does that; the function epilogue
       collapses inited back to saved_inited. */
    if ((err = graph->file->ha_rnd_init(true))) goto done;

    if (found == 0) {
      vidx_stats.save_retry_dup_key++;
      if ((err = clear_stale_tref_node(graph))) goto done;
    } else if (found != HA_ERR_KEY_NOT_FOUND) {
      err = found;
      goto done;
    }
    /* found == HA_ERR_KEY_NOT_FOUND: fall through to ha_write_row */
  }

  err = graph->file->ha_write_row(graph->record[0]);
  if (err == HA_ERR_FOUND_DUPP_KEY) {
    /* TOCTOU fallback: another trx inserted the same tref between our
       pre-check (or first attempt for the no-tref branch) and this
       ha_write_row.  Clear via GEN_CLUST-first path and retry once. */
    vidx_stats.save_retry_dup_key++;
    if ((err = clear_stale_tref_node(graph))) goto done;
    err = graph->file->ha_write_row(graph->record[0]);
  }
  if (err) goto done;
  /* Only update gref/state after a confirmed successful insert.
     If ha_write_row() fails we must NOT call position(), copy ref, set
     stored=true, or cache the node: position() after a failed write is
     undefined, the copied ref would be garbage, and caching with an
     invalid gref would later cause ha_rnd_pos() failures and graph
     traversal inconsistencies.  Leaving stored=false lets callers
     propagate the error cleanly without any partially-updated state. */
  graph->file->position(graph->record[0]);
  memcpy(gref(), graph->file->ref, gref_len());
  stored = true;
  ctx->cache_node(this);
done:
  /* Restore caller-visible inited.  The pre-check and clear_stale_tref_node
     paths leave inited == RND (because m_prebuilt->index needs to be
     clustered for downstream ha_rnd_pos / ha_update_row).  If the caller
     entered with NONE (first-insert branch in mhnsw_insert), collapse
     RND back to NONE so ha_external_lock(F_UNLCK) does not assert. */
  if (graph->file->inited != saved_inited && saved_inited == handler::NONE) {
    graph->file->ha_index_or_rnd_end();
  }
  return err;
}

/*
  NEIGHBORS-only update path for update_second_degree_neighbors.

  Why split out from save_new_row:
  save_new_row always rewrites FIELD_TREF from this->tref() — the
  per-MHNSW_Trx cache snapshot taken when the node was first loaded.
  If the on-disk FIELD_TREF was concurrently NULLed by another trx's
  invalidate, that write "revives" the stale IDX_TREF entry, which any
  subsequent ha_write_row of the same tref then collides with as
  ER_DUP_KEY (1022).

  save_neighbors_only() takes the disk before-image (record[1]) as the
  authority for FIELD_TREF/FIELD_VEC/FIELD_LAYER and only overwrites
  FIELD_NEIGHBORS.  IDX_TREF entries are never touched here, so this
  function cannot create or remove a tref UNIQUE entry — eliminating the
  cross-trx neighbor-revive race that produces dup_key under concurrent
  attachable-DML writers.

  Prerequisites (gref-ordered prefetch protocol):
   - Caller (update_second_degree_neighbors) ran inside the gref-ordered
     prefetch loop in mhnsw_insert, so this row's GEN_CLUST X-lock is
     already held by the current attachable_trx_rw (reentrant).
   - graph->file->inited == RND (set by ha_rnd_init(0) in mhnsw_insert).
*/
int FVectorNode::save_neighbors_only(TABLE *graph) {
  DBUG_TRACE;
  assert(stored);
  assert(neighbors);

  vidx_stats.neighbor_save_count++;

  /* Load the current on-disk row into record[1]. */
  int err = graph->file->ha_rnd_pos(graph->record[1], gref(), komodo::GM_NONE);
  if (err == HA_ERR_KEY_NOT_FOUND) {
    /* Row was hard-deleted between the gref-ordered prefetch and now
       (rare).  Skip the neighbor update — graph stays slightly stale,
       repaired on the next traversal that hits this neighbor. */
    vidx_stats.neighbor_save_skip_not_found++;
    return 0;
  }
  if (err) return err;

  /* record[0] = record[1] (preserve disk FIELD_TREF / FIELD_VEC /
     FIELD_LAYER as-is — do NOT trust the per-Trx cache snapshot here). */
  restore_record(graph, record[1]);

  /* Re-encode the in-memory neighbor list into FIELD_NEIGHBORS only. */
  size_t total_size = 0;
  for (size_t i = 0; i <= max_layer; i++) {
    if (unlikely(neighbors[i].num > 255)) return HA_ERR_CRASHED;
    total_size += 1 + gref_len() * neighbors[i].num;
  }

  auto *neighbor_blob =
      static_cast<uchar *>(my_safe_alloca(total_size, MAX_ALLOCA_SZ));
  Scope_guard neighbor_blob_guard{
      [neighbor_blob, total_size]() {
        my_safe_afree(neighbor_blob, total_size, MAX_ALLOCA_SZ);
      }};
  uchar *ptr = neighbor_blob;
  for (size_t i = 0; i <= max_layer; i++) {
    *ptr++ = (uchar)(neighbors[i].num);
    for (size_t j = 0; j < neighbors[i].num; j++, ptr += gref_len())
      memcpy(ptr, neighbors[i].links[j]->gref(), gref_len());
  }
  graph->field[FIELD_NEIGHBORS]->store((const char *)neighbor_blob, total_size,
                                       &my_charset_bin);

  err = graph->file->ha_update_row(graph->record[1], graph->record[0]);
  if (err == HA_ERR_RECORD_IS_THE_SAME) err = 0;
  return err;
}

/* -------------------- Preload (dbms_vidx.preload) -------------------- */
/*
  Single source of the per-node in-memory footprint, kept in lockstep with
  what alloc_node_internal() + alloc_vec() + alloc_neighborhood() request
  from the node arena (root) and the vector payload arena; that lockstep
  is what makes the pre-flight estimates trustworthy enough to hard-block
  and to size builds on.  Both estimators below derive from it:

  - fixed chunk: FVectorNode object + gref + tref + vector (FP16, SIMD
    aligned via FVector::alloc_size -- the same function the real
    allocator uses, so the AVX2/AVX512 padding is accounted for);
  - adjacency chunk: affine in the layer (a layer-0 base plus a constant
    increment per extra layer), identical formula to alloc_neighborhood.

  @p layer may be fractional: the adjacency term is affine, so an
  expected layer yields the exact expected footprint (prebuild), while an
  integral measured layer reproduces the exact allocator request
  (preload).
*/
static double node_footprint_bytes(size_t dimensions, uint m, uint gref_len,
                                   uint tref_len, double layer) {
  const double fixed = sizeof(FVectorNode) + gref_len + tref_len +
                       FVector::alloc_size(dimensions);
  const double layer0 = sizeof(Neighborhood) +
                        sizeof(FVectorNode *) * (MY_ALIGN(m, 4) * 2);
  const double per_layer =
      sizeof(Neighborhood) + sizeof(FVectorNode *) * MY_ALIGN(m, 8);

  return fixed + layer0 + per_layer * layer;
}

/* Per-node footprint of one existing graph row.  Preload always runs on a
   shared ctx, i.e. the inline payload layout (evictable_vectors is a
   synchronous-DDL-only mode); every term is an exact integer below 2^53,
   so the double round-trip is lossless. */
static size_t preload_node_bytes(const MHNSW_Share *ctx, uint8_t layer) {
  return static_cast<size_t>(node_footprint_bytes(
      ctx->vec_len, ctx->M, ctx->gref_len, ctx->tref_len, layer));
}

/*
  Overhead / clamp constants for the preload estimator.  PRELOAD_OVERHEAD
  scales the raw per-node sum up by 10% so the estimate is a reliable UPPER
  bound on the real cache_allocated_size() (measured ~7% tail padding in
  init_sql_alloc's 1MB chunks on a 1M-row / 768-dim / M=16 graph).  Keeping
  it an upper bound preserves the invariant "Gate 1 passes => Gate 3 never
  fires": an accepted preload never aborts half-way with a cap overshoot.
*/
constexpr size_t PRELOAD_OVERHEAD_NUM = 110;
constexpr size_t PRELOAD_OVERHEAD_DEN = 100;

/* FIELD_LAYER is on-disk data; clamp it before it drives the
   neighbourhood-size arithmetic in preload_node_bytes(). */
constexpr longlong HNSW_MAX_LAYER = 100;

/*
  Stream-style writer for the optional err_reason string.  Builds a message
  through operator<< and assigns it to the destination when the temporary
  is destroyed at the end of the full expression.  A null destination
  discards everything, so call sites need no guard and no printf format
  string / width casts:

      set_reason(err_reason) << "no vector index on column '" << col << "'";
*/
class set_reason {
 public:
  explicit set_reason(std::string *dst) : m_dst(dst) {}
  set_reason(const set_reason &) = delete;
  set_reason &operator=(const set_reason &) = delete;
  ~set_reason() {
    if (m_dst != nullptr) *m_dst = m_os.str();
  }
  template <typename T>
  set_reason &operator<<(const T &value) {
    if (m_dst != nullptr) m_os << value;
    return *this;
  }

 private:
  std::string *const m_dst;
  std::ostringstream m_os;
};

/*
  Preload reads the graph as a stand-alone read-only operation
  (MHNSW_Share::acquire(for_update=false) + an F_RDLCK hlindex lock), so it
  does NOT need set_consistent_read(): that flag exists to make the DML-path
  graph reads (load_entry_point_node, mhnsw_invalidate) take an MVCC
  snapshot WITHOUT row locks while running inside a write transaction, which
  does not apply here.  The rnd-scan cursor is paired with ha_rnd_end via a
  Scope_guard at each call site.
*/

/*
  Pick which vector index to preload: the vector index whose first key part
  matches @p vec_column case-insensitively.  @p vec_column is non-empty (the
  SQL layer rejects an empty column).  Returns 0 and sets *out_vkey on a
  match, else HA_ERR_UNSUPPORTED with a filled-in err_reason.

  NB: a table currently carries at most one vector index (DDL rejects more
  with ER_NOT_SUPPORTED_YET); matching by column name keeps this correct for
  free once multiple vector indexes per table are supported.
*/
static int resolve_vector_key(const TABLE *table, const char *vec_column,
                              uint *out_vkey, std::string &err_reason) {
  const uint key_end = table->s->total_keys;
  for (uint k = table->s->keys; k < key_end; k++) {
    if (!(table->key_info[k].flags & HA_VECTOR)) continue;
    const Field *f = table->key_info[k].key_part[0].field;
    if (f != nullptr && f->field_name != nullptr &&
        my_strcasecmp(system_charset_info, f->field_name, vec_column) == 0) {
      *out_vkey = k;
      return 0;
    }
  }
  set_reason(&err_reason) << "no vector index on column '" << vec_column << "'";
  return HA_ERR_UNSUPPORTED;
}

/*
  Pass 1 (estimate): scan every graph row and accumulate the exact un-scaled
  per-node footprint via preload_node_bytes().  Returns 0 on a clean full
  scan and fills *out_raw_bytes / *out_rows.
*/
static int estimate_graph_footprint(TABLE *graph, const MHNSW_Share *ctx,
                                    size_t *out_raw_bytes, ha_rows *out_rows,
                                    std::string &err_reason) {
  if (int err = graph->file->ha_rnd_init(true)) {
    set_reason(&err_reason) << "ha_rnd_init failed (estimate pass)";
    return err;
  }
  Scope_guard rnd_end{[graph]() { graph->file->ha_rnd_end(); }};

  size_t raw = 0;
  ha_rows rows = 0;
  int err;
  while ((err = graph->file->ha_rnd_next(graph->record[0])) == 0) {
    longlong layer = graph->field[FIELD_LAYER]->val_int();
    if (layer < 0)
      layer = 0;
    else if (layer > HNSW_MAX_LAYER)
      layer = HNSW_MAX_LAYER;
    raw += preload_node_bytes(ctx, static_cast<uint8_t>(layer));
    rows++;
  }
  if (err != HA_ERR_END_OF_FILE) {
    set_reason(&err_reason) << "scan failed during estimate pass";
    return err;
  }
  *out_raw_bytes = raw;
  *out_rows = rows;
  return 0;
}

/*
  Pass 2 (load): materialise every graph node into the shared cache.  After
  each node Gate 3 re-checks the live allocation against the cap to catch an
  estimate undershoot.  Returns 0 on a clean full scan.
*/
static int load_graph_nodes(TABLE *graph, MHNSW_Share *ctx, ha_rows total_rows,
                            std::string &err_reason) {
  if (int err = graph->file->ha_rnd_init(true)) {
    set_reason(&err_reason) << "ha_rnd_init failed (load pass)";
    return err;
  }
  Scope_guard rnd_end{[graph]() { graph->file->ha_rnd_end(); }};

  ha_rows loaded = 0;
  int err;
  while ((err = graph->file->ha_rnd_next(graph->record[0])) == 0) {
    graph->file->position(graph->record[0]);
    FVectorNode *node = ctx->get_node(graph->file->ref);
    if (unlikely(node == nullptr)) {
      set_reason(&err_reason)
          << "out of memory after loading " << loaded << "/" << total_rows
          << " nodes (cache used " << ctx->cache_allocated_size()
          << " bytes, cap " << max_cache_size << " bytes)";
      return HA_ERR_OUT_OF_MEM;
    }
    /* load_from_record reads record[0], positioned just above. */
    if ((err = node->load_from_record(graph)) != 0) {
      ctx->uncache_node(node);  /* drop the half-initialised stub */
      set_reason(&err_reason) << "failed to decode a graph node";
      return err;
    }
    loaded++;

    /* Gate 3: live allocation overshot the cap (estimate undershoot). */
    if (ctx->cache_allocated_size() > max_cache_size) {
      set_reason(&err_reason)
          << "cache exceeded vidx_hnsw_cache_size while loading: used "
          << ctx->cache_allocated_size() << " bytes after " << loaded << "/"
          << total_rows << " nodes, cap " << max_cache_size
          << " bytes (estimate undershot); raise vidx_hnsw_cache_size and "
             "retry";
      return HA_ERR_OUT_OF_MEM;
    }
  }
  if (err != HA_ERR_END_OF_FILE) {
    set_reason(&err_reason) << "scan failed during load pass";
    return err;
  }
  return 0;
}

/*
  Full-graph cache preload backing dbms_vidx.preload(table).

  Orchestrates four steps, each delegated to a focused helper:
    1. resolve_vector_key()       pick the vector index (by column or sole)
    2. hlindex_open/lock + acquire bind the graph handler and shared ctx
    3. estimate_graph_footprint() + Gate 1 (capacity): refuse up-front if
       the scaled estimate exceeds max_cache_size -- loading past the cap is
       pointless because release() resets() the whole graph afterwards.
    4. load_graph_nodes()         materialise every node (Gate 3 re-checks
       the live allocation to catch an estimate undershoot).

  On any "won't fit" / "didn't fit" / scan-failure condition the relevant
  helper fills @p err_reason and returns non-zero (caller surfaces it via
  ER_VECTOR_INDEX_USAGE).

  @p vec_column names the indexed column of the vector index to preload.
  When null/empty the single vector index on the table is used (error if
  there is more than one); passing the column is what makes this usable
  once a table can carry multiple vector indexes.

  When @p check_only is true, Step 4 is skipped: the function runs the
  estimate pass and Gate 1 only, returning 0 if the graph would fit (so the
  caller can validate capacity without loading) and the same OUT_OF_MEM
  diagnostic if it would not.  Note this is NOT a zero-allocation probe:
  Step 2's acquire() still loads the single entry point node on a cold
  share; check_only only suppresses the full-graph materialisation of
  Step 4.

  On success @p stats carries the outcome (see Preload_stats); its fields
  are zero-initialised so an early failure still leaves well-defined values.
*/
int mhnsw_preload(TABLE *table, const char *vec_column, bool check_only,
                  Preload_stats &stats, std::string &err_reason) {
  DBUG_TRACE;

  stats = Preload_stats{};

  /* ---- Step 1: pick the vector index and open its auxiliary graph ----
     The base table's auxiliary graph (table->hlindex_active) is opened on
     demand, not by open_and_lock_tables; mirror TABLE::hlindexes_on_insert(). */
  if (table->s->hlindexes() < 1) {
    set_reason(&err_reason) << "table has no vector index";
    return HA_ERR_UNSUPPORTED;
  }
  uint vkey = 0;
  if (int err = resolve_vector_key(table, vec_column, &vkey, err_reason))
    return err;

  if (int err = table->hlindex_open(vkey)) {
    set_reason(&err_reason) << "failed to open vector auxiliary table";
    return err;
  }
  TABLE *graph = table->hlindex_active;
  if (graph == nullptr) {
    set_reason(&err_reason) << "table has no vector index";
    return HA_ERR_UNSUPPORTED;
  }

  /* ---- Step 2: lock the graph handler and acquire the shared context ----
     hlindex_lock binds the graph handler to this THD (sets graph->in_use,
     ha_external_lock(F_RDLCK) to associate the InnoDB prebuilt with the
     current trx).  Without it ha_rnd_init dereferences an unbound prebuilt
     and fails with a spurious OOM.  Mirrors TABLE::hlindex_read_first. */
  if (int err = table->hlindex_lock(vkey)) {
    set_reason(&err_reason) << "failed to lock vector auxiliary table";
    return err;
  }
  Scope_guard hlindex_unlock{[table, graph]() {
    if (graph->in_use) {
      graph->file->ha_external_lock(table->in_use, F_UNLCK);
      graph->in_use = nullptr;
    }
  }};

  /* acquire(for_update=false): shared MHNSW_Share path; sets vec_len /
     byte_len via load_entry_point_node and takes a commit_lock rdlock +
     refcnt that the matching release() drops.  acquire() ref-counts ctx
     and takes the rdlock on success AND on the empty-graph END_OF_FILE
     path; only the get_from_share OOM path leaves ctx null.  Build the
     release guard IMMEDIATELY -- before the err checks -- so every early
     return unwinds the refcount + rdlock (mirrors mhnsw_read_first);
     otherwise a partial acquire would leak the commit_lock rdlock and
     permanently block later DML do_commit (which needs the wrlock). */
  MHNSW_Share *ctx = nullptr;
  int err = MHNSW_Share::acquire(&ctx, table, false);
  Scope_guard ctx_guard{[&ctx, table]() {
    if (ctx) ctx->release(table);
  }};
  if (err == HA_ERR_END_OF_FILE) return 0;  /* empty graph: trivial success */
  if (err) {
    set_reason(&err_reason) << "failed to acquire vector index context";
    return err;
  }
  /* No vectors yet (empty/uninitialised graph): nothing to preload, treat
     as a trivial success.  Mirror the END_OF_FILE path -- do NOT set
     err_reason, since a zero return means success and the caller only
     surfaces err_reason on failure. */
  if (ctx->vec_len == 0) return 0;

  /* ---- Step 3: Pass 1 estimate + Gate 1 (capacity) ----
     Scale the raw per-node sum up to a reliable UPPER bound, then refuse
     up-front if it would not fit: loading past the cap is pointless because
     release() resets() the whole graph afterwards. */
  size_t raw_bytes = 0;
  ha_rows rows = 0;
  if ((err = estimate_graph_footprint(graph, ctx, &raw_bytes, &rows,
                                      err_reason)))
    return err;

  const size_t est =
      raw_bytes / PRELOAD_OVERHEAD_DEN * PRELOAD_OVERHEAD_NUM +
      (raw_bytes % PRELOAD_OVERHEAD_DEN) * PRELOAD_OVERHEAD_NUM /
          PRELOAD_OVERHEAD_DEN;
  stats.est_bytes = est;
  stats.total_rows = rows;

  if (est > max_cache_size) {
    set_reason(&err_reason)
        << "estimated " << est << " bytes needed for " << rows
        << " nodes, but vidx_hnsw_cache_size is only " << max_cache_size
        << " bytes; raise vidx_hnsw_cache_size to at least the estimate "
           "and retry";
    return HA_ERR_OUT_OF_MEM;
  }

  /* check_only: the graph fits; report current usage and skip the full
     load (Step 4).  used_bytes reflects whatever the cold-share acquire()
     already materialised (the entry point), not zero. */
  if (check_only) {
    stats.used_bytes = ctx->cache_allocated_size();
    return 0;
  }

  /* ---- Step 4: Pass 2 load (Gate 3 re-checks the live allocation) ---- */
  if ((err = load_graph_nodes(graph, ctx, rows, err_reason)))
    return err;

  stats.used_bytes = ctx->cache_allocated_size();
  return 0;
}

/* ------------- Prebuild check (dbms_vidx.prebuild_check) ------------- */

/*
  Expected per-node build footprint of a graph that does not exist yet:
  node_footprint_bytes() with the measured layer replaced by its
  expectation.  draw_hnsw_layer() draws floor(-ln(U) / ln(M)), so
  P(layer >= k) = M^-k and E[layer] = sum_k M^-k = 1 / (M - 1); the
  adjacency cost is affine in the layer, so substituting E[layer] gives
  the exact expected cost (the running-top cap in the draw only lowers
  the true mean, keeping this a slight upper bound).

  gref_len is the InnoDB hidden row-id length: the auxiliary graph table
  declares no PRIMARY KEY and its unique tref index is nullable (see
  create_dd_table), so position() yields the 6-byte row id.
*/
static double prebuild_expected_node_bytes(uint dimensions, uint m,
                                           uint tref_len) {
  constexpr uint gref_row_id_len = 6;
  const uint m_safe = std::max(m, 2u); /* guard 1/(M-1); real M >= 2 */

  return node_footprint_bytes(dimensions, m_safe, gref_row_id_len, tref_len,
                              /*layer=*/1.0 / (m_safe - 1));
}

/*
  Exact per-column non-NULL row counts: one full base-table scan.  NULL
  vectors produce no graph node and persistent statistics lag behind
  DML, so the scan is the only source the report can hard-promise on;
  it reads strictly less than the ALTER it sizes.
*/
static int count_nonnull_vector_rows(TABLE *table,
                                     const std::vector<Field *> &fields,
                                     std::vector<ha_rows> *counts,
                                     std::string &err_reason) {
  THD *thd = table->in_use;

  /* The row template materialises only read_set columns; an unmarked
     column's null bit is stale in record[0], so mark every counted
     column before the scan starts. */
  for (Field *f : fields) bitmap_set_bit(table->read_set, f->field_index());

  if (int err = table->file->ha_rnd_init(true)) {
    set_reason(&err_reason) << "ha_rnd_init failed (row-count scan)";
    return err;
  }
  Scope_guard rnd_end{[table]() { table->file->ha_rnd_end(); }};

  int err;
  while ((err = table->file->ha_rnd_next(table->record[0])) == 0) {
    /* Test hook: park the scan between fetched rows so a concurrent KILL
       can land while the row count is in flight (sequenced before the
       killed check so the wake-up takes the interrupted exit). */
    DEBUG_SYNC(thd, "vidx_prebuild_scan_row");
    if (thd->killed) {
      set_reason(&err_reason) << "killed during the row-count scan";
      return HA_ERR_QUERY_INTERRUPTED;
    }
    for (size_t i = 0; i < fields.size(); i++)
      if (!fields[i]->is_null()) (*counts)[i]++;
  }
  if (err != HA_ERR_END_OF_FILE) {
    set_reason(&err_reason) << "scan failed during the row count";
    return err;
  }
  return 0;
}

int mhnsw_prebuild_check(TABLE *table, uint default_m,
                         std::vector<Prebuild_estimate> *out,
                         std::string &err_reason) {
  const TABLE_SHARE *share = table->s;
  const uint tref_len = table->file->ref_length;

  /* Enumerate the VECTOR columns, each with its existing vector KEY
     when one is defined (same first-key-part match as
     resolve_vector_key). */
  std::vector<Field *> fields;
  std::vector<const KEY *> keys;
  for (uint i = 0; i < share->fields; i++) {
    Field *f = table->field[i];
    if (!f->is_vector()) continue;

    const KEY *vkey = nullptr;
    for (uint k = share->keys; k < share->total_keys; k++) {
      const KEY *key = table->key_info + k;
      if ((key->flags & HA_VECTOR) && key->key_part[0].field == f) {
        vkey = key;
        break;
      }
    }
    fields.push_back(f);
    keys.push_back(vkey);
  }
  if (fields.empty()) {
    set_reason(&err_reason) << "table has no VECTOR column";
    return HA_ERR_UNSUPPORTED;
  }

  std::vector<ha_rows> counts(fields.size(), 0);
  if (int err = count_nonnull_vector_rows(table, fields, &counts, err_reason))
    return err;

  for (size_t i = 0; i < fields.size(); i++) {
    Prebuild_estimate est;
    est.column = fields[i]->field_name;
    est.index_name = keys[i] != nullptr ? keys[i]->name : nullptr;
    est.dimensions =
        down_cast<const Field_vector *>(fields[i])->get_dimensions();
    est.m = keys[i] != nullptr ? keys[i]->vector_m : default_m;
    est.rows = counts[i];

    /* Same 10% MEM_ROOT-tail scaling as the preload estimator. */
    const double raw =
        static_cast<double>(est.rows) *
        prebuild_expected_node_bytes(est.dimensions, est.m, tref_len);
    est.est_bytes = static_cast<ulonglong>(
        std::ceil(raw * PRELOAD_OVERHEAD_NUM / PRELOAD_OVERHEAD_DEN));

    out->push_back(est);
  }
  return 0;
}

}  // namespace hnsw
}  // namespace vidx
