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

#include "mysql/plugin.h"                    // THDVAR
#include "sql/dd/cache/dictionary_client.h"  // dd::cache::Dictionary_client
#include "sql/dd/properties.h"               // dd::Properties
#include "sql/dd/string_type.h"              // dd::String_type
#include "sql/dd/types/index.h"              // dd::Index
#include "sql/dd/types/index_element.h"      // dd::Index_element
#include "sql/dd/types/schema.h"             // dd::Schema
#include "sql/dd/types/table.h"              // dd::Table
#include "sql/field.h"                       // Field
#include "sql/item_strfunc.h"                // vidx::Item_func_vec_distance
#include "sql/key.h"                         // KEY
#include "sql/sql_base.h"                    // get_from_share

#include "sql/consensus/consensus_err.h"  // xp::error
#include "vidx/vidx_log.h"                // vidx::log_slave_dml_err
#include "sql/log.h"                       // get_backtrace_str
#include "vidx/vidx_aux_dml.h"
#include "vidx/vidx_common.h"
#include "vidx/vidx_hnsw_ctx.h"            // file-static class hierarchy

namespace vidx {
namespace hnsw {


/* -------------------- External Vars Definition -------------------- */
MHNSW_Trx::MHNSW_hton MHNSW_Trx::hnsw_hton;

void *trx_handler = &MHNSW_Trx::hnsw_hton;

ulonglong max_cache_size = DEF_CACHE_SIZE;

ulong lock_timeout_ms = DEF_LOCK_TIMEOUT_MS;

/* -------------------- Static Functions Definition -------------------- */
template <typename T>
static void copy_option(dd::Properties *to, dd::Properties *from,
                        const char *key) {
  if (from->exists(key)) {
    T value;
    from->get(key, &value);
    to->set(key, value);
  }
}

static dd::Column *fill_dd_add_columns(
    dd::Table *hlindex_dd, const char *name, const char *type_name,
    dd::enum_column_types type, size_t char_length, bool nullable,
    uint numeric_precision, const CHARSET_INFO *charset,
    bool is_explicit_collation, bool is_numeric_scale_null,
    size_t default_value_len, uint nr [[maybe_unused]]) {
  dd::Column *col = hlindex_dd->add_column();
  assert(col->ordinal_position() == nr + 1);
  dd::Properties *options = &col->options();
  col->set_name(name);
  col->set_column_type_utf8(type_name);
  col->set_type(type);
  col->set_char_length(char_length);
  col->set_numeric_precision(numeric_precision);
  col->set_hidden(dd::Column::enum_hidden_type::HT_VISIBLE);
  col->set_collation_id(charset->number);
  col->set_is_explicit_collation(is_explicit_collation);
  if (!is_numeric_scale_null) {
    col->set_numeric_scale(0);
  }
  col->set_nullable(nullable);
  col->set_has_no_default(!nullable);
  col->set_default_value_null(nullable);
  if (!nullable) {
    assert(default_value_len > 0);
    /* Zero-filled default payload; String_type owns its buffer. */
    col->set_default_value(dd::String_type(default_value_len, '\0'));
  }

  options->set("interval_count", 0);
  return col;
}

static dd::Index *fill_dd_add_indexes(dd::Table *hlindex_dd, const char *name,
                                      bool is_unique, uint nr, dd::Column *col,
                                      const uint64_t length) {
  dd::Index *index = hlindex_dd->add_index();
  index->set_name(name);
  index->set_algorithm(dd::Index::IA_BTREE);
  index->set_algorithm_explicit(false);
  index->set_visible(true);
  index->set_type(is_unique ? dd::Index::IT_UNIQUE : dd::Index::IT_MULTIPLE);
  index->set_ordinal_position(nr + 1);
  index->set_generated(false);
  index->set_engine(hlindex_dd->engine());
  index->options().set("flags", 0);

  col->set_column_key(is_unique ? dd::Column::CK_UNIQUE
                                : dd::Column::CK_MULTIPLE);

  dd::Index_element *index_elem = index->add_element(col);
  index_elem->set_length(length);

  return index;
}

/*
  selects best neighbors from the list of candidates plus one extra candidate

  one extra candidate is specified separately to avoid appending it to
  the Neighborhood candidates, which might be already at its max size.
*/
static int select_neighbors_core(MHNSW_Share *ctx [[maybe_unused]],
                                 TABLE *graph, size_t layer,
                                 FVectorNode &target,
                                 const Neighborhood &candidates,
                                 FVectorNode *extra_candidate,
                                 size_t max_neighbor_connections,
                                 Queue<Visited> &pq, MEM_ROOT *root,
                                 bool may_load) {
  DBUG_TRACE;
  const size_t discarded_sz = sizeof(Visited **) * max_neighbor_connections;
  auto *discarded = static_cast<Visited **>(my_safe_alloca(discarded_sz, MAX_ALLOCA_SZ));
  Scope_guard discarded_guard{
      [discarded, discarded_sz]() { my_safe_afree(discarded, discarded_sz, MAX_ALLOCA_SZ); }};
  size_t discarded_num = 0;
  Neighborhood &neighbors = target.neighbors[layer];

  for (size_t i = 0; i < candidates.num; i++) {
    FVectorNode *node = candidates.links[i];
    if (may_load) {
      if (int err = node->load(graph)) return err;
    } else if (unlikely(!node->vec)) {
      /* DDL workers own no handler. Every candidate must already be resident
         in the private build context. */
      return HA_ERR_INTERNAL_ERROR;
    }
    pq.push(new (root) Visited(node, node->distance_to(target.vec)));
  }
  if (extra_candidate)
    pq.push(new (root) Visited(extra_candidate,
                               extra_candidate->distance_to(target.vec)));

  assert(pq.elements());
  neighbors.num = 0;

  while (pq.elements() && neighbors.num < max_neighbor_connections) {
    Visited *vec = pq.pop();
    FVectorNode *const node = vec->node;
    /*
      Alpha pruning assumes nonnegative distances: dividing by alpha
      only relaxes the bound for d >= 0, and the 32*FLT_EPSILON floor
      would swallow every negative INNER_PRODUCT distance, discarding
      all mutually-similar candidates.  For d < 0 fall back to the
      plain Algorithm 4 comparison dist(c,s) <= dist(c,t) (the
      alpha-free form used by hnswlib and pgvector), which keeps the
      diversification meaningful in the negative domain.
    */
    const float target_dista =
        vec->distance_to_target >= 0
            ? std::max(32 * FLT_EPSILON, vec->distance_to_target / alpha)
            : vec->distance_to_target;
    bool discard = false;
    for (size_t i = 0; i < neighbors.num; i++)
      if ((discard =
               node->distance_to(neighbors.links[i]->vec) <= target_dista))
        break;
    if (!discard)
      target.push_neighbor(layer, node);
    else if (discarded_num + neighbors.num < max_neighbor_connections)
      discarded[discarded_num++] = vec;
  }

  for (size_t i = 0;
       i < discarded_num && neighbors.num < max_neighbor_connections; i++)
    target.push_neighbor(layer, discarded[i]->node);

  return 0;
}

int select_neighbors(MHNSW_Share *ctx, TABLE *graph, size_t layer,
                     FVectorNode &target, const Neighborhood &candidates,
                     FVectorNode *extra_candidate,
                     size_t max_neighbor_connections) {
  Queue<Visited> pq;
  if (unlikely(pq.init(max_ef, false, Visited::cmp))) return HA_ERR_OUT_OF_MEM;
  return select_neighbors_core(ctx, graph, layer, target, candidates,
                               extra_candidate, max_neighbor_connections, pq,
                               graph->in_use->mem_root, /*may_load=*/true);
}

int select_neighbors(MHNSW_Share *ctx, size_t layer, FVectorNode &target,
                     const Neighborhood &candidates,
                     FVectorNode *extra_candidate,
                     size_t max_neighbor_connections,
                     Search_work_area *work_area) {
  Queue<Visited> &pq = work_area->m_select_pq;
  /* Materialize the entire candidate set before select_neighbors_core
     mutates target.neighbors[layer]: resolve every miss inline on the
     executor's load channel (an invariant of Search_work_area). */
  for (size_t i = 0; i < candidates.num; i++) {
    FVectorNode *node = candidates.links[i];
    if (likely(node->vec)) continue;
    if (int err = node->load(work_area->m_load_graph)) return err;
  }
  if (extra_candidate && unlikely(!extra_candidate->vec)) {
    if (int err = extra_candidate->load(work_area->m_load_graph)) return err;
  }

  if (!pq.is_inited()) {
    if (unlikely(pq.init(max_ef, false, Visited::cmp)))
      return HA_ERR_OUT_OF_MEM;
  } else {
    pq.clear();
  }
  return select_neighbors_core(ctx, /*graph=*/nullptr, layer, target,
                               candidates, extra_candidate,
                               max_neighbor_connections, pq, &work_area->m_root,
                               /*may_load=*/false);
}

int link_reverse_neighbor(MHNSW_Share *ctx, size_t layer,
                          FVectorNode *node, FVectorNode *neigh,
                          uint max_neighbors, bool *changed,
                          Search_work_area *work_area) {
  *changed = false;
  /* REVERSE is intentionally non-retryable. Its leader preflight must have
     materialized every historical destination before workers can mutate an
     adjacency list. */
  if (unlikely(!neigh->vec || !neigh->neighbors || layer > neigh->max_layer))
    return HA_ERR_INTERNAL_ERROR;
  Neighborhood &neighbors = neigh->neighbors[layer];
  if (neighbors.num < max_neighbors) {
    neigh->push_neighbor(layer, node);
    *changed = true;
    return 0;
  }
  if (int err = select_neighbors(ctx, layer, *neigh, neighbors, node,
                                 max_neighbors, work_area))
    return err;
  for (size_t i = 0; i < neighbors.num; i++) {
    if (neighbors.links[i] == node) {
      *changed = true;
      return 0;
    }
  }
  return 0;
}

static int update_second_degree_neighbors(MHNSW_Share *ctx, TABLE *graph,
                                          size_t layer, FVectorNode *node) {
  DBUG_TRACE;
  const uint max_neighbors = ctx->max_neighbors(layer);
  /*
    On replica apply threads, shorten innodb_lock_wait_timeout to
    hnsw_lock_timeout_ms (default 1 s) so that concurrent writers on
    the graph table fail fast instead of stalling apply progress for
    the full default timeout.
  */
  ScopedLockWaitTimeout scoped_timeout(graph->file,
                                       graph->in_use->slave_thread);

  /*
    Sort the per-layer neighbor list ascending by gref.  The cross-trx
    deadlock guarantee is now provided by prefetch_neighbor_locks_in_
    gref_order, which X-locks the UNION across all layers before
    update_second_degree_neighbors runs — once those locks are held,
    update_2nd's per-layer iteration order no longer affects deadlock
    formation (InnoDB row locks are reentrant on the same trx_t).

    What this per-layer sort still buys us:
      (a) deterministic in-memory neighbor order for HNSW's path-
          sensitive search_layer / select_neighbors traversal — the
          ANN top-K result depends on this order;
      (b) several mtr tests pin specific result ids that assume this
          order;
      (c) cost is negligible (M is small, default 6).

    Removing it would not re-introduce deadlocks, but would destabilise
    ANN results and break those tests.
  */
  Neighborhood &nb = node->neighbors[layer];
  const size_t gref_len = node->gref_len();
  std::sort(nb.links, nb.links + nb.num, [gref_len](FVectorNode *a, FVectorNode *b) {
    return memcmp(a->gref(), b->gref(), gref_len) < 0;
  });

  for (size_t i = 0; i < nb.num; i++) {
    FVectorNode *neigh = nb.links[i];
    Neighborhood &neighneighbors = neigh->neighbors[layer];
    if (neighneighbors.num < max_neighbors)
      neigh->push_neighbor(layer, node);
    else if (int err = select_neighbors(ctx, graph, layer, *neigh,
                                        neighneighbors, node, max_neighbors))
      return err;
    /* See save_neighbors_only docblock for why save_new_row would
       cause cross-trx neighbor revive + ER_DUP_KEY here. */
    if (int err = neigh->save_neighbors_only(graph)) {
      if (err == HA_ERR_LOCK_WAIT_TIMEOUT && graph->in_use->slave_thread) {
        vidx::log_slave_dml_warn(graph, __func__, "neigh->save_neighbors_only",
                                 err);
        vidx_stats.save_neighbor_skip_lock_timeout++;
        continue;
      }
      return err;
    }
  }
  return 0;
}

static inline float generous_furthest(const Queue<Visited> &q, float maxd,
                                      float g) {
  float d0 = maxd * g / 2;
  float d = q.top()->distance_to_target;
  /*
    Negative-domain guard (INNER_PRODUCT distances are -dot and can be
    negative or unbounded below; maxd/diameter only tracks positive
    distances and stays 0 on all-nonnegative-dot datasets):
      - d0 <= 0 makes x blow up to +-inf or 0/0, both of which turn
        into NaN through the sigmoid (inf/inf), so every later
        comparison against furthest_best is false and the search
        freezes once the best-heap is full;
      - d < 0 flips the generosity direction: d * (1 + eps) is SMALLER
        than d, so the "relaxed" bound becomes tighter than the true
        furthest kept result and prunes candidates that would improve
        the result set.
    Returning d degenerates to the exact bound (best.top()), which is
    the standard HNSW termination rule used by hnswlib and pgvector.
    EUCLIDEAN/COSINE keep the upstream MariaDB heuristic: their
    distances are >= 0 and d0 > 0 once the first positive distance is
    seen.
  */
  if (d0 <= 0.0f || d <= 0.0f) return d;
  float k = 5;
  float x = (d - d0) / d0;
  float sigmoid =
      k * x / std::sqrt(1 + (k * k - 1) * x * x);  // or any other sigmoid
  return d * (1 + (g - 1) / 2 * (1 - sigmoid));
}

/*
  @param[in/out] inout    in: start nodes, out: result nodes
*/
int search_layer(MHNSW_Share *ctx, TABLE *graph, const FVector *target,
                 float threshold, uint result_size, size_t layer,
                 Neighborhood *inout, bool construction,
                 Search_work_area *work_area) {
  DBUG_TRACE;
  assert(inout->num > 0);
  DBUG_PRINT("info", ("search_layer for layer %lu", layer));

  /* Initial candidates are dereferenced by VisitedSet::create(). Materialize
     all non-resident nodes before constructing any partial search state. */
  if (work_area) {
    assert(work_area->m_load_graph != nullptr);
    for (size_t i = 0; i < inout->num; i++) {
      FVectorNode *node = inout->links[i];
      if (likely(node->vec)) continue;
      if (int err = node->load(work_area->m_load_graph)) return err;
    }
  }

  MEM_ROOT *const root =
      work_area ? &work_area->m_root : graph->in_use->mem_root;
  Queue<Visited> local_candidates, local_best;
  bool skip_deleted;
  uint ef = result_size;
  float generosity = 1.1f + ctx->M / 500.0f;

  if (construction) {
    skip_deleted = false;
    if (ef > 1) ef = std::max(ctx->ef_construction, ef);
  } else {
    skip_deleted = layer == 0;
    if (!work_area && (ef > 1 || layer == 0))
      ef = std::max(get_ef_search(graph->in_use), ef);
  }

  // WARNING! heuristic here
  const double est_heuristic = 8 * std::sqrt(ctx->max_neighbors(layer));
  /*
   * MDEV-35897: cap Bloom Filter pre-allocation to ~77% of actual graph
   * node count (divided by 1.3).
   *
   * The original code estimated est_size solely from a power of ef, which
   * grossly overestimates when ef_search is large but the graph is small,
   * causing PatternedSimdBloomFilter to allocate far more than necessary.
   *
   * MariaDB: snapshots node count into TABLE::used_stat_records, read here.
   * MySQL 8.0: reads MHNSW_Share::graph_node_count (approximate, snapshotted
   * in acquire()). Under MHNSW_Trx path graph_node_count=0, no cap applied
   * (falls back to original behavior, acceptable).
   */
  double est_size_d = est_heuristic * std::pow(ef, ctx->ef_power);
  if (ctx->graph_node_count > 0)
    est_size_d = std::min(est_size_d, ctx->graph_node_count / 1.3);
  const uint est_size = static_cast<uint>(std::max(1.0, est_size_d));
  VisitedSet visited(root, target, est_size);

  Queue<Visited> *candidates_ptr = &local_candidates;
  Queue<Visited> *best_ptr = &local_best;
  if (work_area) {
    if (unlikely(ef > max_ef)) return HA_ERR_INTERNAL_ERROR;
    if (!work_area->m_candidates.is_inited() &&
        unlikely(work_area->m_candidates.init(max_ef, false, Visited::cmp)))
      return HA_ERR_OUT_OF_MEM;
    if (!work_area->m_best.is_inited() &&
        unlikely(work_area->m_best.init(max_ef, true, Visited::cmp)))
      return HA_ERR_OUT_OF_MEM;
    work_area->m_candidates.clear();
    work_area->m_candidates.set_max_elements(max_ef);
    work_area->m_best.clear();
    work_area->m_best.set_max_elements(ef);
    candidates_ptr = &work_area->m_candidates;
    best_ptr = &work_area->m_best;
  } else {
    if (unlikely(local_candidates.init(max_ef, false, Visited::cmp)) ||
        unlikely(local_best.init(ef, true, Visited::cmp)))
      return HA_ERR_OUT_OF_MEM;
  }
  Queue<Visited> &candidates = *candidates_ptr;
  Queue<Visited> &best = *best_ptr;

  assert(inout->num <= result_size);
  float max_distance = ctx->diameter;
  for (size_t i = 0; i < inout->num; i++) {
    Visited *v = visited.create(inout->links[i]);
    max_distance = std::max(max_distance, v->distance_to_target);
    candidates.safe_push(v); /* MDEV-35745 */
    if ((skip_deleted && v->node->deleted) || threshold > NEAREST) continue;
    best.push(v);
  }

  float furthest_best = best.is_empty()
                            ? FLT_MAX
                            : generous_furthest(best, max_distance, generosity);
  while (candidates.elements()) {
    const Visited &cur = *candidates.pop();
    if (cur.distance_to_target > furthest_best && best.is_full())
      break;  // All possible candidates are worse than what we have

    visited.flush();

    Neighborhood &neighbors = cur.node->neighbors[layer];
    FVectorNode **links = neighbors.links, **end = links + neighbors.num;
    /* Scan the adjacency list one probe batch at a time: visited.seen()
       tests VISITED_PROBE_BATCH links per call, one uint8_t mask bit per
       link.  Reading a full batch past neighbors.num is safe by
       construction -- Neighborhood::init zero-fills up to the batch
       multiple and the VisitedSet records nullptr as seen. */
    for (; links < end; links += VISITED_PROBE_BATCH) {
      uint8_t res = visited.seen(links);
      if (res == 0xff) continue;

      /* Collect this batch's unseen resident nodes, then compute their
         distances.  A 4-way batched AVX512 dot kernel (four accumulator
         chains, Faiss fvec_inner_product_batch_4 style) was tried here and
         rejected on measurement: the int16 single-vector kernel already
         runs at its latency limit, and the batch variant measured *slower*
         (768D x 100k, W=4: ALTER 64.4/66.2s -> 62.2/61.7s and 2000 top-100
         probes 2.74s -> 2.50s after removal). */
      FVectorNode *todo[VISITED_PROBE_BATCH];
      float dists[VISITED_PROBE_BATCH];
      size_t ntodo = 0;
      for (size_t i = 0; i < VISITED_PROBE_BATCH; i++) {
        if (res & (1 << i)) continue;
        FVectorNode *node = links[i];
        if (work_area) {
          if (unlikely(!node->vec)) {
            /* Read-through: resolve the miss inline on the executor's own
               load channel (worker: private RU instance; leader: the DDL
               handler).  No vidx_stats update here -- several workers run
               this concurrently and the pool's convention is that only the
               leader folds counters (see fold_reverse_dirty). */
            if (int err = node->load(work_area->m_load_graph)) return err;
          }
        } else if (int err = node->load(graph)) {
          return err;
        }
        todo[ntodo++] = node;
      }
      size_t j = 0;
      for (; j < ntodo; j++) dists[j] = todo[j]->distance_to(target);

      for (size_t i = 0; i < ntodo; i++) {
        Visited *v = visited.create(todo[i], dists[i]);
        if (v->distance_to_target <= threshold) continue;
        if (!best.is_full()) {
          max_distance = std::max(max_distance, v->distance_to_target);
          candidates.safe_push(v); /* MDEV-35745 */
          if (skip_deleted && v->node->deleted) continue;
          best.push(v);
          furthest_best = generous_furthest(best, max_distance, generosity);
        } else if (v->distance_to_target < furthest_best) {
          candidates.safe_push(v);
          if (skip_deleted && v->node->deleted) continue;
          if (v->distance_to_target < best.top()->distance_to_target) {
            best.replace_top(v);
            furthest_best = generous_furthest(best, max_distance, generosity);
          }
        }
      }
    }
  }
  /* Build workers keep the shared context read-only. The leader/normal path
     retains the baseline learned-parameter and query-stat updates. */
  if (!work_area)
    set_if_bigger(ctx->diameter, max_distance);  // not atomic, but it's ok
  /*
   * MDEV-35897: remove the old *2 coefficient.
   *
   * The old logic used visited.count*2 as an estimate of "actual visits",
   * which overestimates and inflates ef_power, causing next search's
   * est_size to grow even larger in a positive feedback loop.
   * Without *2, ef_power only grows when visited nodes truly exceed
   * est_size, which is more accurate.
   */
  if (!work_area && ef > 1 && visited.count > est_size) {
    double ef_power =
        std::log(visited.count / est_heuristic) / std::log(ef);
    set_if_bigger(ctx->ef_power, ef_power);  // not atomic, but it's ok
  }

  if (!work_area) graph->hlindex_search_stats.nodes_visited += visited.count;

  while (best.elements() > result_size) best.pop();

  inout->num = best.elements();
  for (FVectorNode **links = inout->links + inout->num; best.elements();)
    *--links = best.pop()->node;

  return 0;
}

/* -------------------- Class Functions Definition -------------------- */

int MHNSW_Trx::MHNSW_hton::do_savepoint_rollback(handlerton *, THD *thd,
                                                 void *) {
  /* vidx auxiliary writes go through Attachable_trx_rw and are committed
     to the SE eagerly (per-row); they do NOT participate in user SAVEPOINT
     semantics.  Resetting the per-trx caches here only forces the next
     access to reload from disk so any base-table changes that ARE rolled
     back by the user trx surface a fresh graph view.  The aux-table writes
     committed before the savepoint are not undone — same limitation as
     XA ROLLBACK; see vidx_xa_advanced.test. */
  for (auto trx = static_cast<MHNSW_Trx *>(thd_get_ha_data(thd, &hnsw_hton));
       trx; trx = trx->next)
    trx->reset(nullptr);
  return 0;
}

int MHNSW_Trx::MHNSW_hton::do_rollback(handlerton *ht, THD *thd, bool all) {
  if (is_stmt_within_outer_trx(thd, all))
    return do_savepoint_rollback(ht, thd, nullptr);

  /* Snapshot+detach before destroying (see do_commit for rationale). */
  auto trx = static_cast<MHNSW_Trx *>(thd_get_ha_data(thd, &hnsw_hton));
  thd_set_ha_data(thd, &hnsw_hton, nullptr);

  MHNSW_Trx *trx_next;
  for (; trx; trx = trx_next) {
    trx_next = trx->next;
    trx->~MHNSW_Trx();
  }
  return 0;
}

int MHNSW_Trx::MHNSW_hton::do_commit(handlerton *, THD *thd, bool all) {
  DBUG_TRACE;
  if (is_stmt_within_outer_trx(thd, all)) return 0;

  /* Snapshot the list head and detach ha_data BEFORE destruction so
     any hook re-entry during the per-trx cleanup (e.g. a callback
     that fires under LOCK_open in release_table_share) cannot reach
     a half-destroyed MHNSW_Trx via thd_get_ha_data. */
  auto trx = static_cast<MHNSW_Trx *>(thd_get_ha_data(thd, &hnsw_hton));
  thd_set_ha_data(thd, &hnsw_hton, nullptr);

  MHNSW_Trx *trx_next;
  for (; trx; trx = trx_next) {
    trx_next = trx->next;
    if (trx->table_id) {
      const MDL_key *key = trx->table_id->get_key();
      mysql_mutex_lock(&LOCK_open);
      TABLE_SHARE *share = get_table_share(thd, key->db_name(), key->name(),
                                           (const char *)key->ptr() + 1,
                                           key->length() - 1, false);
      mysql_mutex_unlock(&LOCK_open);

      if (share) {
        auto ctx = (share->hlindex_shares && share->hlindex_shares[trx->hlindex_slot])
                       ? MHNSW_Share::get_from_share(share, nullptr,
                                                     trx->hlindex_slot)
                       : nullptr;
        if (ctx) {
          mysql_rwlock_wrlock(&ctx->commit_lock);
          ctx->version++;
          if (trx->list_of_nodes_is_lost)
            ctx->reset(share);
          else {
            // consider copying nodes from trx to shared cache when it makes
            // sense. for ann_benchmarks it does not.
            // also, consider flushing only changed nodes (a flag in the node)
            for (FVectorNode &from : trx->get_cache())
              if (FVectorNode *node = ctx->find_node(from.gref()))
                node->vec = nullptr;
            ctx->start = nullptr;
          }
          ctx->release(true, share);
        }

        mysql_mutex_lock(&LOCK_open);
        release_table_share(share);
        mysql_mutex_unlock(&LOCK_open);
      }
    }

    trx->~MHNSW_Trx();
  }
  return 0;
}

int MHNSW_Trx::get_from_thd(MHNSW_Trx **out, TABLE *table,
                            bool for_update) {
  DBUG_TRACE;
  *out = nullptr;
  if (!table->file->has_transactions()) return 0;

  THD *thd = table->in_use;

  /* Register on current trn_ctx. */
  auto trx = static_cast<MHNSW_Trx *>(thd_get_ha_data(thd, &hnsw_hton));
  if (!for_update && !trx) return 0;

  while (trx && (trx->table_id != table->mdl_ticket ||
                 trx->hlindex_slot != table->hlindex_active_slot))
    trx = trx->next;
  if (!trx) {
    trx = new (thd->get_transaction()->transaction_memroot()) MHNSW_Trx(table);
    if (!trx) return HA_ERR_OUT_OF_MEM;
    trx->next = static_cast<MHNSW_Trx *>(thd_get_ha_data(thd, &hnsw_hton));
    assert(trx->next != trx);
    thd_set_ha_data(thd, &hnsw_hton, trx);
    if (!trx->next) {
      /* MDEV-37068 Can't find record in 't1' on INSERT to Vector table */
      if (thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))
        trans_register_ha(thd, true, &hnsw_hton, 0);
      trans_register_ha(thd, false, &hnsw_hton, 0);
    }
  }
  trx->refcnt++;
  *out = trx;
  return 0;
}

MHNSW_Share *MHNSW_Share::get_from_share(TABLE_SHARE *share, TABLE *table,
                                         uint slot) {
  DBUG_TRACE;
  share->lock_share();
  Scope_guard unlock_share_guard{[share]() { share->unlock_share(); }};
  auto ctx =
      static_cast<MHNSW_Share *>(share->hlindex_shares[slot]->hlindex_data);
  if (!ctx && table) {
    ctx = new (&share->hlindex_shares[slot]->mem_root) MHNSW_Share(table);
    if (!ctx) return nullptr;
    /* Restore the learned search-quality params written back by the
       previous generation's reset() (same lock_share critical section).
       First-ever open restores the NSDMI cold-start defaults (0.6 / 0)
       — a no-op.  Publish the inherited values for tests/diagnostics. */
    ctx->ef_power = share->hlindex_shares[slot]->hlindex_learned_ef_power;
    ctx->diameter = share->hlindex_shares[slot]->hlindex_learned_diameter;
    vidx_stats.last_ef_power_init = ctx->ef_power.load();
    vidx_stats.last_diameter_init = ctx->diameter.load();
    share->hlindex_shares[slot]->hlindex_data = ctx;
    ctx->refcnt++;
  }
  if (ctx) {
    ctx->refcnt++;
    /*
     * MDEV-35897: snapshot node count under share_lock.
     *
     * node_cache.size() is exact while holding the lock; search_layer
     * reads graph_node_count lock-free, so it may be stale, but it's
     * only used for Bloom Filter memory estimation -- approximate is fine.
     *
     * MariaDB writes this to TABLE::used_stat_records (thread-local, no
     * concurrent write conflict); here we update the MHNSW_Share member
     * directly -- concurrent writes just overwrite with similar values,
     * no correctness issue.
     */
    ctx->graph_node_count = ctx->node_cache.size();
    /* Piggyback a cache-size snapshot under lock_share. */
    ctx->report_cache_usage();
  }
  return ctx;
}

int MHNSW_Share::acquire(MHNSW_Share **ctx, TABLE *table, bool for_update) {
  DBUG_TRACE;

  /* Deterministic failure contract: on any error *ctx is nullptr, so the
     caller's Scope_guard can safely no-op. */
  *ctx = nullptr;

  MHNSW_Trx *trx = nullptr;
  int err = MHNSW_Trx::get_from_thd(&trx, table, for_update);
  if (err) return err;
  if (!(*ctx = trx)) {
    *ctx = MHNSW_Share::get_from_share(table->s, table,
                                       table->hlindex_active_slot);
    if (!*ctx) return HA_ERR_OUT_OF_MEM;
    if (table->file->has_transactions())
      mysql_rwlock_rdlock(&(*ctx)->commit_lock);
  }
  /* Test hook: pretend this generation has fully learned its params,
     so a forced reset (vidx_force_reset_on_release) exercises the
     write-back/restore cycle without a large dataset. */
  DBUG_EXECUTE_IF("vidx_force_learned_params", {
    (*ctx)->ef_power = 0.95;
    (*ctx)->diameter = 0.5f;
  });
  return load_entry_point_node(*ctx, table->hlindex_active);
}

/* copy the vector, preprocessed as needed; nullptr on OOM (evictable-vector
   layout only -- the inline layout writes into space alloc_node reserved) */
const FVector *FVectorNode::make_vec(const void *v) {
  void *mem = ctx->evictable_vectors() ? ctx->alloc_vec(this)
                                     : tref() + tref_len();
  if (unlikely(!mem)) return nullptr;
  return FVector::create(ctx->metric, mem, v, ctx->byte_len);
}

FVectorNode::FVectorNode(MHNSW_Share *ctx_, const void *gref_)
    : ctx(ctx_), stored(true), deleted(false), vec_evicted(false) {
  memcpy(gref(), gref_, gref_len());
}

FVectorNode::FVectorNode(MHNSW_Share *ctx_, const void *tref_, const void *vec_)
    : ctx(ctx_), stored(false), deleted(false), vec_evicted(false) {
  assert(tref_);
  memset(gref(), 0xff, gref_len());  // important: larger than any real gref
  memcpy(tref(), tref_, tref_len());
  vec = make_vec(vec_);
}

float FVectorNode::distance_to(const FVector *other) const {
  return vec->distance_to(other, ctx->vec_len);
}

int FVectorNode::alloc_neighborhood(uint8_t layer) {
  DBUG_TRACE;
  if (neighbors) return 0;
  DBUG_PRINT("info", ("alloc_neighborhood %u", layer));

  auto mem = (Neighborhood *)ctx->alloc_neighborhood(layer);
  if (unlikely(!mem)) return HA_ERR_OUT_OF_MEM;
  /* Set max_layer and neighbors together after successful allocation
     to keep the object state consistent. */
  max_layer = layer;
  neighbors = mem;
  auto ptr = (FVectorNode **)(neighbors + (layer + 1));
  for (size_t i = 0; i <= layer; i++)
    ptr = neighbors[i].init(ptr, ctx->max_neighbors(i));
  return 0;
}

int FVectorNode::load_slow(TABLE *graph) {
  DBUG_TRACE;
  vidx_stats.load_vec_misses++;

  assert(stored);
  // trx: consider loading nodes from shared, when it makes sense
  // for ann_benchmarks it does not
  /*
    Use consistent (non-locking) read to load graph node data.
    FVectorNode::load() is pure read-only cache population — it
    materializes the node vector into memory and is never followed by a
    write on this row.  A consistent (MVCC snapshot) read is the
    correct semantic: graph traversal must not acquire row locks.

    Without consistent read, ha_rnd_pos under F_WRLCK would acquire
    LOCK_X, which would block on:
      - Concurrent attachable inner transactions writing the graph
        row (per-row commit holds LOCK_X briefly).
      - Other read-only graph traversals that did acquire short-lived
        locks (deadlock risk).
  */
  graph->file->set_consistent_read(true);
  int err = graph->file->ha_rnd_pos(graph->record[0], gref(), komodo::GM_NONE);
  graph->file->set_consistent_read(false);
  if (err) return err;
  vidx_stats.decode_vec_node_count++;
  return load_from_record(graph);
}

int FVectorNode::load_from_record(TABLE *graph) {
  assert(ctx->byte_len);
  DBUG_TRACE;

  uint ticket = ctx->lock_node(this);
  Scope_guard guard{[this, ticket]() { ctx->unlock_node(ticket); }};

  if (vec) return 0;

  String buf, *v;
  if (!vec_evicted) {
    v = graph->field[FIELD_TREF]->val_str(&buf);
    deleted = graph->field[FIELD_TREF]->is_null();
    if (!deleted) {
      if (unlikely(v->length() != tref_len())) return HA_ERR_CRASHED;
      memcpy(tref(), v->ptr(), v->length());
    }
  }

  v = graph->field[FIELD_VEC]->val_str(&buf);
  if (unlikely(!v)) return HA_ERR_CRASHED;

  if (v->length() != FVector::data_size(ctx->vec_len)) return HA_ERR_CRASHED;
  /* Evictable-vector layout: payload slot from the ctx vector arena (see
     alloc_vec), reclaimable by a DDL window handover while this node object
     and its adjacency stay resident.  Inline layout: the space alloc_node
     reserved right after tref. */
  void *vmem = ctx->evictable_vectors() ? ctx->alloc_vec(this)
                                      : tref() + tref_len();
  if (unlikely(!vmem)) return HA_ERR_OUT_OF_MEM;
  FVector *vec_ptr = FVector::align_ptr(vmem);
  memcpy(vec_ptr->data(), v->ptr(), v->length());
  vec_ptr->postprocess(ctx->vec_len);
  /* postprocess() recomputes the EUCLIDEAN abs2 only; re-apply the
     metric convention (COSINE 0.5 / INNER_PRODUCT 0), see fixup_abs2. */
  vec_ptr->fixup_abs2(ctx->metric);

  if (vec_evicted) {
    /* Vector-evicted survivor of a DDL window handover: the in-memory
       adjacency is complete and possibly NEWER than the on-disk
       FIELD_NEIGHBORS (reverse edges are flushed per window), so only the
       payload is refilled — never re-parse tref/layer/neighbors. */
    vec = vec_ptr;
    return 0;
  }

  longlong layer = graph->field[FIELD_LAYER]->val_int();
  if (layer < 0 || layer > 100)  // 10e30 nodes at M=2, more at larger M's
  {
    xp::warn(ER_XP_0) << " FVectorNode::load_from_record: invalid layer, layer=" << layer;
    return HA_ERR_CRASHED;
  }

  if (int err = alloc_neighborhood(static_cast<uint8_t>(layer))) return err;

  v = graph->field[FIELD_NEIGHBORS]->val_str(&buf);
  if (unlikely(!v)) return HA_ERR_CRASHED;

  // <N> <gref> <gref> ... <N> ...etc...
  uchar *ptr = (uchar *)v->ptr(), *end = ptr + v->length();
  for (size_t i = 0; i <= max_layer; i++) {
    DBUG_PRINT("info", ("decode neighbors layer: %lu", i));
    if (unlikely(ptr >= end)) return HA_ERR_CRASHED;
    size_t grefs = *ptr++;
    if (unlikely(grefs > ctx->max_neighbors(i))) return HA_ERR_CRASHED;
    if (unlikely(ptr + grefs * gref_len() > end)) return HA_ERR_CRASHED;
    neighbors[i].num = grefs;
    /* One cache_lock acquisition for the whole list, not one per gref --
       see MHNSW_Share::get_nodes for the contention measurement. */
    if (unlikely(!ctx->get_nodes(ptr, grefs, neighbors[i].links)))
      return HA_ERR_OUT_OF_MEM;
    ptr += grefs * gref_len();
  }
  vec = vec_ptr;  // must be done at the very end
  return 0;
}

void FVectorNode::push_neighbor(size_t layer, FVectorNode *other) {
  assert(neighbors[layer].num < ctx->max_neighbors(layer));
  neighbors[layer].links[neighbors[layer].num++] = other;
}

size_t FVectorNode::tref_len() const { return ctx->tref_len; }
size_t FVectorNode::gref_len() const { return ctx->gref_len; }
uchar *FVectorNode::gref() const { return const_cast<uchar *>(reinterpret_cast<const uchar *>(this + 1)); }
uchar *FVectorNode::tref() const { return gref() + gref_len(); }

const uchar *FVectorNode::get_key(const void *elem, size_t *key_len, my_bool) {
  *key_len = static_cast<const FVectorNode *>(elem)->gref_len();
  return static_cast<const FVectorNode *>(elem)->gref();
}

/* -------------------- External Functions Definition -------------------- */
std::unique_ptr<dd::Table> create_dd_table(THD *thd, const char *table_name,
                                           KEY *key, dd::Table *dd_table,
                                           TABLE *table, const char *db_name,
                                           const uint tref_len) {
  assert(key->flags & HA_VECTOR);
  assert(key->user_defined_key_parts == 1);

  const dd::Schema *schema = nullptr;
  if (thd->dd_client()->acquire(db_name, &schema)) {
    return nullptr;  // Error is reported by the dictionary subsystem.
  }

  if (schema == nullptr) {
    my_error(ER_BAD_DB_ERROR, MYF(0), db_name);
    return nullptr;
  }

  std::unique_ptr<dd::Table> tab_obj(schema->create_table(thd));
  dd::Table *hlindex_dd = tab_obj.get();

  /* Basic */
  hlindex_dd->set_name(table_name);
  hlindex_dd->set_hidden(dd::Abstract_table::HT_HIDDEN_HLINDEX);
  hlindex_dd->set_engine(dd_table->engine());
  hlindex_dd->set_comment(
      dd::String_type(key->comment.str, key->comment.length));
  hlindex_dd->set_row_format(dd_table->row_format());
  hlindex_dd->set_partition_type(dd::Table::PT_NONE);
  hlindex_dd->set_subpartition_type(dd::Table::ST_NONE);
  /* some properties have been set in Schema_impl::create_table(). */

  /* Options */
  dd::Properties *hlindex_options = &hlindex_dd->options();
  dd::Properties *table_options = &dd_table->options();

  hlindex_options->set("pack_record", true);
  hlindex_options->set("avg_row_length", 0);
  hlindex_options->set("stats_sample_pages", 0);
  hlindex_options->set("keys_disabled", 0);
  hlindex_options->set("stats_auto_recalc", HA_STATS_AUTO_RECALC_DEFAULT);

  copy_option<dd::String_type>(hlindex_options, table_options, "compress");
  copy_option<dd::String_type>(hlindex_options, table_options, "encrypt_type");
  copy_option<uint32_t>(hlindex_options, table_options, "storage");
  copy_option<uint32_t>(hlindex_options, table_options, "key_block_size");

  /* Options for vector */
  hlindex_options->set("__vector_m__", key->vector_m);
  hlindex_options->set("__vector_distance__", key->vector_distance);
  hlindex_options->set("__vector_ef_construction__",
                       key->vector_ef_construction);
  /* key_part->fieldnr is 1-based here: the KEY always comes from a
     TABLE_SHARE rebuilt via build_hlindex_key (which reads ordinal_position
     from DD) or from prepare_hlindex_meta (enriched CREATE path).
     Both sources produce 1-based fieldnr, so no "+1" adjustment needed.

     History: the old code did "+1" assuming a 0-based fieldnr passed
     directly from mysql_prepare_create_table via the removed last_key
     parameter, but that path no longer exists after the multi-index
     refactoring (prepare_hlindex_meta now bridges the gap).  The old
     "+1" was also a latent bug on the TRUNCATE fallback path
     (get_vec_key() returns 1-based), but never triggered because the
     TRUNCATE fallback KEY comes from TABLE_SHARE (already 1-based),
     not from the 0-based mysql_prepare_create_table output. */
  hlindex_options->set("__vector_column__", key->key_part->fieldnr);
  hlindex_options->set("__vector_index_name__", dd::String_type(key->name));

  /* Columns */
  char type_tref[255];
  snprintf(type_tref, sizeof(type_tref), "varbinary(%d)", tref_len);
  const char *name_layer = "layer";
  const char *name_tref = "tref";
  dd::Column *col_layer = fill_dd_add_columns(
      hlindex_dd, name_layer, "tinyint", dd::enum_column_types::TINY, 4, false,
      3, &my_charset_utf8mb4_0900_ai_ci, false, false, 1, FIELD_LAYER);
  dd::Column *col_tref = fill_dd_add_columns(
      hlindex_dd, name_tref, type_tref, dd::enum_column_types::VARCHAR,
      tref_len, true, 0, &my_charset_bin, true, true, 0, FIELD_TREF);
  fill_dd_add_columns(hlindex_dd, "vec", "blob", dd::enum_column_types::BLOB,
                      65535, false, 0, &my_charset_bin, true, true,
                      2 + portable_sizeof_char_ptr, FIELD_VEC);
  fill_dd_add_columns(hlindex_dd, "neighbors", "blob",
                      dd::enum_column_types::BLOB, 65535, false, 0,
                      &my_charset_bin, true, true, 2 + portable_sizeof_char_ptr,
                      FIELD_NEIGHBORS);
  /* system columns like ROW_ID will be added in storage engine. */

  /* Indexes */
  fill_dd_add_indexes(hlindex_dd, name_tref, true, IDX_TREF, col_tref,
                      tref_len);
  fill_dd_add_indexes(hlindex_dd, name_layer, false, IDX_LAYER, col_layer, 1);
  /* Primary key will be added in storage engine. */

  /* Add the InnoDB system columns DB_ROW_ID, DB_TRX_ID, DB_ROLL_PTR. */
  if (table->file->get_extra_columns_and_keys(nullptr, nullptr, nullptr, 0,
                                              hlindex_dd)) {
    return nullptr;
  }

  return tab_obj;
}

/*
  Indep-DML scope and ctx-acquire helpers moved to
  sql/vidx/vidx_aux_dml.cc (namespace vidx::aux_dml).  Callers
  here use vidx::aux_dml::Hlindex_dml_scope (RAII wrapping the
  Attachable_trx_rw boundary) + MHNSW_Trx::get_from_thd() directly.

  Why Attachable_trx_rw, not Autonomous_trx_rw:
    vidx auxiliary-table writes MUST NOT emit binlog — the base-table
    row event is what slaves replay; on the applier our hooks re-run
    mhnsw_{insert,delete,invalidate} against the slave-local hlindex.
    If the attachable also wrote binlog, the auxiliary mutation would
    be applied twice on the slave.  Attachable_trx_rw suppresses
    OPTION_BIN_LOG via Sub_statement_state.  Sequence's nextval is
    the opposite case (must be binlogged to survive outer rollback)
    and picks Autonomous_trx_rw instead.
*/

/*
  Self-contained DML primitives (mhnsw_insert / mhnsw_invalidate /
  mhnsw_delete_all).  Pure DML body: no routing decision, no
  Attachable_trx_rw boundary, no hlindex_lock — those are owned by the
  caller's Hlindex_dml_scope (see vidx::aux_dml::Hlindex_dml_scope).

  Caller pre-conditions when entering these primitives:
    1. table->file->ref populated via table->file->position(...) (for
       insert / invalidate; not needed for delete_all).
    2. An active vidx::aux_dml::Hlindex_dml_scope on table — its ctor
       takes the hlindex_lock and (on indep path) begins the
       Attachable_trx_rw; its commit() does the matching F_UNLCK and
       trans_commit.
    3. Caller has already checked scope.init_err() and short-circuited
       if non-zero, so we know hlindex_lock succeeded.

  MHNSW_Trx::get_from_thd registers on the CURRENT trn_ctx (the
  attachable's transaction when called from inside an Hlindex_dml_scope),
  so do_commit / do_rollback fire when the attachable commits.  This
  means the shared cache is invalidated after each attachable commit,
  ensuring subsequent reads reload from disk truth.

  load_entry_point_node is called unconditionally in all three (indep
  and non-indep), see the in-body comment for the defensive rationale
  (DDL / force_non_indep flows may arrive with an empty ctx).

  mhnsw_insert receives the @p indep routing flag from the caller (the
  same value passed to Hlindex_dml_scope) — no internal recompute, since
  that flag is purely informational here (orphan-accounting gate, see
  the from_update branches below).
*/
int mhnsw_insert(TABLE *table, KEY *keyinfo, bool from_update, bool indep) {
  DBUG_TRACE;
  THD *thd = table->in_use;
  TABLE *graph = table->hlindex_active;

  assert(table->file->has_transactions());
  MHNSW_Trx *trx_ctx = nullptr;
  int err = MHNSW_Trx::get_from_thd(&trx_ctx, table, /*for_update=*/true);
  if (err) {
    /* Pre-old_map failures cannot route through func_end (the column
       map is not saved yet), so replicate its orphan accounting here:
       on the indep UPDATE leg the prior mhnsw_invalidate has already
       committed a soft-delete and this failure leaves it unmatched. */
    if (from_update && indep) vidx_stats.update_partial_orphan++;
    return err;
  }
  MHNSW_Share *ctx = trx_ctx;
  if (!ctx) {
    if (from_update && indep) vidx_stats.update_partial_orphan++;
    return HA_ERR_OUT_OF_MEM;
  }
  Scope_guard ctx_guard{[ctx, table]() { ctx->release(table); }};

  /* Load graph entry-point: ctx->start drives first-insert (empty
     graph) vs append.  HA_ERR_END_OF_FILE means empty graph; the
     first-insert branch below picks it up via ctx->start == nullptr. */
  err = load_entry_point_node(ctx, graph);
  if (err == HA_ERR_END_OF_FILE) err = 0;
  if (err) {
    vidx::log_slave_dml_err(table, __func__, "load_entry_point_node", err);
    if (from_update && indep) vidx_stats.update_partial_orphan++;
    return err;
  }

  /* For ASSERT_COLUMN_MARKED_FOR_READ in val_str() */
  my_bitmap_map *old_map = dbug_tmp_use_all_columns(table, table->read_set);
  Field *vec_field = keyinfo->key_part->field;
  String buf, *res = vec_field->val_str(&buf);

  vidx_stats.insert_count++;

  /* metadata are checked on open */
  assert(graph);
  assert(keyinfo->flags & HA_VECTOR);
  assert(vec_field->binary());
  assert(vec_field->cmp_type() == STRING_RESULT);
  assert(res);  // ER_INDEX_CANNOT_HAVE_NULL
  assert(table->file->ref_length <= graph->field[FIELD_TREF]->field_length);
  assert(res->length() > 0 && res->length() % 4 == 0);

  /* Narrowed failure tag for slave-side error log; func_end below
     emits a single structured line per failure with this tag. */
  const char *log_op = "mhnsw_insert";

  DBUG_EXECUTE_IF("failed_before_vidx_dml", {
    my_error(ER_VECTOR_INDEX_USAGE, MYF(0), "debug failed before vidx dml.");
    err = 1;
    log_op = "failed_before_vidx_dml(debug)";
  });

  if (err) goto func_end;

  /* First-insert detection: load_entry_point_node returned
     HA_ERR_END_OF_FILE (graph is empty), translated above to 0 with
     ctx->start left nullptr. */
  if (!ctx->start) {
    ctx->set_lengths(res->length());
    auto buff = ctx->alloc_node();
    if (unlikely(!buff)) {
      err = HA_ERR_OUT_OF_MEM;
      log_op = "first-insert: alloc_node";
      goto func_end;
    }

    FVectorNode *target = new (buff)
        FVectorNode(ctx, table->file->ref, res->ptr());
    if ((err = target->alloc_neighborhood(0))) {
      log_op = "first-insert: alloc_neighborhood";
      goto func_end;
    }
    if (!((err = target->save_new_row(graph)))) ctx->start = target;
    else log_op = "first-insert: target->save_new_row";
    goto func_end;
  }

  if (ctx->byte_len != res->length()) {
    /* Custom diagnostic log carries two extra fields (ctx->byte_len /
       res->length) that the structured log_slave_dml_err format does
       not have slots for; emit it directly here.  func_end below still
       fires the standard structured line and runs the column-map
       restore so cleanup stays uniform. */
    if (thd->slave_thread) {
      xp::error(ER_XP_0) << "mhnsw_insert: byte_len mismatch"
                         << ", ctx->byte_len=" << ctx->byte_len
                         << ", res->length=" << res->length()
                         << ", table=" << table->s->db.str << "."
                         << table->s->table_name.str;
    }
    err = HA_ERR_CRASHED;
    log_op = "byte_len_mismatch";
    goto func_end;
  }

{
  MEM_ROOT temp_root(key_memory_vidx_mem, MEM_ROOT_BLOCK_SIZE);
  MEM_ROOT *saved_mem_root = thd->mem_root;
  thd->mem_root = &temp_root;

  Scope_guard guard_mem{
      [thd, saved_mem_root]() { thd->mem_root = saved_mem_root; }};

  const size_t max_found = ctx->max_neighbors(0);
  Neighborhood candidates;
  candidates.init(
      new (thd->mem_root) FVectorNode *[max_found + VISITED_PROBE_BATCH - 1],
      max_found);
  candidates.links[candidates.num++] = ctx->start;

  const double NORMALIZATION_FACTOR = 1 / std::log(ctx->M);
  double log = -std::log(my_rnd(&thd->rand)) * NORMALIZATION_FACTOR;
  const uint8_t max_layer = candidates.links[0]->max_layer;
  uint8_t target_layer =
      std::min<uint8_t>(static_cast<uint8_t>(std::floor(log)),
                        max_layer + 1);
  int cur_layer;

  auto buff = ctx->alloc_node();
  if (unlikely(!buff)) {
    err = HA_ERR_OUT_OF_MEM;
    log_op = "alloc_node";
    goto func_end;
  }

  FVectorNode *target = new (buff)
      FVectorNode(ctx, table->file->ref, res->ptr());

  if ((err = target->alloc_neighborhood(target_layer))) {
    log_op = "alloc_neighborhood";
    goto func_end;
  }

  if ((err = graph->file->ha_rnd_init(0)) > 0) {
    log_op = "ha_rnd_init";
    goto func_end;
  }
  Scope_guard guard_graph{[graph]() { graph->file->ha_rnd_end(); }};

  /*
    The search_layer phase is pure read-only graph traversal:
    FVectorNode::load() uses set_consistent_read(true) for each
    ha_rnd_pos call, so all reads are MVCC snapshot reads holding no
    row locks.  This means the traversal cannot be blocked by — nor
    can it block — any concurrent writer on the graph table
    (attachable inner-trx writes, user SELECT/SHOW on the auxiliary
    table).  No ScopedLockWaitTimeout is needed here.
  */
  for (cur_layer = max_layer; cur_layer > target_layer; cur_layer--) {
    if ((err = search_layer(ctx, graph, target->vec, NEAREST, 1, cur_layer,
                            &candidates, false)) > 0) {
      log_op = "search_layer(upper)";
      goto func_end;
    }
  }

  for (; cur_layer >= 0; cur_layer--) {
    uint max_neighbors = ctx->max_neighbors(cur_layer);
    if ((err = search_layer(ctx, graph, target->vec, NEAREST, max_neighbors,
                            cur_layer, &candidates, true)) > 0) {
      log_op = "search_layer";
      goto func_end;
    }

    if ((err = select_neighbors(ctx, graph, cur_layer, *target, candidates,
                                0, max_neighbors)) > 0) {
      log_op = "select_neighbors";
      goto func_end;
    }
  }

  {
    /*
      On replica apply threads, shorten innodb_lock_wait_timeout so
      that concurrent writers on the graph table fail fast instead
      of stalling apply progress for the full default timeout.
    */
    ScopedLockWaitTimeout scoped_timeout_save(graph->file,
                                              thd->slave_thread);
    err = target->save_new_row(graph);
  }
  if (err > 0) {
    if (err == HA_ERR_LOCK_WAIT_TIMEOUT && thd->slave_thread) {
      vidx::log_slave_dml_warn(table, __func__, "target->save_new_row", err);
      vidx_stats.save_target_skip_lock_timeout++;
      /* On the indep UPDATE path the prior mhnsw_invalidate already
          committed a soft-delete in its own attachable; swallowing
          this insert leaves a real orphan that the caller cannot
          detect from the return code (we are about to return 0).
          Account it here so update_partial_orphan stays coherent
          with reality. */
      if (from_update && indep) vidx_stats.update_partial_orphan++;
      err = 0;
      goto func_end;
    }
    log_op = "target->save_new_row";
    goto func_end;
  }

  if (target_layer > max_layer) ctx->start = target;

  PrefetchResult prefetch_result = PrefetchResult::Ok;
  if ((err = prefetch_neighbor_locks_in_gref_order(
            table, graph, target, target_layer, ctx, thd,
            &prefetch_result)) > 0) {
    log_op = "prefetch_lock";
    goto func_end;
  }
  /* Slave path tolerated a lock-wait-timeout: skip update_2nd
      entirely.  Entering it without the prefetched LOCK_X would re-
      open the very deadlock window prefetch exists to close. */
  if (prefetch_result == PrefetchResult::OkSkipUpdate2nd) goto func_end;

  for (cur_layer = target_layer; cur_layer >= 0; cur_layer--) {
    if ((err = update_second_degree_neighbors(ctx, graph, cur_layer,
                                              target)) > 0) {
      log_op = "update_second_degree_neighbors";
      goto func_end;
    }
  }
}

func_end:
  /* Centralised orphan accounting.  Two distinct triggers, gated by
     {from_update && indep}:
       - save_target_skip_lock_timeout swallow above already bumped
         update_partial_orphan and set err=0; we re-bump here only on
         err != 0, so each scenario gets exactly one ++.
       - any other failure exit path on the indep UPDATE leg leaves the
         prior mhnsw_invalidate's soft-delete unmatched — that IS an
         orphan, and the caller used to bump from hlindexes_on_update.
         Doing it here keeps the policy "orphan is mhnsw_insert's
         business" in one place. */
  if (err && from_update && indep) vidx_stats.update_partial_orphan++;
  if (err) vidx::log_slave_dml_err(table, __func__, log_op, err);
  dbug_tmp_restore_column_map(table->read_set, old_map);
  return err;
}

int mhnsw_read_first(TABLE *table, KEY *, Item *dist) {
  DBUG_TRACE;
  THD *thd = table->in_use;
  TABLE *graph = table->hlindex_active;
  auto *fun = static_cast<Item_func_vec_distance *>(dist->real_item());
  ulonglong limit = fun->get_limit();
  assert(fun);

  vidx_stats.query_count++;

  limit = std::min<ulonglong>(limit, max_ef);

  String buf, *res = fun->get_const_arg()->val_str(&buf);
  MHNSW_Share *ctx;

  /* removed into IndexScanIterator<Reverse>::Init() */
  /* if (int err = table->file->ha_rnd_init(0)) return err; */

  int err = MHNSW_Share::acquire(&ctx, table, false);
  Scope_guard guard{[ctx, table]() { ctx->release(table); }};
  if (err) return err;

  /*
    Validate the query vector at execution time.
    The optimizer (get_key) only checks structural eligibility (field has a
    matching vector key); byte-length validation is deferred here where the
    parsed vector is actually consumed.
  */
  if (!res || res->length() != ctx->byte_len ||
      res->length() % sizeof(float)) {
    my_error(ER_WRONG_ARGUMENTS, MYF(0), fun->func_name());
    return HA_ERR_GENERIC;
  }

  Neighborhood candidates;
  candidates.init(
      new (thd->mem_root) FVectorNode *[limit + VISITED_PROBE_BATCH - 1],
      limit);

  // one could put all max_layer nodes in candidates
  // but it has no effect on the recall or speed
  FVectorNode *start = ctx->start;
  if (unlikely(!start)) return HA_ERR_END_OF_FILE;
  candidates.links[candidates.num++] = start;

  const longlong max_layer = start->max_layer;
  auto target = FVector::create(
      ctx->metric, thd_alloc(thd, FVector::alloc_size(ctx->vec_len)),
      res->ptr(), res->length());

  if ((err = graph->file->ha_rnd_init(0)) > 0) return err;

  /* Reset the per-query traversal counter on the graph table.  Both the
     first search below and every continuation re-search in
     mhnsw_read_next accumulate visited.count into this slot; the total
     is published to the base table in mhnsw_read_end. */
  graph->hlindex_search_stats = {};

  for (size_t cur_layer = max_layer; cur_layer > 0; cur_layer--) {
    if ((err = search_layer(ctx, graph, target, NEAREST, 1, cur_layer,
                            &candidates, false)) > 0) {
      graph->file->ha_rnd_end();
      return err;
    }
  }

  if ((err = search_layer(ctx, graph, target, NEAREST, static_cast<uint>(limit),
                          0, &candidates, false)) > 0) {
    graph->file->ha_rnd_end();
    return err;
  }

  auto result = new (thd->mem_root) Search_context(&candidates, ctx, target);
  graph->context = result;

  return mhnsw_read_next(table);
}

int mhnsw_read_next(TABLE *table) {
  DBUG_TRACE;
  auto result = static_cast<Search_context *>(table->hlindex_active->context);
  /* Drain the already-found candidates.  Skip-and-continue on orphan
     refs: under the indep DML path the aux-graph entry can outlive the
     base row (e.g. invalidate committed independently, then the user
     statement fails and rolls back).  Without that skip a single orphan
     bubbles up to IndexScanIterator::HandleError, which treats it as
     end-of-scan and truncates the LIMIT prematurely (see
     Docs/ann_recall_shrinkage_investigation.md §3).  See
     drain_skipping_orphans in vidx_hnsw_ops.cc for the loop body —
     the skip is accounted in Vidx_read_skip_orphan so operators can
     spot sustained orphan pressure. */
  if (int err = drain_skipping_orphans(table, result); err != HA_ERR_END_OF_FILE)
    return err;
  if (!result->found.num) return HA_ERR_END_OF_FILE;

  TABLE *graph = table->hlindex_active;
  MHNSW_Share *ctx = result->ctx->dup(table->file->has_transactions());
  Scope_guard guard_ctx{[&ctx, table]() { ctx->release(table); }};

  if (ctx->version != result->ctx_version) {
    // oops, shared ctx was modified, need to switch to MHNSW_Trx
    MHNSW_Share *trx;
    graph->file->ha_rnd_end();
    int err = MHNSW_Share::acquire(&trx, table, true);
    Scope_guard guard_trx{[&trx, table]() { trx->release(table); }};
    if (int err2 = graph->file->ha_rnd_init(0)) err = err ? err : err2;
    if (err) return err;
    for (size_t i = 0; i < result->found.num; i++) {
      FVectorNode *node = trx->get_node(result->found.links[i]->gref());
      if (unlikely(!node)) return HA_ERR_OUT_OF_MEM;
      if ((err = node->load(graph))) return err;
      result->found.links[i] = node;
    }
    ctx->release(false, table->s);  // release shared ctx
    result->ctx = trx->dup(false);  // replace it with trx
    result->ctx_version = trx->version;
    std::swap(trx, ctx);  // free shared ctx in this scope, keep trx
  }

  float new_threshold =
      result->found.links[result->found.num - 1]->distance_to(result->target);

  if (int err = search_layer(ctx, graph, result->target, result->threshold,
                             static_cast<uint>(result->pos), 0, &result->found,
                             false))
    return err;
  result->pos = 0;
  result->threshold = new_threshold + FLT_EPSILON;
  return mhnsw_read_next(table);
}

int mhnsw_read_end(TABLE *table) {
  auto result = static_cast<Search_context *>(table->hlindex_active->context);
  /* Publish the full traversal cost (first search + all continuation
     re-searches) to the base table, where EXPLAIN ANALYZE reads it.
     Done here, at scan end, so every re-search in mhnsw_read_next is
     accounted for. */
  table->hlindex_search_stats.nodes_visited =
      table->hlindex_active->hlindex_search_stats.nodes_visited;
  result->ctx->release(false, table->s);
  table->hlindex_active->context = 0;
  table->hlindex_active->file->ha_index_or_rnd_end();
  return 0;
}

void mhnsw_free(TABLE_SHARE *share) {
  if (!share->hlindex_shares) return;
  for (uint slot = 0; slot < share->hlindexes(); slot++) {
    TABLE_SHARE *graph_share = share->hlindex_shares[slot];
    if (!graph_share || !graph_share->hlindex_data) continue;

    auto ctx = static_cast<MHNSW_Share *>(graph_share->hlindex_data);
    if (ctx->get_refcnt() > 1)
      xp::error(ER_XP_0) << "mhnsw_free: refcnt leak"
                         << ", table=" << share->db.str << "."
                         << share->table_name.str
                         << ", slot=" << slot
                         << ", refcnt=" << ctx->get_refcnt()
                         << ", " << get_backtrace_str();
    ctx->~MHNSW_Share();
    graph_share->hlindex_data = nullptr;
  }
}

int mhnsw_invalidate(TABLE *table, KEY *keyinfo [[maybe_unused]]) {
  DBUG_TRACE;
  TABLE *graph = table->hlindex_active;

  assert(table->file->has_transactions());
  MHNSW_Trx *trx_ctx = nullptr;
  int err = MHNSW_Trx::get_from_thd(&trx_ctx, table, /*for_update=*/true);
  if (err) return err;
  MHNSW_Share *ctx = trx_ctx;
  if (!ctx) return HA_ERR_OUT_OF_MEM;
  Scope_guard ctx_guard{[ctx, table]() { ctx->release(table); }};

  /* Load (or rebind to) the entry-point before the invalidate body
     UNCONDITIONALLY — both for indep and non-indep paths.  Reason:
     ctx->get_node() can hash-miss and alloc a tombstone node sized
     by ctx->vec_len (=0 if load_start was skipped), which a
     subsequent mhnsw_insert in the same trx would overflow on
     neighbor writes.  The non-indep path used to assume "caller
     already loaded the graph", but DDL / force_non_indep flows can
     reach invalidate as the first DML in the trx with ctx->vec_len
     still 0 — so we cannot trust the implicit contract.  load is
     idempotent and cheap (ctx->start cache hit) when the trx is
     already loaded, making the unconditional path a safe defensive
     normalisation. */
  err = load_entry_point_node(ctx, graph);
  if (err == HA_ERR_END_OF_FILE) err = 0;
  if (err) {
    vidx::log_slave_dml_err(table, __func__, "load_entry_point_node", err);
    return err;
  }

  vidx_stats.invalidate_count++;

  DBUG_EXECUTE_IF("failed_before_vidx_dml", {
    my_error(ER_VECTOR_INDEX_USAGE, MYF(0), "debug failed before vidx dml.");
    vidx::log_slave_dml_err(table, __func__, "failed_before_vidx_dml(debug)",
                            1);
    return 1;
  });

  /* metadata are checked on open */
  assert(graph);
  assert(keyinfo->flags & HA_VECTOR);
  assert(table->file->ref_length <= graph->field[FIELD_TREF]->field_length);
  /* Caller must have called file->position(record[1]) before the
     Hlindex_dml_scope that wraps us — see Hlindex_dml_scope CONTRACT.
     Verify that file->ref was written (not all-zero). */
  assert(table->file->ref_length > 0);
  DBUG_EXECUTE_IF("vidx_invalidate_assert_ref", {
    bool all_zero = true;
    for (uint i = 0; i < table->file->ref_length; i++) {
      if (table->file->ref[i] != 0) { all_zero = false; break; }
    }
    assert(!all_zero);
  });

  /* Two-step IDX_TREF -> GEN_CLUST locate + TOCTOU + soft-delete + cache
     align — see invalidate_locate_and_softdelete in vidx_hnsw_ops.cc
     for the full lock-order rationale and TOCTOU outcomes. */
  return invalidate_locate_and_softdelete(table, graph, ctx);
}

int mhnsw_delete_all(TABLE *table, KEY *keyinfo [[maybe_unused]]) {
  DBUG_TRACE;
  TABLE *graph = table->hlindex_active;

  assert(table->file->has_transactions());
  MHNSW_Trx *trx_ctx = nullptr;
  int err = MHNSW_Trx::get_from_thd(&trx_ctx, table, /*for_update=*/true);
  if (err) return err;
  MHNSW_Share *ctx = trx_ctx;
  if (!ctx) return HA_ERR_OUT_OF_MEM;
  Scope_guard ctx_guard{[ctx, table]() { ctx->release(table); }};

  /* Load entry-point unconditionally (same defensive rationale as
     mhnsw_invalidate: DDL / force_non_indep flows may enter delete_all
     as the first DML in the trx with ctx uninitialised; load is
     idempotent and cheap when already loaded).  ctx->reset() at the
     bottom relies on a consistent ctx->vec_len/byte_len snapshot to
     clear the right MEM_ROOT extent. */
  err = load_entry_point_node(ctx, graph);
  if (err == HA_ERR_END_OF_FILE) err = 0;
  if (err) {
    vidx::log_slave_dml_err(table, __func__, "load_entry_point_node", err);
    return err;
  }

  /* metadata are checked on open */
  assert(graph);
  assert(keyinfo->flags & HA_VECTOR);

  DBUG_EXECUTE_IF("failed_before_vidx_dml", {
    my_error(ER_VECTOR_INDEX_USAGE, MYF(0), "debug failed before vidx dml.");
    vidx::log_slave_dml_err(table, __func__, "failed_before_vidx_dml(debug)",
                            1);
    return 1;
  });

  if ((err = graph->file->delete_all_rows())) {
    vidx::log_slave_dml_err(table, __func__, "delete_all_rows", err);
    return err;
  }

  /* Reset trx-local cache.  ctx->reset only touches in-memory MEM_ROOT
     and node_cache — no InnoDB interaction, safe inside attachable. */
  ctx->reset(table->s);

  return 0;
}
}  // namespace hnsw
}  // namespace vidx
