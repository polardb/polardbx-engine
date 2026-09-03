/* Copyright (c) 2025, 2025, Alibaba and/or its affiliates. All rights reserved.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

/** @file sql/vidx/vidx_ddl_build.cc
 Vector index DDL direct graph build.

 Created 2026-08-04 by jianhua.sjh
 *******************************************************/

/*
  Vector index DDL-thread direct graph build

  Only ever called from the DDL thread that owns a synchronous vector index
  build; the Worker pool never dispatches these functions independently.

  This path reads rows straight from the copied #sql table, builds within a
  soft-windowed private context, and persists live graph rows before rename.
  With vidx_ddl_threads > 1 every memory window uses micro-batch parallel
  construction. Workers report non-resident historical nodes at a barrier;
  only the DDL leader materializes them through the graph handler, then the
  unfinished searches retry. There is no post-rename repair path: any failure
  aborts the ALTER and discards the intermediate table.
*/

#include <algorithm>
#include <atomic>
#include <cmath>
#include <cstring>
#include <functional>
#include <memory>
#include <vector>

#include "map_helpers.h"
#include "mutex_lock.h"  // MUTEX_LOCK
#include "mysql/psi/mysql_cond.h"
#include "mysql/psi/mysql_mutex.h"
#include "mysql/psi/mysql_stage.h"  // mysql_stage_inc_work_completed
#include "mysql/psi/mysql_thread.h"
#include "sql/debug_sync.h"  // DEBUG_SYNC test hook
#include "sql/malloc_allocator.h"
#include "sql/mysqld.h"  // stage_vector_index_build
#include "sql/sql_error.h"  // Diagnostics_area
#include "sql/sql_lex.h"    // LEX::sql_command
#include "sql/sql_thd_internal_api.h"  // create_internal_thd
#include "sql/consensus/consensus_err.h"  // xp::warn
#include "sql/transaction.h"           // trans_commit_stmt
#include "vidx/vidx_hnsw_ctx.h"
#include "vidx/vidx_index.h"

namespace vidx {
namespace hnsw {

/* DDL-local PFS keys. Keeping them in this translation unit prevents the
   standalone DDL feature from depending on any background service. */
static PSI_mutex_key key_vidx_ddl_build_pool_mutex;
static PSI_cond_key key_vidx_ddl_build_pool_go_cond;
static PSI_cond_key key_vidx_ddl_build_pool_done_cond;
static PSI_thread_key key_thread_vidx_ddl_build_worker;

void init_ddl_build_instrumentation() {
#ifdef HAVE_PSI_INTERFACE
  static PSI_mutex_info mutexes[] = {
      {&key_vidx_ddl_build_pool_mutex, "Ddl_build_pool::mutex", 0, 0,
       PSI_DOCUMENT_ME}};
  static PSI_cond_info conds[] = {
      {&key_vidx_ddl_build_pool_go_cond, "Ddl_build_pool::go", 0, 0,
       PSI_DOCUMENT_ME},
      {&key_vidx_ddl_build_pool_done_cond, "Ddl_build_pool::done", 0, 0,
       PSI_DOCUMENT_ME}};
  static PSI_thread_info threads[] = {
      {&key_thread_vidx_ddl_build_worker, "vidx_ddl_build_worker",
       "vidx_ddl", 0, 0, PSI_DOCUMENT_ME}};
  mysql_mutex_register("vidx", mutexes, array_elements(mutexes));
  mysql_cond_register("vidx", conds, array_elements(conds));
  mysql_thread_register("vidx", threads, array_elements(threads));
#endif
}

using Ddl_node_vector =
    std::vector<FVectorNode *, Malloc_allocator<FVectorNode *>>;

/* Direct-build-only dirty set. The baseline synchronous DML updater writes
   reverse edges immediately; DDL delays only its private graph writes until
   the end of a build window. Keeping this type local prevents that semantic
   difference from leaking into normal INSERT/UPDATE. */
struct Dirty_neighbor_set {
  /* Every destination node whose list changed in this build window. */
  malloc_unordered_set<FVectorNode *> m_nodes{key_memory_vidx_mem};
  bool note(FVectorNode *node) { return m_nodes.insert(node).second; }
};

static uint8_t draw_hnsw_layer(THD *thd, double normalization_factor,
                               uint8_t max_allowed_layer) {
  const double log_val =
      -std::log(my_rnd(&thd->rand)) * normalization_factor;
  return std::min<uint8_t>(static_cast<uint8_t>(std::floor(log_val)),
                           max_allowed_layer);
}

/* Build only the target's forward links. The DDL builder owns allocation,
   reverse links, persistence, and publication. */
static int connect_forward_neighbors(THD *thd, TABLE *graph,
                                     MHNSW_Share *ctx, FVectorNode *entry,
                                     FVectorNode *target,
                                     uint8_t target_layer) {
  const size_t max_found = ctx->max_neighbors(0);
  const int link_top = std::min(static_cast<int>(target_layer),
                                static_cast<int>(entry->max_layer));
  Neighborhood candidates;
  candidates.init(
      new (thd->mem_root) FVectorNode *[max_found + VISITED_PROBE_BATCH - 1],
      max_found);
  candidates.links[candidates.num++] = entry;

  for (int layer = static_cast<int>(entry->max_layer);
       layer > static_cast<int>(target_layer); layer--) {
    if (int err = search_layer(ctx, graph, target->vec, NEAREST, 1, layer,
                               &candidates, false))
      return err;
  }

  for (int layer = link_top; layer >= 0; layer--) {
    const uint max_neighbors = ctx->max_neighbors(layer);
    if (int err = search_layer(ctx, graph, target->vec, NEAREST, max_neighbors,
                               layer, &candidates, true))
      return err;
    if (int err = select_neighbors(ctx, graph, layer, *target, candidates,
                                   nullptr, max_neighbors))
      return err;
  }
  return 0;
}

static void sort_layer_neighbors_by_gref(FVectorNode *node, size_t layer) {
  Neighborhood &neighbors = node->neighbors[layer];
  const size_t gref_len = node->gref_len();
  std::sort(neighbors.links, neighbors.links + neighbors.num,
            [gref_len](FVectorNode *lhs, FVectorNode *rhs) {
              return memcmp(lhs->gref(), rhs->gref(), gref_len) < 0;
            });
}

/* DDL counterpart of the baseline synchronous updater: mutate only the
   private in-memory graph and remember every changed destination. The bulk
   writer persists the distinct destinations once the window is complete. */
static int update_build_reverse_neighbors(
    MHNSW_Share *ctx, TABLE *graph, size_t layer, FVectorNode *node,
    Dirty_neighbor_set *dirty_neighbors) {
  const uint max_neighbors = ctx->max_neighbors(layer);
  Neighborhood &neighbors = node->neighbors[layer];
  sort_layer_neighbors_by_gref(node, layer);
  for (size_t i = 0; i < neighbors.num; i++) {
    FVectorNode *neighbor = neighbors.links[i];
    Neighborhood &reverse = neighbor->neighbors[layer];
    bool changed = false;
    if (reverse.num < max_neighbors) {
      neighbor->push_neighbor(layer, node);
      changed = true;
    } else {
      if (int err = select_neighbors(ctx, graph, layer, *neighbor, reverse,
                                     node, max_neighbors))
        return err;
      for (size_t j = 0; j < reverse.num; j++) {
        if (reverse.links[j] == node) {
          changed = true;
          break;
        }
      }
    }
    if (!changed) continue;
    dirty_neighbors->note(neighbor);
  }
  return 0;
}

/* Per-index aggregate threaded through the direct-build phases. */
struct Direct_build_state {
  /* The four inputs fixed for the whole slot build come in through the
     constructor; everything below the ctor is runtime state with its
     lifecycle in direct_build_one_index and the window loop. */
  Direct_build_state(TABLE *graph, Field *vec_field, size_t budget,
                     ha_rows rows_total)
      : m_graph(graph),
        m_vec_field(vec_field),
        m_budget(budget),
        m_rows_total(rows_total) {}

  TABLE *m_graph;     /* this slot's graph table (the ALTER's handler) */
  Field *m_vec_field; /* indexed vector column of the intermediate table */
  size_t m_budget; /* snapshotted soft threshold of one build window */
  /* This window's nodes awaiting direct bulk persistence. */
  Ddl_node_vector m_built{
      Malloc_allocator<FVectorNode *>(key_memory_vidx_mem)};
  Dirty_neighbor_set m_dirty_neighbors; /* prior-window dirty neighbors */
  FVectorNode *m_local_top = nullptr; /* private graph's entry point */
  bool m_budget_exhausted = false;     /* stopped on the memory budget */
  bool m_warned_memory_budget = false; /* sizing warning emitted once */
  bool m_graph_rnd_open = false;       /* graph rnd scan currently open */
  /* Completed handovers.  Regime selector for the handover policy: the
     capacity-sufficient mainline never hands over more than once (measured
     windows <= 1 at 1M x 2.0GB), while a budget-deficit build crosses
     dozens of times, so ordinal >= 2 identifies the deficit regime three
     orders of magnitude away from the mainline. */
  uint m_handovers = 0;
  ha_rows m_rows_done = 0; /* rows built across all persisted windows */
  ha_rows m_rows_total;    /* copied rows this slot must build (sizing hint) */
};

/* ===== In-memory HNSW construction ===== */

/* Allocate the node and its neighborhood arrays in the build ctx (tref and
   vector are copied by the FVectorNode constructor).  Returns nullptr on
   cache-budget exhaustion, already counted. */
static FVectorNode *alloc_build_node(MHNSW_Share *ctx, const char *tref_data,
                                     const char *vec_data,
                                     uint8_t target_layer) {
  auto *buff = ctx->alloc_node();
  if (!buff) return nullptr;
  FVectorNode *target =
      new (buff) FVectorNode(ctx, (const uchar *)tref_data, vec_data);
  if (!target->vec || target->alloc_neighborhood(target_layer)) return nullptr;
  return target;
}

/* Wire @p target into the in-memory graph: greedy descent from the entry
   point, per-layer construction search + neighbor selection for the node's
   OWN lists, then the reverse edges.  Returns true when the search/select
   phase failed — the node must NOT be published (its own lists are
   incomplete).  A reverse-edge failure is fatal as well: the helper may
   already have linked the target from an existing node before a later
   neighbor fails, so keeping the target or committing the window would
   publish a partially reachable graph.

   Reverse edges make the graph bidirectional; without them search_layer
   cannot navigate and every insert degrades to an O(n) scan (O(n^2)
   build).  Every changed neighbor is recorded into *dirty_neighbors: neighbors
   built in THIS batch are persisted wholesale by the bulk writer's pass 2
   (full NEIGHBORS rewrite), while dirty neighbors from PRIOR windows —
   already persisted with final grefs — are re-persisted by its pass 3
   (save_neighbors_only), which keeps the on-disk graph bidirectional
   across windows. */
static bool link_build_node(THD *thd, TABLE *graph, MHNSW_Share *ctx,
                            FVectorNode *target, uint8_t target_layer,
                            FVectorNode *local_top,
                            Dirty_neighbor_set *dirty_neighbors) {
  const int link_top = std::min((int)target_layer, (int)local_top->max_layer);
  if (connect_forward_neighbors(thd, graph, ctx, local_top, target,
                                target_layer))
    return true;
  for (int rev_layer = link_top; rev_layer >= 0; rev_layer--) {
    int reverse_err = 0;
    /* Ordinary (non lock-fatal) error used by the W=1 DDL regression. */
    DBUG_EXECUTE_IF("vidx_ddl_inject_reverse_error",
                    { reverse_err = HA_ERR_INTERNAL_ERROR; });
    if (!reverse_err)
      reverse_err = update_build_reverse_neighbors(
          ctx, graph, rev_layer, target, dirty_neighbors);
    if (reverse_err) return true;
  }
  return false;
}

/* Build one direct-DDL node in memory and link it into the graph rooted at
   *local_top.  The FVectorNode constructor copies tref and vec into the
   private build ctx, so no staging buffer is needed.  Search work_area uses
   @p temp_root. nullptr is fatal because an incomplete direct build has no
   post-rename retry source. */
static FVectorNode *build_one_node(THD *thd, TABLE *graph, MHNSW_Share *ctx,
                                   const char *tref_data, const char *vec_data,
                                   size_t vec_len, MEM_ROOT &temp_root,
                                   FVectorNode **local_top,
                                   Dirty_neighbor_set *dirty_neighbors) {
  if (ctx->byte_len == 0) ctx->set_lengths(vec_len);
  if (ctx->byte_len != vec_len) return nullptr;

  /* Search work_area (candidate arrays, queues) allocates from temp_root for
     the duration; the node itself lives in the ctx root. */
  MEM_ROOT *saved_mem_root = thd->mem_root;
  thd->mem_root = &temp_root;
  Scope_guard mem_root_guard{
      [thd, saved_mem_root]() { thd->mem_root = saved_mem_root; }};

  const uint8_t layer_cap =
      *local_top ? static_cast<uint8_t>((*local_top)->max_layer + 1) : 0;
  const uint8_t target_layer =
      draw_hnsw_layer(thd, 1.0 / std::log(ctx->M), layer_cap);
  FVectorNode *target =
      alloc_build_node(ctx, tref_data, vec_data, target_layer);
  if (!target) return nullptr;

  /* First row of an empty graph: no linking possible, the node IS the
     entry point. */
  if (!*local_top) {
    target->stored = true;
    *local_top = target;
    return target;
  }

  if (link_build_node(thd, graph, ctx, target, target_layer, *local_top,
                      dirty_neighbors))
    return nullptr;

  target->stored = true;
  /* node_cache registration is deferred to persist_build_window pass 1:
     only there does the node get its durable gref, and the ctor's
     placeholder gref must never be a hash key. */
  if (target_layer > (*local_top)->max_layer) *local_top = target;
  return target;
}

/* Read the next base-table row and decode its vector column — the row
   admission shared by the parallel and serial window loops, so
   the read_set bitmap dance and the strict build error contract live
   in exactly one place. Budget checks stay with the callers: W=1 checks
   before each row, while W>1 checks only after a complete published batch.
   ROW_FATAL covers handler errors, KILL and
   a non-NULL yet undecodable vector, failing the
   ALTER rather than silently under-indexing the table. */
enum class Build_row_verdict { ROW_OK, ROW_EOF, ROW_SKIP, ROW_FATAL };
static Build_row_verdict read_next_vector_row(THD *thd, TABLE *new_table,
                                              Field *vec_field, String *vec_buf,
                                              String **vec_res) {
  const int rerr = new_table->file->ha_rnd_next(new_table->record[0]);
  if (rerr == HA_ERR_END_OF_FILE) return Build_row_verdict::ROW_EOF;
  if (rerr) return Build_row_verdict::ROW_FATAL;
  if (thd->killed) return Build_row_verdict::ROW_FATAL;

  /* Progress measures copied rows inspected per vector-index slot. NULL
     vectors are valid no-entry rows, so they still complete one unit. */
  mysql_stage_inc_work_completed(thd->m_stage_progress_psi, 1);

  /* NULL vector: no index entry (same contract as hlindexes_on_insert's
     check_vector_is_null). */
  if (vec_field->is_real_null()) return Build_row_verdict::ROW_SKIP;
  my_bitmap_map *old_map = tmp_use_all_columns(new_table, new_table->read_set);
  *vec_res = vec_field->val_str(vec_buf);
  tmp_restore_column_map(new_table->read_set, old_map);
  if (!*vec_res || (*vec_res)->length() == 0)
    return Build_row_verdict::ROW_FATAL;
  new_table->file->position(new_table->record[0]);
  return Build_row_verdict::ROW_OK;
}

/* ===== Micro-batch parallel DDL build (W > 1) =====

   W short-lived workers (created per DDL, joined before return -- there is
   no global InnoDB DDL thread pool to hand tasks to) run the read-only
   candidate searches and the sharded reverse-edge install; the DDL thread
   reads and admits one batch, then generates the reverse-edge ops and
   serially publishes it before reading the next batch.  Workers never touch
   the THD, any handler, or the shared ctx allocator. */

/* Per-row task: node allocated/layered by the DDL thread; a worker fills
   m_layer_cands[0..m_searched_top] by read-only graph searches, resolving any
   cache miss inline on its own load channel. */
struct Ddl_build_task {
  FVectorNode *m_node{nullptr}; /* allocated/layered by the DDL thread */
  Neighborhood *m_layer_cands{nullptr}; /* [0..m_node->max_layer] */
  int m_searched_top{0}; /* min(m_node->max_layer, snapshot top layer) */
  int m_link_top{-1};    /* top layer the worker selected neighbors at */
};

/* One reverse-edge install, materialized by the leader from the frozen
   post-search batch state and applied by the destination's shard owner.
   changed carries the dirty intent back to the leader (the dirty set is
   not thread-safe — single writer only). */
struct Reverse_edge_op {
  FVectorNode *m_neigh; /* destination list; shard key */
  FVectorNode *m_node;  /* the batch row being reverse-linked */
  uint8_t m_layer;      /* destination layer of the edge */
  bool m_changed;       /* out: list mutated (dirty intent) */
};

using Reverse_edge_ops =
    std::vector<Reverse_edge_op, Malloc_allocator<Reverse_edge_op>>;

struct Ddl_build_pool {
  mysql_mutex_t m_mutex;   /* protects the dispatch handshake state */
  mysql_cond_t m_go, m_done; /* dispatch published / all consumers done */
  uint64_t m_batch_seq{0}; /* leader increments to publish a dispatch */
  uint m_running{0};       /* workers still consuming the current dispatch */
  uint m_nstarted{0};      /* worker threads actually created */
  bool m_shutdown{false};  /* set by the dtor: workers exit at wakeup */
  /* What the current dispatch consumes: per-row candidate searches
     (SEARCH, stage 2) or per-shard reverse-edge installs (REVERSE,
     stage 3a). */
  enum class Phase { SEARCH, REVERSE } m_phase{Phase::SEARCH};

  MHNSW_Share *m_ctx{nullptr}; /* current window's build ctx */
  TABLE *m_graph{nullptr};     /* the ALTER's own graph handler */
  FVectorNode *m_top{nullptr}; /* batch-start entry point snapshot */
  Ddl_build_task *m_tasks{nullptr}; /* [m_ntasks], the current batch */
  uint m_ntasks{0};   /* rows admitted into the current batch */
  uint m_cand_cap{0}; /* per-layer candidate buffer capacity */
  Reverse_edge_ops *m_shard_ops{nullptr}; /* [m_nshards] */
  uint m_nshards{0};  /* reverse-op shards (one per consumer slot) */
  /* First error of the whole build (graph binding, search, or reverse
     install), CAS'd by the failing consumer.  Doubles as the abort brake:
     non-zero stops every consumer from claiming further work.  Sticky
     across dispatches -- never reset (see dispatch()). */
  std::atomic<int> m_first_err{0};
  std::atomic<uint> m_next{0}; /* claim cursor, reset per dispatch */

  /* Worker-private graph TABLE instances for read-through loading.
     Opened detached by the leader before the threads start (see
     vidx::hlindex_open_worker_instance), claimed one per worker via
     m_graph_claim, rebound to each worker's internal THD, and closed by
     the leader after the pool joined.  Exactly m_nworker_graphs workers
     are started, so every running worker owns a load channel; an open
     shortfall only lowers the effective parallelism (0 = leader-inline,
     the leader loads on the DDL graph handler). */
  TABLE **m_worker_graphs{nullptr};
  uint m_nworker_graphs{0};
  std::atomic<uint> m_graph_claim{0};

  /* Sticky first-error: only the first reporter wins, later errors keep
     the original root cause. */
  void note_first_err(int err) {
    int expected = 0;
    m_first_err.compare_exchange_strong(expected, err,
                                        std::memory_order_relaxed);
  }

  /* Publish one dispatch without consuming or waiting (the leader overlaps
     its own work with the workers); join_dispatch is the blocking
     counterpart.  A leader that wants a share of the phase simply runs it
     between the two calls — the zero-worker fallback re-runs the phase,
     which is a no-op once the claim cursor is exhausted.  Definitions
     follow run_build_phase below. */
  void dispatch(Phase ph);
  void join_dispatch(Search_work_area *leader_work_area);
};

/* Per-row worker phase: greedy ef=1 descent from the batch-start snapshot, a
   construction search per linking layer, then in-batch compensation and the
   neighbor selection for this row's OWN adjacency lists.  Earlier in-batch
   rows are added as direct candidates, but the worker does not traverse their
   newly installed adjacency; W>1 therefore builds a valid deterministic HNSW
   graph, not a graph promised to match W=1 byte-for-byte.  The row's node is
   thread-confined until the DDL thread links it, so writing its neighbor lists
   needs no lock.  All work_area memory is thread-private. */
static int search_build_task(Ddl_build_pool *pool, uint idx,
                             Search_work_area *work_area) {
  MHNSW_Share *ctx = pool->m_ctx;
  FVectorNode *top = pool->m_top;
  Ddl_build_task *t = &pool->m_tasks[idx];
  FVectorNode *node = t->m_node;
  const size_t cap0 = ctx->max_neighbors(0);
  int running_top = top->max_layer;
  /* MY_ALIGN(cap0, VISITED_PROBE_BATCH): init() memsets that many slots
     (its rounding contract, see the Neighborhood docblock). */
  auto *buf = static_cast<FVectorNode **>(work_area->m_root.Alloc(
      sizeof(FVectorNode *) * MY_ALIGN(cap0, VISITED_PROBE_BATCH)));
  if (unlikely(!buf)) return HA_ERR_OUT_OF_MEM;
  Neighborhood work;
  work.init(buf, cap0);
  work.links[work.num++] = top;

  t->m_link_top = -1;
  for (int L = 0; L <= node->max_layer; L++) {
    t->m_layer_cands[L].num = 0;
    node->neighbors[L].num = 0;
  }

  for (int cur = top->max_layer; cur > t->m_searched_top; cur--)
    if (int err = search_layer(ctx, pool->m_graph, node->vec, NEAREST, 1, cur,
                               &work, false, work_area))
      return err;
  for (int cur = t->m_searched_top; cur >= 0; cur--) {
    const uint maxn = ctx->max_neighbors(cur);
    if (int err = search_layer(ctx, pool->m_graph, node->vec, NEAREST, maxn,
                               cur, &work, true, work_area))
      return err;
    /* Snapshot this layer's result; the work array descends further. */
    Neighborhood &out = t->m_layer_cands[cur];
    memcpy(out.links, work.links, work.num * sizeof(FVectorNode *));
    out.num = work.num;
  }

  /* In-batch compensation: rows 0..idx-1 join the candidate lists directly,
     so row idx can select earlier rows even though all workers searched the
     common batch-start graph.  This preserves their candidacy, not traversal
     through adjacency installed earlier in the batch. */
  for (uint j = 0; j < idx; j++) {
    FVectorNode *prev = pool->m_tasks[j].m_node;
    const int shared = std::min<int>(node->max_layer, prev->max_layer);
    for (int L = 0; L <= shared; L++) {
      Neighborhood &c = t->m_layer_cands[L];
      if (c.num < pool->m_cand_cap) c.links[c.num++] = prev;
    }
    running_top = std::max<int>(running_top, prev->max_layer);
  }

  /* Same bound the serial link phase derives from its running local_top:
     snapshot top raised by every earlier in-batch row. */
  t->m_link_top = std::min<int>(node->max_layer, running_top);
  for (int L = t->m_link_top; L >= 0; L--) {
    Neighborhood &c = t->m_layer_cands[L];
    if (c.num == 0) {
      /* Unreachable per design: every layer L <= m_link_top has at least one
         candidate — L <= m_searched_top layers get the construction-search
         result, and L > m_searched_top only enters m_link_top when an earlier
         in-batch row reaches L, which the compensation loop above then
         appended.  Enforce instead of defending (a silent skip would just
         hide a broken invariant as a degraded graph). */
      return HA_ERR_INTERNAL_ERROR;
    }
    if (int err = select_neighbors(ctx, L, *node, c,
                                   /*extra_candidate=*/nullptr,
                                   ctx->max_neighbors(L), work_area))
      return err;
  }
  return 0;
}

static void run_search_tasks(Ddl_build_pool *pool,
                             Search_work_area *work_area) {
  for (;;) {
    const uint i = pool->m_next.fetch_add(1, std::memory_order_relaxed);
    if (i >= pool->m_ntasks) break;
    if (pool->m_first_err.load(std::memory_order_relaxed) != 0) break;
    if (int err = search_build_task(pool, i, work_area))
      pool->note_first_err(err);
    work_area->clear_for_reuse();
  }
}

/* Stage 3a consumer: claim whole shards; a shard owns every op whose
   destination hashes to it, so its lists have exactly one writer.  The
   leader-generated per-destination order (row, layer desc, sorted list
   position) makes the result deterministic and equivalent to applying this
   already-generated reverse-op stream serially.  It does not imply that the
   W>1 candidate graph matches a W=1 build.  Any error stops the build (strict:
   the direct path cannot repair an incomplete graph after rename). */
static void run_reverse_tasks(Ddl_build_pool *pool,
                              Search_work_area *work_area) {
  for (;;) { /* claim shards until exhausted */
    const uint s = pool->m_next.fetch_add(1, std::memory_order_relaxed);
    if (s >= pool->m_nshards) break;
    if (pool->m_first_err.load(std::memory_order_relaxed) != 0) break;
    for (Reverse_edge_op &op : pool->m_shard_ops[s]) {
      if (int err = link_reverse_neighbor(
              pool->m_ctx, op.m_layer, op.m_node, op.m_neigh,
              pool->m_ctx->max_neighbors(op.m_layer), &op.m_changed,
              work_area)) {
        pool->note_first_err(err);
        break;
      }
    }
    work_area->clear_for_reuse();
  }
}

static void run_build_phase(Ddl_build_pool *pool, Search_work_area *work_area) {
  if (pool->m_phase == Ddl_build_pool::Phase::SEARCH)
    run_search_tasks(pool, work_area);
  else
    run_reverse_tasks(pool, work_area);
}

/* Per-worker graph binding: the internal THD, the claimed TABLE instance,
   and a minimal default Table_ref.  The Table_ref exists because
   ha_innobase::store_lock unconditionally reads
   table->pos_in_table_list->lock_descriptor() (SKIP LOCKED/NOWAIT switch);
   a default-constructed descriptor yields SELECT_ORDINARY. */
struct Ddl_worker_graph_binding {
  THD *m_thd{nullptr};     /* internal THD at READ UNCOMMITTED */
  TABLE *m_table{nullptr}; /* claimed worker graph instance */
  Table_ref m_ref;         /* backs pos_in_table_list->lock_descriptor() */
};

/* Bind one leader-opened graph TABLE instance to this worker thread: an
   internal THD at READ UNCOMMITTED plus the hlindex_lock() rebind sequence.

   READ UNCOMMITTED is a requirement, not a shortcut: window 2+ loads read
   graph rows persisted by the ALTER's own still-uncommitted transaction
   from a *different* transaction, and it is the only isolation level whose
   non-locking read returns the newest row version without a visibility
   check (row_search_mvcc "Do nothing" branch).  It also opens no read
   view (purge stays unblocked) and takes no row locks (no lock-wait hang
   on kill).  Torn rows are not a concern: searches and graph-table writes
   are separated by the dispatch barriers, so worker reads never overlap a
   persist pass.

   store_lock() must run BEFORE external_lock(): it is the only call that
   copies the session isolation level into the InnoDB trx, and only while
   trx->n_mysql_tables_in_use == 0 (external_lock increments it).  Setting
   the THD fields alone leaves the trx at its default REPEATABLE READ and
   every read-through rnd_pos fails with HA_ERR_KEY_NOT_FOUND.

   The THD must also read as a plain SELECT (lex->sql_command): for any
   other command store_lock() turns TL_READ into a locking read, and its
   S record locks wait on the ALTER transaction's own uncommitted graph
   rows until the lock timeout fails the whole build.

   The load channel is an invariant of every running worker (only
   m_nworker_graphs workers are started, one claimed instance each), so a
   binding failure here -- internal THD allocation or the lock/init
   sequence, both OOM-grade events -- fails the whole ALTER through
   m_first_err instead of degrading (strict: same fail-closed contract as
   every other build error). */
static void ddl_worker_bind_graph(Ddl_build_pool *pool,
                                  Ddl_worker_graph_binding *binding) {
  const uint slot = pool->m_graph_claim.fetch_add(1, std::memory_order_relaxed);
  TABLE *t =
      slot < pool->m_nworker_graphs ? pool->m_worker_graphs[slot] : nullptr;
  THR_LOCK_DATA *dummy_data = nullptr;
  THR_LOCK_DATA **dummy_to = &dummy_data;
  if (t == nullptr) {
    pool->note_first_err(HA_ERR_INTERNAL_ERROR);
    return;
  }

  THD *thd = create_internal_thd();
  DBUG_EXECUTE_IF("vidx_ddl_worker_bind_fail", {
    if (thd != nullptr) destroy_internal_thd(thd);
    thd = nullptr;
  });
  if (thd == nullptr) {
    pool->note_first_err(HA_ERR_OUT_OF_MEM);
    return;
  }
  thd->variables.transaction_isolation = ISO_READ_UNCOMMITTED;
  thd->tx_isolation = ISO_READ_UNCOMMITTED;
  /* A plain-SELECT command keeps store_lock() on the non-locking read
     path (see the docblock above). */
  thd->lex->sql_command = SQLCOM_SELECT;

  t->file->rebind_psi();
  t->file->ha_extra(HA_EXTRA_RESET_STATE);
  t->reset();
  t->set_created();
  t->use_all_columns();
  t->in_use = thd;
  t->pos_in_table_list = &binding->m_ref;
  /* Rebind rollback shared by every failure below: detach the instance and
     destroy the internal THD.  Disarmed once the binding is published. */
  Scope_guard rebind_rollback_guard{[t, thd]() {
    t->in_use = nullptr;
    t->pos_in_table_list = nullptr;
    destroy_internal_thd(thd);
  }};

  t->file->store_lock(thd, dummy_to, TL_READ);

  if (t->file->ha_external_lock(thd, F_RDLCK)) {
    pool->note_first_err(HA_ERR_LOCK_WAIT_TIMEOUT);
    return;
  }
  if (t->file->ha_rnd_init(true)) {
    t->file->ha_external_lock(thd, F_UNLCK);
    trans_commit_stmt(thd);
    trans_commit(thd);
    pool->note_first_err(HA_ERR_INTERNAL_ERROR);
    return;
  }
  rebind_rollback_guard.commit();
  binding->m_thd = thd;
  binding->m_table = t;
}

/* Detach the worker's graph instance and end the read-only transaction on
   the owning thread; the leader closes the TABLE after the pool joined. */
static void ddl_worker_unbind_graph(Ddl_worker_graph_binding *binding) {
  if (binding->m_table != nullptr) {
    binding->m_table->file->ha_rnd_end();
    binding->m_table->file->ha_external_lock(binding->m_thd, F_UNLCK);
    binding->m_table->in_use = nullptr;
    binding->m_table->pos_in_table_list = nullptr;
  }
  if (binding->m_thd != nullptr) {
    trans_commit_stmt(binding->m_thd);
    trans_commit(binding->m_thd);
    destroy_internal_thd(binding->m_thd);
  }
}

extern "C" {
static void *ddl_build_worker_func(void *arg) {
  my_thread_init();
  auto *pool = static_cast<Ddl_build_pool *>(arg);
  Search_work_area work_area;
  Ddl_worker_graph_binding binding;
  ddl_worker_bind_graph(pool, &binding);
  work_area.m_load_graph = binding.m_table;
  uint64_t seen = 0;
  for (;;) {
    {
      MUTEX_LOCK(mutex_guard, &pool->m_mutex);
      while (pool->m_batch_seq == seen && !pool->m_shutdown)
        mysql_cond_wait(&pool->m_go, &pool->m_mutex);
      if (pool->m_shutdown) break;
      seen = pool->m_batch_seq;
    }

    /* A worker whose graph binding failed set m_first_err and must not
       claim tasks (its load channel is null); it still participates in
       the running/done handshake so the leader's join can not hang. */
    if (binding.m_table != nullptr) run_build_phase(pool, &work_area);

    {
      MUTEX_LOCK(mutex_guard, &pool->m_mutex);
      if (--pool->m_running == 0) mysql_cond_signal(&pool->m_done);
    }
  }
  ddl_worker_unbind_graph(&binding);
  my_thread_end();
  return nullptr;
}
}  // extern "C"

void Ddl_build_pool::dispatch(Phase ph) {
  m_next.store(0, std::memory_order_relaxed);
  /* m_first_err is sticky across dispatches on purpose: any error --
     including a worker graph-binding failure raised before or between
     dispatches -- must fail the whole build, never be reset. */
  MUTEX_LOCK(mutex_guard, &m_mutex);
  m_phase = ph;
  m_batch_seq++;
  m_running = m_nstarted;
  mysql_cond_broadcast(&m_go);
}

void Ddl_build_pool::join_dispatch(Search_work_area *leader_work_area) {
  /* Zero started workers (no graph instance opened, or thread-create
     failures): the leader is the only consumer left, run the phase inline
     on its own load channel like the historical leader-share model. */
  if (m_nstarted == 0) run_build_phase(this, leader_work_area);
  MUTEX_LOCK(mutex_guard, &m_mutex);
  while (m_running > 0) mysql_cond_wait(&m_done, &m_mutex);
}

/* Rows per dispatch: W searches run concurrently while multiple rows per
   thread amortize the mutex/condvar handoff (one dispatch per W rows
   measured 250k dispatches for 1M rows at W=4).  Kept small enough that
   the in-batch blind window -- rows that reach each other only through
   the direct candidate append, not through graph search -- stays
   negligible relative to the graph size (m_batch_rows-1 rows, vs an
   ef_construction-wide candidate set).

   Independent of the vector width on purpose: batch-level structures
   (tasks, candidate buffers, reverse-op shards) hold node pointers and
   candidate metadata only -- vector payloads live in the ctx arena and
   are governed by the vidx_hnsw_cache_size window budget, not by the
   batch.  Row width only enters through the per-row search cost, and it
   moves in the safe direction: wider vectors make each task more
   expensive, shrinking the dispatch-handoff share of a fixed-size batch
   even further.

   Measured at 8: W=1..16 self-speedup 12.8x and no recall regression
   (768D x 1M); 4/16/32 were not swept, so 8 is a working point, not a
   proven optimum.  Changing it changes m_batch_rows = W * this, which the
   exact-batch-count assertions in vidx_ddl_parallel_build.test and
   vidx_ddl_parallel_windows.test hardcode -- update them together. */
static constexpr uint DDL_BUILD_ROWS_PER_THREAD = 8;

/* RAII state for one parallel index-slot build: the worker pool is reused
   across windows, then shut down and joined by the destructor no matter which
   exit path the build takes. It also owns one task buffer + candidate arena
   (recycled after each batch is published), and the stage-3a reverse-edge
   shards (one op vector per consumer slot, leader included;
   destination-pointer hashing keeps every adjacency list single-writer). */
struct Ddl_parallel_build {
  Ddl_build_pool m_pool; /* dispatch/join state shared with the workers */
  std::vector<my_thread_handle> m_handles; /* joinable worker threads */
  Search_work_area m_leader_work_area; /* leader's own share of a phase */
  std::vector<Ddl_build_task> m_tasks; /* one batch of per-row tasks */
  /* Per-batch scratch (candidate buffers, preload targets); recycled after
     each publish. */
  MEM_ROOT m_batch_root{key_memory_vidx_mem, MEM_ROOT_BLOCK_SIZE};
  std::vector<Reverse_edge_ops> m_shard_ops; /* one op vector per shard */
  uint m_batch_rows; /* rows per dispatch: W * DDL_BUILD_ROWS_PER_THREAD */
  size_t m_cand_cap; /* per-layer candidate buffer capacity */
  double m_normalization_factor; /* 1 / ln M, for the layer draw */
  bool m_eof = false; /* base-table scan exhausted */

  Ddl_parallel_build(TABLE *graph, MHNSW_Share *ctx, uint W,
                     TABLE **worker_graphs, uint nworker_graphs)
      : m_handles(nworker_graphs),
        m_batch_rows(W * DDL_BUILD_ROWS_PER_THREAD),
        m_normalization_factor(1.0 / std::log(ctx->M)) {
    mysql_mutex_init(key_vidx_ddl_build_pool_mutex, &m_pool.m_mutex,
                     MY_MUTEX_INIT_FAST);
    mysql_cond_init(key_vidx_ddl_build_pool_go_cond, &m_pool.m_go);
    mysql_cond_init(key_vidx_ddl_build_pool_done_cond, &m_pool.m_done);
    m_pool.m_ctx = ctx;
    m_pool.m_graph = graph;
    m_pool.m_worker_graphs = worker_graphs;
    m_pool.m_nworker_graphs = nworker_graphs;
    /* The leader's load channel is the ALTER's own graph handler: loads on
       it read the same uncommitted transaction (the historical serial-path
       behaviour), so leader-inline phases resolve misses like any worker. */
    m_leader_work_area.m_load_graph = graph;
    m_shard_ops.reserve(W);
    for (uint i = 0; i < W; ++i)
      m_shard_ops.emplace_back(
          Malloc_allocator<Reverse_edge_op>(key_memory_vidx_mem));
    m_pool.m_shard_ops = m_shard_ops.data();
    m_pool.m_nshards = W;
    /* Per-layer candidate buffer capacity, factor by factor:
         max_neighbors(0)   the construction search returns at most this
                            many candidates per layer (layer 0 is the
                            widest: 2*M);
       + m_batch_rows       in-batch compensation may append up to
                            m_batch_rows-1 earlier rows of the same batch
                            (bounded by the c.num < m_cand_cap guard).
       MY_ALIGN(.., VISITED_PROBE_BATCH) matches the adjacency-array
       allocation habit only: these buffers are filled per-slot and never
       scanned by the batched bloom probes (those read node adjacency
       lists, which go through Neighborhood::init), so the rounding is not
       a read-width requirement here. */
    m_cand_cap =
        MY_ALIGN(ctx->max_neighbors(0) + m_batch_rows, VISITED_PROBE_BATCH);
    m_pool.m_cand_cap = static_cast<uint>(m_cand_cap);
    m_tasks.resize(m_batch_rows);
    start_workers();
  }

  /* Create one joinable worker per successfully opened graph instance:
     every running worker owns a load channel.  Any attr or thread-create
     failure only lowers m_pool.m_nstarted (0 workers = leader-inline
     fallback, see join_dispatch). */
  void start_workers() {
    my_thread_attr_t attr;
    const bool attr_initialized =
        !DBUG_EVALUATE_IF("vidx_ddl_build_fail_thread_attr_init", 1,
                          my_thread_attr_init(&attr));
    if (!attr_initialized) return;
    const bool attr_configured = !DBUG_EVALUATE_IF(
        "vidx_ddl_build_fail_thread_attr_setdetachstate", 1,
        my_thread_attr_setdetachstate(&attr, MY_THREAD_CREATE_JOINABLE));
    if (attr_configured) {
      for (uint i = 0; i < m_pool.m_nworker_graphs; i++) {
        if (mysql_thread_create(key_thread_vidx_ddl_build_worker,
                                &m_handles[i], &attr, ddl_build_worker_func,
                                &m_pool))
          break; /* 0 workers falls back to the leader. */
        m_pool.m_nstarted++;
      }
    }
    my_thread_attr_destroy(&attr);
  }

  /* Workers are idle at both calls. dispatch() takes the pool mutex after
     bind_window(), publishing the new ctx to every waiter. detach_window()
     removes every raw pointer into the old window before its arena dies. */
  void bind_window(MHNSW_Share *ctx) {
    MUTEX_LOCK(mutex_guard, &m_pool.m_mutex);
    assert(m_pool.m_running == 0);
    m_pool.m_ctx = ctx;
    m_eof = false;
  }

  void detach_window() {
    {
      MUTEX_LOCK(mutex_guard, &m_pool.m_mutex);
      assert(m_pool.m_running == 0);
      for (Reverse_edge_ops &ops : m_shard_ops) ops.clear();
      for (Ddl_build_task &task : m_tasks) {
        task.m_node = nullptr;
        task.m_layer_cands = nullptr;
        task.m_link_top = -1;
      }
      m_pool.m_ctx = nullptr;
      m_pool.m_top = nullptr;
      m_pool.m_tasks = nullptr;
      m_pool.m_ntasks = 0;
    }
    m_leader_work_area.clear_for_reuse();
    m_batch_root.ClearForReuse();
  }

  ~Ddl_parallel_build() {
    /* Inner scope: the mutex must be released before joining the workers
       and, above all, before mysql_mutex_destroy below. */
    {
      MUTEX_LOCK(mutex_guard, &m_pool.m_mutex);
      m_pool.m_shutdown = true;
      mysql_cond_broadcast(&m_pool.m_go);
    }
    for (uint i = 0; i < m_pool.m_nstarted; i++)
      my_thread_join(&m_handles[i], nullptr);
    mysql_mutex_destroy(&m_pool.m_mutex);
    mysql_cond_destroy(&m_pool.m_go);
    mysql_cond_destroy(&m_pool.m_done);
  }
};

/* ---- Stage 1a (leader): read one batch and materialize its
   graph-independent state -- ha_rnd_next, NULL / decode checks,
   position(), node allocation and the tref+vec copy.  It runs only after
   the previous batch has been published, so one task buffer is sufficient.
   The very first row of an empty graph takes the full serial build path via
   build_one_node and does not occupy a parallel slot. */
static bool read_build_batch(THD *thd, TABLE *new_table, MHNSW_Share *ctx,
                             MEM_ROOT &temp_root, Direct_build_state &state,
                             Ddl_parallel_build &build, uint *out_n) {
  uint n = 0;
  while (n < build.m_batch_rows && !build.m_eof) {
    String vec_buf, *vec_res = nullptr;
    switch (read_next_vector_row(thd, new_table, state.m_vec_field, &vec_buf,
                                 &vec_res)) {
      case Build_row_verdict::ROW_EOF:
        build.m_eof = true;
        continue; /* loop condition now fails: single epilogue exit */
      case Build_row_verdict::ROW_FATAL:
        return true;
      case Build_row_verdict::ROW_SKIP:
        continue;
      case Build_row_verdict::ROW_OK:
        break;
    }

    if (!state.m_local_top) {
      /* First row of the empty graph: full serial path (also gives the
         workers an entry point to search from). */
      FVectorNode *node =
          build_one_node(thd, state.m_graph, ctx,
                         reinterpret_cast<const char *>(new_table->file->ref),
                         vec_res->ptr(), vec_res->length(), temp_root,
                         &state.m_local_top, &state.m_dirty_neighbors);
      if (!node) return true;
      state.m_built.push_back(node);
      continue; /* does not occupy a parallel slot */
    }

    if (ctx->byte_len == 0) ctx->set_lengths(vec_res->length());
    if (ctx->byte_len != vec_res->length()) return true;

    auto *nbuf = ctx->alloc_node();
    if (!nbuf) return true;
    build.m_tasks[n].m_node = new (nbuf)
        FVectorNode(ctx, reinterpret_cast<const uchar *>(new_table->file->ref),
                    vec_res->ptr());
    if (unlikely(!build.m_tasks[n].m_node->vec)) return true;
    n++;
  }
  *out_n = n;
  return false;
}

/* ---- Stage 1b (leader, serial prologue): the graph-dependent part of
   row admission, run only AFTER the previous batch is fully published --
   layer draws stay on thd->rand in row order and are capped by the
   running top INCLUDING earlier in-batch rows, so the layer sequence is
   byte-identical to the serial build's; m_searched_top snapshots the
   just-updated entry point; candidate buffers come from this batch's
   arena. */
static bool prepare_build_batch(THD *thd, Direct_build_state &state,
                                Ddl_parallel_build &build,
                                std::vector<Ddl_build_task> &buf, uint n,
                                MEM_ROOT &arena) {
  uint running_top = state.m_local_top ? state.m_local_top->max_layer : 0;
  for (uint i = 0; i < n; i++) {
    Ddl_build_task &t = buf[i];
    const uint8_t target_layer = draw_hnsw_layer(
        thd, build.m_normalization_factor,
        static_cast<uint8_t>(running_top + 1));
    if (t.m_node->alloc_neighborhood(target_layer)) return true;
    if (target_layer > running_top) running_top = target_layer;
    t.m_link_top = -1;
    t.m_searched_top =
        std::min<int>(target_layer, state.m_local_top->max_layer);
    const uint n_layers = static_cast<uint>(target_layer) + 1;
    t.m_layer_cands = static_cast<Neighborhood *>(
        arena.Alloc(sizeof(Neighborhood) * n_layers));
    if (!t.m_layer_cands) return true;
    for (uint L = 0; L < n_layers; L++) {
      auto *cb = static_cast<FVectorNode **>(
          arena.Alloc(sizeof(FVectorNode *) * build.m_cand_cap));
      if (!cb) return true;
      t.m_layer_cands[L].links = cb;
      t.m_layer_cands[L].num = 0;
    }
  }
  return false;
}

/* ---- Stage 3a op generation (leader): materialize every reverse-edge op
   from the frozen post-search state (within one batch no row's own
   adjacency is ever mutated before it is scanned here: in-batch
   compensation only lets row i see rows j<i, so reverse edges from row j
   land exclusively on pre-batch nodes or on in-batch nodes m<j whose op
   generation reads nothing afterwards — the serial stage 3 relied on the
   same fact via its row order).  Ops are bucketed by destination-pointer
   hash: one shard = one writer for every list it touches, and within a
   shard the (row, layer desc, gref-sorted position) generation order is
   preserved per destination.  select_neighbors reads only vectors and the
   destination's own list, never other lists, so the sharded application is
   deterministic for this frozen op stream.  No cross-W graph-identity claim
   is made.  No locks anywhere. */
static void generate_reverse_ops(Ddl_parallel_build &build,
                                 std::vector<Ddl_build_task> &buf, uint nrows) {
  const uint nshards = build.m_pool.m_nshards;
  for (uint s = 0; s < nshards; s++) build.m_shard_ops[s].clear();
  for (uint i = 0; i < nrows; i++) {
    Ddl_build_task &t = buf[i];
    for (int L = t.m_link_top; L >= 0; L--) {
      sort_layer_neighbors_by_gref(t.m_node, L);
      Neighborhood &nb = t.m_node->neighbors[L];
      for (size_t p = 0; p < nb.num; p++) {
        /* >>6 decorrelates MEM_ROOT allocation strides before the mod. */
        const uint s = static_cast<uint>(
            (reinterpret_cast<uintptr_t>(nb.links[p]) >> 6) % nshards);
        build.m_shard_ops[s].push_back(
            {nb.links[p], t.m_node, static_cast<uint8_t>(L), false});
      }
    }
  }
}

/* REVERSE mutates adjacency in place; keep its I/O profile deterministic by
   materializing, while every worker is stopped, the destination and every
   member of its current list. This is sufficient even
   for lists that are not full yet: later members introduced by this batch are
   batch nodes and already resident, so a subsequent re-prune has no new I/O
   dependency. */
static int preload_reverse_dependencies(THD *thd,
                                        Ddl_parallel_build &build) {
  struct Preload_target {
    FVectorNode *m_node;
    uint8_t m_layer;
  };

  size_t ntargets = 0;
  size_t pos = 0;
  ulonglong loaded = 0;
  for (const Reverse_edge_ops &ops : build.m_shard_ops) ntargets += ops.size();
  if (ntargets == 0) return 0;
  auto *targets = static_cast<Preload_target *>(
      build.m_batch_root.Alloc(sizeof(Preload_target) * ntargets));
  if (!targets) return HA_ERR_OUT_OF_MEM;
  for (const Reverse_edge_ops &ops : build.m_shard_ops)
    for (const Reverse_edge_op &op : ops)
      targets[pos++] = {op.m_neigh, op.m_layer};
  /* Many source rows can target the same adjacency list. Preflight each
     (destination, layer) once instead of rescanning its M members per op. */
  std::sort(targets, targets + ntargets,
            [](const Preload_target &lhs, const Preload_target &rhs) {
              if (lhs.m_node == rhs.m_node) return lhs.m_layer < rhs.m_layer;
              return std::less<FVectorNode *>()(lhs.m_node, rhs.m_node);
            });

  auto ensure_resident = [&](FVectorNode *node) -> int {
    if (thd->killed) return HA_ERR_INTERNAL_ERROR;
    if (likely(node && node->vec)) return 0;
    if (unlikely(!node || !node->stored ||
                 build.m_pool.m_ctx->find_node(node->gref()) != node))
      return HA_ERR_INTERNAL_ERROR;
    if (int err = node->load(build.m_pool.m_graph)) return err;
    if (unlikely(!node->vec)) return HA_ERR_INTERNAL_ERROR;
    loaded++;
    return 0;
  };

  for (size_t i = 0; i < ntargets; i++) {
    if (i && targets[i].m_node == targets[i - 1].m_node &&
        targets[i].m_layer == targets[i - 1].m_layer)
      continue;
    FVectorNode *destination = targets[i].m_node;
    if (int err = ensure_resident(destination)) return err;
    if (unlikely(!destination->neighbors ||
                 targets[i].m_layer > destination->max_layer))
      return HA_ERR_INTERNAL_ERROR;
    Neighborhood &neighbors = destination->neighbors[targets[i].m_layer];
    for (size_t j = 0; j < neighbors.num; j++)
      if (int err = ensure_resident(neighbors.links[j])) return err;
  }
  if (loaded) {
    vidx_stats.ddl_parallel_load_rounds++;
    vidx_stats.ddl_parallel_loaded_nodes += loaded;
  }
  return 0;
}

/* Fold the workers' dirty intents into the dirty-neighbor set in
   shard-major order (deterministic; same-destination ops never span
   shards). */
static void fold_reverse_dirty(Ddl_parallel_build &build,
                               Dirty_neighbor_set *dirty_neighbors) {
  for (auto &shard : build.m_shard_ops)
    for (const Reverse_edge_op &op : shard)
      if (op.m_changed) dirty_neighbors->note(op.m_neigh);
}

/* ---- Stage 3b (serial): publish in row order — entry-point maintenance
   and the built list stay leader-only.  node_cache registration is
   deferred to persist_build_window pass 1: only there does the node get
   its durable gref, and the ctor's placeholder gref must never be a hash
   key. */
static void publish_build_batch(Direct_build_state &state,
                                std::vector<Ddl_build_task> &buf, uint nrows) {
  for (uint i = 0; i < nrows; i++) {
    FVectorNode *target = buf[i].m_node;
    target->stored = true;
    if (target->max_layer > state.m_local_top->max_layer)
      state.m_local_top = target;
    state.m_built.push_back(target);
  }
  vidx_stats.ddl_parallel_batches++;
}

static bool ddl_direct_build_parallel(THD *thd, TABLE *new_table,
                                      MHNSW_Share *ctx, MEM_ROOT &temp_root,
                                      Ddl_parallel_build &build,
                                      Direct_build_state &state) {
  DBUG_TRACE;
  build.bind_window(ctx);
  Scope_guard detach_window_guard{[&build]() { build.detach_window(); }};

  for (;;) {
    if (thd->killed) return true;

    uint nrows = 0;
    if (read_build_batch(thd, new_table, ctx, temp_root, state, build,
                         &nrows))
      return true;
    if (nrows == 0) return false;
    if (prepare_build_batch(thd, state, build, build.m_tasks, nrows,
                            build.m_batch_root))
      return true;

    /* ---- Stage 2: parallel read-only search (one dispatch; every
       executor resolves its own misses on its load channel). ---- */
    build.m_pool.m_top = state.m_local_top;
    build.m_pool.m_tasks = build.m_tasks.data();
    build.m_pool.m_ntasks = nrows;
    if (thd->killed) return true;
    build.m_pool.dispatch(Ddl_build_pool::Phase::SEARCH);
    build.m_pool.join_dispatch(&build.m_leader_work_area);
    if (build.m_pool.m_first_err.load(std::memory_order_relaxed) != 0)
      return true;

    /* ---- Stage 3a: sharded parallel reverse-edge install. ---- */
    generate_reverse_ops(build, build.m_tasks, nrows);
    if (preload_reverse_dependencies(thd, build)) return true;
    if (thd->killed) return true;
    build.m_pool.dispatch(Ddl_build_pool::Phase::REVERSE);
    /* Leader's own share of the phase. */
    run_build_phase(&build.m_pool, &build.m_leader_work_area);
    build.m_pool.join_dispatch(&build.m_leader_work_area);
    if (build.m_pool.m_first_err.load(std::memory_order_relaxed) != 0) {
      /* Strict: work_area OOM mid-install leaves a partial private graph
         (the serial path's per-row best-effort break is not reproducible
         across shards, and stage 2 already fails the build on the same
         condition). */
      return true;
    }
    if (thd->killed) return true;
    fold_reverse_dirty(build, &state.m_dirty_neighbors);

    /* ---- Stage 3b: serial publish in row order. ---- */
    publish_build_batch(state, build.m_tasks, nrows);
    DEBUG_SYNC(thd, "vidx_direct_build_after_batch");
    /* Test hook: crash mid-build, after rows of a batch are published into
       the uncommitted DDL transaction.  Recovery must roll the whole ALTER
       back -- no half-built graph may survive the restart. */
    DBUG_EXECUTE_IF("vidx_ddl_crash_mid_build", DBUG_SUICIDE(););
    build.m_batch_root.ClearForReuse();
    if (build.m_eof) return false;
    /* vidx_hnsw_cache_size is a soft graph-window hint for DDL, not a
       correctness gate or a hard RSS cap. A micro-batch is admitted and
       completed atomically; only after publish may the next batch be deferred
       to a fresh window. */
    if (ctx->cache_allocated_size() >= state.m_budget) {
      state.m_budget_exhausted = true;
      return false;
    }
  }
}

/* ===== Bulk write ===== */

/* DDL direct build pass-1 primitive: insert one built node as a fresh live
   graph row.  The graph table is brand new (created by this ALTER) and the
   builder is its only writer, so no IDX_TREF dup pre-check is needed (the
   historical save_new_row TOCTOU machinery existed for concurrent DML).
   NEIGHBORS is stored as the valid empty encoding (one zero count byte per
   layer): pass 2 rewrites every built node's NEIGHBORS from the in-memory
   lists once ALL grefs are durable, so serializing the (partly unassigned)
   neighbor grefs here would only double the blob volume in undo/redo.
   No reader exists between the passes — the ALTER holds MDL X and the row
   is uncommitted DDL-transaction state.  Binlog is suppressed like every
   other graph write (replicas build their own graph from the replicated
   ALTER). */
static int direct_insert_live_node(TABLE *graph, MHNSW_Share *ctx,
                                   FVectorNode *node) {
  THD *thd = graph->in_use;
  /* One zero count byte per layer — loadable empty-neighborhood shape. */
  uchar empty_layers[256];
  const size_t total_size = (size_t)node->max_layer + 1;
  memset(empty_layers, 0, total_size);

  my_bitmap_map *old_map = dbug_tmp_use_all_columns(graph, graph->write_set);
  Scope_guard bitmap_guard{[graph, old_map]() {
    dbug_tmp_restore_column_map(graph->write_set, old_map);
  }};

  /* Test hook: simulate the disk running full on the first live-row write.
     Raise the same diagnostics InnoDB raises for DB_OUT_OF_FILE_SPACE so
     the ALTER fails with ER_RECORD_FILE_FULL; the fail-fast discipline
     must then discard the intermediate table. */
  DBUG_EXECUTE_IF("vidx_ddl_inject_disk_full", {
    my_error(ER_RECORD_FILE_FULL, MYF(0), graph->s->table_name.str);
    return HA_ERR_RECORD_FILE_FULL;
  });

  restore_record(graph, s->default_values);
  graph->field[FIELD_LAYER]->store(node->max_layer, /*unsigned_val=*/false);
  graph->field[FIELD_TREF]->set_notnull();
  graph->field[FIELD_TREF]->store((const char *)node->tref(), node->tref_len(),
                                  &my_charset_bin);
  graph->field[FIELD_VEC]->set_notnull();
  graph->field[FIELD_VEC]->store((const char *)node->vec->data(),
                                 FVector::data_size(ctx->vec_len),
                                 &my_charset_bin);
  graph->field[FIELD_NEIGHBORS]->set_notnull();
  graph->field[FIELD_NEIGHBORS]->store((const char *)empty_layers, total_size,
                                       &my_charset_bin);

  /* Skip the IDX_TREF unique dup-probe: trefs come from the base table's
     rowid/PK scan, so uniqueness holds by construction — InnoDB honors
     OPTION_RELAXED_UNIQUE_CHECKS and saves one secondary-index lookup per
     inserted row. binlog_log_row() centrally suppresses HLINDEX row events. */
  const ulonglong saved_opt = thd->variables.option_bits;
  thd->variables.option_bits = saved_opt | OPTION_RELAXED_UNIQUE_CHECKS;
  int err = graph->file->ha_write_row(graph->record[0]);
  thd->variables.option_bits = saved_opt;
  if (err) return err;

  graph->file->position(graph->record[0]);
  memcpy(node->gref(), graph->file->ref, node->gref_len());
  return 0;
}

/* Bulk-write passes 2+3.  Returns true on any write error or KILL; direct
   build has no retry source, so the caller fails the ALTER.

   Pass 2: re-serialize every built node's NEIGHBORS blob now that ALL
   built nodes carry their durable on-disk gref.  Pass 1 persists in build
   order, so an edge to a later-written node lands on disk as an
   uninitialized gref; invisible while the in-memory graph stays warm, but
   a cache reload would follow dangling edges and silently drop nodes.
   save_neighbors_only rewrites only FIELD_NEIGHBORS (TREF/VEC/LAYER keep
   the disk before-image); rows are reentrant on this transaction.

   Pass 3: re-persist dirty PRIOR-window neighbors.  Window 2+ adds reverse
   edges to nodes persisted by earlier windows; those live only in memory
   until written back, and losing them on a cache reload would leave
   cross-window subgraphs unreachable.  This window's own nodes were fully
   rewritten by pass 2 and are filtered out; the remainder is persisted in
   gref order. */
static int direct_save_neighbors(TABLE *graph, FVectorNode *node) {
  assert(node->stored);
  assert(node->neighbors);

  /* Unlike the online-DML helper, HA_ERR_KEY_NOT_FOUND is fatal here: this
     private, brand-new graph has no concurrent deleter, so swallowing it
     would let ALTER succeed with a structurally incomplete graph. */
  if (int err = graph->file->ha_rnd_pos(graph->record[1], node->gref(),
                                        komodo::GM_NONE))
    return err;
  restore_record(graph, record[1]);

  size_t total_size = 0;
  for (size_t layer = 0; layer <= node->max_layer; layer++) {
    if (unlikely(node->neighbors[layer].num > 255)) return HA_ERR_CRASHED;
    total_size += 1 + node->gref_len() * node->neighbors[layer].num;
  }

  auto *blob =
      static_cast<uchar *>(my_safe_alloca(total_size, MAX_ALLOCA_SZ));
  Scope_guard blob_guard{[blob, total_size]() {
    my_safe_afree(blob, total_size, MAX_ALLOCA_SZ);
  }};

  uchar *ptr = blob;
  for (size_t layer = 0; layer <= node->max_layer; layer++) {
    *ptr++ = static_cast<uchar>(node->neighbors[layer].num);
    for (size_t j = 0; j < node->neighbors[layer].num;
         j++, ptr += node->gref_len())
      memcpy(ptr, node->neighbors[layer].links[j]->gref(), node->gref_len());
  }

  graph->field[FIELD_NEIGHBORS]->store(
      reinterpret_cast<const char *>(blob), total_size, &my_charset_bin);
  const int err =
      graph->file->ha_update_row(graph->record[1], graph->record[0]);
  return err == HA_ERR_RECORD_IS_THE_SAME ? 0 : err;
}

/* Pass 1 of the window bulk write: INSERT every built node as a live row.
   position() assigns the durable gref; the node is then registered in the
   node cache under that gref -- later windows resolve it via
   get_node/find_node (reverse preload, read-through loads) and the payload
   reclaim relies on one canonical pointer per gref.  Strict discipline:
   ANY failed insert (not just lock-fatal) aborts the build -- the caller
   fails the ALTER and the intermediate table is dropped. */
static bool persist_live_rows(TABLE *graph, MHNSW_Share *ctx,
                              Ddl_node_vector &built) {
  THD *bulk_thd = graph->in_use;
  for (FVectorNode *node : built) {
    if (bulk_thd->killed /* ALTER fails, DDL trx rolls back */ ||
        direct_insert_live_node(graph, ctx, node) ||
        unlikely(!ctx->cache_node_checked(node)))
      return true;
  }
  return false;
}

/* Pass 2 of the window bulk write: rewrite every built node's NEIGHBORS
   from the in-memory lists now that ALL built nodes carry their durable
   gref (see the pass rationale above direct_save_neighbors). */
static bool persist_built_neighbors(TABLE *graph, Ddl_node_vector &built) {
  THD *bulk_thd = graph->in_use;
  for (FVectorNode *node : built) {
    int serr = 0;
    /* Skip the physical write and return an ordinary handler error. */
    DBUG_EXECUTE_IF("vidx_ddl_inject_neighbor_fixup_error",
                    { serr = HA_ERR_INTERNAL_ERROR; });
    if (bulk_thd->killed || serr != 0 || direct_save_neighbors(graph, node))
      return true;
  }
  return false;
}

/* Pass 3 of the window bulk write: re-persist dirty PRIOR-window neighbors
   in gref order; this window's own nodes were fully rewritten by pass 2
   and are filtered out (see the pass rationale above
   direct_save_neighbors). */
static bool persist_prior_dirty_neighbors(
    TABLE *graph, Ddl_node_vector &built,
    Dirty_neighbor_set *dirty_neighbors) {
  if (dirty_neighbors == nullptr || dirty_neighbors->m_nodes.empty())
    return false;

  THD *bulk_thd = graph->in_use;
  const uint gl = graph->file->ref_length;

  malloc_unordered_set<const FVectorNode *> in_window{
      key_memory_vidx_mem};
  in_window.reserve(built.size());
  for (FVectorNode *node : built) in_window.insert(node);

  Ddl_node_vector neighbor_write_order{
      Malloc_allocator<FVectorNode *>(key_memory_vidx_mem)};
  neighbor_write_order.reserve(dirty_neighbors->m_nodes.size());
  for (FVectorNode *n : dirty_neighbors->m_nodes)
    if (in_window.count(n) == 0) neighbor_write_order.push_back(n);
  std::sort(neighbor_write_order.begin(), neighbor_write_order.end(),
            [gl](const FVectorNode *a, const FVectorNode *b) {
              return memcmp(a->gref(), b->gref(), gl) < 0;
            });

  for (FVectorNode *n : neighbor_write_order) {
    if (bulk_thd->killed || direct_save_neighbors(graph, n)) return true;
  }
  return false;
}

/* Bulk write of one build window: three ordered passes (see the pass
   docs above), short-circuited on the first failure; every outcome runs
   the common ha_rnd_end epilogue.  Returns true when the window is
   poisoned; direct build has no retry source, so the caller fails the
   ALTER. */
static bool persist_build_window(TABLE *graph, MHNSW_Share *ctx,
                                 Ddl_node_vector &built,
                                 Dirty_neighbor_set *dirty_neighbors) {
  if (built.empty()) return false;
  if (graph->file->ha_rnd_init(true) != 0) {
    /* Cannot position for the bulk write: NOTHING was persisted, so the
       direct path must fail the ALTER.  Returning "clean" here would let
       direct_build_one_index treat the window as fully built and the
       ALTER succeed with an unpersisted (silently empty) graph. */
    return true;
  }

  const bool write_poisoned =
      persist_live_rows(graph, ctx, built) ||
      persist_built_neighbors(graph, built) ||
      persist_prior_dirty_neighbors(graph, built, dirty_neighbors);

  graph->file->ha_rnd_end();
  /* Braces are required, not cosmetic: DEBUG_SYNC expands to nothing in a
     non-debug build, leaving an empty if body that -Werror=empty-body rejects
     under MYSQL_MAINTAINER_MODE.  Pre-existing baseline defect, surfaced by
     the first Release build of this tree. */
  if (!write_poisoned) {
    DEBUG_SYNC(graph->in_use, "vidx_ddl_direct_build_after_fixup");
  }
  return write_poisoned;
}

/* ===== Synchronous DDL direct build (pre-rename) ===== */

/* Build every vector index of the ALTER COPY intermediate table straight
   from its rows. One private build ctx per slot
   (real per-index M/ef_construction — never attached to the #sql share).

   Streaming single pass: each scanned row is built into the graph
   immediately (FVectorNode's ctor copies tref+vec into the build ctx; no
   staging buffer).  The whole build stays in one uncommitted DDL transaction;
   each cache window is bulk-persisted by writing live rows (pass 1), fixing
   NEIGHBORS grefs (pass 2), and re-persisting prior-window dirty neighbors
   (pass 3). Strictness beats repair: a row that cannot be built fails the
   ALTER and the intermediate table is discarded.

   Memory-budget exhaustion: vidx_hnsw_cache_size is sampled as a soft window
   hint. Once a micro-batch starts it always finishes, even if node admission,
   leader-mediated historical loads or reverse dependencies cross the hint.
   After publish the window is bulk-persisted (still inside the uncommitted DDL
   txn), the private ctx is rebuilt and the scan continues in parallel.
   Cross-window reverse edges ride the per-window Dirty_neighbor_set through
   pass 3. Every copied row is graph-live when the ALTER returns. */
/* Serial build window: read base rows and link them in memory one by one
   until EOF, the memory budget, or a fatal error. Used only when the session
   explicitly sets vidx_ddl_threads=1. */
static bool run_serial_build_window(THD *thd, TABLE *new_table,
                                    MHNSW_Share *ctx, MEM_ROOT &temp_root,
                                    Direct_build_state &state) {
  for (;;) {
    /* Budget check BEFORE the row read: a break leaves the row in the
       cursor for the next window (an after-read check would silently
       swallow the row it broke on).  !built.empty() is the per-window
       progress guarantee: window 2+ starts with byte_len already set
       (entry-point load) and a ctx root whose first block may alone reach
       the budget, so an unconditional check would admit zero rows and
       stall the scan forever. The parallel path instead checks only after a
       complete micro-batch has been published. */
    if (!state.m_built.empty() && ctx->byte_len != 0 &&
        ctx->cache_allocated_size() >= state.m_budget) {
      state.m_budget_exhausted = true;
      return false;
    }

    if (thd->killed) return true;
    String vec_buf, *vec_res = nullptr;
    switch (read_next_vector_row(thd, new_table, state.m_vec_field, &vec_buf,
                                 &vec_res)) {
      case Build_row_verdict::ROW_EOF:
        return false;
      case Build_row_verdict::ROW_FATAL:
        return true;
      case Build_row_verdict::ROW_SKIP:
        continue;
      case Build_row_verdict::ROW_OK:
        break;
    }

    FVectorNode *node = build_one_node(
        thd, state.m_graph, ctx,
        reinterpret_cast<const char *>(new_table->file->ref), vec_res->ptr(),
        vec_res->length(), temp_root, &state.m_local_top,
        &state.m_dirty_neighbors);
    if (!node) return true; /* strict: do not publish an incomplete graph */
    state.m_built.push_back(node);

    /* Per-row work_area (search candidates/queues) only — nodes live in
       the ctx root, so the work_area root can be recycled every row. */
    temp_root.ClearForReuse();
  }
}

/* Emit the one-time sizing hint when the graph crosses the soft DDL window
   hint (window 2+ pays on-demand handler reloads).  The client warning is
   fixed text — no table name (the statement context identifies the ALTER,
   and the #sql intermediate's random name would only confuse and
   destabilise recorded test results) and no numbers.  The actionable
   projection goes to the error log only: it is run-dependent, and per-row
   cost so far linearly projects the full-graph need. */
static void warn_ddl_window_sizing_once(THD *thd, Direct_build_state &state) {
  if (state.m_warned_memory_budget) return;
  state.m_warned_memory_budget = true;

  push_warning_printf(
      thd, Sql_condition::SL_WARNING, ER_VECTOR_INDEX_USAGE,
      "Vector index build reached the soft graph-window threshold "
      "(vidx_hnsw_cache_size); continuing in bounded windows with "
      "on-demand node reloads. In-flight work may temporarily exceed "
      "the threshold");

  if (state.m_rows_done > 0 && state.m_rows_total >= state.m_rows_done) {
    const ulonglong needed_mb =
        ((ulonglong)state.m_budget * state.m_rows_total / state.m_rows_done +
         (1ULL << 20) - 1) >>
        20;
    xp::warn(ER_XP_0) << "vidx DDL build crossed vidx_hnsw_cache_size ("
                      << state.m_budget << " bytes) after " << state.m_rows_done
                      << " of " << state.m_rows_total
                      << " rows; a single-pass build is projected to need "
                         "vidx_hnsw_cache_size >= "
                      << needed_mb << " MB";
  }
}

/* Window handover after a bulk write reached the memory budget.  This
   window's nodes are durable rows of the still-uncommitted DDL transaction,
   so every in-memory copy is refetchable.  Two policies, selected by the
   handover ordinal (see Direct_build_state::m_handovers):

   - First handover: rebuild a fresh private ctx and reload the entry point
     from the on-disk graph.  Pre-existing behavior; the capacity-sufficient
     mainline hands over at most once, so its path stays byte-identical.
   - Second handover onward (budget-deficit regime): free every vector
     payload slot while the node and adjacency structures stay resident —
     the next window refills payloads on demand (one FIELD_VEC read per
     miss, mostly through worker read-through) instead of re-faulting the
     whole reload working set through full node loads, which collapsed
     window admission to a few hundred rows once the graph outgrew the
     budget.
     Fallback: when the resident structures alone crowd out the payload
     headroom, rebuild from scratch so a window can always admit new rows.

   m_built/m_dirty_neighbors were just persisted by pass 3; the lists are
   cleared, and under the keep policy the node pointers they held stay
   valid in the surviving arena. */
static bool direct_build_window_handover(
    std::unique_ptr<MHNSW_Share> &ctx_holder, MHNSW_Share **ctx, THD *thd,
    uint slot, TABLE *new_table, KEY *keyinfo, Direct_build_state &state) {
  state.m_handovers++;
  warn_ddl_window_sizing_once(thd, state);

  state.m_built.clear();
  state.m_dirty_neighbors.m_nodes.clear();
  state.m_budget_exhausted = false;

  bool vectors_evicted = state.m_handovers >= 2 &&
                         (*ctx)->evictable_vectors() &&
                         (*ctx)->evict_ddl_vectors(state.m_budget);
  DBUG_EXECUTE_IF("vidx_ddl_force_full_handover", vectors_evicted = false;);
  if (vectors_evicted) {
    /* Structures kept: the running top node survives with its adjacency and
       only lost its payload — refill it so the window starts from a
       searchable entry.  ctx->start is not maintained by the DDL build;
       state.m_local_top is its equivalent here and stays valid across the
       handover (the node object lives in the surviving arena). */
    FVectorNode *entry = state.m_local_top;
    if (!entry || state.m_graph->file->ha_rnd_init(true)) return true;
    state.m_graph_rnd_open = true;
    return entry->load(state.m_graph) != 0;
  }

  state.m_local_top = nullptr;
  /* Rebuild in the evictable-vector layout: this ctx belongs to the
     budget-deficit regime, where the next handover keeps the node and
     adjacency structures and reclaims payload slots.  The first window's
     ctx (and with it the capacity-sufficient mainline, which never reaches
     this function) stays on the inline baseline layout. */
  ctx_holder = std::make_unique<MHNSW_Share>(
      slot, new_table->file->ref_length, state.m_graph->file->ref_length,
      static_cast<uint>(keyinfo->vector_m),
      static_cast<uint>(keyinfo->vector_ef_construction),
      (distance_kind)keyinfo->vector_distance, /*evictable_vectors=*/true);
  *ctx = ctx_holder.get();
  if (load_entry_point_node(*ctx, state.m_graph)) {
    /* Window 2+ implies at least one persisted live row; EOF or any
       handler error here is a broken invariant — fail the ALTER. */
    return true;
  }
  state.m_local_top = (*ctx)->start;
  if (!state.m_local_top || state.m_graph->file->ha_rnd_init(true)) return true;
  state.m_graph_rnd_open = true;
  return false;
}

/* Opening another worker channel is optional only when the leader's
   transaction remains healthy and the failure is a local resource/open-file
   shortfall.  Every other failure (including an invariant failure that did
   not raise a diagnostic) must abort the ALTER. */
static bool worker_graph_open_can_degrade(THD *thd,
                                          const Diagnostics_area &worker_da) {
  if (thd->killed || thd->transaction_rollback_request ||
      thd->is_fatal_error() || !worker_da.is_error())
    return false;

  switch (worker_da.mysql_errno()) {
    case ER_OUT_OF_RESOURCES:
    case ER_CANT_OPEN_FILE:
      return true;
    default:
      return false;
  }
}

static void propagate_worker_graph_open_error(
    THD *thd, const Diagnostics_area &worker_da) {
  if (!worker_da.is_error()) return;
  Diagnostics_area *stmt_da = thd->get_stmt_da();
  if (!stmt_da->is_error())
    stmt_da->set_error_status(worker_da.mysql_errno(), worker_da.message_text(),
                              worker_da.returned_sqlstate());
  stmt_da->copy_sql_conditions_from_da(thd, &worker_da);
}

/* Open up to @p n_threads worker-private graph TABLE instances for
   read-through loading (deficit-regime window 2+), appended to
   *worker_graphs. A recoverable shortfall lowers the effective parallelism
   (0 = leader-inline on the DDL handler) without leaving an error in the
   statement diagnostics area. Fatal failures are propagated to the caller.
   n_threads <= 1 is the serial path and opens nothing. */
static bool open_worker_graph_instances(
    TABLE *new_table, uint key, uint n_threads,
    std::vector<TABLE *> *worker_graphs) {
  if (n_threads <= 1) return false;
  THD *thd = new_table->in_use;
  worker_graphs->reserve(n_threads);
  for (uint i = 0; i < n_threads; i++) {
    Diagnostics_area worker_da(false);
    const bool saved_is_slave_error = thd->is_slave_error;
    thd->push_diagnostics_area(&worker_da, false);

    TABLE *t = nullptr;
    int open_error = 0;
    DBUG_EXECUTE_IF("vidx_ddl_worker_open_shortfall", {
      if (i > 0) {
        my_error(ER_OUT_OF_RESOURCES, MYF(0));
        open_error = 1;
      }
    });
    if (open_error == 0)
      open_error = vidx::hlindex_open_worker_instance(new_table, key, &t);

    thd->pop_diagnostics_area();
    if (open_error != 0) {
      if (worker_graph_open_can_degrade(thd, worker_da)) {
        thd->is_slave_error = saved_is_slave_error;
        break;
      }
      propagate_worker_graph_open_error(thd, worker_da);
      return true;
    }
    worker_graphs->push_back(t);
  }
  return false;
}

/* Build ONE vector index slot of the ALTER COPY intermediate: window loop
   of (parallel-or-serial in-memory build -> bulk persist -> handover),
   ending on EOF or a fatal error. */
static bool direct_build_one_index(THD *thd, TABLE *new_table, uint key,
                                   MEM_ROOT &temp_root, ha_rows copied) {
  KEY *keyinfo = new_table->key_info + key;
  const uint slot = key - new_table->s->keys;
  /* One worker pool per index slot, reused across every memory window.
     W==1 keeps the serial loop byte-for-byte. The setting is independent of
     InnoDB's own DDL thread pool. */
  const uint n_threads = get_ddl_threads(thd);

  if (new_table->hlindex_open(key) || new_table->hlindex_lock(key)) return true;
  TABLE *graph = new_table->hlindex_active;

  /* Private build ctx: real per-index parameters from key metadata; never
     registered on the transient #sql share. Destroyed with this scope --
     the final table's shared ctx is created lazily on first use.  The
     window handover rebuilds it with the same parameters and a fresh
     arena. */
  auto ctx_holder = std::make_unique<MHNSW_Share>(
      slot, new_table->file->ref_length, graph->file->ref_length,
      static_cast<uint>(keyinfo->vector_m),
      static_cast<uint>(keyinfo->vector_ef_construction),
      (distance_kind)keyinfo->vector_distance);
  MHNSW_Share *ctx = ctx_holder.get();

  Direct_build_state state(graph, keyinfo->key_part->field,
                           static_cast<size_t>(max_cache_size), copied);

  if (new_table->file->ha_rnd_init(true)) return true;
  Scope_guard base_scan_guard{[new_table]() { new_table->file->ha_rnd_end(); }};
  /* Graph rnd scan stays open for leader-mediated historical node loads;
     closed before the bulk write, which manages its own scan. */
  if (graph->file->ha_rnd_init(true)) return true;
  state.m_graph_rnd_open = true;
  Scope_guard graph_scan_guard{[&state]() {
    if (state.m_graph_rnd_open) state.m_graph->file->ha_rnd_end();
  }};

  /* Worker-private graph instances (see open_worker_graph_instances).
     Declared before the pool and closed by the guard AFTER the pool
     destructor joined the workers (reverse destruction order), when every
     instance is detached again. */
  std::vector<TABLE *> worker_graphs;
  Scope_guard worker_graphs_guard{[&worker_graphs]() {
    for (TABLE *t : worker_graphs) {
      closefrm(t, false);
      my_free(t);
    }
  }};
  if (open_worker_graph_instances(new_table, key, n_threads, &worker_graphs))
    return true;

  std::unique_ptr<Ddl_parallel_build> parallel_build;
  if (n_threads > 1)
    parallel_build = std::make_unique<Ddl_parallel_build>(
        graph, ctx, n_threads, worker_graphs.data(),
        static_cast<uint>(worker_graphs.size()));

  for (;;) {
    if (parallel_build) {
      if (ddl_direct_build_parallel(thd, new_table, ctx, temp_root,
                                    *parallel_build, state))
        return true;
    } else {
      if (run_serial_build_window(thd, new_table, ctx, temp_root, state))
        return true;
    }

    graph->file->ha_rnd_end();
    state.m_graph_rnd_open = false;
    const bool window_has_nodes = !state.m_built.empty();
    if (persist_build_window(graph, ctx, state.m_built,
                             &state.m_dirty_neighbors))
      return true;
    if (window_has_nodes) vidx_stats.ddl_build_windows++;
    state.m_rows_done += state.m_built.size();
    if (thd->killed) return true;
    temp_root.ClearForReuse();

    if (!state.m_budget_exhausted) return false; /* EOF: slot fully built */

    if (direct_build_window_handover(ctx_holder, &ctx, thd, slot, new_table,
                                     keyinfo, state))
      return true;
  }
}

static bool ddl_direct_build_impl(THD *thd, TABLE *new_table, ha_rows copied) {
  DBUG_TRACE;
  if (thd->killed) return true;
  vidx_stats.ddl_direct_builds_active++;
  Scope_guard active_build_guard{
      []() { vidx_stats.ddl_direct_builds_active--; }};
  MEM_ROOT temp_root(key_memory_vidx_mem, MEM_ROOT_BLOCK_SIZE);

  /* Stage progress total: finish_ddl_direct_build() switched to
     stage_vector_index_build before this call; every copied row is built once
     per vector index slot, so the exact work total is copied × slot count.
     WORK_COMPLETED accrues in the per-path build loops. */
  mysql_stage_set_work_estimated(
      thd->m_stage_progress_psi,
      (ulonglong)copied * (new_table->s->total_keys - new_table->s->keys));

  /* hlindex_lock takes external_lock(F_WRLCK) on graph handlers. The baseline
     reset_hlindexes() only detaches them because ordinary DML has already
     passed through the handler unlock cascade; direct DDL has not. Release
     explicitly on every success/error/KILL exit before detaching. */
  Scope_guard hlindex_unlock_guard{[new_table]() {
    if (new_table->hlindex_tables != nullptr) {
      auto range = new_table->hlindex_tables_range();
      for (TABLE **it = range.first; it != range.second; ++it) {
        if (*it && (*it)->in_use &&
            (*it)->file->get_lock_type() != F_UNLCK)
          (void)(*it)->file->ha_external_lock(new_table->in_use, F_UNLCK);
      }
    }
    new_table->reset_hlindexes();
  }};

  for (uint key = new_table->s->keys; key < new_table->s->total_keys; key++) {
    if (direct_build_one_index(thd, new_table, key, temp_root, copied))
      return true;
  }
  return thd->killed;
}

/* A COPY ALTER whose result has vector indexes must build them synchronously.
   Mark the intermediate TABLE before the copy loop suppresses row-by-row
   graph maintenance. */
void prepare_ddl_direct_build(TABLE *new_table, bool is_noop) {
  new_table->vidx_ddl_direct_build_target =
      !is_noop && new_table->s->hlindexes() > 0;
}

/* Called after the row copy and before rename.  The graph writes remain in
   the ALTER transaction; any failure lets the caller discard the #sql table. */
bool finish_ddl_direct_build(THD *thd, TABLE *new_table, ha_rows copied) {
  if (!new_table->vidx_ddl_direct_build_target) return false;

  THD_STAGE_INFO(thd, stage_vector_index_build);
  DEBUG_SYNC(thd, "vidx_ddl_direct_build_start");

  /* copy_data_between_tables() releases the COPY target before returning,
     while the direct builder scans that same handler and refreshes its
     statistics. Reacquire a write external lock for the whole post-copy
     phase: handler scan/analyze entry points require a locked non-temporary
     TABLE, and InnoDB may retain LOCK_X state from the copy so F_RDLCK is not
     a valid transition here. */
  int lock_error = new_table->file->ha_external_lock(thd, F_WRLCK);
  bool failed = lock_error != 0;
  if (lock_error != 0 && !thd->is_error())
    new_table->file->print_error(lock_error, MYF(0));

  if (!failed) failed = ddl_direct_build_impl(thd, new_table, copied);

  if (!failed) {
    /* Refresh the persistent statistics of the #sql target before rename.

       Every earlier index-build shape keeps statistics valid without help:
       - INPLACE ADD INDEX recalculates explicitly in its commit phase
         (alter_stats_rebuild() / alter_stats_norebuild()).
       - A plain COPY ALTER inserts base rows until the very end; every
         insert re-queues the #sql table for the background statistics
         recalc (row_update_statistics_if_needed()), so a queue entry
         survives the commit and is processed right after the rename.
       - The pre-direct-build vidx path built the graph row by row inside
         the copy loop, so its base-row inserts also spanned the whole
         ALTER -- same self-healing shape as a plain COPY.

       The direct build breaks that implicit chain: the copy phase is
       short, and the long graph build phase only reads the base table
       (its writes go to the graph aux table).  The background thread
       drops the queue entry while this ALTER still holds the MDL, and
       nothing re-queues the table afterwards.  Without the refresh the
       empty-table row written at CREATE (n_rows=0) is renamed to the
       final table name; ORDER BY vec_distance queries then estimate
       records=0 and reject the index just built
       (test_if_cheaper_vector_ordering: limit > records/4), and no DML
       ever triggers a recalc.  Independent of vidx_ddl_threads: W=1 has
       the same two-phase shape.

       The table was fully scanned moments ago, so the page sampling is
       cheap.  Statistics stay advisory: a failure only warns. */
    HA_CHECK_OPT check_opt{};
    if (new_table->file->ha_analyze(thd, &check_opt) != HA_ADMIN_OK) {
      xp::warn(ER_XP_0) << "vidx DDL build could not refresh persistent "
                           "statistics of "
                        << new_table->s->db.str << "."
                        << new_table->s->table_name.str
                        << "; run ANALYZE TABLE so ORDER BY vector queries "
                           "pick the new index";
    }
  }

  /* Balance the post-copy lock on every build/analyze outcome. */
  if (lock_error == 0) {
    lock_error = new_table->file->ha_external_lock(thd, F_UNLCK);
    if (lock_error != 0) {
      failed = true;
      if (!thd->is_error()) new_table->file->print_error(lock_error, MYF(0));
    }
  }

  /* Sole update point of Vidx_ddl_build_error_count, placed where `failed`
     is final so lock, build and unlock failures all count: the build is
     strictly fail-fast, so one internally failed direct-build ALTER counts
     exactly once; a KILL is the user's decision, not a build error. */
  if (failed && !thd->killed) vidx_stats.ddl_build_error_count++;

  /* The ALTER statement needs a diagnostics-area error to fail with. A
     KILL already raised ER_QUERY_INTERRUPTED; handler-level failures inside
     the build are converted here. */
  if (failed && !thd->is_error()) {
    if (thd->killed)
      thd->send_kill_message();
    else
      my_error(ER_VECTOR_INDEX_FAILED, MYF(0), "Build", "vector index",
               new_table->s->db.str, new_table->s->table_name.str,
               "direct build", "graph construction failed (see server log)");
  }
  return failed;
}

}  // namespace hnsw
}  // namespace vidx
