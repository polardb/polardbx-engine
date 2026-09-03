#ifndef VIDX_HNSW_CTX_INCLUDED
#define VIDX_HNSW_CTX_INCLUDED

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
  *** INTERNAL HEADER — DO NOT INCLUDE FROM OUTSIDE sql/vidx/ ***

  Shared by sql/vidx/vidx_hnsw.cc, sql/vidx/vidx_hnsw_ops.cc and the
  synchronous DDL builder. Exposes the internal class hierarchy (FVector /
  FVectorNode / MHNSW_Share / MHNSW_Trx / Visited / VisitedSet /
  Search_context / ScopedLockWaitTimeout) plus shared constants/enums/forward
  declarations.

  External callers must use vidx/vidx_hnsw.h (public API).
*/

#include <atomic>
#include <cfloat>
#include <cmath>
#include <cstring>
#include <vector>

#include "m_ctype.h"                  // my_hash_sort_bin
#include "mutex_lock.h"               // MUTEX_LOCK
#include "my_byteorder.h"             // get_float
#include "my_inttypes.h"              // uintxx_t
#include "mysql/plugin.h"             // thd_test_options
#include "scope_guard.h"              // Scope_guard
#include "sql/handler.h"              // handler, handlerton
#include "sql/psi_memory_key.h"       // key_memory_vidx_mem
#include "sql/sql_class.h"            // THD
#include "sql/table.h"                // TABLE, TABLE_SHARE
#include "vidx/bloom_filters.h"       // PatternedSimdBloomFilter
#include "vidx/my_atomic_wrapper.h"   // Atomic_relaxed
#include "vidx/sql_hset.h"            // Hash_set
#include "vidx/sql_queue.h"           // Queue
#include "vidx/vidx_common.h"         // EF_POWER_DEF / DIAMETER_DEF
#include "sql/item_strfunc.h"          // Item_str_func (needed by vidx_func.h)
#include "vidx/vidx_func.h"           // distance_kind
#include "vidx/vidx_hnsw.h"           // public vidx::hnsw exports
#include "vidx/vidx_stats.h"          // vidx_stats

namespace vidx {
namespace hnsw {

/* -------------------- Macros / Constants -------------------- */
/* Distance-threshold sentinel used by initial graph search.  It must be below
   every finite distance value: inner-product distance is -dot, so exact or
   high-norm matches can be <= -1.0 and are valid nearest neighbors. */
static constexpr float NEAREST = -FLT_MAX;

// Algorithm parameters
static constexpr float alpha = 1.1f;
// ef_construction is now per-index; see MHNSW_Share::ef_construction

/* simplify and unify my_safe_alloca usage */
#define MAX_ALLOCA_SZ 4096

/* Pulled from vidx_index.cc (sysvar binding lives there).
   Takes const THD*: reading a session variable does not modify the THD. */
uint get_ef_search(const THD *thd);
/* Session-local resources for synchronous COPY-DDL graph construction. */
uint get_ddl_threads(const THD *thd);

/* -------------------- Forward decls / Enums -------------------- */
class MHNSW_Share;
class FVectorNode;

enum Graph_table_fields { FIELD_LAYER, FIELD_TREF, FIELD_VEC, FIELD_NEIGHBORS };
enum Graph_table_indices { IDX_TREF, IDX_LAYER };

/* Outcome of prefetch_neighbor_locks_in_gref_order — see runtime.cc.  */
enum class PrefetchResult { Ok, OkSkipUpdate2nd };

/* -------------------- FVector -------------------- */
/*
  One vector, an array of coordinates in ctx->vec_len dimensions
*/
#pragma pack(push, 1)
struct FVector {
  static constexpr size_t data_header = sizeof(float);
  static constexpr size_t alloc_header = data_header + sizeof(float);

  float abs2, scale;
  int16_t dims[4];

  uchar *data() const { return const_cast<uchar *>(reinterpret_cast<const uchar *>(&scale)); }

  static size_t data_size(size_t n) { return data_header + n * 2; }

  static size_t data_to_value_size(size_t data_size) {
    return (data_size - data_header) * 2;
  }

  static const FVector *create(distance_kind metric, void *mem, const void *src,
                               size_t src_len) {
    DBUG_TRACE;
    float scale = 0, *v = const_cast<float *>(static_cast<const float *>(src));
    size_t vec_len = src_len / sizeof(float);
    for (size_t i = 0; i < vec_len; i++)
      scale = std::max(scale, std::abs(get_float(v + i)));

    FVector *vec = align_ptr(mem);
    vec->scale = scale ? scale / 32767 : 1;
    /* MDEV-37055 */
    if (std::round(scale / vec->scale) > 32767)
      vec->scale = std::nextafter(vec->scale, FLT_MAX);
    for (size_t i = 0; i < vec_len; i++)
      vec->dims[i] =
          static_cast<int16_t>(std::round(get_float(v + i) / vec->scale));
    vec->postprocess(vec_len);
    /* fix vector length when cosine (skip zero vectors: abs2 == 0) */
    if (metric == COSINE && vec->abs2 > 0.0f)
      vec->scale /= std::sqrt(2 * vec->abs2);
    vec->fixup_abs2(metric);
    return vec;
  }

  /*
    Apply the metric-specific abs2 convention on top of the raw
    postprocess() value (which is the EUCLIDEAN one).

    Must be called BOTH at creation time and after reloading a stored
    vector from the graph table: only scale+dims are persisted, so
    load_from_record() recomputes abs2 via postprocess() and would
    otherwise silently fall back to EUCLIDEAN semantics.
      - COSINE: create() burns the normalization into the stored scale,
        so the recomputed abs2 is already ~0.5 for non-zero vectors;
        forcing exactly 0.5f keeps reloaded nodes bit-identical to
        freshly created ones and restores the zero-vector convention
        (distance 1.0 to everything), which the raw recompute loses.
      - INNER_PRODUCT: abs2=0 => distance_to() degenerates to -(v·w),
        i.e. negative dot product; min-heap then yields max inner
        product.
  */
  void fixup_abs2(distance_kind metric) {
    if (metric == COSINE)
      abs2 = 0.5f;
    else if (metric == INNER_PRODUCT)
      abs2 = 0;
  }

  void postprocess(size_t vec_len) {
    fix_tail(vec_len);
    abs2 = scale * scale * dot_product(dims, dims, vec_len) / 2;
  }

#ifdef AVX2_IMPLEMENTATION
  /************* AVX2 *****************************************************/
  static constexpr size_t AVX2_bytes = 256 / 8;
  static constexpr size_t AVX2_dims = AVX2_bytes / sizeof(int16_t);

  AVX2_IMPLEMENTATION
  static float dot_product(const int16_t *v1, const int16_t *v2, size_t len) {
    typedef float v8f __attribute__((vector_size(AVX2_bytes)));
    union {
      v8f v;
      __m256 i;
    } tmp;
    __m256i *p1 = const_cast<__m256i *>(reinterpret_cast<const __m256i *>(v1));
    __m256i *p2 = const_cast<__m256i *>(reinterpret_cast<const __m256i *>(v2));
    v8f d = {0};
    for (size_t i = 0; i < (len + AVX2_dims - 1) / AVX2_dims; p1++, p2++, i++) {
      tmp.i = _mm256_cvtepi32_ps(_mm256_madd_epi16(*p1, *p2));
      d += tmp.v;
    }
    return d[0] + d[1] + d[2] + d[3] + d[4] + d[5] + d[6] + d[7];
  }

  AVX2_IMPLEMENTATION
  static size_t alloc_size(size_t n) {
    return alloc_header + MY_ALIGN(n * 2, AVX2_bytes) + AVX2_bytes - 1;
  }

  AVX2_IMPLEMENTATION
  static FVector *align_ptr(void *ptr) {
    return (FVector *)(MY_ALIGN(((intptr)ptr) + alloc_header, AVX2_bytes) -
                       alloc_header);
  }

  AVX2_IMPLEMENTATION
  void fix_tail(size_t vec_len) {
    bzero(dims + vec_len, (MY_ALIGN(vec_len, AVX2_dims) - vec_len) * 2);
  }
#endif

#ifdef AVX512_IMPLEMENTATION
  /************* AVX512 ****************************************************/
  static constexpr size_t AVX512_bytes = 512 / 8;
  static constexpr size_t AVX512_dims = AVX512_bytes / sizeof(int16_t);

  AVX512_IMPLEMENTATION
  static float dot_product(const int16_t *v1, const int16_t *v2, size_t len) {
    __m512i *p1 = const_cast<__m512i *>(reinterpret_cast<const __m512i *>(v1));
    __m512i *p2 = const_cast<__m512i *>(reinterpret_cast<const __m512i *>(v2));
    __m512 d = _mm512_setzero_ps();
    for (size_t i = 0; i < (len + AVX512_dims - 1) / AVX512_dims;
         p1++, p2++, i++)
      d = _mm512_add_ps(d, _mm512_cvtepi32_ps(_mm512_madd_epi16(*p1, *p2)));
    return _mm512_reduce_add_ps(d);
  }

  AVX512_IMPLEMENTATION
  static size_t alloc_size(size_t n) {
    return alloc_header + MY_ALIGN(n * 2, AVX512_bytes) + AVX512_bytes - 1;
  }

  AVX512_IMPLEMENTATION
  static FVector *align_ptr(void *ptr) {
    return (FVector *)(MY_ALIGN(((intptr)ptr) + alloc_header, AVX512_bytes) -
                       alloc_header);
  }

  AVX512_IMPLEMENTATION
  void fix_tail(size_t vec_len) {
    bzero(dims + vec_len, (MY_ALIGN(vec_len, AVX512_dims) - vec_len) * 2);
  }
#endif

  /*
    ARM NEON implementation. A microbenchmark shows 1.7x dot_product()
    performance improvement compared to regular -O2/-O3 builds and 2.4x compared
    to builds with auto-vectorization disabled.

    There seem to be no performance difference between vmull+vmull_high and
    vmull+vmlal2_high implementations.
  */

#ifdef NEON_IMPLEMENTATION
  static constexpr size_t NEON_bytes = 128 / 8;
  static constexpr size_t NEON_dims = NEON_bytes / sizeof(int16_t);

  static float dot_product(const int16_t *v1, const int16_t *v2, size_t len) {
    int64_t d = 0;
    for (size_t i = 0; i < (len + NEON_dims - 1) / NEON_dims; i++) {
      int16x8_t p1 = vld1q_s16(v1);
      int16x8_t p2 = vld1q_s16(v2);
      d += vaddlvq_s32(vmull_s16(vget_low_s16(p1), vget_low_s16(p2))) +
           vaddlvq_s32(vmull_high_s16(p1, p2));
      v1 += NEON_dims;
      v2 += NEON_dims;
    }
    return static_cast<float>(d);
  }

  static size_t alloc_size(size_t n) {
    return alloc_header + MY_ALIGN(n * 2, NEON_bytes) + NEON_bytes - 1;
  }

  static FVector *align_ptr(void *ptr) {
    return (FVector *)(MY_ALIGN(((intptr)ptr) + alloc_header, NEON_bytes) -
                       alloc_header);
  }

  void fix_tail(size_t vec_len) {
    bzero(dims + vec_len, (MY_ALIGN(vec_len, NEON_dims) - vec_len) * 2);
  }
#endif

  /************* no-SIMD default ******************************************/
#ifdef DEFAULT_IMPLEMENTATION
  DEFAULT_IMPLEMENTATION
  static float dot_product(const int16_t *v1, const int16_t *v2, size_t len) {
    int64_t d = 0;
    for (size_t i = 0; i < len; i++) d += int32_t(v1[i]) * int32_t(v2[i]);
    return static_cast<float>(d);
  }

  DEFAULT_IMPLEMENTATION
  static size_t alloc_size(size_t n) { return alloc_header + n * 2; }

  DEFAULT_IMPLEMENTATION
  static FVector *align_ptr(void *ptr) { return (FVector *)ptr; }

  DEFAULT_IMPLEMENTATION
  void fix_tail(size_t) {}
#endif

  float distance_to(const FVector *other, size_t vec_len) const {
    return abs2 + other->abs2 -
           scale * other->scale * dot_product(dims, other->dims, vec_len);
  }
};
#pragma pack(pop)

/* -------------------- Neighborhood -------------------- */

/* Batch width of the visited-set membership probe and, by extension, the
   allocation quantum of every adjacency array.  The value is pinned by
   PatternedSimdBloomFilter::Query(), which tests this many node addresses
   per call and reports one bit per link in its uint8_t result mask; the
   SIMD kernels behind it are written for exactly that width.
   Neighborhood::init() zero-fills every links array up to a multiple of
   this width, so search_layer can scan adjacency lists a full batch at a
   time -- nullptr tail entries are pre-recorded as seen (see VisitedSet).
   Widening it requires a wider Query() mask type and new kernels first. */
static constexpr size_t VISITED_PROBE_BATCH = 8;

/*
  An array of pointers to graph nodes

  It's mainly used to store all neighbors of a given node on a given layer.

  An array is fixed size, 2*M for the zero layer, M for other layers
  see MHNSW_Share::max_neighbors().

  Number of neighbors is zero-padded to multiples of VISITED_PROBE_BATCH
  (for the SIMD Bloom filter).

  Also used as a simply array of nodes in search_layer, the array size
  then is defined by ef or efConstruction.
*/
struct Neighborhood {
  FVectorNode **links;
  size_t num;
  FVectorNode **init(FVectorNode **ptr, size_t n) {
    num = 0;
    links = ptr;
    n = MY_ALIGN(n, VISITED_PROBE_BATCH);
    bzero(ptr, n * sizeof(*ptr));
    return ptr + n;
  }
};

/* -------------------- FVectorNode -------------------- */
/*
  One node in a graph = one row in the graph table

  stores a vector itself, ref (= position) in the graph (= hlindex)
  table, a ref in the main table, and an array of Neighborhood's, one
  per layer.

  It's lazily initialized, may know only gref, everything else is
  loaded on demand.

  On the other hand, on INSERT the new node knows everything except
  gref - which only becomes known after ha_write_row.

  Allocated on memroot in two chunks. One is the same size for all nodes
  and stores FVectorNode object, gref, tref, and vector. The second
  stores neighbors, all Neighborhood's together, its size depends
  on the number of layers this node is on.

  There can be millions of nodes in the cache and the cache size
  is constrained by max_cache_size, so every byte matters here
*/
#pragma pack(push, 1)
class FVectorNode {
 private:
  MHNSW_Share *ctx;

  const FVector *make_vec(const void *v);

 public:
  const FVector *vec = nullptr;
  Neighborhood *neighbors = nullptr;
  uint8_t max_layer = 0;
  /* vec_evicted: the vector payload was reclaimed by a DDL window handover
     while the adjacency stayed complete and authoritative (possibly newer
     than the on-disk FIELD_NEIGHBORS).  load() then refills the vector
     only.  Never set outside the synchronous-DDL private ctx. */
  bool stored : 1, deleted : 1, vec_evicted : 1;

  FVectorNode(MHNSW_Share *ctx_, const void *gref_);
  FVectorNode(MHNSW_Share *ctx_, const void *tref_, const void *vec_);
  int alloc_neighborhood(uint8_t layer);
  float distance_to(const FVector *other) const;
  /**
   * Inline fast path: if node vector is already loaded (cache hit from
   * preload), return immediately without function call overhead.
   * The slow path (actual InnoDB read) is in load_slow().
   */
  inline int load(TABLE *graph) {
    if (likely(vec)) return 0;
    return load_slow(graph);
  }
  int load_slow(TABLE *graph);
  int load_from_record(TABLE *graph);
  /* vidx graph-table writes are split into two semantically
     distinct primitives by caller intent:

     save_new_row()         — INSERT a fresh target row (ha_write_row
                              with IDX_TREF pre-check + dup-key fallback).
                              Called exclusively from mhnsw_insert for
                              the freshly-created target node.

     save_neighbors_only()  — UPDATE just FIELD_NEIGHBORS of an existing
                              row (the disk before-image is authoritative
                              for FIELD_TREF / VEC / LAYER, never
                              overwritten from the per-MHNSW_Trx cache).
                              Called exclusively from update_second_
                              degree_neighbors to propagate the new
                              target into each neighbor's link list.

     The previous monolithic save() (a single function with an
     if(stored) split) used to overwrite the whole row in both cases —
     under concurrent writers the stale cache.tref() could be written
     back over a NULLed disk FIELD_TREF, "reviving" the stale IDX_TREF
     entry and producing ER_DUP_KEY 1022 on the next target.INSERT
     of the same tref. */
  int save_new_row(TABLE *graph);
  int save_neighbors_only(TABLE *graph);
  size_t tref_len() const;
  size_t gref_len() const;
  uchar *gref() const;
  uchar *tref() const;
  void push_neighbor(size_t layer, FVectorNode *v);

  static const uchar *get_key(const void *elem, size_t *key_len, my_bool);
};
#pragma pack(pop)

/* -------------------- MHNSW_Share -------------------- */
/*
  Shared algorithm context. The graph.

  Normally stored in TABLE_SHARE and on TABLE_SHARE::mem_root. Synchronous
  COPY DDL instead owns a private instance for each bounded build window.
  Stores the complete graph in MHNSW_Share::root,
  The mapping gref->FVectorNode is in the node_cache.
  Both root and node_cache are protected by a cache_lock, but it's
  needed when loading nodes and is not used when the whole graph is in memory.
  Graph can be traversed concurrently by different threads, as traversal
  changes neither nodes nor the ctx.
  Nodes can be loaded concurrently by different threads, this is protected
  by a partitioned node_lock.
  reference counter allows flushing the graph without interrupting
  concurrent searches.
  MyISAM automatically gets exclusive write access because of the TL_WRITE,
  but InnoDB has to use a dedicated ctx->commit_lock for that
*/
/* Evictable-vector payload store, private to the synchronous-DDL direct
   build: vector payloads live in uniform slots of a dedicated arena so a
   window handover can free them wholesale while the node and adjacency
   structures stay resident.  Normal-path shares never allocate one
   (MHNSW_Share::vec_arena stays nullptr; the baseline layout embeds the
   vector inline after the node's tref).  All fields follow the owning
   share's cache_lock discipline; slot owners are mutated only at DDL
   leader safe points (no worker running). */
struct Evictable_vector_arena {
  /* Vector payloads only (FVector allocations), split from the share's
     root so evict_ddl_vectors() can drop every payload in bulk. */
  MEM_ROOT m_vec_root;
  /* Slot registry: m_all_slots grows append-only with the arena high-water
     mark; m_free_slots feeds alloc_vec() reuse; a slot with a null owner
     header is free.  Plain malloc'd vectors: ~16B/slot bookkeeping,
     outside the budget like the node_cache hash. */
  std::vector<uchar *> m_all_slots;
  std::vector<uchar *> m_free_slots;
  /* Net payload bytes in live slots.  The budget must count slots, not
     m_vec_root.allocated_size(): a MEM_ROOT's geometrically growing current
     block counts hundreds of unused MB into allocated_size(), and with
     two arenas that tail waste doubled -- measured to fire the 2.0GB
     window budget at ~780k of 1M rows that the single-arena baseline
     held with room to spare. */
  size_t m_used_bytes{0};

  Evictable_vector_arena() {
    init_sql_alloc(key_memory_vidx_mem, &m_vec_root, 1024 * 1024);
  }
  /* ~MEM_ROOT() clears m_vec_root; the vectors free themselves. */
};

class MHNSW_Share {
  mysql_mutex_t cache_lock;
  mysql_mutex_t node_lock[8];
  mysql_mutex_t start_lock;

  void cache_internal(FVectorNode *node) {
    assert(node->stored);
    node_cache.insert(node);
  }
  void *alloc_node_internal() {
    /* Inline layout embeds the vector right after tref (baseline hot
       path); the evictable-vector layout keeps payloads in vec_arena slots
       (see alloc_vec) so a merely referenced neighbor stub reserves no
       vector space and the DDL window handover can reclaim payloads
       wholesale. */
    return root.Alloc(sizeof(FVectorNode) + gref_len + tref_len +
                      (vec_arena ? 0 : FVector::alloc_size(vec_len)));
  }

  /* One payload slot: [FVectorNode *owner][FVector area].  Uniform size
     per index (vec_len is fixed once set_lengths ran), which makes freed
     slots directly reusable. */
  size_t vec_slot_size() const {
    return sizeof(FVectorNode *) + FVector::alloc_size(vec_len);
  }

  void init_locks_and_alloc() {
    mysql_rwlock_init(PSI_INSTRUMENT_ME, &commit_lock);
    mysql_mutex_init(PSI_INSTRUMENT_ME, &cache_lock, MY_MUTEX_INIT_FAST);
    mysql_mutex_init(PSI_INSTRUMENT_ME, &start_lock, MY_MUTEX_INIT_FAST);
    for (uint i = 0; i < array_elements(node_lock); i++)
      mysql_mutex_init(PSI_INSTRUMENT_ME, node_lock + i, MY_MUTEX_INIT_SLOW);
    init_sql_alloc(key_memory_vidx_mem, &root, 1024 * 1024);
    vidx_stats.hnsw_share_count++;
  }

 protected:
  std::atomic<uint> refcnt{0};
  MEM_ROOT root;
  /* Evictable-vector payload arena; allocated only by the synchronous-DDL
     constructor (see Evictable_vector_arena), nullptr on every normal-path
     share.  Owned: released by ~MHNSW_Share(). */
  Evictable_vector_arena *vec_arena{nullptr};
  Hash_set<FVectorNode> node_cache{key_memory_vidx_mem, FVectorNode::get_key};
  /* Last reported root.allocated_size(); protected by lock_share. */
  size_t reported_cache_bytes{0};

 public:
  ulonglong version = 0;  // protected by commit_lock
  mysql_rwlock_t commit_lock;
  size_t vec_len = 0;
  size_t byte_len = 0;
  /* Cold-start at the shared defaults; survive cache resets via the
     hlindex TABLE_SHARE write-back/restore cycle (see reset() /
     get_from_share()). */
  Atomic_relaxed<double> ef_power{EF_POWER_DEF};  // bloom filter size heuristic
  Atomic_relaxed<float> diameter{DIAMETER_DEF};   // generosity heuristic
  /*
   * MDEV-35897: approximate graph node count, snapshotted in get_from_share()
   * on each acquire().
   *
   * MariaDB stores this in TABLE::used_stat_records (thread-local field).
   * MySQL 8.0 has no equivalent, so we store it directly in MHNSW_Share.
   * Written under share_lock; read lock-free in search_layer (approximate
   * value is acceptable).
   *
   * Purpose: cap the Bloom Filter est_size in search_layer to the actual
   * graph size, preventing PatternedSimdBloomFilter from over-allocating
   * when ef_search is set much larger than the graph.
   */
  size_t graph_node_count = 0;
  FVectorNode *start = 0;
  /* Slot of this vector index: keynr = s->keys + hlindex_slot. */
  const uint hlindex_slot;
  const uint tref_len;
  const uint gref_len;
  const uint M;
  const uint ef_construction;
  distance_kind metric;
  /* Payload layout switch.  false (default: shared/query/Trx ctx and the
     first DDL build window): the vector is embedded right after the node's
     tref -- the baseline layout, byte-identical hot path.  true (only the
     ctx a DDL window handover rebuilds, i.e. the budget-deficit regime):
     payloads live in uniform vec_arena slots so evict_ddl_vectors can free
     them wholesale while the node and adjacency structures stay resident.
     Constant per ctx (vec_arena is set at construction only); nodes never
     mix layouts. */
  bool evictable_vectors() const { return vec_arena != nullptr; }

  MHNSW_Share(TABLE *t)
      : hlindex_slot(t->hlindex_active_slot),
        tref_len(t->file->ref_length),
        gref_len(t->hlindex_active->file->ref_length),
        M(static_cast<uint>(t->s->key_info[t->s->keys + hlindex_slot].vector_m)),
        ef_construction(
            static_cast<uint>(
                t->s->key_info[t->s->keys + hlindex_slot].vector_ef_construction)),
        metric((distance_kind)t->s->key_info[t->s->keys + hlindex_slot]
                   .vector_distance) {
    assert(t->s->key_info[t->s->keys + hlindex_slot].flags & HA_VECTOR);
    init_locks_and_alloc();
    /*
     * No MEM_ROOT capacity cap here.  release() already checks
     * allocated_size() > max_cache_size and triggers reset + lazy
     * rebuild when exceeded.  A hard cap via set_max_capacity() would
     * cause Alloc() to return nullptr *before* allocated_size crosses
     * the threshold, so release() would never detect the overshoot
     * and never reset -- leaving every subsequent alloc to fail.
     */
  }

  /* Private construction context used by synchronous COPY DDL. It is never
     attached to TABLE_SHARE and is destroyed when the build window ends. */
  MHNSW_Share(uint hlindex_slot_arg, uint tref_len_arg, uint gref_len_arg,
              uint M_arg, uint ef_construction_arg, distance_kind metric_arg,
              bool evictable_vectors_arg = false)
      : hlindex_slot(hlindex_slot_arg),
        tref_len(tref_len_arg),
        gref_len(gref_len_arg),
        M(M_arg),
        ef_construction(ef_construction_arg),
        metric(metric_arg) {
    if (evictable_vectors_arg) vec_arena = new Evictable_vector_arena();
    init_locks_and_alloc();
  }

  /*
   * report_cache_usage — update vidx_stats.cache_usage_bytes with the delta
   * since the last call.
   *
   * Must be called while the caller holds share->lock_share() (i.e. the
   * TABLE_SHARE::LOCK_open-derived share mutex).  That lock serialises
   * concurrent calls for the *same* share.  Concurrent calls from *different*
   * shares are a benign data race on the global counter (x86-64 aligned
   * 8-byte store is tear-free; a lost update is a small transient error
   * corrected on the next report).
   */
  void report_cache_usage() {
    size_t current = cache_allocated_size();
    long long delta = (long long)current - (long long)reported_cache_bytes;
    vidx_stats.cache_usage_bytes += delta;
    reported_cache_bytes = current;
  }

  virtual ~MHNSW_Share() {
    /* Subtract the last reported amount so the global counter does not leak. */
    vidx_stats.cache_usage_bytes -= (long long)reported_cache_bytes;
    delete vec_arena; /* nullptr on normal-path shares */
    /* No need to call free_root(&root, MYF(0)), because MEM_ROOT::Clear() will
    be called by MEM_ROOT::~MEM_ROOT() */
    mysql_rwlock_destroy(&commit_lock);
    mysql_mutex_destroy(&cache_lock);
    mysql_mutex_destroy(&start_lock);
    for (size_t i = 0; i < array_elements(node_lock); i++)
      mysql_mutex_destroy(node_lock + i);

    vidx_stats.hnsw_share_count--;
  }

  uint lock_node(FVectorNode *ptr) {
    ulong nr1 = 1, nr2 = 4;
    my_hash_sort_bin(0, (const uchar *)&ptr, sizeof(ptr), &nr1, &nr2);
    uint ticket = nr1 % array_elements(node_lock);
    mysql_mutex_lock(node_lock + ticket);
    return ticket;
  }

  void unlock_node(uint ticket) { mysql_mutex_unlock(node_lock + ticket); }

  uint max_neighbors(size_t layer) const {
    return (layer ? 1 : 2) * M;  // heuristic from the paper
  }

  void set_lengths(size_t len) {
    byte_len = len;
    vec_len = len / sizeof(float);
  }

  void lock_start() { mysql_mutex_lock(&start_lock); }
  void unlock_start() { mysql_mutex_unlock(&start_lock); }

  static int acquire(MHNSW_Share **ctx, TABLE *table, bool for_update);
  static MHNSW_Share *get_from_share(TABLE_SHARE *share, TABLE *table,
                                    uint slot);

  virtual void reset(TABLE_SHARE *share) {
    DBUG_TRACE;
    share->lock_share();
    if (share->hlindex_shares != nullptr &&
        static_cast<MHNSW_Share *>(share->hlindex_shares[hlindex_slot]->hlindex_data) ==
            this) {
      /* Write back the learned search-quality params so the next
         generation does not cold-start at 0.6 / 0 (recall regression:
         under-sized visited Bloom filter + narrowed beam window).
         set_if_bigger: both values are monotonically learned, so
         keeping the max is the safe side under any interleaving of
         generations on this slot.  Local copies: concurrent searchers
         may still bump the atomics while we tear down. */
      TABLE_SHARE *hl_share = share->hlindex_shares[hlindex_slot];
      const double cur_ef_power = ef_power.load();
      const float cur_diameter = diameter.load();
      set_if_bigger(hl_share->hlindex_learned_ef_power, cur_ef_power);
      set_if_bigger(hl_share->hlindex_learned_diameter, cur_diameter);
      hl_share->hlindex_data = nullptr;
      --refcnt;
    }
    /* Re-report current cache size while we still hold lock_share so the
       global counter stays in sync.  Base reset() does NOT call root.Clear()
       — only detaches hlindex_data; the underlying memory is freed by the
       eventual ~MHNSW_Share() destructor (see release()). */
    report_cache_usage();
    share->unlock_share();
  }

  void release(TABLE *table) {
    DBUG_TRACE;
    return release(table->file->has_transactions(), table->s);
  }

  virtual void release(bool can_commit, TABLE_SHARE *share) {
    DBUG_TRACE;
    if (can_commit) mysql_rwlock_unlock(&commit_lock);
    bool force_reset = false;
    DBUG_EXECUTE_IF("vidx_force_reset_on_release", force_reset = true;);
    if (force_reset || cache_allocated_size() > max_cache_size) reset(share);
    assert(refcnt.load() > 0);
    if (--refcnt == 0) this->~MHNSW_Share();  // XXX reuse
  }

  virtual MHNSW_Share *dup(bool can_commit) {
    DBUG_TRACE;
    ut_a(refcnt.load() > 0);
    refcnt++;
    if (can_commit) mysql_rwlock_rdlock(&commit_lock);
    return this;
  }

  /* find-or-create the canonical node for @p gref; caller holds cache_lock */
  FVectorNode *get_node_internal(const void *gref) {
    FVectorNode *node = node_cache.find(gref, gref_len);
    if (!node) {
      vidx_stats.load_node_misses++;
      DBUG_PRINT("info", ("load_node_misses: %llu",
                          vidx_stats.load_node_misses));
      void *mem = alloc_node_internal();
      /* MEM_ROOT exceeded max_cache_size, cache full, return nullptr */
      if (unlikely(!mem)) return nullptr;
      node = new (mem) FVectorNode(this, gref);
      /* Under cache_lock a prior node with this gref cannot appear between
         find() and insert(). Therefore insert failure is OOM, not a benign
         duplicate. Fail closed: returning an untracked stub would allow the
         same gref to acquire multiple pointers and break destination-shard
         single-writer ownership during parallel DDL. */
      if (unlikely(node_cache.insert(node))) return nullptr;
    } else {
      vidx_stats.load_node_hits++;
    }
    return node;
  }

  FVectorNode *get_node(const void *gref) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    return get_node_internal(gref);
  }

  /* Resolve one node's whole neighbor list with a single cache_lock
     acquisition, writing the canonical nodes to out[0..n-1].

     Taking the lock per gref made this global mutex the scaling limit of
     concurrent loading: a list holds up to 2*M grefs, so loading one node
     cost tens of lock round-trips.  Measured on 768D/1M with a graph past
     the memory budget, eight concurrent loaders convoyed at 0.19 core each
     and ~19k node loads/s in total, while the mutex was held for only ~8%
     of one core -- the cost was the acquisition count, not the hold time,
     so batching per list removes the contention and every individual hold
     still stays short.

     Returns false on OOM (cache full) with out[] partially written; the
     caller abandons the node either way. */
  bool get_nodes(const uchar *grefs, size_t n, FVectorNode **out) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    for (size_t i = 0; i < n; i++, grefs += gref_len) {
      out[i] = get_node_internal(grefs);
      if (unlikely(!out[i])) return false;
    }
    return true;
  }

  /* used on INSERT, gref isn't known, so cannot cache the node yet */
  void *alloc_node() {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    return alloc_node_internal();
  }

  /* Allocate one vector payload slot for @p owner: reuse a reclaimed slot
     when available, otherwise grow m_vec_root by one uniform slot.  Returns
     the FVector area (owner header filled), nullptr on OOM.  Same lock as
     node allocs; callers may hold a node_lock (node_lock -> cache_lock is
     the established order, see alloc_neighborhood). */
  void *alloc_vec(FVectorNode *owner) {
    DBUG_TRACE;
    assert(vec_arena);
    uchar *slot = nullptr;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    if (!vec_arena->m_free_slots.empty()) {
      slot = vec_arena->m_free_slots.back();
      vec_arena->m_free_slots.pop_back();
    } else if ((slot = static_cast<uchar *>(
                    vec_arena->m_vec_root.Alloc(vec_slot_size())))) {
      vec_arena->m_all_slots.push_back(slot);
    }
    if (unlikely(!slot)) return nullptr;
    memcpy(slot, &owner, sizeof(owner));
    vec_arena->m_used_bytes += vec_slot_size();
    return slot + sizeof(FVectorNode *);
  }

  /* explicitly cache the node after alloc_node() */
  void cache_node(FVectorNode *node) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    cache_internal(node);
  }

  /* cache_node with an OOM verdict, for the DDL bulk persist: the node's
     durable gref was just assigned and later windows resolve it through
     get_node/find_node (read-through loads) while the payload
     reclaim walks the slot registry — a silently missed insert would fork
     duplicate stubs for the gref and clobber neighbor lists on the next
     persist.  Fresh grefs cannot collide, so failure here is OOM. */
  bool cache_node_checked(FVectorNode *node) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    assert(node->stored);
    return !node_cache.insert(node);
  }

  /* Drop a stub node from the cache when its load_from_record failed
     halfway, so a subsequent get_node(same_gref) does not return the
     half-initialised stub.  The MEM_ROOT slot stays leaked until ctx
     is destroyed — same lifetime model as the rest of node_cache —
     but the cache mapping is corrected. */
  void uncache_node(FVectorNode *node) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    node_cache.remove(node);
  }

  /* Find the canonical node without creating one. Besides trx-context merge,
     the DDL leader uses this to validate worker-reported historical stubs. */
  FVectorNode *find_node(const void *gref) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    return node_cache.find(gref, gref_len);
  }

  void *alloc_neighborhood(size_t max_layer) {
    DBUG_TRACE;
    MUTEX_LOCK(cache_lock_guard, &cache_lock);
    return root.Alloc(sizeof(Neighborhood) * (max_layer + 1) +
                      sizeof(FVectorNode *) *
                          (MY_ALIGN(M, 4) * 2 +
                           MY_ALIGN(M, VISITED_PROBE_BATCH) * max_layer));
  }
  uint get_refcnt() const { return refcnt.load(); }

  /* Read-only view of the LIVE cache footprint: node/adjacency arena plus
     net payload-slot bytes.  root keeps the baseline allocated_size()
     accounting; the payload side counts live slots only (see
     Evictable_vector_arena::m_used_bytes for why m_vec_root.allocated_size()
     must not be used). */
  size_t cache_allocated_size() const {
    return root.allocated_size() + (vec_arena ? vec_arena->m_used_bytes : 0);
  }

  /* Synchronous-DDL window handover: free every vector payload slot while
     the node and adjacency structures stay resident.  The next window
     refills payloads on demand (one FIELD_VEC read per miss) instead of
     rebuilding the whole graph through full node loads, and it starts with
     the entire payload arena free, so it has a full-budget runway.

     Partial (aged-out slice) reclaim was tried and rejected on evidence:
     it leaves the graph just under the budget, so the next window
     re-crosses almost immediately and re-faults the payloads it just
     evicted -- 21 to 583 windows and 800s-4147s against 445s for this
     free-all handover (1M x 2.0GB, W=16).

     Returns false only when the resident node/adjacency structures alone
     leave no payload headroom -- the caller then rebuilds the ctx from
     scratch.
     Single-threaded call site: the leader between windows, after
     persist_build_window -- every cached payload is refetchable. */
  bool evict_ddl_vectors(size_t budget) {
    DBUG_TRACE;
    assert(vec_arena);
    const size_t slot_sz = vec_slot_size();
    for (uchar *slot : vec_arena->m_all_slots) {
      FVectorNode *owner;
      memcpy(&owner, slot, sizeof(owner));
      if (!owner) continue; /* already free */
      if (likely(owner->vec)) {
        /* A node with a payload but no adjacency cannot exist at a window
           boundary (inserts complete before handover); skip it defensively
           so `vec_evicted` always implies a complete adjacency. */
        assert((const uchar *)owner->vec >= slot &&
               (const uchar *)owner->vec < slot + slot_sz);
        assert(owner->neighbors);
        if (unlikely(!owner->neighbors)) continue;
        owner->vec_evicted = true;
        owner->vec = nullptr;
      }
      /* !owner->vec: slot leaked by a failed load (payload never
         published); reclaim it without touching the node's state. */
      FVectorNode *none = nullptr;
      memcpy(slot, &none, sizeof(none));
      vec_arena->m_free_slots.push_back(slot);
      vec_arena->m_used_bytes -= slot_sz;
    }
    return budget > root.allocated_size() + slot_sz;
  }
};

/* -------------------- MHNSW_Trx -------------------- */
/*
  This is a non-shared context that exists within one transaction.

  At the end of the transaction it's either discarded (on rollback)
  or merged into the shared ctx (on commit).

  trx's are stored in thd->ha_data[hnsw_hton.slot] in a single-linked
  list, one instance per TABLE_SHARE, allocated on the
  thd->transaction->mem_root.
*/
class MHNSW_Trx : public MHNSW_Share {
 public:
  MDL_ticket *table_id;
  bool list_of_nodes_is_lost = false;
  MHNSW_Trx *next = nullptr;

  MHNSW_Trx(TABLE *table) : MHNSW_Share(table), table_id(table->mdl_ticket) {
    vidx_stats.hnsw_trx_count++;
  }

  /* ~MHNSW_Share() destructor chain invokes MEM_ROOT::~MEM_ROOT(). */
  ~MHNSW_Trx() {
    vidx_stats.hnsw_trx_count--;
  }

  void reset(TABLE_SHARE *) override {
    DBUG_TRACE;
    node_cache.clear();
    root.Clear();
    if (vec_arena) {
      vec_arena->m_vec_root.Clear();
      vec_arena->m_all_slots.clear();
      vec_arena->m_free_slots.clear();
      vec_arena->m_used_bytes = 0;
    }
    /* After Clear() allocated_size() drops to 0; sync the global counter so
       the next report_cache_usage() computes the correct delta. */
    vidx_stats.cache_usage_bytes -= (long long)reported_cache_bytes;
    reported_cache_bytes = 0;
    start = nullptr;
    list_of_nodes_is_lost = true;
  }
  void release(bool, TABLE_SHARE *) override {
    DBUG_TRACE;
    assert(refcnt.load() > 0);
    if (--refcnt == 0 && cache_allocated_size() > max_cache_size)
      reset(nullptr);
  }

  MHNSW_Share *dup(bool) override {
    refcnt++;
    return this;
  }

  static int get_from_thd(MHNSW_Trx **trx, TABLE *table, bool for_update);

  // it's okay in a transaction-local cache, there's no concurrent access
  Hash_set<FVectorNode> &get_cache() { return node_cache; }

  /* fake handlerton to use thd->ha_data and to get notified of commits */
  static struct MHNSW_hton : public handlerton {
    MHNSW_hton() {
      db_type = DB_TYPE_HLINDEX_HELPER;
      flags = HTON_NOT_USER_SELECTABLE | HTON_HIDDEN;
      /* savepoint_offset will be set in setup_transaction_participant(). */
      savepoint_offset = 0;
      savepoint_set = [](handlerton *, THD *, void *) { return 0; };
      savepoint_rollback_can_release_mdl = [](handlerton *, THD *) {
        return true;
      };
      savepoint_rollback = do_savepoint_rollback;
      /*
        XA cleanup rule: flush share-cache invalidation eagerly only when
        THIS thd is about to lose its Transaction_ctx; otherwise defer to
        the natural do_commit / do_rollback hook (and the unconditional
        disconnect fallback in THD::cleanup if the client drops first).

        Two "thd loses ctx" cases route to do_commit at PREPARE time:
          - Detached XA (xa_detach_on_prepare = ON): detach_xa_transaction
            moves the ctx to the global cache; later XA COMMIT / ROLLBACK
            runs on a recovery thd via commit_by_xid / rollback_by_xid
            with no MHNSW_Trx attached.
          - Slave applier (is_engine_ha_data_detached() == true): right
            after PREPARE, applier_reset_xa_trans calls attach_native_trx
            + trn_ctx->cleanup which clears the SE participant list and
            wipes m_mem_root WITHOUT firing our commit hook — leaving
            ha_data[hnsw_hton.slot].ha_ptr dangling.  A subsequent DML
            placement-news a fresh MHNSW_Trx at the same address and the
            self-loop trips assert(trx->next != trx).  An applier never
            relies on the "preserve uncommitted state across XA ROLLBACK"
            invariant — it replays the master's decided outcome.

        prepare/close_connection are no-ops: aux-table InnoDB rows are
        committed eagerly by Hlindex_dml_scope long before XA PREPARE, and
        ordinary commit/rollback always fires on the owning thd before
        teardown.  Only the disconnect-on-XA-PREPARED edge needs the
        explicit forwarder — handled in sql_class.cc.
      */
      close_connection = 0;
      commit = do_commit;
      rollback = do_rollback;
      prepare = [](handlerton *, THD *, bool) -> int { return 0; };
      recover = [](handlerton *, XA_recover_txn *, uint, MEM_ROOT *) {
        return 0;
      };
      set_prepared_in_tc = [](handlerton *ht, THD *thd) -> int {
        if (is_xa_tran_detached_on_prepare(thd) ||
            thd->is_engine_ha_data_detached()) {
          return do_commit(ht, thd, true);
        }
        return 0;
      };
    }
    static int do_commit(handlerton *, THD *thd, bool);
    static int do_rollback(handlerton *, THD *thd, bool);
    static int do_savepoint_rollback(handlerton *, THD *thd, void *);
  } hnsw_hton;
};

/* -------------------- Visited / VisitedSet / Search_context -------------------- */
/* one visited node during the search. caches the distance to target */
struct Visited {
  FVectorNode *node;
  const float distance_to_target;
  Visited(FVectorNode *n, float d) : node(n), distance_to_target(d) {}
  static int cmp(void *, uchar *a_, uchar *b_) {
    const Visited *a = (const Visited *)a_;
    const Visited *b = (const Visited *)b_;
    return a->distance_to_target < b->distance_to_target   ? -1
           : a->distance_to_target > b->distance_to_target ? 1
                                                           : 0;
  }
};

/*
  a factory to create Visited and keep track of already seen nodes

  note that PatternedSimdBloomFilter works in blocks of 8 elements,
  so on insert they're accumulated in nodes[], on search the caller
  provides VISITED_PROBE_BATCH addresses at once. we record 0x0 as "seen"
  so that the caller could pad the input with nullptr's
*/
class VisitedSet {
  MEM_ROOT *root;
  const FVector *target;
  PatternedSimdBloomFilter<FVectorNode> map;
  const FVectorNode *nodes[8] = {0, 0, 0, 0, 0, 0, 0, 0};
  size_t idx = 1;  // to record 0 in the filter
 public:
  uint count = 0;
  VisitedSet(MEM_ROOT *root, const FVector *target, uint size)
      : root(root), target(target), map(size, 0.01f) {}
  Visited *create(FVectorNode *node) {
    auto *v = new (root) Visited(node, node->distance_to(target));
    insert(node);
    count++;
    return v;
  }
  /* Same as create() but with a distance the caller already computed
     (batched distance kernel) — skips the per-node distance_to call. */
  Visited *create(FVectorNode *node, float dist) {
    auto *v = new (root) Visited(node, dist);
    insert(node);
    count++;
    return v;
  }
  void insert(const FVectorNode *n) {
    nodes[idx++] = n;
    if (idx == 8) flush();
  }
  void flush() {
    if (idx) map.Insert(nodes);
    idx = 0;
  }
  uint8_t seen(FVectorNode **nodes) { return map.Query(nodes); }
};

/* Per-thread working set for parallel DDL construction. Workers own no THD
   or handler of the ALTER, so all temporary allocations and priority queues
   stay here. */
struct Search_work_area {
  /* search_layer's frontier/result queues and select_neighbors' heap. */
  Queue<Visited> m_candidates, m_best, m_select_pq;
  MEM_ROOT m_root{key_memory_vidx_mem, 65536}; /* per-task scratch arena */
  /* Load channel of this executor -- an invariant, never null while a
     work-area search runs.  A cache miss is always resolved inline on it:
     - worker: a private graph TABLE instance bound to the worker's internal
       READ-UNCOMMITTED THD (read-through);
     - leader: the ALTER's own graph handler (same transaction, uncommitted
       rows directly visible).
     A worker that cannot establish its channel fails the ALTER instead of
     degrading (fail closed, see ddl_worker_bind_graph). */
  TABLE *m_load_graph{nullptr};

  void clear_for_reuse() {
    if (m_candidates.is_inited()) m_candidates.clear();
    if (m_best.is_inited()) m_best.clear();
    if (m_select_pq.is_inited()) m_select_pq.clear();
    m_root.ClearForReuse();
  }
};

struct Search_context {
  Neighborhood found;
  MHNSW_Share *ctx;
  const FVector *target;
  ulonglong ctx_version;
  size_t pos = 0;
  float threshold = NEAREST / 2;
  Search_context(Neighborhood *n, MHNSW_Share *s, const FVector *v)
      : found(*n), ctx(s->dup(false)), target(v), ctx_version(ctx->version) {}
};

/* -------------------- ScopedLockWaitTimeout -------------------- */
/**
  RAII guard that temporarily shortens the lock wait timeout on a handler.

  When @p active is true, the ctor reads hnsw_lock_timeout_ms, rounds
  up to seconds, calls handler::set_lock_wait_timeout(secs); the dtor
  calls set_lock_wait_timeout(0) which restores the snapshotted value
  (see ha_innobase::set_lock_wait_timeout).  When @p active is false
  the guard is a no-op.

  Used on replica apply threads to make write operations that conflict
  with concurrent attachable inner-trx writers (or any other short-lived
  contender) on the graph table fail quickly instead of waiting the
  default 50 s, so apply progress isn't stalled by a single hot row.

  Single-level only — current callers do not nest.
*/
class ScopedLockWaitTimeout {
  handler *h_;
  bool active_;

 public:
  ScopedLockWaitTimeout(handler *h, bool active) : h_(h), active_(active) {
    if (active_) {
      /* Round up ms -> seconds, clamp to >= 1 (handler timeout is
         second-granular; sub-second values would round to 0). */
      h_->set_lock_wait_timeout(
          std::max<ulong>(1, (lock_timeout_ms + 999) / 1000));
    }
  }
  ~ScopedLockWaitTimeout() {
    if (active_) h_->set_lock_wait_timeout(0);  // restore snapshotted value
  }
  ScopedLockWaitTimeout(const ScopedLockWaitTimeout &) = delete;
  ScopedLockWaitTimeout &operator=(const ScopedLockWaitTimeout &) = delete;
};

/* -------------------- Module-internal forward decls -------------------- */
/* Helpers extracted to vidx_hnsw_ops.cc. */
inline bool is_stmt_within_outer_trx(THD *thd, bool all) {
  /* MDEV-37068: when we get called for a single statement (all=false)
     inside an outer transaction, only release statement-level resources;
     commit/rollback proper happens later when the outer trx ends. */
  return !all && thd_test_options(thd, OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN);
}

int load_entry_point_node(MHNSW_Share *ctx, TABLE *graph);

/* Core HNSW operations reused by the synchronous DDL builder. A non-null
   work_area selects the worker-safe, memory-resident-only execution path. */
int search_layer(MHNSW_Share *ctx, TABLE *graph, const FVector *target,
                 float threshold, uint result_size, size_t layer,
                 Neighborhood *inout, bool construction,
                 Search_work_area *work_area = nullptr);
int select_neighbors(MHNSW_Share *ctx, TABLE *graph, size_t layer,
                     FVectorNode &target, const Neighborhood &candidates,
                     FVectorNode *extra_candidate,
                     size_t max_neighbor_connections);
int select_neighbors(MHNSW_Share *ctx, size_t layer, FVectorNode &target,
                     const Neighborhood &candidates,
                     FVectorNode *extra_candidate,
                     size_t max_neighbor_connections,
                     Search_work_area *work_area);
int link_reverse_neighbor(MHNSW_Share *ctx, size_t layer,
                          FVectorNode *node, FVectorNode *neigh,
                          uint max_neighbors, bool *changed,
                          Search_work_area *work_area);

int prefetch_neighbor_locks_in_gref_order(TABLE *table, TABLE *graph,
                                          FVectorNode *target,
                                          int target_layer,
                                          const MHNSW_Share *ctx,
                                          THD *thd,
                                          PrefetchResult *out_result);

void invalidate_align_cache(MHNSW_Share *ctx, const uchar *gref,
                            ulonglong &skip_counter);

/* Two-step IDX_TREF -> GEN_CLUST locate + TOCTOU + soft-delete + cache
   align.  Body of mhnsw_invalidate factored out so the wrapper retains
   its baseline shape (acquire ctx -> scope -> hlindex_lock ->
   load_entry_point -> call helper -> scope.commit).  See definition
   for invariants on graph->record[0] / handler::inited. */
int invalidate_locate_and_softdelete(TABLE *table, TABLE *graph,
                                     MHNSW_Share *ctx);

/* Drain Search_context::found from pos forward, skipping orphan refs
   (HA_ERR_KEY_NOT_FOUND / HA_ERR_RECORD_DELETED produced by the indep
   DML rollback contract).  Returns 0 when a real row is positioned
   into table->record[0]; HA_ERR_END_OF_FILE when the drain exhausts;
   any other handler error when ha_rnd_pos hard-fails. */
int drain_skipping_orphans(TABLE *table, Search_context *result);

}  // namespace hnsw
}  // namespace vidx

#endif /* VIDX_HNSW_CTX_INCLUDED */
