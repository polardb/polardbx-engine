#ifndef VIDX_STATS_INCLUDED
#define VIDX_STATS_INCLUDED

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

#include <atomic>
#include <cstdint>

#include "my_inttypes.h"  // ulonglong
#include "vidx/vidx_common.h"  // vidx::hnsw::EF_POWER_DEF / DIAMETER_DEF

struct SHOW_VAR;

namespace vidx {

/*
  Global vidx monitoring counters.

  Concurrency model
  -----------------
  Most fields are plain ulonglong.  Hot-path ++ under concurrency relies
  on x86-64 aligned 8-byte stores being tear-free; a lost update on a
  monitoring counter is benign — it shifts the displayed value by 1, well
  inside the noise of any operational trend the operator is looking at.
  Promoting every field to std::atomic costs nothing on x86-64 (LOCK XADD
  vs INC), but it has two real downsides we want to avoid:
    - SHOW_LONGLONG cannot bind to an atomic field's raw address (strict-
      aliasing UB; GCC has been observed to hoist the read out of the
      show-vars loop), so every field would need a SHOW_FUNC trampoline.
    - It encourages "atomic == correct" thinking and hides that on weaker
      ISAs a few of these counters DO need atomic semantics; mixing the
      types makes the design choice explicit per field.

  Two specific counters are std::atomic<int64_t> because losing a ±1 there
  would falsify resource-leak diagnostics:
    - hnsw_trx_count   — live MHNSW_Trx instances
    - hnsw_share_count — live MHNSW_Share instances
  A drift of just 1 would look like a permanent leak; atomic guarantees
  the running sum exactly matches the ctor/dtor invocation count.

  cache_usage_bytes is plain long long; concurrent updates for the same
  share are serialised by lock_share(), and cross-share races are bounded
  by the next report_cache_usage() call.
*/
struct Vidx_stats {
  // --- Cache Performance ---
  ulonglong load_node_hits{0};
  ulonglong load_node_misses{0};
  /*
   * Approximate total MEM_ROOT bytes of all live MHNSW_Share objects.
   * Updated via report_cache_usage() under TABLE_SHARE::lock_share();
   * concurrent calls from different shares race benignly (a lost update
   * is corrected on the next report).  Signed: brief windows of negative
   * delta are possible between MHNSW_Share dtor and the next report.
   */
  long long cache_usage_bytes{0};
  /* hnsw_share_count: live MHNSW_Share instances (incremented in the
     base ctor — MHNSW_Trx, as a subclass, also bumps it).
     hnsw_trx_count: live MHNSW_Trx instances only.
     hnsw_share_count - hnsw_trx_count = "pure" share-cache contexts
     (table-share-scoped, no per-trx caching).  Useful for distinguishing
     the two cache populations during memory diagnostics; the diff
     collapses to ~0 in deployments where every workload acquires
     per-trx caches.  Atomic because a single missed ±1 would falsify
     the leak-detection invariant (running count == ctor count - dtor
     count). */
  std::atomic<long long> hnsw_trx_count{0};
  std::atomic<long long> hnsw_share_count{0};

  // --- Acquire Statistics ---
  ulonglong acquire_start_hits{0};      /* acquire() found ctx->start cached */
  ulonglong acquire_start_misses{0};    /* acquire() loaded entry point from btree */
  ulonglong decode_start_node_count{0}; /* load_from_record in acquire path */

  // --- Learned-parameter inheritance ---
  /** ef_power / diameter inherited by the most recently created
      MHNSW_Share (written in get_from_share() under lock_share; cross-
      share races are benign last-writer-wins, same model as
      cache_usage_bytes).  Lets regression tests and online diagnostics
      confirm — without gdb — that the learned search-quality params
      survive a cache reset.  The shared cold-start defaults mean
      "first-ever open of an index, or learned values lost". */
  double last_ef_power_init{hnsw::EF_POWER_DEF};
  double last_diameter_init{hnsw::DIAMETER_DEF};

  // --- Load Path Statistics ---
  ulonglong load_vec_misses{0};       /* ha_rnd_pos btree access executed */
  ulonglong decode_vec_node_count{0}; /* load_from_record in load() path */

  // --- Query Statistics ---
  ulonglong query_count{0};
  ulonglong insert_count{0};
  ulonglong update_count{0};
  /** mhnsw_invalidate() entry count.  Named "invalidate" rather
      than "delete" because vidx aux-table never hard-deletes nodes —
      it soft-clears FIELD_TREF (see clear_stale_tref_node) and lets
      the next insert/maintenance compact.  TRUNCATE / mhnsw_delete_all
      goes through a separate path and is NOT counted here. */
  ulonglong invalidate_count{0};
  /** mhnsw_read_next(): orphan node skipped — the graph still held
      a node whose tref points at a base row removed by an outer-trx
      ROLLBACK (or by post-XA-PREPARE crash recovery).  Skipping
      preserves ANN recall under sustained orphan pressure (without
      the skip, IndexScanIterator::HandleError treats KEY_NOT_FOUND
      as end-of-scan and truncates the LIMIT prematurely).  See
      Docs/ann_recall_shrinkage_investigation.md §3 for the root
      cause and Docs/vidx_orphan_cleanout_design.md for the
      proactive cleanup proposal this counter motivates. */
  ulonglong read_skip_orphan{0};

  // --- Independent-trx DML commit health ---
  /** Hlindex_dml_scope::commit(): inner trans_commit succeeded. */
  ulonglong indep_dml_commit{0};
  /** Hlindex_dml_scope::commit(): inner trans_commit / lock / rollback path
      failed with any non-deadlock error.  Deadlock victims are counted
      separately in indep_dml_deadlock so operators can distinguish
      "real" indep-path breakage from contention. */
  ulonglong indep_dml_commit_error{0};
  /** Hlindex_dml_scope::commit(): inner attachable trx was the deadlock
      victim (HA_ERR_LOCK_DEADLOCK).  Tracks lock-conflict pressure on
      the auxiliary HNSW graph table; high frequency suggests reducing
      concurrent updates over the same vector neighborhood or auditing
      gref-based lock ordering. */
  ulonglong indep_dml_deadlock{0};

  /*
    DML routing / tolerance / skip counters.  Mixed audience:
      - save_skip_xxx / invalidate_skip_xxx / neighbor_save_skip_xxx /
        save_skip_lock_timeout / prefetch_lock_timeout family is mostly
        replica self-heal; non-zero on a replica deserves attention as
        a potential graph-divergence signal.
      - update_partial_orphan and update_skipped fire on master under
        normal load (the latter is the hot-path optimization for
        non-vector UPDATEs); investigate via SHOW GLOBAL STATUS LIKE
        'Vidx_%' alongside indep_dml_commit / indep_dml_deadlock.
  */
  // --- Tolerance / Self-Heal / DML-Routing Statistics ---
  /** Node::save(): dup-key collision (HA_ERR_FOUND_DUPP_KEY) — clear & retry. */
  ulonglong save_retry_dup_key{0};
  /** mhnsw_invalidate(): TREF row not found — invalidate skipped. */
  ulonglong invalidate_skip_not_found{0};
  /** mhnsw_invalidate(): lock timeout on TREF index read — invalidate skipped. */
  ulonglong invalidate_skip_read_timeout{0};
  /** mhnsw_invalidate(): lock timeout on soft-delete update — disk row left as
      orphan, in-memory cache still marked deleted. */
  ulonglong invalidate_skip_update_timeout{0};
  /** mhnsw_invalidate() / clear_stale_tref_node(): TOCTOU — between the
      MVCC IDX_TREF lookup (Step1) and the LOCK_X ha_rnd_pos (Step2) the
      target row's FIELD_TREF was changed by another concurrent
      attachable trx (NULLified or rewritten).  The current operation is
      skipped: the other trx already did the equivalent work, or the row
      no longer matches our intent. */
  ulonglong invalidate_skip_tref_changed{0};
  /** target->save_new_row(): replica lock-timeout on the inserting
      node's first write — node body NOT persisted on disk; the
      slave's graph diverges from the primary by missing this node
      until the next touch revives it.  Higher impact than the
      neighbor variant below: a missed target write means the node
      is not searchable at all. */
  ulonglong save_target_skip_lock_timeout{0};
  /** neigh->save_neighbors_only(): replica lock-timeout on a
      neighbor's reverse-edge update — the neighbor row stays on
      disk, only its NEIGHBORS field is stale; ANN search still
      finds the new node via its own forward edges, but recall on
      the affected layer drops by one neighbor.  Self-heals on the
      next maintenance pass touching this neighbor. */
  ulonglong save_neighbor_skip_lock_timeout{0};
  /** update_second_degree_neighbors(): neigh->save_neighbors_only() call count.
      Replaces the legacy neigh->save() in update_2nd; only NEIGHBORS field is
      written, FIELD_TREF/VEC/LAYER are preserved from disk before-image to
      avoid reviving stale IDX_TREF entries via per-Trx cache snapshot. */
  ulonglong neighbor_save_count{0};
  /** save_neighbors_only(): graph row was hard-deleted between the gref-
      ordered prefetch and the second ha_rnd_pos — neighbor update skipped
      (in-memory graph stays slightly stale, corrected by next maintenance
      pass). */
  ulonglong neighbor_save_skip_not_found{0};
  /** mhnsw_insert(): LOCK_WAIT_TIMEOUT on the gref-ordered prefetch X-lock.
      The prefetch is a deadlock-avoidance write lock taken before
      update_second_degree_neighbors.  When this counter rises, raise
      innodb_lock_wait_timeout or reduce concurrent writers on
      overlapping vector neighborhoods. */
  ulonglong prefetch_lock_timeout{0};
  /** hlindexes_on_update() (invalidate + insert): invalidate inner trx
      committed but the follow-up insert inner trx failed; old vidx node
      soft-deleted on disk but new one missing, leaving an "orphan"
      interval.  Self-heals on the next UPDATE/INSERT touching the same
      base row; high frequency suggests auditing replica apply or
      per-row deadlock pressure. */
  ulonglong update_partial_orphan{0};
  /** hlindexes_on_insert(): with multiple vector indexes on the indep
      path each per-key graph INSERT commits in its own attachable; a
      failure on a later key leaves the earlier keys' committed graph
      rows orphaned once the user statement rolls back.  Same self-heal
      model as update_partial_orphan (next DML touching the base row),
      but tracked separately so multi-index INSERT pressure is visible. */
  ulonglong insert_partial_orphan{0};
  /** hlindexes_on_update(): UPDATE skipped vidx invalidate + insert
      because the vector and the primary key (TREF) didn't change.
      Common case for workloads like "UPDATE t SET status=... WHERE id=
      ..." that touch only non-vector columns.  Skipping keeps the base
      table's PRIMARY / secondary-index X locks short-lived, materially
      reducing ABBA deadlock probability between concurrent UPDATE and
      INSERT on the base table. */
  ulonglong update_skipped{0};

  // --- Synchronous DDL build statistics ---
  /** Completed W>1 construction micro-batches. */
  ulonglong ddl_parallel_batches{0};
  /** Successfully persisted direct-build memory windows. */
  ulonglong ddl_build_windows{0};
  /** Reverse-preload barriers that loaded historical nodes. */
  ulonglong ddl_parallel_load_rounds{0};
  /** Historical nodes materialized by the DDL leader before REVERSE. */
  ulonglong ddl_parallel_loaded_nodes{0};
  /** Direct-build ALTERs failed by an internal error (one count per failed
      statement, updated only in finish_ddl_direct_build; KILLs excluded). */
  ulonglong ddl_build_error_count{0};
  /** Concurrent direct DDL builds.  Plain gauge under the same benign-tear
      model as the counters above: only concurrent COPY-DDL leaders touch it,
      and a lost ±1 merely skews this monitoring value, not correctness. */
  long long ddl_direct_builds_active{0};
};

/* Global statistics instance — defined in sql/vidx/vidx_stats.cc */
extern Vidx_stats vidx_stats;

/* SHOW GLOBAL STATUS LIKE 'Vidx_%' table.  Defined in
   sql/vidx/vidx_stats.cc and consumed by plugin_init() via
   add_status_vars().  Sentinel-terminated array (last element has
   name == nullptr). */
extern SHOW_VAR vidx_status_variables[];

}  // namespace vidx

#endif  // VIDX_STATS_INCLUDED
