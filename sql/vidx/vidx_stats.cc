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

#include "vidx/vidx_stats.h"

#include "mysql/plugin.h"  // SHOW_VAR, SHOW_LONGLONG, SHOW_FUNC, SHOW_SCOPE_GLOBAL

namespace vidx {

/* Global statistics instance. */
Vidx_stats vidx_stats;

/* Three SHOW_FUNC trampolines for the non-ulonglong fields — see
   vidx_stats.h struct docblock for why these three are typed differently
   from the rest. */

/* cache_usage_bytes is signed; clamp negative transients to 0 for display. */
static int show_cache_usage(THD *, SHOW_VAR *var, char *buf) {
  long long v = vidx_stats.cache_usage_bytes;
  *reinterpret_cast<longlong *>(buf) = v < 0 ? 0LL : v;
  var->type = SHOW_LONGLONG;
  var->value = buf;
  return 0;
}

/* hnsw_trx_count / hnsw_share_count are std::atomic<long long>; go
   through load() rather than reinterpret-casting the field address,
   since the latter is strict-aliasing UB and GCC has been observed to
   hoist the read out of the show-vars loop (caught by
   vidx_force_non_indep_switch.test). */
static int show_hnsw_trx_count(THD *, SHOW_VAR *var, char *buf) {
  *reinterpret_cast<longlong *>(buf) =
      vidx_stats.hnsw_trx_count.load(std::memory_order_relaxed);
  var->type = SHOW_LONGLONG;
  var->value = buf;
  return 0;
}

static int show_hnsw_share_count(THD *, SHOW_VAR *var, char *buf) {
  *reinterpret_cast<longlong *>(buf) =
      vidx_stats.hnsw_share_count.load(std::memory_order_relaxed);
  var->type = SHOW_LONGLONG;
  var->value = buf;
  return 0;
}

/* The remaining entries are plain ulonglong — SHOW_LONGLONG binds
   directly to the field address (no strict-aliasing issue since the
   storage *is* an unsigned-integer type). */
SHOW_VAR vidx_status_variables[] = {
    // --- Cache Performance ---
    {"Vidx_load_node_hits", (char *)&vidx_stats.load_node_hits,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_load_node_misses", (char *)&vidx_stats.load_node_misses,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_cache_usage", (char *)show_cache_usage, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_hnsw_trx_count", (char *)show_hnsw_trx_count, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_hnsw_share_count", (char *)show_hnsw_share_count, SHOW_FUNC,
     SHOW_SCOPE_GLOBAL},

    // --- Acquire Statistics ---
    {"Vidx_acquire_start_hits", (char *)&vidx_stats.acquire_start_hits,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_acquire_start_misses", (char *)&vidx_stats.acquire_start_misses,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_decode_start_node_count",
     (char *)&vidx_stats.decode_start_node_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    // --- Learned-parameter inheritance ---
    // Plain double fields; SHOW_DOUBLE binds directly to the address
    // (my_fcvt with 6-digit precision, deterministic output for MTR).
    {"Vidx_last_ef_power_init", (char *)&vidx_stats.last_ef_power_init,
     SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},

    {"Vidx_last_diameter_init", (char *)&vidx_stats.last_diameter_init,
     SHOW_DOUBLE, SHOW_SCOPE_GLOBAL},

    // --- Load Path Statistics ---
    {"Vidx_load_vec_misses", (char *)&vidx_stats.load_vec_misses,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_decode_vec_node_count",
     (char *)&vidx_stats.decode_vec_node_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    // --- Query Statistics ---
    {"Vidx_query_count", (char *)&vidx_stats.query_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_insert_count", (char *)&vidx_stats.insert_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_update_count", (char *)&vidx_stats.update_count, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_invalidate_count", (char *)&vidx_stats.invalidate_count,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_read_skip_orphan", (char *)&vidx_stats.read_skip_orphan,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    // --- Slave Tolerance / Skip Statistics ---
    // Non-zero values on a replica indicate potential graph divergence.
    {"Vidx_save_retry_dup_key", (char *)&vidx_stats.save_retry_dup_key,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_invalidate_skip_not_found",
     (char *)&vidx_stats.invalidate_skip_not_found, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_invalidate_skip_read_timeout",
     (char *)&vidx_stats.invalidate_skip_read_timeout, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_invalidate_skip_update_timeout",
     (char *)&vidx_stats.invalidate_skip_update_timeout, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_invalidate_skip_tref_changed",
     (char *)&vidx_stats.invalidate_skip_tref_changed, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_save_target_skip_lock_timeout",
     (char *)&vidx_stats.save_target_skip_lock_timeout, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_save_neighbor_skip_lock_timeout",
     (char *)&vidx_stats.save_neighbor_skip_lock_timeout, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    // --- NEIGHBORS-only update for update_second_degree_neighbors ---
    {"Vidx_neighbor_save_count", (char *)&vidx_stats.neighbor_save_count,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_neighbor_save_skip_not_found",
     (char *)&vidx_stats.neighbor_save_skip_not_found, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_prefetch_lock_timeout", (char *)&vidx_stats.prefetch_lock_timeout,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_update_partial_orphan",
     (char *)&vidx_stats.update_partial_orphan, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_insert_partial_orphan",
     (char *)&vidx_stats.insert_partial_orphan, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_update_skipped", (char *)&vidx_stats.update_skipped,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    // --- Independent-trx DML commit health ---
    {"Vidx_indep_dml_commit", (char *)&vidx_stats.indep_dml_commit,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_indep_dml_commit_error",
     (char *)&vidx_stats.indep_dml_commit_error, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_indep_dml_deadlock", (char *)&vidx_stats.indep_dml_deadlock,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    // --- Synchronous DDL build ---
    {"Vidx_ddl_build_windows", (char *)&vidx_stats.ddl_build_windows,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_ddl_parallel_batches", (char *)&vidx_stats.ddl_parallel_batches,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {"Vidx_ddl_parallel_load_rounds",
     (char *)&vidx_stats.ddl_parallel_load_rounds, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_ddl_parallel_loaded_nodes",
     (char *)&vidx_stats.ddl_parallel_loaded_nodes, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_ddl_direct_builds_active",
     (char *)&vidx_stats.ddl_direct_builds_active, SHOW_LONGLONG,
     SHOW_SCOPE_GLOBAL},

    {"Vidx_ddl_build_error_count", (char *)&vidx_stats.ddl_build_error_count,
     SHOW_LONGLONG, SHOW_SCOPE_GLOBAL},

    {nullptr, nullptr, SHOW_LONG, SHOW_SCOPE_GLOBAL}};

}  // namespace vidx
