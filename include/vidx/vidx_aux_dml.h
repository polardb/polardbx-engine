#ifndef VIDX_AUX_DML_INCLUDED
#define VIDX_AUX_DML_INCLUDED

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

#include <memory>

#include "my_inttypes.h"  // uint

class THD;
struct TABLE;

namespace vidx {
namespace aux_dml {

/*
  Routing rule for auxiliary-table DML: independent-trx path or not?

  Returns true  → Attachable_trx_rw per-row commit (Hlindex_dml_scope).
  Returns false → run inside the caller's transaction.

  Decision:
    1. force_non_indep == ON          → false (escape hatch for DML).
    2. CF_AUTO_COMMIT_TRANS is set    → false (DDL).
    3. Otherwise                      → true  (default DML).

  See vidx_ddl_non_indep_path.test (default matrix) and
  vidx_force_non_indep_switch.test (escape-hatch matrix).
*/
bool need_independent_trx(THD *thd);

/*
  DML update skip-contract: return true when an UPDATE on @p table at
  vector key @p key need NOT touch the auxiliary index — both the
  vector content AND the TREF (the InnoDB GEN_CLUST row reference for
  the row, derived from the explicit PRIMARY KEY when present, else
  from DB_ROW_ID) are unchanged.

  Caller contract:
    - record[0] holds the NEW row image; record[1] holds the OLD.
    - Vector field is identified by key_info[key].key_part->field.
    - Only the "old non-NULL → new non-NULL" subcase is answered here.
      NULL transitions (NULL→non-NULL = INSERT, non-NULL→NULL =
      INVALIDATE, NULL→NULL = no-op) must be classified by the caller
      before this is invoked.

  Rationale: workloads like "UPDATE t SET status=... WHERE id=..."
  touch only non-vector columns and should not pay a full
  attachable + invalidate + insert cycle, which lengthens the base
  table's PRIMARY / secondary-index X-lock window and materially
  raises ABBA deadlock probability under concurrency.  See
  vidx_update_skip_contract.test for the full coverage matrix.
*/
bool update_can_skip(const TABLE *table, uint key);

/*
  Emergency restart-only escape hatch (PLUGIN_VAR_READONLY — set in
  my.cnf and restart mysqld).  Storage lives here in vidx_aux_dml.cc;
  MYSQL_SYSVAR_BOOL(force_non_indep, ...) registration stays in
  vidx_index.cc so all vidx sysvars are registered in one place.

  When ON (boot default OFF):
   - Plain DML AND row-driven DDL both routed through the legacy
     non-indep path (aux writes commit with the user / DDL trx).
   - Use only when a regression on the indep DML path needs a
     restart-time revert.

  The flag does NOT promote DDL to the indep path; the DDL branch is
  intentionally always non-indep (see need_independent_trx() docblock
  for the rationale).  ON only adds DML to that branch.
*/
extern bool force_non_indep;

/*
  RAII scope wrapping an Attachable_trx_rw used for vidx auxiliary-
  table DML.  Replaces the prior run_in_attachable_rw template by
  factoring the attachable boundary (in_sub_stmt save/clear, GTID
  guard, begin_attachable, hlindex re-bind, F_UNLCK, trans_commit_stmt
  + trans_commit / trans_rollback) into ctor / commit() / dtor so
  callers no longer need a lambda.

  CONTRACT — inside this scope, NO method may be called on table->file
  (BASE handler).  Any base-handler method call would silently rebind
  m_prebuilt->trx to the attachable trx via ha_innobase::update_thd();
  the outer trx's later operations on this handler would then run on a
  freed attachable trx pointer.  Caller MUST stage anything it needs
  from the base handler (e.g. table->file->position() to populate
  table->file->ref) BEFORE constructing the scope.  All aux-table writes
  MUST go through table->hlindex->file->*.

  hlindex_lock binding is taken inside the ctor (after begin_attachable
  on the indep path) so prebuilt->trx is bound to the attachable's
  inner trx, not the user trx.  Non-indep callers go through the same
  ctor but the lock then binds to the outer user trx (existing
  behavior).  On indep, the matching F_UNLCK is performed in commit();
  on non-indep, the lock persists to statement end as before.

  Usage:
    table->file->position(record);                // stage ref BEFORE scope
    Hlindex_dml_scope scope(table, key_idx, indep);
    if (int e = scope.init_err()) return scope.commit(e);
    err = mhnsw_xxx(table, key_info + key_idx);   // pure DML body
    return scope.commit(err);                     // commit OR rollback

  indep == false  : the scope is a no-op (no attachable, no boundary
                    work).  commit() returns the caller's err verbatim.
  indep == true   : full attachable scope.  commit() returns 0 on
                    success or HA_ERR_GENERIC on commit failure; on
                    err != 0 it rolls back and returns err.

  Atomicity contract with main XA
  -------------------------------
  An aux-table write made through Hlindex_dml_scope commits in its own
  Attachable_trx_rw and is therefore NOT atomic with the user / DDL
  transaction that drove it.  The two interesting failure modes:

    A. Outer trx rolls back AFTER inner commit succeeded
       — aux table holds a node for a base row the user trx ended up
         not writing.  Self-heals on the next INSERT / INVALIDATE that
         touches the same base row, and on the search side the orphan
         resolves on the next ANN probe via the tref-mismatch / not-
         found branches in mhnsw_invalidate (see
         invalidate_skip_not_found / invalidate_skip_tref_changed
         counters in vidx_stats.h).

    B. XA PREPARE succeeds, then crash, then recovery ROLLBACK
       — same shape as A but the orphan window can survive a restart.
         The recovery path runs lazy invalidate on the next user touch;
         vidx already tolerates this via the same self-heal logic.

  The trade-off:  routing aux DML inside the user trx (the legacy
  non-indep path, still reachable via force_non_indep) makes the two
  atomic but extends the base table's PRIMARY / secondary-index X-lock
  window through the entire HNSW graph maintenance, which surfaced as
  ABBA deadlocks against concurrent INSERT / UPDATE on the base table.
  See vidx_neighbor_save_high_conc.test for the deadlock-free property
  the indep path delivers.

  Operators investigating recovered orphans:  watch the SHOW STATUS
  counters Vidx_invalidate_skip_not_found and
  Vidx_invalidate_skip_tref_changed — a sustained delta there after
  crash recovery is the in-band signal that lazy invalidate is cleaning
  up orphans.
*/
class Hlindex_dml_scope {
 public:
  /* @p key_idx is the vector key index whose hlindex aux-table will be
     locked inside the ctor (after begin_attachable on the indep path). */
  Hlindex_dml_scope(TABLE *table, uint key_idx, bool indep);
  ~Hlindex_dml_scope();

  /* Non-zero when a DBUG injection point inside ctor requested an
     immediate failure (e.g. vidx_indep_dml_fail_after_begin_attach,
     vidx_indep_dml_fail_before_load_start).  Callers should check
     right after construction and short-circuit to commit():
       Hlindex_dml_scope scope(table, key_idx, indep);
       if (int e = scope.init_err()) return scope.commit(e);
     This preserves the legacy DBUG semantics where the inject points
     abort the body BEFORE it executes (so no aux-table writes happen
     even if rollback would have undone them anyway). */
  int init_err() const;

  /* Finalize the scope.  Pass the caller's running error code:
       err == 0   → commit the attachable (returns 0 on success,
                    HA_ERR_GENERIC on commit failure).
       err != 0   → rollback the attachable (returns err).
     In either case the scope is marked committed; the dtor becomes a
     no-op.  Must be called exactly once per active (indep) scope. */
  int commit(int err);

  Hlindex_dml_scope(const Hlindex_dml_scope &) = delete;
  Hlindex_dml_scope &operator=(const Hlindex_dml_scope &) = delete;

 private:
  class Impl;
  std::unique_ptr<Impl> impl_;
};

}  // namespace aux_dml
}  // namespace vidx

#endif  // VIDX_AUX_DML_INCLUDED
