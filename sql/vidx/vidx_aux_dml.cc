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

#include "vidx/vidx_aux_dml.h"

#include <optional>

#include "my_base.h"             // HA_ERR_GENERIC, HA_ERR_LOCK_DEADLOCK
#include "my_dbug.h"             // DBUG_EXECUTE_IF, DBUG_SUICIDE
#include "mysqld_error.h"        // ER_LOCK_DEADLOCK
#include "sql/field.h"           // Field
#include "sql/handler.h"         // handler
#include "sql/key.h"             // KEY
#include "sql/sql_class.h"       // THD
#include "sql/sql_lex.h"         // LEX
#include "sql/sql_parse.h"       // sql_command_flags, CF_AUTO_COMMIT_TRANS
#include "sql/table.h"           // TABLE
#include "sql/thd_raii.h"        // Implicit_substatement_state_guard
#include "sql/transaction.h"     // trans_commit_stmt, trans_rollback
#include "vidx/vidx_log.h"       // vidx::log_slave_dml_err
#include "vidx/vidx_stats.h"     // vidx::vidx_stats

namespace vidx {
namespace aux_dml {

bool force_non_indep = false;

bool update_can_skip(const TABLE *table, uint key) {
  const KEY *keyinfo = table->key_info + key;
  const Field *vec_field = keyinfo->key_part->field;
  const ptrdiff_t offset = table->record[1] - table->record[0];

  /* Vector content changed: full invalidate + insert required. */
  if (vec_field->cmp_binary_offset(static_cast<uint>(offset)) != 0)
    return false;

  /* TREF identity check.  vidx aux-table TREF is the InnoDB GEN_CLUST
     row reference, derived from the explicit PRIMARY KEY when present
     else from DB_ROW_ID (a hidden auto-increment).  DB_ROW_ID never
     changes across UPDATE, so a no-explicit-PK table can always skip
     when the vector is unchanged.  When the table has a UNIQUE NOT
     NULL key promoted to clustered (no "PRIMARY" name), TABLE_SHARE
     still flags primary_key < MAX_KEY, so use the semantic API rather
     than name comparison.  Cold path: UPDATE on PK columns is rare. */
  if (table->s->is_missing_primary_key()) return true;

  /* Explicit (or promoted) PK: check every PK field for change. */
  const KEY &pk = table->key_info[table->s->primary_key];
  for (uint i = 0; i < pk.user_defined_key_parts; i++) {
    const Field *pkf = pk.key_part[i].field;
    if (pkf->cmp_binary_offset(static_cast<uint>(offset)) != 0) return false;
  }
  return true;
}

bool need_independent_trx(THD *thd) {
  if (force_non_indep) return false;
  if (thd->lex && (sql_command_flags[thd->lex->sql_command] &
                   CF_AUTO_COMMIT_TRANS)) {
    return false;
  }
  /* lex == nullptr corresponds to bootstrap / SHOW / internal-maintenance
     contexts that do not issue vidx DML — the return value is therefore
     inconsequential.  Default to "indep" for future-proofing: an
     Attachable_trx_rw is self-contained, so an unexpected caller from
     such a context still gets the safer (committed-on-its-own) path. */
  return true;
}

namespace {

/* Bump the matching commit-failure counter.  Both the body-failure
   path (caller returned non-zero from inside the scope) and the
   trans_commit_* path (commit itself failed) call this so the trio
   {commit, commit_error, deadlock} is the single source of truth.
   @p e is the handler error code if known (for the body path), else
   0; we also peek at thd->stmt_da to pick up deadlocks raised inside
   trans_commit. */
void account_commit_failure(THD *thd, int e) {
  const bool is_deadlock =
      e == HA_ERR_LOCK_DEADLOCK ||
      (thd->get_stmt_da()->is_error() &&
       thd->get_stmt_da()->mysql_errno() == ER_LOCK_DEADLOCK);
  if (is_deadlock) {
    vidx::vidx_stats.indep_dml_deadlock++;
  } else {
    vidx::vidx_stats.indep_dml_commit_error++;
  }
}

/* Translate the SQL-layer error currently stashed in thd->stmt_da
   into a handler-layer error code, so commit() can return the most
   informative non-zero code instead of a flat HA_ERR_GENERIC.  Only
   the cases that matter for slave-side retry / upper-layer routing
   are mapped explicitly; everything else falls back to HA_ERR_GENERIC
   to preserve the prior wire behaviour.

   Why we DO NOT map ER_DUP_ENTRY: HA_ERR_FOUND_DUPP_KEY would
   surface back into mhnsw_insert / hlindexes_on_insert and bubble
   up through TABLE handler routing.  At the user level, INSERT
   ... ON DUPLICATE KEY UPDATE / INSERT IGNORE both inspect that
   handler error and consult get_dup_key() / dup_ref to rewrite
   the INSERT into an UPDATE — that machinery would aim at the
   vidx auxiliary table whose key_nr is meaningless to the user
   query, risking either an assert or silently choosing the wrong
   row.  A dup-key surfacing during trans_commit on the inner
   attachable trx is nearly impossible (InnoDB caught it during
   ha_write_row inside the body, where it was already counted in
   save_retry_dup_key); folding the residual case into
   HA_ERR_GENERIC is the safer default. */
int commit_da_to_ha_err(THD *thd) {
  if (!thd->get_stmt_da()->is_error()) return HA_ERR_GENERIC;
  switch (thd->get_stmt_da()->mysql_errno()) {
    case ER_LOCK_DEADLOCK:        return HA_ERR_LOCK_DEADLOCK;
    case ER_LOCK_WAIT_TIMEOUT:    return HA_ERR_LOCK_WAIT_TIMEOUT;
    default:                      return HA_ERR_GENERIC;
  }
}

/* Mirror Update_I_S_statistics_ctx (sql/dd/info_schema/table_stats.cc)
   and Reload_sequence_cache_ctx: trans_rollback_stmt + trans_rollback
   inside the attachable scope rolls back the inner trx; the caller's
   end_attachable_transaction (in dtor) then restores the outer state. */
int rollback_and_log(THD *thd, TABLE *table, const char *op, int e) {
  trans_rollback_stmt(thd);
  trans_rollback(thd);
  account_commit_failure(thd, e);
  vidx::log_slave_dml_err(table, "Hlindex_dml_scope", op, e);
  return e;
}

}  // anonymous namespace

class Hlindex_dml_scope::Impl {
 public:
  Impl(TABLE *table, uint key_idx, bool indep)
      : table_(table), thd_(table->in_use), indep_(indep) {
    assert(table_->hlindex_active != nullptr);

    if (indep_) {
      assert(!thd_->is_attachable_rw_transaction_active());

      /* save/clear in_sub_stmt.  trans_commit_stmt bails out when
         in_sub_stmt > 0.  vidx DML reaches here from triggers / SPs /
         sub-queries.  Mirrors Reload_sequence_cache_ctx. */
      saved_in_sub_stmt_ = thd_->in_sub_stmt;
      thd_->in_sub_stmt = 0;

      /* Construct Implicit_substatement_state_guard BEFORE the attachable
         so LIFO order keeps it alive across ~Attachable_trx.  Without it
         our trans_commit would consume the outer thd's gtid_next and
         trigger commit-order wait — on slave applier this surfaces as
         ER_GTID_NEXT_TYPE_UNDEFINED_GTID (MY-001837). */
      substatement_guard_.emplace(thd_);

      thd_->begin_attachable_rw_transaction();
      attachable_open_ = true;

      /* Clear hlindex->in_use so the hlindex_lock below allocates a fresh
         InnoDB trx_t (attachable cleared thd's ha_data). */
      table_->hlindex_active->in_use = nullptr;

      DBUG_EXECUTE_IF("vidx_indep_dml_fail_after_begin_attach",
                      init_err_ = HA_ERR_GENERIC; return;);
    }

    /* hlindex_lock binds prebuilt->trx to the current thd's trx.  On the
       indep path that's the attachable's inner trx (so the F_UNLCK at
       commit time tears down only the attachable side).  On non-indep
       it binds to the user trx and stays held to statement end as
       before (the hlindex_lock impl is idempotent — checks in_use). */
    if (int e = table_->hlindex_lock(key_idx)) {
      init_err_ = e;
      vidx::log_slave_dml_err(table_, "Hlindex_dml_scope", "hlindex_lock", e);
      return;
    }
    /* Track lock acquisition: commit() / dtor must F_UNLCK iff this is
       true.  ha_external_lock(F_UNLCK) is NOT a no-op on a never-locked
       handler — update_thd() rebinds prebuilt->trx and end_stmt()
       decrements in_depth, which asserts when called against a trx
       that was never begin_stmt()'d. */
    hlindex_locked_ = true;

    /* Inside attachable, hlindex inherits base's pos_in_table_list which
       triggers readonly_flag_check (handler.cc) under LOCK TABLES WRITE
       and the LTM-mode bypasses don't apply.  Outside attachable the
       caller's trx already passed the SQL-layer check so leave it
       intact.  hlindex is internal auxiliary, so bypassing readonly
       within the attachable scope is safe. */
    if (indep_) table_->hlindex_active->pos_in_table_list = nullptr;

    /* fail_before_load_start fires AFTER hlindex_lock (mirrors the
       original injection point right before load_entry_point_node).
       commit() must therefore F_UNLCK on the init_err path when
       hlindex_locked_ is true — see commit() below. */
    DBUG_EXECUTE_IF("vidx_indep_dml_fail_before_load_start",
                    init_err_ = HA_ERR_GENERIC;);
  }

  ~Impl() {
    if (!indep_) return;
    if (!committed_ && attachable_open_) {
      /* Caller returned without commit(), or threw.  Best-effort
         cleanup with diagnostic log + commit-error counter bump so
         ops can spot missed commits via SHOW STATUS as well as
         error.log.  Account before logging so the counter delta and
         the log entry stay paired in failure post-mortems.  F_UNLCK
         only when hlindex_lock had succeeded — see hlindex_locked_
         comment above. */
      if (hlindex_locked_) {
        table_->hlindex_active->file->ha_external_lock(thd_, F_UNLCK);
        table_->hlindex_active->in_use = nullptr;
      }
      trans_rollback_stmt(thd_);
      trans_rollback(thd_);
      account_commit_failure(thd_, 0);
      vidx::log_slave_dml_err(table_, "Hlindex_dml_scope",
                              "uncommitted_dtor_rollback", 0);
    }
    if (attachable_open_) thd_->end_attachable_transaction();
    thd_->in_sub_stmt = saved_in_sub_stmt_;
  }

  [[nodiscard]] int init_err() const { return init_err_; }

  int commit(int err) {
    if (!indep_) return err;  /* no-op */
    assert(!committed_);
    committed_ = true;

    /* Caller contract: if init_err_ != 0 the caller MUST have
       short-circuited via the `if (int e = scope.init_err()) return
       scope.commit(e);` idiom (passing init_err_ back as @p err).
       Asserting here catches any future caller that forgets to
       check init_err_ — without the assert, DBUG-injected init
       failures would silently shadow a different real error code
       passed into err. */
    assert(!init_err_ || err == init_err_);

    /* F_UNLCK iff hlindex_lock had succeeded — ha_external_lock(F_UNLCK)
       is NOT a no-op on a never-locked InnoDB handler (it asserts
       in_depth > 0).  This branch covers both:
         - normal path: body ran, lock held, unlock + commit/rollback;
         - init_err path with fail_before_load_start: lock was taken
           inside ctor before init_err was set, must be released here. */
    int unlock_err = 0;
    if (hlindex_locked_) {
      unlock_err = table_->hlindex_active->file->ha_external_lock(thd_, F_UNLCK);
      table_->hlindex_active->in_use = nullptr;
      /* Confirm m_lock_type was demoted: cascade-unlock (handler.cc)
         gates on get_lock_type() != F_UNLCK; leaving F_WRLCK here would
         route a later cascade through with a stale @p thd. */
      assert(unlock_err != 0 ||
             table_->hlindex_active->file->get_lock_type() == F_UNLCK);
    }

    /* Short-circuit when the caller signalled init failure: roll back
       only, never commit.  F_UNLCK above already released the lock if
       it was held. */
    if (init_err_) {
      return rollback_and_log(thd_, table_, "init_err(debug)", err);
    }

    if (err != 0 || unlock_err != 0) {
      if (err == 0) err = unlock_err;
      return rollback_and_log(
          thd_, table_,
          unlock_err ? "ha_external_lock(F_UNLCK)" : "body", err);
    }

    DBUG_EXECUTE_IF("vidx_indep_dml_crash_before_commit", DBUG_SUICIDE(););
    DBUG_EXECUTE_IF("vidx_indep_dml_fail_before_commit",
                    return rollback_and_log(thd_, table_,
                                            "fail_before_commit(debug)",
                                            HA_ERR_GENERIC););

    /* ignore_global_read_lock=true: hlindex is internal auxiliary.
       Do NOT call trans_rollback on commit failure — ha_commit_trans
       already rolls back the SE side; double-rollback would disturb
       outer trx state.  Mirrors store_statistics_record.  Sequential
       trans_commit_stmt + trans_commit (NOT 2PC — this is just the
       SQL-layer stmt vs trx accounting separation) finalizes the
       attachable inner trx. */
    if (trans_commit_stmt(thd_, true) || trans_commit(thd_, true)) {
      const int ha_err = commit_da_to_ha_err(thd_);
      account_commit_failure(thd_, ha_err);  // covers deadlock peek too
      vidx::log_slave_dml_err(table_, "Hlindex_dml_scope::commit",
                              "trans_commit", ha_err);
      return ha_err;
    }
    vidx::vidx_stats.indep_dml_commit++;
    DBUG_EXECUTE_IF("vidx_indep_dml_crash_after_commit", DBUG_SUICIDE(););
    return 0;
  }

 private:
  TABLE *const table_;
  THD *const thd_;
  const bool indep_;
  bool attachable_open_{false};
  bool hlindex_locked_{false};
  bool committed_{false};
  uint saved_in_sub_stmt_{0};
  int init_err_{0};
  /* substatement_guard_ is declared last so it is destroyed first among
     members (C++ reverse declaration order).  All members outlive the
     dtor body by language rule, so the guard is alive throughout
     ~Impl(); the "last" placement is a defensive convention that keeps
     it destroyed before any future member with a non-trivial dtor.
     std::optional gives delayed construction in the indep branch
     without paying the cost on the non-indep path. */
  std::optional<Implicit_substatement_state_guard> substatement_guard_;
};

Hlindex_dml_scope::Hlindex_dml_scope(TABLE *table, uint key_idx, bool indep)
    : impl_(std::make_unique<Impl>(table, key_idx, indep)) {}

Hlindex_dml_scope::~Hlindex_dml_scope() = default;

int Hlindex_dml_scope::init_err() const { return impl_->init_err(); }

int Hlindex_dml_scope::commit(int err) { return impl_->commit(err); }

}  // namespace aux_dml
}  // namespace vidx
