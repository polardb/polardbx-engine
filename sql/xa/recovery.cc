/* Copyright (c) 2022, Oracle and/or its affiliates. Copyright (c) 2023, 2024, Alibaba and/or its affiliates.

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

#include "sql/xa/recovery.h"

#include "my_loglevel.h"
#include "mysql/components/services/log_builtins.h"
#include "sql/handler.h"
#include "sql/mysqld.h"
#include "sql/komodo0xlog.h"

namespace xa {
namespace recovery {

bool recover_prepared_in_tc_one_ht(THD *, plugin_ref plugin,
                                                 void *arg) {
  handlerton *ht = plugin_data<handlerton *>(plugin);
  xarecover_st *info = static_cast<struct xarecover_st *>(arg);

  if (ht->state == SHOW_OPTION_YES && ht->recover_prepared_in_tc) {
    if (info->recovery->recover_prepared_in_tc(ht, info)) return true;
  }
  return false;
}

bool recover_one_ht(THD *, plugin_ref plugin, void *arg) {
  handlerton *ht = plugin_data<handlerton *>(plugin);
  xarecover_st *info = static_cast<struct xarecover_st *>(arg);
  int got;

  if (ht->state == SHOW_OPTION_YES && ht->recover) {
    recovery_statistics external_stats{{0, 0, 0}, {0, 0, 0}};
    recovery_statistics internal_stats{{0, 0, 0}, {0, 0, 0}};
    while (
        (got = ht->recover(
             ht, info->list, info->len,
             Recovered_xa_transactions::instance().get_allocated_memroot())) >
        0) {
      assert(got <= info->len);
      LogErr(INFORMATION_LEVEL, ER_XA_RECOVER_FOUND_TRX_IN_SE, got,
             ha_resolve_storage_engine_name(ht));

      for (int i = 0; i < got; ++i) {
        auto &xa_trx = info->list[i];
        my_xid xid = xa_trx.id.get_my_xid();

        if (!xid) {  // Externally coordinated transaction
          info->recovery->recover_one_external_trx(*info, *ht, xa_trx, external_stats);
          ++info->found_foreign_xids;
          continue;
        }

        if (info->dry_run) {  // No information provided w.r.t TC state so,
                              // nothing to do in regards to internally
                              // coordinated transactions
          ++info->found_my_xids;
          continue;
        }

        // Internally coordinated transaction
        info->recovery->recover_one_internal_trx(*info, *ht, xa_trx, xid, internal_stats);
      }
      if (got < info->len) break;
    }
    bool has_failures =
        xa::recovery::has_failures(internal_stats) || xa::recovery::has_failures(external_stats);
    LogErr(has_failures ? ERROR_LEVEL : SYSTEM_LEVEL,
           ER_BINLOG_CRASH_RECOVERY_ENGINE_RESULTS,
           ha_resolve_storage_engine_name(ht),
           print_stats(internal_stats, external_stats).data());
    DBUG_EXECUTE_IF("xa_recovery_error_reporting", return has_failures;);
  }
  return false;
}

void recover_one_internal_trx(xarecover_st const &info, handlerton &ht,
                              XA_recover_txn const &xa_trx, my_xid xid,
                              recovery_statistics &stats) {
  /** Prepare xa spec if have. */
  komodo::XA_specification *xa_spec =
      (info.spec_list ? info.spec_list->find(xid) : nullptr);

  if (info.commit_list ? info.commit_list->count(xid) != 0
                       : tc_heuristic_recover == TC_HEURISTIC_RECOVER_COMMIT) {
    enum xa_status_code exec_status;
    if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
      exec_status = generate_xa_recovery_error();
    } else {
      exec_status = ht.commit_by_xid(&ht, const_cast<XID *>(&xa_trx.id), xa_spec);
    }

    if (exec_status == XA_OK)
      add_to_stats<STATS_SUCCESS, STATS_COMMITTED>(stats);
    else {
      add_to_stats<STATS_FAILURE, STATS_COMMITTED>(stats);
      report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_COMMIT_FAILED, xid,
                                  ht, exec_status);
    }
  } else {
    /** Internal trx never own xa spec since we didn't found xid.*/
    assert(xa_spec == nullptr);

    enum xa_status_code exec_status;
    if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false))
      exec_status = generate_xa_recovery_error();
    else
      exec_status =
          ht.rollback_by_xid(&ht, const_cast<XID *>(&xa_trx.id), xa_spec);

    if (exec_status == XA_OK)
      add_to_stats<STATS_SUCCESS, STATS_ROLLEDBACK>(stats);
    else {
      add_to_stats<STATS_FAILURE, STATS_ROLLEDBACK>(stats);
      report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_ROLLBACK_FAILED, xid,
                                  ht, exec_status);
    }
  }
}

void recover_one_external_trx(xarecover_st const &info, handlerton &ht,
                              XA_recover_txn const &xa_trx,
                              recovery_statistics &stats) {
  auto state{enum_ha_recover_xa_state::NOT_FOUND};

  if (info.xa_list != nullptr) {
    state = info.xa_list->find(xa_trx.id);
  }

  /** Prepare xa spec if have. */
  komodo::XA_specification *xa_spec =
      (info.spec_list ? info.spec_list->find(xa_trx.id) : nullptr);

  switch (state) {
    case enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE:
    case enum_ha_recover_xa_state::COMMITTED: {
      if (ht.commit_by_xid != nullptr) {
        enum xa_status_code exec_status;
        if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
          exec_status = generate_xa_recovery_error();
        } else {
          exec_status =
              ht.commit_by_xid(&ht, const_cast<XID *>(&xa_trx.id), xa_spec);
        }

        if (exec_status == XA_OK) {
          add_to_stats<STATS_SUCCESS, STATS_COMMITTED>(stats);
          break;
        } else
          report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_COMMIT_FAILED,
                                      xa_trx.id, ht, exec_status,
                                      /*is_xa*/ true);
      }
      add_to_stats<STATS_FAILURE, STATS_COMMITTED>(stats);
      break;
    }
    case enum_ha_recover_xa_state::NOT_FOUND:
    case enum_ha_recover_xa_state::PREPARED_IN_SE:
    case enum_ha_recover_xa_state::ROLLEDBACK: {
      if (ht.rollback_by_xid != nullptr) {
        enum xa_status_code exec_status;
        if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
          exec_status = generate_xa_recovery_error();
        } else {
          exec_status =
              ht.rollback_by_xid(&ht, const_cast<XID *>(&xa_trx.id), xa_spec);
        }
        if (exec_status == XA_OK) {
          add_to_stats<STATS_SUCCESS, STATS_ROLLEDBACK>(stats);
          break;
        } else
          report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_ROLLBACK_FAILED,
                                      xa_trx.id, ht, exec_status,
                                      /*is_xa*/ true);
      }
      add_to_stats<STATS_FAILURE, STATS_ROLLEDBACK>(stats);
      break;
    }
    case enum_ha_recover_xa_state::PREPARED_IN_TC: {
      if (!Recovered_xa_transactions::instance().add_prepared_xa_transaction(
              &xa_trx)) {
        if (ht.set_prepared_in_tc_by_xid != nullptr) {
          enum xa_status_code exec_status;
          if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
            exec_status = generate_xa_recovery_error();
          } else {
            exec_status = ht.set_prepared_in_tc_by_xid(
                &ht, const_cast<XID *>(&xa_trx.id), xa_spec);
          }

          if (exec_status == XA_OK) {
            add_to_stats<STATS_SUCCESS, STATS_PREPARED>(stats);
            break;
          } else
            report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_PREPARE_FAILED,
                                        xa_trx.id, ht, exec_status,
                                        /*is_xa*/ true);
        }
      }
      add_to_stats<STATS_FAILURE, STATS_PREPARED>(stats);
      break;
    }
  }
}

bool has_failures(recovery_statistics const &stats) {
  auto &failure = std::get<STATS_FAILURE>(stats);
  auto [fail_committed, fail_rolledback, fail_prepared] = failure;
  return fail_committed + fail_rolledback + fail_prepared != 0;
}

std::string print_stats(recovery_statistics const &internal_stats,
                        recovery_statistics const &external_stats) {
  std::ostringstream oss;
  auto has_metrics =
      print_stat(internal_stats, "internal transaction(s)", oss);
  has_metrics =
      print_stat(external_stats, "XA transaction(s)", oss) || has_metrics;
  if (!has_metrics)
    oss << "No attempts to commit, rollback or prepare any transactions."
        << std::flush;
  return oss.str();
}

bool print_stat(recovery_statistics const &stats, std::string const &trx_type,
                std::ostringstream &oss) {
  bool has_metrics{false};
  auto &[failure, success] = stats;

  auto [s_committed, s_rolledback, s_prepared] = success;
  if ((s_committed + s_rolledback + s_prepared) != 0) {
    has_metrics = true;
    oss << "Successfully" << std::flush;

    if (s_committed != 0) oss << " committed " << s_committed;
    if (s_rolledback != 0)
      oss << (s_committed != 0 ? "," : "") << " rolled back " << s_rolledback;
    if (s_prepared != 0)
      oss << (s_committed + s_rolledback != 0 ? "," : "") << " prepared "
          << s_prepared;

    oss << " " << trx_type << ". " << std::flush;
  }

  auto [f_committed, f_rolledback, f_prepared] = failure;
  if ((f_committed + f_rolledback + f_prepared) != 0) {
    has_metrics = true;
    oss << "Failed to" << std::flush;

    if (f_committed != 0) oss << " commit " << f_committed;
    if (f_rolledback != 0)
      oss << (f_committed != 0 ? "," : "") << " rollback " << f_rolledback;
    if (f_prepared != 0)
      oss << (f_committed + f_rolledback != 0 ? "," : "") << " prepare "
          << f_prepared;

    oss << " " << trx_type << "." << std::flush;
  }

  return has_metrics;
}

template <typename ID>
void report_trx_recovery_error(int error, ID const &xid, handlerton const &ht,
                               enum xa_status_code failure_code, bool is_xa) {
  assert(error == ER_BINLOG_CRASH_RECOVERY_COMMIT_FAILED ||
         error == ER_BINLOG_CRASH_RECOVERY_ROLLBACK_FAILED ||
         error == ER_BINLOG_CRASH_RECOVERY_PREPARE_FAILED);

  std::ostringstream oss;
  oss << (is_xa ? "XA " : "") << "transaction " << xid << std::flush;

  std::string failure;
  switch (failure_code) {
    case XAER_ASYNC: {
      failure.assign("XAER_ASYNC");
      break;
    }
    case XAER_RMERR: {
      failure.assign("XAER_RMERR");
      break;
    }
    case XAER_NOTA: {
      failure.assign("XAER_NOTA");
      break;
    }
    case XAER_INVAL: {
      failure.assign("XAER_INVAL");
      break;
    }
    case XAER_PROTO: {
      failure.assign("XAER_PROTO");
      break;
    }
    case XAER_RMFAIL: {
      failure.assign("XAER_RMFAIL");
      break;
    }
    case XAER_DUPID: {
      failure.assign("XAER_DUPID");
      break;
    }
    case XAER_OUTSIDE: {
      failure.assign("XAER_OUTSIDE");
      break;
    }
    case XA_OK: {
      assert(false);
      break;
    }
  }
  LogErr(INFORMATION_LEVEL, error, oss.str().data(),
         ha_resolve_storage_engine_name(&ht), failure.data());
}

enum xa_status_code generate_xa_recovery_error() {
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_async", return XAER_ASYNC;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_rmerr", return XAER_RMERR;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_nota", return XAER_NOTA;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_inval", return XAER_INVAL;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_proto", return XAER_PROTO;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_rmfail", return XAER_RMFAIL;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_dupid", return XAER_DUPID;);
  DBUG_EXECUTE_IF("xa_recovery_error_xaer_outside", return XAER_OUTSIDE;);
  return XA_OK;
}
}
}  // namespace
