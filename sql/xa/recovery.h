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

#ifndef XA_RECOVERY_H_INCLUDED
#define XA_RECOVERY_H_INCLUDED

#include "sql/handler.h"  // XA_recover_txn
#include "sql/komodo0handler.h"
#include "sql/sql_class.h"  // THD
#include "sql/xa.h"         // Xid_commit_list, Xa_state_list, XID, ...
#include "sql/binlog/recovery.h"

namespace xa {
namespace recovery {
/**
  Callback to be invoked by `ha_recover` over each storage engine plugin.

  1. It invokes `handler::recover_prepared_in_tc(XA_recover_txn[])`
     callback from the plugin interface, in order to retrieve the list of
     transactions that are, at the moment, in prepared in TC state within
     the storage engine.
  2. For all externally coordinated transactions that are in prepared in TC
     state, adds each to the list of recoverable XA transactions
     `xarecover_st::xa_list` in `PREPARED` state.

  @param thd The THD session object to be used in recovering the
             transactions.
  @param plugin The plugin interface for the storage engine to recover the
                transactions for.
  @param arg A pointer to `struct xarecover_st` that hold the information
             about the transaction coordinator state.

  @return false if the recovery ended successfully, false otherwise.
 */
bool recover_prepared_in_tc_one_ht(THD *thd, plugin_ref plugin, void *arg);
/**
  Callback to be invoked by `ha_recover` over each storage engine plugin.

  1. It invokes `handler::recover(XA_recover_txn[])` callback from the
     plugin interface, in order to retrieve the list of transactions that
     are, at the moment, in prepared state within the storage engine.
  2. Invokes `recover_one_internal_trx` for all the retrieved transactions
     that are internally coordinated transactions.
  3. Invokes `recover_one_external_trx` for all the retrieved transactions
     that are externally coordinated transactions.

  @param thd The THD session object to be used in recovering the
             transactions.
  @param plugin The plugin interface for the storage engine to recover the
                transactions for.
  @param arg A pointer to `struct xarecover_st` that hold the information
             about the transaction coordinator state.

  @return false if the recovery ended successfully, false otherwise.
 */
bool recover_one_ht(THD *thd, plugin_ref plugin, void *arg);

/**
  Processes an internally coordinated transaction against the transaction
  coordinator internal state.

  Searches the TC information on committed transactions,
  `xarecover_st::commit_list` for the XID of the transaction:

  1. If found and the TC recovery heuristics is
     `TC_HEURISTIC_RECOVER_COMMIT`, commits the transaction in the storage
     engine.
  2. If not, rolls it back in the storage engine.

  @param info TC internal state w.r.t transaction state
  @param ht The plugin interface for the storage engine to recover the
            transaction for
  @param xa_trx The information about the transaction to be recovered
  @param xid The internal XID of the transaction to be recovered
  @param stats Repository of statistical information about transaction
               recovery success and failures
 */
void recover_one_internal_trx(xarecover_st const &info, handlerton &ht,
                              XA_recover_txn const &xa_trx, my_xid xid,
                              recovery_statistics &stats);
/**
  Processes an externally coordinated transaction against the transaction
  coordinator internal state.

  Searches the TC information on externally coordinated transactions,
  `xarecover_st::xa_list` for the XID of the transaction:

  1. If found, checks the state of the transaction:
     a. If the state is `COMMITTED`, the transaction is committed in the
        storage engine.
     b. If the state is `ROLLEDBACK`, the transaction is rolled back in the
        storage engine.
     c. If the state is `PREPARED`, the transaction is kept in prepared
        state, added to the list of recovered transactions, visible with
        `XA RECOVER` and storage engines are instructed to move the
        transaction to the `PREPARED_IN_TC` state.
  2. If not, rolls it back in the storage engine.

  @param info TC internal state w.r.t to transaction state
  @param ht The plugin interface for the storage engine to recover the
            transaction for
  @param xa_trx The information about the transaction to be recovered
  @param stats Repository of statistical information about transaction
               recovery success and failures
 */
void recover_one_external_trx(xarecover_st const &info, handlerton &ht,
                              XA_recover_txn const &xa_trx,
                              recovery_statistics &stats);
/**
  Changes the given stats object by adding 1 to the given counter `counter`
  in the tuple `state`.

  @param stats The statistics object to change the counter for.
 */
template <size_t state, size_t counter>
void add_to_stats(recovery_statistics &stats) {
  ++std::get<counter>(std::get<state>(stats));
}
/**
  Checks if there are any non-zero failure counters in the fiven statistics
  object.

  @param stats The statistics object to check for non-zero failure
               counters.

  @return true if any non-zero failure counters were found, false otherwise.
 */
bool has_failures(recovery_statistics const &stats);
/**
  Composes a string presenting the statistics for the given objects.

  @param internal_stats The object containing statistics for internally
                        coordinated transactions.
  @param external_stats The object containing statistics for externally
                        transactions.

  @return a string with the textual representation of the given statistics.
 */
std::string print_stats(recovery_statistics const &internal_stats,
                        recovery_statistics const &external_stats);
/**
  Prints to the given string stream the textual representation of the
  statistics stored in the given object.

  @param stats The object containing the statistics.
  @param trx_type A string containing the textual description of the type
                  of transaction the statistics refer to.
  @param oss The string stream to print the textual representation to.

  @return true if any non-zero counter was found and something was written
          to the string stream, false otherwise.
 */
bool print_stat(recovery_statistics const &stats, std::string const &trx_type,
                std::ostringstream &oss);
/**
  Logs a message to the error log about failure to commit, rollback or
  prepare a transaction. The severity level used is INFORMATION_LEVEL.

  @param error The error to be reported, one of
                ER_BINLOG_CRASH_RECOVERY_COMMIT_FAILED,
                ER_BINLOG_CRASH_RECOVERY_ROLLBACK_FAILED,
                ER_BINLOG_CRASH_RECOVERY_PREPARE_FAILED
  @param id The identifier of the transaction. A templated type is used to
             allow to print either the internal ID for internally
             coordinated transactions or the XID for externally coordinated
             transactions.
  @param ht The handlerton for the storage engine that failed to complete
            the action.
  @param failure_code The `xa_status_code` failure code returned by the
                      SE. If no `xa_status_code` is returned from SE, XA_OK
                      should passed to avoing adding the code to the
                      message.
  @param is_xa Whether or not the `id` refers to an XA transaction.
 */
template <typename ID>
void report_trx_recovery_error(int error, ID const &id, handlerton const &ht,
                               enum xa_status_code failure_code,
                               bool is_xa = false);
/**
  Returns an XA status code according to active debug symbols. If none of
  the targeted debug symbols are active, will return XA_OK.

  @return one of XAER_ASYNC, XAER_RMERR, XAER_NOTA, XAER_INVAL, XAER_PROTO,
          XAER_RMFAIL, XAER_DUPID, XAER_OUTSIDE if associated debug symbol
          is active, XA_OK otherwise.
 */
enum xa_status_code generate_xa_recovery_error();
}  // namespace recovery
}  // namespace xa
#endif  // XA_RECOVERY_H_INCLUDED
