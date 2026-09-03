/*****************************************************************************

Copyright (c) 2013, 2020, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/
/**
 * @file komodo0xlog0recovery.h
 * @brief Consensus recovery class for Komodo Lizard system.
 *
 * This file defines the consensus recovery class used for
 * recovering transactions and log state in the Komodo Lizard system,
 * including support for consensus protocol and XA transactions.
 *
 */
#ifndef KOMODO_XLOG_RECOVERY_H
#define KOMODO_XLOG_RECOVERY_H

#include <memory>
#include "sql/binlog/recovery.h"
#include "sql/consensus_recovery_manager.h"
#include "sql/log_event.h"
#include "sql/xa/recovery.h"
namespace komodo {

/**
 * @class Consensus_xlog_recovery
 * @brief Implements recovery logic for consensus protocol.
 *
 * This class extends Binlog_recovery to provide recovery mechanisms
 * for consensus-based replication, including handling of consensus log events,
 * XA transactions, and integration with the consensus recovery manager.
 *
 * Responsibilities include:
 * - Processing consensus log events and maintaining recovery state.
 * - Managing recovery of both internal and external XA transactions.
 * - Tracking and updating consensus log indexes and terms during recovery.
 * - Coordinating with Consensus_recovery_manager for transaction state.
 */
class Consensus_xlog_recovery : public binlog::Binlog_recovery {
 public:
  explicit Consensus_xlog_recovery(
      bool is_logger, Binlog_file_reader *binlog_file_reader = nullptr)
      : binlog::Binlog_recovery(binlog_file_reader),
        m_current_index(0),
        m_valid_index(0),
        m_current_term(0),
        m_current_length(0),
        m_current_flag(0),
        m_rci_ev(nullptr),
        m_start_pos(BIN_LOG_HEADER_SIZE),
        m_end_pos(m_start_pos),
        m_recover_term(0),
        m_log_content(),
        m_begin_consensus(false),
        m_blob_index_list(),
        m_blob_term_list(),
        m_blob_flag_list(),
        m_gtid(),
        m_ev_start_pos(0),
        m_query_ev(nullptr),
        m_is_logger(is_logger) {
    m_recovery_manager.init();
  }
  ~Consensus_xlog_recovery() { m_recovery_manager.cleanup(); }

  /**
   * @brief Performs the recovery process for consensus-based replication.
   *
   * This function overrides the base class method to execute the recovery
   * logic specific to consensus protocol, including log event processing,
   * transaction state management, and coordination with the consensus
   * recovery manager.
   *
   * @return Reference to the Binlog_recovery object after recovery.
   */
  Binlog_recovery &recover() override;

  /**
   * @brief Gets the pointer to the consensus recovery manager.
   *
   * @return Pointer to the Consensus_recovery_manager instance.
   */
  Consensus_recovery_manager *get_recovery_manager() {
    return &m_recovery_manager;
  }

  /**
   * @brief Adds an internal XA transaction to the list of pending recoveries.
   *
   * Used for transactions coordinated internally by the system.
   *
   * @param ht Reference to the storage engine handler.
   * @param xa_trx Pointer to the XA transaction to be recovered.
   * @param xa_spec XA specification details for the transaction.
   */
  void add_internal_pending_recovering_trx(
      handlerton &ht, const XA_recover_txn *xa_trx,
      const komodo::XA_specification &xa_spec) override;

  /**
   * @brief Adds an external XA transaction to the list of pending recoveries.
   *
   * Used for transactions coordinated externally.
   *
   * @param ht Reference to the storage engine handler.
   * @param current_state Current recovery state of the XA transaction.
   * @param next_state Next recovery state to transition to.
   * @param xa_trx Pointer to the XA transaction to be recovered.
   * @param xa_spec XA specification details for the transaction.
   */
  void add_external_pending_recovering_trx(
      handlerton &ht, enum_ha_recover_xa_state current_state,
      enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
      const komodo::XA_specification &xa_spec) override;

   //TODO::comment
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
  void recover_one_internal_trx(
      xarecover_st const &info, handlerton &ht, XA_recover_txn const &xa_trx,
      my_xid xid,
      recovery_statistics &stats) override;
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
  void recover_one_external_trx(
      xarecover_st const &info, handlerton &ht, XA_recover_txn const &xa_trx,
      recovery_statistics &stats) override;

  /**
   * @brief Recovers transactions that are in the PREPARED_IN_TC state.
   *
   * Ensures that prepared transactions are correctly handled during recovery.
   *
   * @param ht Pointer to the storage engine handler.
   * @param info Pointer to the transaction coordinator state.
   * @return True if recovery was successful, false otherwise.
   */
  bool recover_prepared_in_tc(handlerton *ht, xarecover_st *info) override;

  /**
   * @brief Gets the last leader's consensus term index.
   *
   * @return The last leader's consensus term index.
   */
  uint64 get_last_leader_term_index() override;

  /**
   * @brief Sets the last leader's consensus term index.
   *
   * @param index The consensus term index to set.
   */
  void set_last_leader_term_index(uint64 index) override;

 private:
  /**
   * @brief Processes a Query_log_event during recovery.
   *
   * Handles the logic for query events encountered in the binlog.
   *
   * @param ev Reference to the Query_log_event.
   */
  void process_query_event(Query_log_event const &ev) override;

  /**
   * @brief Processes an XA COMMIT statement during recovery.
   *
   * @param query The XA COMMIT query string.
   */
  void process_xa_commit(const std::string &query) override;

  /**
   * @brief Processes an XA ROLLBACK statement during recovery.
   *
   * @param query The XA ROLLBACK query string.
   */
  void process_xa_rollback(const std::string &query) override;

  /**
   * @brief Processes an atomic DDL event during recovery.
   *
   * @param ev Reference to the Query_log_event representing the DDL.
   */
  void process_atomic_ddl(Query_log_event const &ev) override;

  /**
   * @brief Processes an XID event during recovery.
   *
   * @param ev Reference to the Xid_log_event.
   */
  void process_xid_event(Xid_log_event const &ev) override;

  /**
   * @brief Processes an XA PREPARE event during recovery.
   *
   * @param ev Reference to the XA_prepare_log_event.
   */
  void process_xa_prepare_event(XA_prepare_log_event const &ev) override;

  /**
   * @brief Processes a consensus log event.
   *
   * Handles the logic for consensus protocol events in the binlog.
   *
   * @param ev Reference to the Consensus_log_event.
   */
  void process_consensus_event(const Consensus_log_event &ev);

  /**
   * @brief Processes a previous consensus index log event.
   *
   * Handles the logic for previous consensus index events in the binlog.
   *
   * @param ev Reference to the Previous_consensus_index_log_event.
   */
  void process_previous_consensus_index_event(
      const Previous_consensus_index_log_event &ev);

  /**
   * @brief Processes an internal XID during recovery.
   *
   * @param unmasked_server_id The server ID.
   * @param xid The internal transaction ID.
   */
  void process_internal_xid(ulong unmasked_server_id, my_xid xid);

  /**
   * @brief Processes an external XID during recovery.
   *
   * @param unmasked_server_id The server ID.
   * @param xid The external transaction ID.
   * @param state The recovery state of the XA transaction.
   */
  void process_external_xid(ulong unmasked_server_id, const XID &xid,
                            enum_ha_recover_xa_state state);

 private:
  // Current consensus log index being processed.
  uint64 m_current_index;

  // Last valid consensus log index after recovery.
  uint64 m_valid_index;

  // Current consensus term being processed.
  uint64 m_current_term;

  // Length of the current consensus log event.
  uint64 m_current_length;

  // Flags for the current consensus log event.
  uint m_current_flag;

  // Pointer to the current Consensus_cluster_info_log_event, if any.
  Consensus_cluster_info_log_event *m_rci_ev;

  // Start position in the binlog for the current event.
  uint64 m_start_pos;

  // End position in the binlog for the current event.
  uint64 m_end_pos;

  // Term used for recovery, updated when a new term is encountered.
  uint64 m_recover_term;

  // Buffer for the content of the current log event.
  std::string m_log_content;

  // Indicates if a consensus log event is being processed.
  bool m_begin_consensus;

  // List of blob log indexes for large events.
  std::vector<uint64> m_blob_index_list;

  // List of blob log terms for large events.
  std::vector<uint64> m_blob_term_list;

  // List of blob log flags for large events.
  std::vector<uint64> m_blob_flag_list;

  // GTID collected for recovery and binlog application.
  Gtid m_gtid;

  // Start position of the current event in the binlog.
  my_off_t m_ev_start_pos;

  // Pointer to the current Query_log_event being processed.
  const Query_log_event *m_query_ev;

  //TODO::combine Consensus_recovery_manager with Consensus_xlog_recovery
  // Consensus recovery manager for managing transaction and log state during
  // recovery.
  Consensus_recovery_manager m_recovery_manager;

  // Indicates if this node is a logger node.
  bool m_is_logger;
};
}  // namespace komodo

#endif  // KOMODO_XLOG_RECOVERY_H
