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
#ifndef XPAXOS_RECOVERY_H
#define XPAXOS_RECOVERY_H

#include <memory>
#include "sql/binlog/recovery.h"
#include "sql/log_event.h"

namespace xp {

// Recovery_manager is a singleton class that manages the recovery process
// for the consensus protocol.
class Recovery_manager {
 public:
  static Recovery_manager &instance() {
    static Recovery_manager instance;
    return instance;
  }

  bool is_xpaxos_instance_recovering() const;

  bool is_xpaxos_instance_initializing() const;

  bool is_xpaxos_instance() const {
    // todo is xpaxos instance by default for now
    return true;
  }

  std::unique_ptr<binlog::Binlog_recovery> create_recovery(
      Binlog_file_reader &binlog_file_reader);
};

// Consensus_binlog_recovery is a subclass of Binlog_recovery that implements
// the recovery process for the consensus protocol.
class Consensus_binlog_recovery : public binlog::Binlog_recovery {
 public:
  explicit Consensus_binlog_recovery(Binlog_file_reader &binlog_file_reader)
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
        m_query_ev(nullptr) {}

  Binlog_recovery &recover() override;

 private:
  void process_query_event(Query_log_event const &ev) override;

  void process_xa_commit(const std::string &query) override;

  void process_xa_rollback(const std::string &query) override;

  void process_atomic_ddl(Query_log_event const &ev) override;

  void process_xid_event(Xid_log_event const &ev) override;

  void process_xa_prepare_event(XA_prepare_log_event const &ev) override;

  void process_consensus_event(const Consensus_log_event &ev);

  void process_previous_consensus_index_event(
      const Previous_consensus_index_log_event &ev);

  void process_internal_xid(ulong unmasked_server_id,my_xid xid);

  void process_external_xid(ulong unmasked_server_id,const XID &xid, enum_ha_recover_xa_state state);

 private:
  uint64 m_current_index;
  uint64 m_valid_index;
  uint64 m_current_term;
  uint64 m_current_length;
  uint m_current_flag;
  Consensus_cluster_info_log_event *m_rci_ev;
  uint64 m_start_pos;
  uint64 m_end_pos;
  uint64 m_recover_term;
  std::string m_log_content;
  bool m_begin_consensus;

  std::vector<uint64> m_blob_index_list;
  std::vector<uint64> m_blob_term_list;
  std::vector<uint64> m_blob_flag_list;

  // collect gtid and start pos for recovery apply binlog
  Gtid m_gtid;
  my_off_t m_ev_start_pos;

  const Query_log_event *m_query_ev;
};

}  // namespace xp

#endif  // XPAXOS_RECOVERY_H
