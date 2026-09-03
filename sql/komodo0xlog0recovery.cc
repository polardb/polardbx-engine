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
 * @file komodo0xlog0recovery.cc
 * @brief Consensus recovery class for Komodo Lizard system.
 *
 * This file defines the consensus recovery class used for
 * recovering transactions and log state in the Komodo Lizard system,
 * including support for consensus protocol and XA transactions.
 *
 */

#include <memory>

#include "libbinlogevents/include/binlog_event.h"
#include "my_sys.h"
#include "sql/binlog.h"
#include "sql/binlog/komodo0binlog0xa0specification.h"
#include "sql/binlog/komodo0recovery.h"
#include "sql/binlog/recovery.h"
#include "sql/binlog/tools/iterators.h"
#include "sql/consensus/consensus_err.h"
#include "sql/consensus_recovery_manager.h"
#include "sql/gcn_log_event.h"
#include "sql/handler.h"
#include "sql/komodo0consensus0xlog.h"
#include "sql/komodo0xlog.h"
#include "sql/komodo0xlog0recovery.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/raii/sentry.h"
#include "sql/system_variables.h"
#include "sql/xa/xid_extract.h"
#include "storage/innobase/include/ut0dbg.h"
#include "sql/xa/recovery.h"

#include "sql/consensus/komodo0fifo0cache.h"

using namespace xa::recovery;

namespace komodo {
binlog::Binlog_recovery &Consensus_xlog_recovery::recover() {
  binlog::tools::Iterator it{m_reader};
  it.set_copy_event_buffer();
  m_valid_pos = m_reader->position();
  int error = 0;

  this->process_format_event(*this->m_reader->format_description_event());

  for (Log_event *ev = it.begin(); ev != it.end() && !error; ev = it.next()) {
    if (ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT) {
      process_consensus_event(dynamic_cast<Consensus_log_event &>(*ev));
    } else if (ev->get_type_code() ==
               binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT) {
      process_previous_consensus_index_event(
          dynamic_cast<Previous_consensus_index_log_event &>(*ev));
    } else if (!ev->is_control_event()) {
      m_end_pos = my_b_tell(m_reader->get_io_cache());

      if (ev->get_type_code() == binary_log::CONSENSUS_CLUSTER_INFO_EVENT) {
        m_rci_ev = dynamic_cast<Consensus_cluster_info_log_event *>(ev);
      } else if (ev->get_type_code() == binary_log::QUERY_EVENT) {
        process_query_event(dynamic_cast<Query_log_event &>(*ev));
      } else if (ev->get_type_code() == binary_log::GCN_LOG_EVENT) {
        process_gcn_event(dynamic_cast<Gcn_log_event &>(*ev));
      } else if (ev->get_type_code() == binary_log::XA_PREPARE_LOG_EVENT) {
        process_xa_prepare_event(dynamic_cast<XA_prepare_log_event &>(*ev));
      } else if (ev->get_type_code() == binary_log::XID_EVENT) {
        process_xid_event(dynamic_cast<Xid_log_event &>(*ev));
      } else if (ev->get_type_code() == binary_log::GTID_LOG_EVENT) {
        process_gtid_event(dynamic_cast<Gtid_log_event &>(*ev));
      }

      // find a integrated consensus log
      if (m_begin_consensus && m_end_pos > m_start_pos &&
          m_end_pos - m_start_pos == m_current_length) {
        if (m_current_flag & Consensus_log_event_flag::FLAG_BLOB) {
          m_blob_index_list.push_back(m_current_index);
          m_blob_term_list.push_back(m_current_term);
          m_blob_flag_list.push_back(m_current_flag);
        } else if (m_current_flag & Consensus_log_event_flag::FLAG_BLOB_END) {
          m_blob_index_list.push_back(m_current_index);
          m_blob_term_list.push_back(m_current_term);
          m_blob_flag_list.push_back(m_current_flag);
          uint64 split_len = opt_consensus_large_event_split_size;
          uint64 blob_start_pos = m_start_pos;
          uint64 blob_end_pos =
              (m_blob_index_list.size() == 1 ? m_end_pos
                                             : (m_start_pos + split_len));
          assert(m_rci_ev == nullptr);
          for (size_t i = 0; i < m_blob_index_list.size(); ++i) {
            m_log_content.assign(ev->temp_buf + blob_start_pos - m_start_pos,
                                 blob_end_pos - blob_start_pos);
            xlog_engine_add_log_to_cache(
                m_blob_term_list[i], m_blob_index_list[i], m_log_content.size(),
                get_uchar_str(m_log_content), false, m_blob_flag_list[i]);
            blob_start_pos = blob_end_pos;
            blob_end_pos = blob_end_pos + split_len > m_end_pos
                               ? m_end_pos
                               : blob_end_pos + split_len;
          }
          m_blob_index_list.clear();
          m_blob_term_list.clear();
          m_blob_flag_list.clear();
          m_begin_consensus = false;
          if (!(m_current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX)) {
            m_valid_index = m_current_index;
          }
        } else {
          fetch_binlog_by_offset(*m_reader, m_start_pos, m_end_pos, m_rci_ev,
                                 m_log_content);
          // copy log to buffer
          xlog_engine_add_log_to_cache(
              m_current_term, m_current_index, m_log_content.size(),
              get_uchar_str(m_log_content), (m_rci_ev != nullptr),
              m_current_flag);
          m_begin_consensus = false;
          if (!(m_current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX)) {
            m_valid_index = m_current_index;
          }
        }
        m_rci_ev = nullptr;
      }
    }

    error = error || it.has_error() || m_reader->has_fatal_error();

    /*
      Rotate event (generated by current instance) at the end of last binlog
      file is not expected. If it's seen, this means binlog rotation failed,
      that is, only ROTATE_EVENT was logged, neither new file was not created
      or it was not registered in binlog index file, this event should be
      truncated.
    */
    if (!this->m_is_malformed && !this->m_in_transaction && !error &&
        !m_begin_consensus &&
        !(m_current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX) &&
        !komodo::is_b_events_before_gtid(ev) && !is_gtid_event(ev) &&
        !(is_rotate_event(ev) && ev->server_id == (uint32)::server_id) &&
        !is_session_control_event(ev)) {
      m_valid_pos = my_b_tell(m_reader->get_io_cache());
    }

    delete ev;
    ev = nullptr;

    if (error || this->m_is_malformed) break;
  }

  xp::system(ER_XP_RECOVERY)
      << "Consensus_xlog_recovery::recover end "
      << ", file_size " << m_reader->ifile()->length() << ", curr_position "
      << m_reader->position() << ", m_current_index " << m_current_index
      << ", m_start_pos " << m_start_pos << ", m_end_pos " << m_end_pos
      << ", m_valid_pos " << m_valid_pos << ", m_valid_index " << m_valid_index
      << ", m_is_malformed " << m_is_malformed << ", m_in_transaction "
      << m_in_transaction << ", m_failure_message " << m_failure_message
      << ", get_error_type  " << m_reader->get_error_type()
      << ", get_error_number " << it.get_error_number()
      << ", get_error_message " << it.get_error_message();

  if (m_start_pos < m_valid_pos && m_end_pos > m_valid_pos) {
    m_end_pos = m_valid_pos;
  }

  auto &log_manager = xlog_engine_get_log_manager();
  // recover current/sync index
  //
  // if the last log is not integrated
  if (m_valid_index != m_current_index) {
    xp::warn(ER_XP_RECOVERY)
        << "last consensus log is not integrated, "
        << "sync index should set to " << m_valid_index << " instead of "
        << m_current_index << ", with valid_pos " << m_valid_pos;
    // truncate cache
    log_manager.set_cache_index(m_valid_index);
    xlog_engine_get_cache_manager()->evict_newest_until(m_valid_index + 1);
  }
  log_manager.set_sync_index(m_valid_index);
  log_manager.set_cache_index(m_valid_index);
  log_manager.set_current_index(m_valid_index + 1);
  log_manager.set_enable_rotate(!(m_current_flag & FLAG_LARGE_TRX));

  if (!this->m_is_malformed && total_ha_2pc > 1) {
    Xa_state_list xa_list{this->m_external_xids};
    komodo::XA_spec_list *spec_list = nullptr;

    if (m_server_version < komodo::XA_SPEC_RECOVERY_SERVER_VERSION_REQUIRED) {
      LogErr(WARNING_LEVEL, ER_XA_SPEC_VERSION_NOT_MATCH, m_server_version,
             komodo::XA_SPEC_RECOVERY_SERVER_VERSION_REQUIRED);
    } else {
      spec_list = m_xa_spec_recovery.xa_spec_list();
    }

    this->m_no_engine_recovery =
        ha_recover(&this->m_internal_xids, &xa_list, spec_list, this);
    if (this->m_no_engine_recovery) {
      this->m_failure_message.assign("Recovery failed in storage engines");
    }
  }
  return *this;
}

void Consensus_xlog_recovery::process_consensus_event(
    const Consensus_log_event &ev) {
  if (m_current_index > ev.get_index()) {
    xp::error(ER_XP_RECOVERY) << "consensus log index out of order";
    exit(-1);
  } else if (m_end_pos < m_start_pos) {
    xp::error(ER_XP_RECOVERY) << "consensus log structure broken";
    exit(-1);
  }

  m_current_index = ev.get_index();
  m_current_term = ev.get_term();
  m_current_length = ev.get_length();
  m_current_flag = ev.get_flag();
  m_end_pos = m_start_pos = my_b_tell(m_reader->get_io_cache());
  m_begin_consensus = true;

  m_ev_start_pos = m_reader->event_start_pos();
}

void Consensus_xlog_recovery::process_previous_consensus_index_event(
    const Previous_consensus_index_log_event &ev) {
  m_current_index = ev.get_index() - 1;
  m_valid_index = m_current_index;
}

void Consensus_xlog_recovery::process_internal_xid(ulong unmasked_server_id,
                                                   my_xid xid) {
  // NOTE:: only own server need recover for xpaxos
  if (unmasked_server_id == server_id) {
    if (m_recover_term == 0 || m_current_term > m_recover_term) {
      m_recovery_manager.clear_trx_in_binlog();
      m_internal_xids.clear();
      m_external_xids.clear();
      m_recover_term = m_current_term;
      m_xa_spec_recovery.clear();
    }
    if (!m_internal_xids.insert(xid).second) {
      this->m_is_malformed = true;
      this->m_failure_message.assign("Xid_log_event holds an invalid XID");
      return;
    }
    m_recovery_manager.add_trx_in_binlog(m_current_index, xid);
    gather_internal_xa_spec(xid, m_xa_spec);
  }
}

void Consensus_xlog_recovery::process_external_xid(
    ulong unmasked_server_id, const XID &xid, enum_ha_recover_xa_state state) {
  // NOTE:: only own server need recover for xpaxos
  if (unmasked_server_id == server_id) {
    if (m_recover_term == 0 || m_current_term > m_recover_term) {
      m_recovery_manager.clear_trx_in_binlog();
      m_internal_xids.clear();
      m_external_xids.clear();
      m_recover_term = m_current_term;
      m_xa_spec_recovery.clear();
    }
    auto found = this->m_external_xids.find(xid);

    enum_ha_recover_xa_state prev_state =
        found != this->m_external_xids.end()
            ? found->second
            : enum_ha_recover_xa_state::NOT_FOUND;

    xp::info(ER_XP_RECOVERY)
        << "Consensus_xlog_recovery::process_external_xid  "
        << ", XID " << xid << ", m_current_index " << m_current_index
        << ", previous state" << (int)(prev_state) << ", state " << (int)state;

    if (found != this->m_external_xids.end()) {
      assert(found->second != enum_ha_recover_xa_state::PREPARED_IN_SE);
      if (state == enum_ha_recover_xa_state::PREPARED_IN_TC ||
          state == enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE) {
        // XA PREPARE EVENT
        if (found->second == enum_ha_recover_xa_state::PREPARED_IN_TC) {
          // If it was found already, must have been committed or rolled back,
          // it can't be in prepared state
          this->m_is_malformed = true;
          this->m_failure_message.assign(
              "XA_prepare_log_event holds an invalid XID");
          return;
        }
      } else {
        // XA COMMIT OR XA ROLLBACK EVENT
        if (found->second != enum_ha_recover_xa_state::PREPARED_IN_TC) {
          // If it was found already, it needs to be in prepared in TC state
          this->m_is_malformed = true;
          return;
        }
      }
    }
    m_external_xids[xid] = state;
    m_recovery_manager.add_trx_in_binlog(m_current_index, xid);
    gather_external_xa_spec(xid, m_xa_spec);
  }
}

void Consensus_xlog_recovery::process_xa_commit(const std::string &query) {
  raii::Sentry<> spec_guard{[&]() -> void { m_xa_spec.transaction_end(); }};

  this->m_is_malformed = this->m_in_transaction;
  this->m_in_transaction = false;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log_event containing `XA COMMIT` inside the boundary of a "
        "sequence of events representing a transaction not yet in prepared "
        "state");
    return;
  }
  m_xa_spec.m_source = komodo::Binlog_xa_specification::Source::XA_COMMIT;
  xa::XID_extractor tokenizer{query, 1};
  process_external_xid(this->m_query_ev->common_header->unmasked_server_id,
                       tokenizer[0], enum_ha_recover_xa_state::COMMITTED);
  if (this->m_is_malformed)
    this->m_failure_message.assign(
        "Query_log_event containing `XA COMMIT` holds an invalid XID");
}

void Consensus_xlog_recovery::process_xa_rollback(const std::string &query) {
  raii::Sentry<> spec_guard{[&]() -> void { m_xa_spec.transaction_end(); }};

  this->m_is_malformed = this->m_in_transaction;
  this->m_in_transaction = false;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log_event containing `XA ROLLBACK` inside the boundary of a "
        "sequence of events representing a transaction not yet in prepared "
        "state");
    return;
  }
  m_xa_spec.m_source = komodo::Binlog_xa_specification::Source::XA_ROLLBACK;
  xa::XID_extractor tokenizer{query, 1};
  process_external_xid(this->m_query_ev->common_header->unmasked_server_id,
                       tokenizer[0], enum_ha_recover_xa_state::ROLLEDBACK);
  if (this->m_is_malformed)
    this->m_failure_message.assign(
        "Query_log_event containing `XA ROLLBACK` holds an invalid XID");
}

void Consensus_xlog_recovery::process_atomic_ddl(Query_log_event const &ev) {
  raii::Sentry<> spec_guard{[&]() -> void { m_xa_spec.transaction_end(); }};

  this->m_is_malformed = this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log event containing a DDL inside the boundary of a sequence of "
        "events representing an active transaction");
    return;
  }

  m_xa_spec.m_source = komodo::Binlog_xa_specification::Source::COMMIT;
  process_internal_xid(ev.common_header->unmasked_server_id, ev.ddl_xid);
}

void Consensus_xlog_recovery::process_xid_event(const Xid_log_event &ev) {
  raii::Sentry<> spec_guard{[&]() -> void { m_xa_spec.transaction_end(); }};

  this->m_is_malformed = !this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Xid_log_event outside the boundary of a sequence of events "
        "representing an active transaction");
    return;
  }
  this->m_in_transaction = false;
  m_xa_spec.m_source = komodo::Binlog_xa_specification::Source::COMMIT;
  process_internal_xid(ev.common_header->unmasked_server_id, ev.xid);
}

void Consensus_xlog_recovery::process_xa_prepare_event(
    const XA_prepare_log_event &ev) {
  raii::Sentry<> spec_guard{[&]() -> void { m_xa_spec.transaction_end(); }};

  this->m_is_malformed = !this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "XA_prepare_log_event outside the boundary of a sequence of events "
        "representing an active transaction");
    return;
  }

  this->m_in_transaction = false;

  XID xid;
  xid = ev.get_xid();
  auto state = ev.is_one_phase()
                   ? enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE
                   : enum_ha_recover_xa_state::PREPARED_IN_TC;
  m_xa_spec.m_source =
      ev.is_one_phase()
          ? komodo::Binlog_xa_specification::Source::XA_COMMIT_ONE_PHASE
          : komodo::Binlog_xa_specification::Source::XA_PREPARE;
  process_external_xid(ev.common_header->unmasked_server_id, xid, state);
}

void Consensus_xlog_recovery::process_query_event(const Query_log_event &ev) {
  m_query_ev = &ev;
  Binlog_recovery::process_query_event(ev);
  m_query_ev = nullptr;
}

uint64 Consensus_xlog_recovery::get_last_leader_term_index() {
  return m_recovery_manager.get_last_leader_term_index();
}

void Consensus_xlog_recovery::set_last_leader_term_index(uint64 index) {
  m_recovery_manager.set_last_leader_term_index(index);
}

void Consensus_xlog_recovery::add_internal_pending_recovering_trx(
    handlerton &ht, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec) {
  m_recovery_manager
      .add_pending_recovering_trx<Pending_recovering_trx::xid_type::INTERNAL>(
          ht, enum_ha_recover_xa_state::PREPARED_IN_SE,
          enum_ha_recover_xa_state::COMMITTED, xa_trx, xa_spec);
}

void Consensus_xlog_recovery::add_external_pending_recovering_trx(
    handlerton &ht, enum_ha_recover_xa_state current_state,
    enum_ha_recover_xa_state next_state, const XA_recover_txn *xa_trx,
    const komodo::XA_specification &xa_spec) {
  m_recovery_manager
      .add_pending_recovering_trx<Pending_recovering_trx::xid_type::EXTERNAL>(
          ht, current_state, next_state, xa_trx, xa_spec);
}

void Consensus_xlog_recovery::recover_one_internal_trx(
    xarecover_st const &info, handlerton &ht, XA_recover_txn const &xa_trx,
    my_xid xid,
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
      exec_status = XA_OK;
      xlog_recovery_add_internal_pending_recovering_trx(

          ht, &xa_trx, *info.spec_list->find(xid));
    }

    if (exec_status == XA_OK)
      add_to_stats<STATS_SUCCESS, STATS_COMMITTED>(stats);
    else {
      add_to_stats<STATS_FAILURE, STATS_COMMITTED>(stats);
      report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_COMMIT_FAILED, xid, ht,
                                exec_status);
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

void Consensus_xlog_recovery::recover_one_external_trx(
    xarecover_st const &info, handlerton &ht, XA_recover_txn const &xa_trx,
    recovery_statistics &stats) {
  auto state{enum_ha_recover_xa_state::NOT_FOUND};

  if (info.xa_list != nullptr) {
    state = info.xa_list->find(xa_trx.id);
  }

  /** Prepare xa spec if have. */
  komodo::XA_specification *xa_spec =
      (info.spec_list ? info.spec_list->find(xa_trx.id) : nullptr);

  bool collect_pending_recovering_trx = false;
  auto state_in_ht{enum_ha_recover_xa_state::NOT_FOUND};
  if (info.xa_list_in_ht != nullptr) {
    state_in_ht = info.xa_list_in_ht->find(xa_trx.id);
  }
  auto iter = m_recovery_manager.external_xids_in_binlog.find(xa_trx.id);
  uint64 consensus_index = 0;
  if (iter != m_recovery_manager.external_xids_in_binlog.end())
    consensus_index = iter->second;
  xp::system(ER_XP_RECOVERY)
      << "recover_one_external_trx"
      << ", state: " << (int)state << ", state_in_ht: " << (int)state_in_ht
      << ", xid: " << xa_trx.id << ", consensus_index: " << consensus_index;

  switch (state) {
    case enum_ha_recover_xa_state::COMMITTED_WITH_ONEPHASE:
    case enum_ha_recover_xa_state::COMMITTED: {
      if (ht.commit_by_xid != nullptr) {
        enum xa_status_code exec_status;
        if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
          exec_status = generate_xa_recovery_error();
        } else {
          exec_status = XA_OK;
          collect_pending_recovering_trx = true;
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
          if (state == enum_ha_recover_xa_state::ROLLEDBACK) {
            exec_status = XA_OK;
            collect_pending_recovering_trx = true;
          } else {
            exec_status =
                ht.rollback_by_xid(&ht, const_cast<XID *>(&xa_trx.id), xa_spec);
          }
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
      if (ht.set_prepared_in_tc_by_xid != nullptr) {
        enum xa_status_code exec_status;
        if (DBUG_EVALUATE_IF("xa_recovery_error_reporting", true, false)) {
          exec_status = generate_xa_recovery_error();
        } else {
          exec_status = XA_OK;
          if (state_in_ht == enum_ha_recover_xa_state::PREPARED_IN_SE) {
            collect_pending_recovering_trx = true;
          }
        }

        if (exec_status == XA_OK) {
          add_to_stats<STATS_SUCCESS, STATS_PREPARED>(stats);
          break;
        } else
          report_trx_recovery_error(ER_BINLOG_CRASH_RECOVERY_PREPARE_FAILED,
                                    xa_trx.id, ht, exec_status,
                                    /*is_xa*/ true);
      }
      add_to_stats<STATS_FAILURE, STATS_PREPARED>(stats);
      break;
    }
  }

  // some trx in engine should forward it status by tc_log,
  // so we need to collect it and forward it after consensus log recovery
  if (collect_pending_recovering_trx) {
    xlog_recovery_add_external_pending_recovering_trx(
        ht, state_in_ht, state, &xa_trx, *info.spec_list->find(xa_trx.id));
  } else if (state == state_in_ht &&
             state_in_ht == enum_ha_recover_xa_state::PREPARED_IN_TC) {
    if (Recovered_xa_transactions::instance().add_prepared_xa_transaction(
            &xa_trx)) {
      xp::fatal(ER_XP_RECOVERY) << "add prepared xa trx failed in collecting";
    }
  }
}

bool Consensus_xlog_recovery::recover_prepared_in_tc(handlerton *ht,
                                                     xarecover_st *info) {
  assert(info->xa_list != nullptr);
  xp::info(ER_XP_RECOVERY) << "recover_prepared_in_tc_one_ht for xa_list";
  if (ht->recover_prepared_in_tc(ht, *info->xa_list)) return true;

  xp::info(ER_XP_RECOVERY)
      << "recover_prepared_in_tc_one_ht for xa_list_in_ht ";
  if (ht->recover_prepared_in_tc(ht, *info->xa_list_in_ht)) return true;

  return false;
}

}  // namespace komodo
