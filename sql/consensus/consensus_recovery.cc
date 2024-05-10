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
#include <memory>

#include "libbinlogevents/include/binlog_event.h"
#include "my_sys.h"
#include "sql/binlog.h"
#include "sql/binlog/binlog_xa_specification.h"
#include "sql/binlog/lizard0recovery.h"
#include "sql/binlog/recovery.h"
#include "sql/binlog/tools/iterators.h"
#include "sql/consensus/consensus_err.h"
#include "sql/consensus/consensus_recovery.h"
#include "sql/consensus_log_manager.h"
#include "sql/consensus_recovery_manager.h"
#include "sql/gcn_log_event.h"
#include "sql/handler.h"
#include "sql/log_event.h"
#include "sql/mysqld.h"
#include "sql/xa/xid_extract.h"
#include "storage/innobase/include/ut0dbg.h"
#include "sql/system_variables.h"

namespace xp {

bool Recovery_manager::is_xpaxos_instance_recovering() const {
  return !opt_initialize && is_xpaxos_instance();
}

std::unique_ptr<binlog::Binlog_recovery> Recovery_manager::create_recovery(
    Binlog_file_reader &binlog_file_reader) {
  binlog::Binlog_recovery *recovery = nullptr;
  if (is_xpaxos_instance()) {
    recovery = new Consensus_binlog_recovery(binlog_file_reader);
  } else {
    recovery = new binlog::Binlog_recovery(binlog_file_reader);
  }
  return std::unique_ptr<binlog::Binlog_recovery>(recovery);
}

binlog::Binlog_recovery &Consensus_binlog_recovery::recover() {
  binlog::tools::Iterator it{&m_reader};
  it.set_copy_event_buffer();
  m_valid_pos = m_reader.position();
  int error = 0;

  this->process_format_event(*this->m_reader.format_description_event());

  for (Log_event *ev = it.begin(); ev != it.end() && !error; ev = it.next()) {
    if (ev->get_type_code() == binary_log::CONSENSUS_LOG_EVENT) {
      process_consensus_event(dynamic_cast<Consensus_log_event &>(*ev));
    } else if (ev->get_type_code() ==
               binary_log::PREVIOUS_CONSENSUS_INDEX_LOG_EVENT) {
      process_previous_consensus_index_event(
          dynamic_cast<Previous_consensus_index_log_event &>(*ev));
    } else if (!ev->is_control_event()) {
      m_end_pos = my_b_tell(m_reader.get_io_cache());

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
          for (size_t i = 0; i < m_blob_index_list.size() && !error; ++i) {
            m_log_content.assign(ev->temp_buf + blob_start_pos - m_start_pos,
                                 blob_end_pos - blob_start_pos);
            const uint64 current_crc32 =
                opt_consensus_checksum
                    ? checksum_crc32(0, get_uchar_str(m_log_content),
                                     m_log_content.size())
                    : 0;
            error = consensus_log_manager.get_fifo_cache_manager()
                        ->add_log_to_cache(
                            m_blob_term_list[i], m_blob_index_list[i],
                            m_log_content.size(), get_uchar_str(m_log_content),
                            false, m_blob_flag_list[i], current_crc32);
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
          fetch_binlog_by_offset(m_reader, m_start_pos, m_end_pos, m_rci_ev,
                                 m_log_content);
          const uint64 current_crc32 =
              opt_consensus_checksum
                  ? checksum_crc32(0, get_uchar_str(m_log_content),
                                   m_log_content.size())
                  : 0;
          // copy log to buffer
          error =
              consensus_log_manager.get_fifo_cache_manager()->add_log_to_cache(
                  m_current_term, m_current_index, m_log_content.size(),
                  get_uchar_str(m_log_content), (m_rci_ev != nullptr),
                  m_current_flag, current_crc32);
          m_begin_consensus = false;
          if (!(m_current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX)) {
            m_valid_index = m_current_index;
          }
        }
        m_rci_ev = nullptr;
      }
    }

    error = error || it.has_error() || m_reader.has_fatal_error();

    /*
      Rotate event (generated by current instance) at the end of last binlog
      file is not expected. If it's seen, this means binlog rotation failed,
      that is, only ROTATE_EVENT was logged, neither new file was not created
      or it was not registered in binlog index file, this event should be
      truncated.
    */
    if (!this->m_is_malformed && !this->m_in_transaction &&
        !error && !m_begin_consensus &&
        !(m_current_flag & Consensus_log_event_flag::FLAG_LARGE_TRX) &&
        !lizard::is_b_events_before_gtid(ev) && !is_gtid_event(ev) &&
        !(is_rotate_event(ev) && ev->server_id == (uint32) ::server_id) &&
        !is_session_control_event(ev)) {
      m_valid_pos = my_b_tell(m_reader.get_io_cache());
    }

    delete ev;
    ev = nullptr;

    if (error || this->m_is_malformed) break;
  }

  xp::system(ER_XP_RECOVERY) << "Consensus_binlog_recovery::recover end "
                           << ", file_size " << m_reader.ifile()->length()
                           << ", curr_position " << m_reader.position()
                           << ", m_current_index " << m_current_index
                           << ", m_start_pos " << m_start_pos << ", m_end_pos "
                           << m_end_pos << ", m_valid_pos " << m_valid_pos
                           << ", m_valid_index " << m_valid_index
                           << ", m_is_malformed " << m_is_malformed
                           << ", m_in_transaction " << m_in_transaction
                           << ", m_failure_message " << m_failure_message
                           << ", get_error_type  " << m_reader.get_error_type()
                           << ", get_error_number " << it.get_error_number()
                           << ", get_error_message " << it.get_error_message();

  if (m_start_pos < m_valid_pos && m_end_pos > m_valid_pos) {
    m_end_pos = m_valid_pos;
  }

  // recover current/sync index
  //
  // if the last log is not integrated
  if (m_valid_index != m_current_index) {
    xp::warn(ER_XP_RECOVERY)
        << "last consensus log is not integrated, "
        << "sync index should set to " << m_valid_index << " instead of "
        << m_current_index << ", with valid_pos " << m_valid_pos;
    // truncate cache
    consensus_log_manager.get_fifo_cache_manager()->trunc_log_from_cache(
        m_valid_index + 1);
  }

  consensus_log_manager.set_sync_index(m_valid_index);
  consensus_log_manager.set_cache_index(m_valid_index);
  consensus_log_manager.set_current_index(m_valid_index + 1);
  consensus_log_manager.set_enable_rotate(!(m_current_flag & FLAG_LARGE_TRX));

  if (!this->m_is_malformed && total_ha_2pc > 1) {
    Xa_state_list xa_list{this->m_external_xids};
    XA_spec_list *spec_list = nullptr;

    if (m_server_version < binlog::XA_SPEC_RECOVERY_SERVER_VERSION_REQUIRED) {
      LogErr(WARNING_LEVEL, ER_XA_SPEC_VERSION_NOT_MATCH, m_server_version,
             binlog::XA_SPEC_RECOVERY_SERVER_VERSION_REQUIRED);
    } else {
      spec_list = m_xa_spec_recovery->xa_spec_list();
    }

    this->m_no_engine_recovery =
        ha_recover(&this->m_internal_xids, &xa_list, spec_list);
    if (this->m_no_engine_recovery) {
      this->m_failure_message.assign("Recovery failed in storage engines");
    }
  }
  return *this;
}

void Consensus_binlog_recovery::process_consensus_event(
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
  m_end_pos = m_start_pos = my_b_tell(m_reader.get_io_cache());
  m_begin_consensus = true;

  m_ev_start_pos = m_reader.event_start_pos();
}

void Consensus_binlog_recovery::process_previous_consensus_index_event(
    const Previous_consensus_index_log_event &ev) {
  m_current_index = ev.get_index() - 1;
  m_valid_index = m_current_index;
}

void Consensus_binlog_recovery::process_internal_xid(ulong unmasked_server_id, my_xid xid) {
  //NOTE:: only own server need recover for xpaxos
  if (unmasked_server_id == server_id) {
    if (m_recover_term == 0 || m_current_term > m_recover_term) {
      consensus_log_manager.get_recovery_manager()->clear_trx_in_binlog();
      m_internal_xids.clear();
      m_external_xids.clear();
      m_recover_term = m_current_term;
      m_xa_spec_recovery->clear();
    }
    if (!m_internal_xids.insert(xid).second) {
      this->m_is_malformed = true;
      this->m_failure_message.assign("Xid_log_event holds an invalid XID");
      return;
    }
    consensus_log_manager.get_recovery_manager()->add_trx_in_binlog(
        m_current_index, xid);
    gather_internal_xa_spec(xid, m_xa_spec);
  }
}

void Consensus_binlog_recovery::process_external_xid(ulong unmasked_server_id,
  const XID &xid, enum_ha_recover_xa_state state) {
  //NOTE:: only own server need recover for xpaxos
  if (unmasked_server_id == server_id) {
    if (m_recover_term == 0 || m_current_term > m_recover_term) {
      consensus_log_manager.get_recovery_manager()->clear_trx_in_binlog();
      m_internal_xids.clear();
      m_external_xids.clear();
      m_recover_term = m_current_term;
      m_xa_spec_recovery->clear();
    }
    auto found = this->m_external_xids.find(xid);
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
    consensus_log_manager.get_recovery_manager()->add_trx_in_binlog(
        m_current_index, xid);
    gather_external_xa_spec(xid, m_xa_spec);
  }
}

void Consensus_binlog_recovery::process_xa_commit(const std::string &query) {
  binlog::Commit_binlog_xa_specification guard(&m_xa_spec);

  this->m_is_malformed = this->m_in_transaction;
  this->m_in_transaction = false;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log_event containing `XA COMMIT` inside the boundary of a "
        "sequence of events representing a transaction not yet in prepared "
        "state");
    return;
  }
  m_xa_spec.m_source = binlog::Binlog_xa_specification::Source::XA_COMMIT;
  xa::XID_extractor tokenizer{query, 1};
  process_external_xid(this->m_query_ev->common_header->unmasked_server_id, tokenizer[0], enum_ha_recover_xa_state::COMMITTED);
  if (this->m_is_malformed)
    this->m_failure_message.assign(
        "Query_log_event containing `XA COMMIT` holds an invalid XID");
}

void Consensus_binlog_recovery::process_xa_rollback(const std::string &query) {
  binlog::Commit_binlog_xa_specification guard(&m_xa_spec);

  this->m_is_malformed = this->m_in_transaction;
  this->m_in_transaction = false;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log_event containing `XA ROLLBACK` inside the boundary of a "
        "sequence of events representing a transaction not yet in prepared "
        "state");
    return;
  }
  m_xa_spec.m_source = binlog::Binlog_xa_specification::Source::XA_ROLLBACK;
  xa::XID_extractor tokenizer{query, 1};
  process_external_xid(this->m_query_ev->common_header->unmasked_server_id, tokenizer[0], enum_ha_recover_xa_state::ROLLEDBACK);
  if (this->m_is_malformed)
    this->m_failure_message.assign(
        "Query_log_event containing `XA ROLLBACK` holds an invalid XID");
}

void Consensus_binlog_recovery::process_atomic_ddl(Query_log_event const &ev) {
  binlog::Commit_binlog_xa_specification guard(&m_xa_spec);

  this->m_is_malformed = this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Query_log event containing a DDL inside the boundary of a sequence of "
        "events representing an active transaction");
    return;
  }

  m_xa_spec.m_source = binlog::Binlog_xa_specification::Source::COMMIT;
  process_internal_xid(ev.common_header->unmasked_server_id, ev.ddl_xid);
}

void Consensus_binlog_recovery::process_xid_event(const Xid_log_event &ev) {
  binlog::Commit_binlog_xa_specification guard(&m_xa_spec);

  this->m_is_malformed = !this->m_in_transaction;
  if (this->m_is_malformed) {
    this->m_failure_message.assign(
        "Xid_log_event outside the boundary of a sequence of events "
        "representing an active transaction");
    return;
  }
  this->m_in_transaction = false;
  m_xa_spec.m_source = binlog::Binlog_xa_specification::Source::COMMIT;
  process_internal_xid(ev.common_header->unmasked_server_id, ev.xid);
}

void Consensus_binlog_recovery::process_xa_prepare_event(
    const XA_prepare_log_event &ev) {
  binlog::Commit_binlog_xa_specification guard(&m_xa_spec);

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
          ? binlog::Binlog_xa_specification::Source::XA_COMMIT_ONE_PHASE
          : binlog::Binlog_xa_specification::Source::XA_PREPARE;
  process_external_xid(ev.common_header->unmasked_server_id, xid, state);
}

void Consensus_binlog_recovery::process_query_event(const Query_log_event &ev) {
  m_query_ev = &ev;
  Binlog_recovery::process_query_event(ev);
  m_query_ev = nullptr;
}

}  // namespace xp
