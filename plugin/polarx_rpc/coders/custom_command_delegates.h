/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

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


//
// Created by zzy on 2022/8/1.
//

#pragma once

#include <utility>

#include "../global_defines.h"
#ifdef MYSQL8PLUS
#include "sql/binlog.h"
#endif

#include "protocol_fwd.h"
#include "streaming_command_delegate.h"

namespace polarx_rpc {
class CsessionBase;

static constexpr ulong DEFAULT_CAPABILITIES =
    CLIENT_FOUND_ROWS | CLIENT_MULTI_RESULTS | CLIENT_DEPRECATE_EOF |
    CLIENT_PS_MULTI_RESULTS;

class CstmtCommandDelegate : public CstreamingCommandDelegate {
 public:
  template <class T>
  CstmtCommandDelegate(CsessionBase &session, CpolarxEncoder &encoder,
                       T &&flush, bool compact_metadata, ulong capabilities)
      : CstreamingCommandDelegate(session, encoder, std::forward<T>(flush),
                                  compact_metadata, capabilities) {}
  ~CstmtCommandDelegate() override { on_destruction(); }

  bool try_send_notices(const uint32_t server_status,
                        const uint32_t statement_warn_count,
                        const uint64_t affected_rows,
                        const uint64_t last_insert_id,
                        const char *const message) override {
    if (defer_on_warning(server_status, statement_warn_count, affected_rows,
                         last_insert_id, message))
      return false;

    msg_enc().encode_notice_rows_affected(affected_rows);
    trigger_on_message(PolarXRPC::ServerMessages::NOTICE);

    if (last_insert_id > 0) {
      msg_enc().encode_notice_generated_insert_id(last_insert_id);
      trigger_on_message(PolarXRPC::ServerMessages::NOTICE);
    }

    if (message && strlen(message) != 0) {
      msg_enc().encode_notice_text_message(message);
      trigger_on_message(PolarXRPC::ServerMessages::NOTICE);
    }

#ifdef MYSQL8PLUS
    // check global leader in transfer
    if (mysql_bin_log.is_in_leader_transfer())
      msg_enc().encode_notice_server_state(
          PolarXRPC::Notice::
              SessionStateChanged_ExtraServerState_IN_LEADER_TRANSFER_FLAG);
#endif
    return true;
  }

  void handle_ok(uint32_t server_status, uint32_t statement_warn_count,
                 uint64_t affected_rows, uint64_t last_insert_id,
                 const char *const message) override {
    handle_out_param_in_handle_ok(server_status);
    CstreamingCommandDelegate::handle_ok(server_status, statement_warn_count,
                                         affected_rows, last_insert_id,
                                         message);
  }

  int end_result_metadata(uint32_t server_status,
                          uint32_t warn_count) override {
    handle_fetch_done_more_results(server_status);
    return CstreamingCommandDelegate::end_result_metadata(server_status,
                                                          warn_count);
  }
};

}  // namespace polarx_rpc
