//
// Created by zzy on 2022/8/1.
//

#pragma once

#include <utility>

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
