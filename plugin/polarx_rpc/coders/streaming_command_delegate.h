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
// Created by zzy on 2022/7/28.
//

#pragma once

#include <functional>

#include "../session/flow_control.h"

#include "encoders/encoding_polarx_chunk.h"
#include "encoders/encoding_polarx_row.h"
#include "encoders/encoding_pool.h"
#include "polarx_encoder.h"

#include "command_delegate.h"
#include "polarx_encoder.h"

namespace polarx_rpc {
class CsessionBase;

class CstreamingCommandDelegate : public CcommandDelegate {
  NO_COPY(CstreamingCommandDelegate)

 protected:
  CsessionBase &session_;
  CpolarxEncoder &encoder_;
  std::function<bool()> flush_;
  const bool compact_metadata_;
  const ulong capabilities_;

  protocol::PolarX_Row_encoder row_enc_;
  protocol::PolarX_Chunk_encoder chunk_enc_;

  /// result set flags
  bool chunk_result_ = false;
  bool feedback_ = false;

  /// status flags
  bool sent_result_ = false;
  bool wait_for_fetch_done_ = false;
  bool handle_ok_received_ = false;
  bool send_notice_deferred_ = false;

  /// for recording current charset
  const CHARSET_INFO *result_cs_ = nullptr;

  /// flow control
  CflowControl *flow_control_ = nullptr;

  /// buf for convert error charset
  char err_msg_buf_[MYSQL_ERRMSG_SIZE]{};

  inline protocol::PolarX_Message_encoder &msg_enc() {
    return encoder_.message_encoder();
  }

  int trigger_on_message(uint8_t msg_type);

  /// meta dealing
  int start_result_metadata(uint num_cols, uint flags,
                            const CHARSET_INFO *result_cs) override;
  int field_metadata(struct st_send_field *field,
                     const CHARSET_INFO *charset) override;
  int end_result_metadata(uint32_t server_status, uint32_t warn_count) override;

  /// row dealing
  int start_row() override;
  bool send_row();
  int end_row() override;
  void abort_row() override;
  ulong get_client_capabilities() override;

  /// row data dealing
  int get_null() override;
  int get_integer(longlong value) override;
  int get_longlong(longlong value, uint unsigned_flag) override;
  int get_decimal(const decimal_t *value) override;
  int get_double(double value, uint32 decimals) override;
  int get_date(const MYSQL_TIME *value) override;
  int get_time(const MYSQL_TIME *value, uint decimals) override;
  int get_datetime(const MYSQL_TIME *value, uint decimals) override;
  int get_string(const char *value, size_t length,
                 const CHARSET_INFO *value_cs) override;

  /// request status dealing
  void handle_ok(uint32_t server_status, uint32_t statement_warn_count,
                 uint64_t affected_rows, uint64_t last_insert_id,
                 const char *message) override;
  void handle_error(uint sql_errno, const char *err_msg,
                    const char *sqlstate) override;
  virtual bool try_send_notices(uint32_t server_status,
                                uint32_t statement_warn_count,
                                uint64_t affected_rows, uint64_t last_insert_id,
                                const char *message);

  enum cs_text_or_binary representation() const override {
    return CS_BINARY_REPRESENTATION;
  }

  void on_destruction();
  bool defer_on_warning(uint32_t server_status, uint32_t statement_warn_count,
                        uint64_t affected_rows, uint64_t last_insert_id,
                        const char *message);
  void handle_fetch_done_more_results(uint32_t server_status);
  void end_result_metadata_handle_fetch(uint32_t server_status);
  void handle_out_param_in_handle_ok(uint32_t server_status);

 public:
  CstreamingCommandDelegate(CsessionBase &session, CpolarxEncoder &encoder,
                            std::function<bool()> &&flush,
                            bool compact_metadata, ulong capabilities);
  ~CstreamingCommandDelegate() override { on_destruction(); }

  void reset() override;

  inline void set_flow_control(CflowControl *flow_control) {
    flow_control_ = flow_control;
  }

  inline void set_chunk_result(bool chunk) { chunk_result_ = chunk; }

  inline void set_feedback(bool feedback) { feedback_ = feedback; }
};

}  // namespace polarx_rpc
