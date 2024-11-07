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

#include "../global_defines.h"
#ifndef MYSQL8
#define MYSQL_SERVER
#endif
#include "sql/sql_class.h"
#include "sql/sql_error.h"

#ifdef MYSQL8
#include "sql/item.h"
#endif

#include <memory>

#include "../common_define.h"
#include "../helper/str_converter.h"
#include "../polarx_rpc.h"
#include "../session/session_base.h"

#include "notices.h"
#include "streaming_command_delegate.h"

namespace polarx_rpc {

CstreamingCommandDelegate::CstreamingCommandDelegate(
    CsessionBase &session, CpolarxEncoder &encoder,
    std::function<bool()> &&flush, bool compact_metadata, ulong capabilities)
    : session_(session),
      encoder_(encoder),
      flush_(std::forward<std::function<bool()>>(flush)),
      compact_metadata_(compact_metadata),
      capabilities_(capabilities),
      row_enc_(&encoder.message_encoder()),
      chunk_enc_(&encoder.message_encoder()) {}

constexpr int k_number_of_pages_that_trigger_flush = 5;

/// return true if error occurs
int CstreamingCommandDelegate::trigger_on_message(uint8_t msg_type) {
  const auto can_buffer =
      ((msg_type == PolarXRPC::ServerMessages::RESULTSET_COLUMN_META_DATA) ||
       (msg_type == PolarXRPC::ServerMessages::RESULTSET_ROW) ||
       (msg_type == PolarXRPC::ServerMessages::NOTICE) ||
       (msg_type == PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE) ||
       (msg_type ==
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_OUT_PARAMS) ||
       (msg_type ==
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS) ||
       (msg_type == PolarXRPC::ServerMessages::RESULTSET_FETCH_SUSPENDED));

  auto buffer_too_big = false;
  auto probe = encoder_.encoding_buffer().m_front;
  auto page_cnt = k_number_of_pages_that_trigger_flush;
  while (probe != nullptr) {
    if (--page_cnt <= 0) {
      buffer_too_big = true;
      break;
    }
    probe = probe->m_next_page;
  }

  DBG_LOG(("send msg: %d", msg_type));
  if (!can_buffer || buffer_too_big)
    /// do flush
    return flush_() ? 0 : -1;
  return 0;
}

/****** Dealing meta ******/
int CstreamingCommandDelegate::start_result_metadata(
    uint num_cols, uint flags, const CHARSET_INFO *result_cs) {
  auto iret =
      CcommandDelegate::start_result_metadata(num_cols, flags, result_cs);
  if (iret != 0) return iret;

  sent_result_ = true;
  result_cs_ = result_cs;
  return 0;
}

int CstreamingCommandDelegate::field_metadata(struct st_send_field *field,
                                              const CHARSET_INFO *charset) {
  auto iret = CcommandDelegate::field_metadata(field, charset);
  if (iret != 0) return iret;

  /// build protobuf inplace
  PolarXRPC::Resultset::ColumnMetaData_FieldType xtype{};
  uint64_t collation = 0;
  uint64_t *collation_ptr = nullptr;
  uint32_t decimals = 0;
  uint32_t *decimals_ptr = nullptr;
  uint32_t length = 0;
  uint32_t *length_ptr = nullptr;
  uint32_t content_type = 0;
  uint32_t *content_type_ptr = nullptr;
  auto is_string = false;

  enum_field_types type = field->type;
  uint32_t flags = 0;

  if (field->flags & NOT_NULL_FLAG) flags |= POLARX_COLUMN_FLAGS_NOT_NULL;

  if (field->flags & PRI_KEY_FLAG) flags |= POLARX_COLUMN_FLAGS_PRIMARY_KEY;

  if (field->flags & UNIQUE_KEY_FLAG) flags |= POLARX_COLUMN_FLAGS_UNIQUE_KEY;

  if (field->flags & MULTIPLE_KEY_FLAG)
    flags |= POLARX_COLUMN_FLAGS_MULTIPLE_KEY;

  if (field->flags & AUTO_INCREMENT_FLAG)
    flags |= POLARX_COLUMN_FLAGS_AUTO_INCREMENT;

  if (MYSQL_TYPE_STRING == type) {
    if (field->flags & SET_FLAG)
      type = MYSQL_TYPE_SET;
    else if (field->flags & ENUM_FLAG)
      type = MYSQL_TYPE_ENUM;
  }

  /// always set collation(mtr use it)
  collation = get_valid_charset_collation(result_cs_, charset);
  collation_ptr = &collation;

  switch (type) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_SHORT:
    case MYSQL_TYPE_INT24:
    case MYSQL_TYPE_LONG:
    case MYSQL_TYPE_LONGLONG:
      length = field->length;
      length_ptr = &length;

      if (field->flags & UNSIGNED_FLAG)
        xtype = PolarXRPC::Resultset::ColumnMetaData::UINT;
      else
        xtype = PolarXRPC::Resultset::ColumnMetaData::SINT;

      if (field->flags & ZEROFILL_FLAG)
        flags |= POLARX_COLUMN_FLAGS_UINT_ZEROFILL;
      break;

    case MYSQL_TYPE_FLOAT:
      if (field->flags & UNSIGNED_FLAG)
        flags |= POLARX_COLUMN_FLAGS_FLOAT_UNSIGNED;
      decimals = field->decimals;
      decimals_ptr = &decimals;
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::FLOAT;
      break;

    case MYSQL_TYPE_DOUBLE:
      if (field->flags & UNSIGNED_FLAG)
        flags |= POLARX_COLUMN_FLAGS_DOUBLE_UNSIGNED;
      decimals = field->decimals;
      decimals_ptr = &decimals;
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::DOUBLE;
      break;

    case MYSQL_TYPE_DECIMAL:
    case MYSQL_TYPE_NEWDECIMAL:
      if (field->flags & UNSIGNED_FLAG)
        flags |= POLARX_COLUMN_FLAGS_DECIMAL_UNSIGNED;
      decimals = field->decimals;
      decimals_ptr = &decimals;
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::DECIMAL;
      break;

    case MYSQL_TYPE_STRING:
      is_string = true;
      flags |= POLARX_COLUMN_FLAGS_BYTES_RIGHTPAD;
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      length = field->length;
      length_ptr = &length;
      break;

    case MYSQL_TYPE_SET:
      is_string = true;
      flags |= POLARX_COLUMN_FLAGS_BYTES_RIGHTPAD;
      xtype = PolarXRPC::Resultset::ColumnMetaData::SET;
      length = field->length;
      length_ptr = &length;
      break;

    case MYSQL_TYPE_TINY_BLOB:
    case MYSQL_TYPE_BLOB:
    case MYSQL_TYPE_MEDIUM_BLOB:
    case MYSQL_TYPE_LONG_BLOB:
    case MYSQL_TYPE_VARCHAR:
    case MYSQL_TYPE_VAR_STRING:
      is_string = true;
      if (field->decimals != 0) {
        decimals = field->decimals;
        decimals_ptr = &decimals;
      }
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      break;

    case MYSQL_TYPE_JSON:
      is_string = true;
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      content_type = POLARX_COLUMN_BYTES_CONTENT_TYPE_JSON;
      content_type_ptr = &content_type;
      length = field->length;
      length_ptr = &length;
      break;

    case MYSQL_TYPE_GEOMETRY:
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      content_type = POLARX_COLUMN_BYTES_CONTENT_TYPE_GEOMETRY;
      content_type_ptr = &content_type;
      break;

    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::TIME;
      decimals = field->decimals;
      decimals_ptr = &decimals;
      break;

    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      flags |= POLARX_COLUMN_FLAGS_DATETIME_TIMESTAMP;
      // fall through
#if __cplusplus >= 201703L
      [[fallthrough]];
#endif
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::DATETIME;
      decimals = field->decimals;
      decimals_ptr = &decimals;
      break;

    case MYSQL_TYPE_YEAR:
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::UINT;
      break;

    case MYSQL_TYPE_ENUM:
      is_string = true;
      flags |= POLARX_COLUMN_FLAGS_BYTES_RIGHTPAD;
      xtype = PolarXRPC::Resultset::ColumnMetaData::ENUM;
      length = field->length;
      length_ptr = &length;
      break;

    case MYSQL_TYPE_NULL:
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      break;

    case MYSQL_TYPE_BIT:
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::BIT;
      break;

    default:
      /// this should never happen, but in new version of MySQL,
      /// we should ignore it
      length = field->length;
      length_ptr = &length;
      xtype = PolarXRPC::Resultset::ColumnMetaData::BYTES;
      break;
  }

  /// fix length
  if (is_string) {
    const CHARSET_INFO *thd_charset =
        current_thd->variables.character_set_results;
    if (charset != &my_charset_bin && thd_charset != nullptr) {
      auto max_length = (field->type >= MYSQL_TYPE_TINY_BLOB &&
                         field->type <= MYSQL_TYPE_BLOB)
                            ? field->length / charset->mbminlen
                            : field->length / charset->mbmaxlen;
      length = char_to_byte_length_safe(max_length, thd_charset->mbmaxlen);
    }
  }

  /// disable chunk if too large
  if ((is_string && length_ptr != nullptr &&
       length > protocol::k_block_size / 2) ||
      MYSQL_TYPE_GEOMETRY == type)
    chunk_result_ = false;

  if (compact_metadata_)
    msg_enc().encode_compact_metadata(xtype, type, collation_ptr, decimals_ptr,
                                      length_ptr, flags != 0 ? &flags : nullptr,
                                      content_type_ptr, &field->flags);
  else {
    CconvertIfNecessary col_name(result_cs_, field->col_name,
                                 ::strlen(field->col_name),
                                 system_charset_info);
    CconvertIfNecessary table_name(result_cs_, field->table_name,
                                   ::strlen(field->table_name),
                                   system_charset_info);
    CconvertIfNecessary db_name(result_cs_, field->db_name,
                                ::strlen(field->db_name), system_charset_info);
    CconvertIfNecessary org_col_name(result_cs_, field->org_col_name,
                                     ::strlen(field->org_col_name),
                                     system_charset_info);
    CconvertIfNecessary org_table_name(result_cs_, field->org_table_name,
                                       ::strlen(field->org_table_name),
                                       system_charset_info);
    msg_enc().encode_full_metadata(
        col_name.get_ptr(), org_col_name.get_ptr(), table_name.get_ptr(),
        org_table_name.get_ptr(), db_name.get_ptr(), "def", xtype, type,
        collation_ptr, decimals_ptr, length_ptr, flags != 0 ? &flags : nullptr,
        content_type_ptr, &field->flags);
  }
  return trigger_on_message(
      PolarXRPC::ServerMessages::RESULTSET_COLUMN_META_DATA);
}

int CstreamingCommandDelegate::end_result_metadata(uint server_status,
                                                   uint warn_count) {
  /// reset for chunk builder
  if (chunk_result_) {
    chunk_enc_.abort_chunk();
    chunk_enc_.chunk_init(field_types_.size());
  }

  CcommandDelegate::end_result_metadata(server_status, warn_count);
  return 0;
}

/****** Dealing row ******/
int CstreamingCommandDelegate::start_row() {
  if (!streaming_metadata_) {
    if (chunk_result_)
      chunk_enc_.begin_row();
    else
      row_enc_.begin_row();
  }
  return 0;
}

bool CstreamingCommandDelegate::send_row() {
  if (chunk_result_)
    chunk_enc_.end_row();
  else
    row_enc_.end_row();

  /// collect sent size and do flow control consume
  const auto sent = encoder_.get_flushed_bytes();
  encoder_.reset_flushed_bytes();

  /// do flow control
  if (sent > 0 && UNLIKELY(flow_control_ != nullptr &&
                           !flow_control_->flow_consume(sent))) {
    /// sent > 0 only when data actually sent, so no need to send chunk
    msg_enc().encode_token_done();
    if (trigger_on_message(::PolarXRPC::ServerMessages::RESULTSET_TOKEN_DONE) !=
        0)
      return false;  /// IO error
    /// now do wait
    if (!flow_control_->flow_wait()) return false;  /// IO error
  }

  return 0 == trigger_on_message(PolarXRPC::ServerMessages::RESULTSET_ROW);
}

int CstreamingCommandDelegate::end_row() {
  if (streaming_metadata_) return 0;

  if (send_row()) {
    // TODO warnings
    return 0;
  }
  return -1;
}

void CstreamingCommandDelegate::abort_row() {
  if (chunk_result_)
    chunk_enc_.abort_row();
  else
    row_enc_.abort_row();
}

ulong CstreamingCommandDelegate::get_client_capabilities() {
  return capabilities_;
}

/****** Getting data ******/
int CstreamingCommandDelegate::get_null() {
  if (chunk_result_)
    chunk_enc_.field_null();
  else
    row_enc_.field_null();
  return 0;
}

int CstreamingCommandDelegate::get_integer(longlong value) {
  auto field_idx =
      chunk_result_ ? chunk_enc_.get_num_fields() : row_enc_.get_num_fields();
  const bool unsigned_flag =
      (field_types_[field_idx].flags & UNSIGNED_FLAG) != 0;
  return get_longlong(value, unsigned_flag);
}

int CstreamingCommandDelegate::get_longlong(longlong value,
                                            uint unsigned_flag) {
  auto field_idx =
      chunk_result_ ? chunk_enc_.get_num_fields() : row_enc_.get_num_fields();
  // This is a hack to workaround server bugs similar to #77787:
  // Sometimes, server will not report a column to be UNSIGNED in the
  // metadata, but will send the data as unsigned anyway. That will cause the
  // client to receive messed up data because signed ints use zigzag encoding,
  // while the client will not be expecting that. So we add some
  // bug-compatibility code here, so that if column metadata reports column to
  // be SIGNED, we will force the data to actually be SIGNED.
  if (unsigned_flag && (field_types_[field_idx].flags & UNSIGNED_FLAG) == 0)
    unsigned_flag = 0;

  // This is a hack to workaround server bug that causes wrong values being
  // sent for TINYINT UNSIGNED type, can be removed when it is fixed.
  if (unsigned_flag && (field_types_[field_idx].type == MYSQL_TYPE_TINY)) {
    value &= 0xff;
  }

  if (chunk_result_) {
    if (unsigned_flag)
      chunk_enc_.field_unsigned_longlong(value);
    else
      chunk_enc_.field_signed_longlong(value);
  } else {
    if (unsigned_flag)
      row_enc_.field_unsigned_longlong(value);
    else
      row_enc_.field_signed_longlong(value);
  }
  return 0;
}

int CstreamingCommandDelegate::get_decimal(const decimal_t *value) {
  if (chunk_result_)
    chunk_enc_.field_decimal(value);
  else
    row_enc_.field_decimal(value);
  return 0;
}

int CstreamingCommandDelegate::get_double(double value, uint32) {
  if (chunk_result_) {
    if (field_types_[chunk_enc_.get_num_fields()].type == MYSQL_TYPE_FLOAT)
      chunk_enc_.field_float(static_cast<float>(value));
    else
      chunk_enc_.field_double(value);
  } else {
    if (field_types_[row_enc_.get_num_fields()].type == MYSQL_TYPE_FLOAT)
      row_enc_.field_float(static_cast<float>(value));
    else
      row_enc_.field_double(value);
  }
  return 0;
}

int CstreamingCommandDelegate::get_date(const MYSQL_TIME *value) {
  if (chunk_result_)
    chunk_enc_.field_date(value);
  else
    row_enc_.field_date(value);
  return 0;
}

int CstreamingCommandDelegate::get_time(const MYSQL_TIME *value, uint) {
  if (chunk_result_)
    chunk_enc_.field_time(value);
  else
    row_enc_.field_time(value);
  return 0;
}

int CstreamingCommandDelegate::get_datetime(const MYSQL_TIME *value, uint) {
  if (chunk_result_)
    chunk_enc_.field_datetime(value);
  else
    row_enc_.field_datetime(value);
  return 0;
}

int CstreamingCommandDelegate::get_string(const char *const value,
                                          size_t length,
                                          const CHARSET_INFO *const value_cs) {
  auto field_idx =
      chunk_result_ ? chunk_enc_.get_num_fields() : row_enc_.get_num_fields();
  const enum_field_types type = field_types_[field_idx].type;
  const unsigned int flags = field_types_[field_idx].flags;

  switch (type) {
    case MYSQL_TYPE_NEWDECIMAL:
      if (chunk_result_)
        chunk_enc_.field_decimal(value, length);
      else
        row_enc_.field_decimal(value, length);
      break;
    case MYSQL_TYPE_SET: {
      CconvertIfNecessary conv(result_cs_, value, length, value_cs);
      if (chunk_result_)
        chunk_enc_.field_set(conv.get_ptr(), conv.get_length());
      else
        row_enc_.field_set(conv.get_ptr(), conv.get_length());
      break;
    }
    case MYSQL_TYPE_BIT:
      if (chunk_result_)
        chunk_enc_.field_bit(value, length);
      else
        row_enc_.field_bit(value, length);
      break;
    case MYSQL_TYPE_STRING:
      if ((flags & SET_FLAG) != 0) {
        CconvertIfNecessary conv(result_cs_, value, length, value_cs);
        if (chunk_result_)
          chunk_enc_.field_set(conv.get_ptr(), conv.get_length());
        else
          row_enc_.field_set(conv.get_ptr(), conv.get_length());
        break;
      }
      // fall through
#if __cplusplus >= 201703L
      [[fallthrough]];
#endif
    default: {
      CconvertIfNecessary conv(result_cs_, value, length, value_cs);
      if (chunk_result_)
        chunk_enc_.field_string(conv.get_ptr(), conv.get_length());
      else
        row_enc_.field_string(conv.get_ptr(), conv.get_length());
      break;
    }
  }
  return 0;
}

/****** Getting execution status ******/
void CstreamingCommandDelegate::handle_ok(uint32_t server_status,
                                          uint32_t statement_warn_count,
                                          uint64_t affected_rows,
                                          uint64_t last_insert_id,
                                          const char *const message) {
  if (sent_result_ && !(server_status & SERVER_MORE_RESULTS_EXISTS)) {
    wait_for_fetch_done_ = false;

    /// flush all chunks before fetch done
    if (chunk_result_ && !chunk_enc_.chunk_empty()) chunk_enc_.send_chunk();

#ifndef MYSQL8
    if (feedback_) {
      auto thd = current_thd;
      PolarXRPC::Resultset::FetchDone msg;
      if (thd->feed_back_index_len > 0) {
        std::string chosen_index(thd->feed_back_index_buf,
                                 thd->feed_back_index_len);
        msg.set_chosen_index(chosen_index);
      }
      msg.set_examined_row_count(thd->get_examined_row_count());
      msg_enc().encode_protobuf_message<protocol::tags::FetchDone::server_id>(
          msg);
    } else
#endif
      msg_enc().encode_fetch_done();
    trigger_on_message(PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE);
  }

  if (!handle_ok_received_ && !wait_for_fetch_done_ &&
      try_send_notices(server_status, statement_warn_count, affected_rows,
                       last_insert_id, message)) {
    msg_enc().encode_stmt_execute_ok();
    trigger_on_message(PolarXRPC::ServerMessages::SQL_STMT_EXECUTE_OK);
  }
}

void CstreamingCommandDelegate::handle_error(uint sql_errno,
                                             const char *const err_msg,
                                             const char *const sqlstate) {
  if (handle_ok_received_) {
    msg_enc().encode_fetch_more_resultsets();
    trigger_on_message(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS);
  }
  handle_ok_received_ = false;

  uint32_t error = 0;
  convert_error_message(err_msg_buf_, sizeof(err_msg_buf_),
                        current_thd->variables.character_set_results, err_msg,
                        strlen(err_msg), system_charset_info, &error);
  CcommandDelegate::handle_error(sql_errno, err_msg_buf_, sqlstate);
}

bool CstreamingCommandDelegate::try_send_notices(
    const uint32_t server_status, const uint32_t statement_warn_count,
    const uint64_t affected_rows, const uint64_t last_insert_id,
    const char *const message) {
  CcommandDelegate::handle_ok(server_status, statement_warn_count,
                              affected_rows, last_insert_id, message);
  return true;
}

void CstreamingCommandDelegate::on_destruction() {
  if (send_notice_deferred_) {
    try_send_notices(info_.server_status, info_.num_warnings,
                     info_.affected_rows, info_.last_insert_id,
                     info_.message.c_str());
    msg_enc().encode_stmt_execute_ok();
    trigger_on_message(PolarXRPC::ServerMessages::SQL_STMT_EXECUTE_OK);
    send_notice_deferred_ = false;
  }
}

bool CstreamingCommandDelegate::defer_on_warning(
    const uint32_t server_status, const uint32_t statement_warn_count,
    const uint64_t affected_rows, const uint64_t last_insert_id,
    const char *const message) {
  if (!send_notice_deferred_) {
    CcommandDelegate::handle_ok(server_status, statement_warn_count,
                                affected_rows, last_insert_id, message);
    if (statement_warn_count > 0) {
      // We cannot send a warning at this point because it would use
      // m_session->data_context() in here and we are already in
      // data_context.execute(). That is why we will deffer the whole notice
      // sending after we are done.
      send_notice_deferred_ = true;
      return true;
    }
  } else {
    send_warnings(session_, encoder_);
    trigger_on_message(PolarXRPC::ServerMessages::NOTICE);
  }
  return false;
}

void CstreamingCommandDelegate::handle_fetch_done_more_results(
    uint32_t server_status) {
  const auto out_params = (server_status & SERVER_PS_OUT_PARAMS) != 0;
  if (handle_ok_received_ && !out_params) {
    msg_enc().encode_fetch_more_resultsets();
    trigger_on_message(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_RESULTSETS);
  }
}

void CstreamingCommandDelegate::end_result_metadata_handle_fetch(
    uint32_t server_status) {
  if ((server_status & SERVER_PS_OUT_PARAMS) != 0) {
    msg_enc().encode_fetch_out_params();
    trigger_on_message(
        PolarXRPC::ServerMessages::RESULTSET_FETCH_DONE_MORE_OUT_PARAMS);
  }
  handle_fetch_done_more_results(server_status);
}

void CstreamingCommandDelegate::handle_out_param_in_handle_ok(
    uint32_t server_status) {
  handle_fetch_done_more_results(server_status);

  const auto out_params = (server_status & SERVER_PS_OUT_PARAMS) != 0;
  if (out_params) wait_for_fetch_done_ = true;

  const auto more_results = (server_status & SERVER_MORE_RESULTS_EXISTS) != 0;
  handle_ok_received_ = sent_result_ && more_results && !out_params;
}

void CstreamingCommandDelegate::reset() {
  sent_result_ = false;
  handle_ok_received_ = false;
  result_cs_ = nullptr;
  CcommandDelegate::reset();
}

}  // namespace polarx_rpc
