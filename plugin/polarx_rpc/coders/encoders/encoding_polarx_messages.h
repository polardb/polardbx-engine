/*
 * Copyright (c) 2019, 2022, Oracle and/or its affiliates.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License, version 2.0,
 * as published by the Free Software Foundation.
 *
 * This program is also distributed with certain software (including
 * but not limited to OpenSSL) that is licensed under separate terms,
 * as designated in a particular file or component or in included license
 * documentation.  The authors of MySQL hereby grant you an additional
 * permission to link the program and your derivative works with the
 * separately licensed software that they have included with MySQL.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License, version 2.0, for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA
 */

#pragma once

#include <cassert>
#include <cstdint>
#include <string>
#include <utility>

#include "my_dbug.h"  // NOLINT(build/include_subdir)

#include "../protocol_fwd.h"
#include "encoding_descriptors.h"
#include "encoding_polarx_protocol.h"

namespace polarx_rpc {
namespace protocol {

template <typename Base_type>
class XMessage_encoder_base : public Base_type {
 private:
  constexpr static PolarXRPC::Notice::Frame_Type k_state_change =
      PolarXRPC::Notice::Frame_Type_SESSION_STATE_CHANGED;

  constexpr static PolarXRPC::Notice::Frame_Scope k_local =
      PolarXRPC::Notice::Frame_Scope_LOCAL;

  constexpr static PolarXRPC::Notice::SessionStateChanged_Parameter k_message =
      PolarXRPC::Notice::SessionStateChanged::PRODUCED_MESSAGE;

  constexpr static PolarXRPC::Notice::SessionStateChanged_Parameter k_expired =
      PolarXRPC::Notice::SessionStateChanged::ACCOUNT_EXPIRED;

  constexpr static PolarXRPC::Notice::SessionStateChanged_Parameter
      k_generated_insert_id =
          PolarXRPC::Notice::SessionStateChanged::GENERATED_INSERT_ID;

  constexpr static PolarXRPC::Datatypes::Scalar_Type k_string =
      PolarXRPC::Datatypes::Scalar_Type_V_STRING;

  constexpr static PolarXRPC::Datatypes::Scalar_Type k_v_uint =
      PolarXRPC::Datatypes::Scalar_Type_V_UINT;

  constexpr static PolarXRPC::Notice::SessionStateChanged_Parameter
      k_rows_affected = PolarXRPC::Notice::SessionStateChanged::ROWS_AFFECTED;

  constexpr static PolarXRPC::Notice::SessionStateChanged_Parameter
      k_client_id = PolarXRPC::Notice::SessionStateChanged::CLIENT_ID_ASSIGNED;

 public:
  // Constructor inheritance doesn't work in solaris
  // for template<T> class X: T {...};
  //
  // ```using Base_type::Base_type;```
  //
  // Using template constructor instead:
  template <typename... Args>
  explicit XMessage_encoder_base(Args &&...args)
      : Base_type(std::forward<Args>(args)...) {}

  void encode_compact_metadata(const uint8_t type, const uint8_t org_type,
                               const uint64_t *collation,
                               const uint32_t *decimals, const uint32_t *length,
                               const uint32_t *flags,
                               const uint32_t *content_type,
                               const uint32_t *org_flags) {
    using Tags = tags::ColumnMetaData;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<Tags::server_id, 100>();
    Base_type::template encode_field_enum<Tags::type>(type);
    Base_type::template encode_field_enum<Tags::original_type>(org_type);
    Base_type::template encode_optional_field_var_uint64<Tags::collation>(
        collation);
    Base_type::template encode_optional_field_var_uint32<
        Tags::fractional_digits>(decimals);
    Base_type::template encode_optional_field_var_uint32<Tags::length>(length);
    Base_type::template encode_optional_field_var_uint32<Tags::flags>(flags);
    Base_type::template encode_optional_field_var_uint32<Tags::content_type>(
        content_type);
    Base_type::template encode_optional_field_var_uint32<Tags::original_flags>(
        org_flags);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_full_metadata(const char *col_name, const char *org_col_name,
                            const char *table_name, const char *org_table_name,
                            const char *db_name, const char *catalog,
                            const uint8_t type, const uint8_t org_type,
                            const uint64_t *collation, const uint32_t *decimals,
                            const uint32_t *length, const uint32_t *flags,
                            const uint32_t *content_type,
                            const uint32_t *org_flags) {
    using Tags = tags::ColumnMetaData;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<Tags::server_id, 100>();
    Base_type::template encode_field_enum<Tags::type>(type);
    Base_type::template encode_field_enum<Tags::original_type>(org_type);
    Base_type::template encode_optional_field_var_uint64<Tags::collation>(
        collation);
    Base_type::template encode_optional_field_var_uint32<
        Tags::fractional_digits>(decimals);
    Base_type::template encode_optional_field_var_uint32<Tags::length>(length);
    Base_type::template encode_optional_field_var_uint32<Tags::flags>(flags);
    Base_type::template encode_optional_field_var_uint32<Tags::content_type>(
        content_type);
    Base_type::template encode_optional_field_var_uint32<Tags::original_flags>(
        org_flags);

    /// following will check buffer size
    Base_type::template encode_field_string<Tags::name>(col_name);
    Base_type::template encode_field_string<Tags::original_name>(org_col_name);
    Base_type::template encode_field_string<Tags::table>(table_name);
    Base_type::template encode_field_string<Tags::original_table>(
        org_table_name);
    Base_type::template encode_field_string<Tags::schema>(db_name);
    Base_type::template encode_field_string<Tags::catalog>(catalog);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice_rows_affected(const uint64_t rows) {
    using FrameTags = tags::Frame;
    using StateTags = tags::SessionStateChanged;
    using ScalarTags = tags::Scalar;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 145>();

    Base_type::template encode_field_const_var_uint<FrameTags::type,
                                                    k_state_change>();
    Base_type::template encode_field_const_enum<FrameTags::scope, k_local>();

    auto field_payload_start =
        Base_type::template begin_delimited_field<FrameTags::payload>();
    Base_type::template encode_field_const_enum<StateTags::param,
                                                k_rows_affected>();

    auto field_value_start =
        Base_type::template begin_delimited_field<StateTags::value>();
    Base_type::template encode_field_const_enum<ScalarTags::type, k_v_uint>();
    Base_type::template encode_field_var_uint64<ScalarTags::v_unsigned_int>(
        rows);
    Base_type::end_delimited_field(field_value_start);
    Base_type::end_delimited_field(field_payload_start);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice_client_id(const uint64_t id) {
    using FrameTags = tags::Frame;
    using StateTags = tags::SessionStateChanged;
    using ScalarTags = tags::Scalar;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 145>();
    Base_type::template encode_field_const_var_uint<FrameTags::type,
                                                    k_state_change>();
    Base_type::template encode_field_const_enum<FrameTags::scope, k_local>();
    auto field_payload_start =
        Base_type::template begin_delimited_field<FrameTags::payload>();
    Base_type::template encode_field_const_enum<StateTags::param,
                                                k_client_id>();
    auto field_value_start =
        Base_type::template begin_delimited_field<StateTags::value>();
    Base_type::template encode_field_const_enum<ScalarTags::type, k_v_uint>();
    Base_type::template encode_field_var_uint64<ScalarTags::v_unsigned_int>(id);
    Base_type::end_delimited_field(field_value_start);
    Base_type::end_delimited_field(field_payload_start);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice_expired() {
    using FrameTags = tags::Frame;
    using StateTags = tags::SessionStateChanged;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 85>();
    Base_type::template encode_field_const_var_uint<FrameTags::type,
                                                    k_state_change>();
    Base_type::template encode_field_const_enum<FrameTags::scope, k_local>();
    auto field_payload_start =
        Base_type::template begin_delimited_field<FrameTags::payload>();
    Base_type::template encode_field_const_enum<StateTags::param, k_expired>();
    Base_type::end_delimited_field(field_payload_start);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice_generated_insert_id(const uint64_t last_insert_id) {
    using FrameTags = tags::Frame;
    using StateTags = tags::SessionStateChanged;
    using ScalarTags = tags::Scalar;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 145>();
    Base_type::template encode_field_const_var_uint<FrameTags::type,
                                                    k_state_change>();
    Base_type::template encode_field_const_enum<FrameTags::scope, k_local>();
    auto field_payload_start =
        Base_type::template begin_delimited_field<FrameTags::payload>();
    Base_type::template encode_field_const_enum<StateTags::param,
                                                k_generated_insert_id>();
    auto field_value_start =
        Base_type::template begin_delimited_field<StateTags::value>();
    Base_type::template encode_field_const_enum<ScalarTags::type, k_v_uint>();
    Base_type::template encode_field_var_uint64<ScalarTags::v_unsigned_int>(
        last_insert_id);
    Base_type::end_delimited_field(field_value_start);
    Base_type::end_delimited_field(field_payload_start);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice_text_message(const std::string &message) {
    using FrameTags = tags::Frame;
    using StateTags = tags::SessionStateChanged;
    using ScalarTags = tags::Scalar;
    using StringTags = tags::String;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 145>();
    Base_type::template encode_field_const_var_uint<FrameTags::type,
                                                    k_state_change>();
    Base_type::template encode_field_const_enum<FrameTags::scope, k_local>();
    auto field_payload_start =
        Base_type::template begin_delimited_field<FrameTags::payload, 4>();
    Base_type::template encode_field_const_enum<StateTags::param, k_message>();
    auto field_value_start =
        Base_type::template begin_delimited_field<StateTags::value, 4>();
    Base_type::template encode_field_const_enum<ScalarTags::type, k_string>();
    auto string_start =
        Base_type::template begin_delimited_field<ScalarTags::v_string, 4>();
    Base_type::template encode_field_string<StringTags::value>(message);
    Base_type::end_delimited_field(string_start);
    Base_type::end_delimited_field(field_value_start);
    Base_type::end_delimited_field(field_payload_start);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_notice(const uint32_t type, const uint32_t scope,
                     const std::string &data) {
    using FrameTags = tags::Frame;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 40>();
    Base_type::template encode_field_var_uint32<FrameTags::type>(type);
    Base_type::template encode_field_enum<FrameTags::scope>(scope);
    Base_type::template encode_field_string<FrameTags::payload>(data);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_global_notice(const uint32_t type, const std::string &data) {
    using FrameTags = tags::Frame;

    /// The buffer size should large enough.
    auto xmsg_start =
        Base_type::template begin_xmessage<FrameTags::server_id, 25>();
    Base_type::template encode_field_var_uint32<FrameTags::type>(type);
    Base_type::template encode_field_string<FrameTags::payload>(data);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_fetch_more_resultsets() {
    Base_type::template empty_xmessage<
        tags::FetchDoneMoreResultsets::server_id>();
  }

  void encode_fetch_out_params() {
    Base_type::template empty_xmessage<
        tags::FetchDoneMoreOutParams::server_id>();
  }

  void encode_fetch_suspended() {
    Base_type::template empty_xmessage<tags::FetchSuspended::server_id>();
  }

  void encode_fetch_done() {
    Base_type::template empty_xmessage<tags::FetchDone::server_id>();
  }

  void encode_token_done() {
    Base_type::template empty_xmessage<tags::TokenDone::server_id>();
  }

  void encode_stmt_execute_ok() {
    Base_type::template empty_xmessage<tags::StmtExecuteOk::server_id>();
  }

  void encode_ok() {
    Base_type::template empty_xmessage<tags::Ok::server_id>();
  }

  void encode_ok(const std::string &message) {
    auto xmsg_start =
        Base_type::template begin_xmessage<tags::Ok::server_id, 5>();
    Base_type::template encode_field_string<tags::Ok::msg>(message);
    Base_type::end_xmessage(xmsg_start);
  }

  void encode_error(const int severity, const uint32_t code,
                    const std::string &msg, const std::string &sql_state) {
    auto xmsg_start =
        Base_type::template begin_xmessage<tags::Error::server_id, 40>();
    Base_type::template encode_field_enum<tags::Error::severity>(severity);
    Base_type::template encode_field_var_uint32<tags::Error::code>(code);
    Base_type::template encode_field_string<tags::Error::msg>(msg);
    Base_type::template encode_field_string<tags::Error::sql_state>(sql_state);
    Base_type::end_xmessage(xmsg_start);
  }

  template <uint8_t id>
  void encode_xmessage(const std::string &serialized_xmessage) {
    auto xmsg_start = Base_type::template begin_xmessage<id, 100>();
    Base_type::encode_raw(
        reinterpret_cast<const uint8_t *>(serialized_xmessage.c_str()),
        serialized_xmessage.length());
    Base_type::end_xmessage(xmsg_start);
  }

  template <uint8_t id>
  void encode_protobuf_message(const ProtoMsg &message) {
    std::string out_serialized;
    message.SerializeToString(&out_serialized);
    encode_xmessage<id>(out_serialized);
  }
};

class PolarX_Message_encoder
    : public XMessage_encoder_base<PolarX_Protocol_encoder> {
 public:
  using XMessage_encoder_base<PolarX_Protocol_encoder>::XMessage_encoder_base;
};

}  // namespace protocol
}  // namespace polarx_rpc
