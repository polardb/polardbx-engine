//
// Created by zzy on 2022/7/29.
//

#pragma once

#include <cstdint>

// TODO use code to generate this

namespace polarx_rpc {
namespace protocol {

namespace tags {

struct Ok {
  static constexpr uint32_t server_id = 0;

  static constexpr uint32_t msg = 1;
};

struct Error {
  static constexpr uint32_t server_id = 1;

  static constexpr uint32_t severity = 1;
  static constexpr uint32_t code = 2;
  static constexpr uint32_t sql_state = 4;
  static constexpr uint32_t msg = 3;
};

struct Capabilities {
  static constexpr uint32_t server_id = 2;

  static constexpr uint32_t capabilities = 1;
};

struct Capability {
  static constexpr uint32_t name = 1;
  static constexpr uint32_t value = 2;
};

struct Any {
  static constexpr uint32_t type = 1;
  static constexpr uint32_t scalar = 2;
  static constexpr uint32_t obj = 3;
  static constexpr uint32_t array = 4;
};

struct Scalar {
  static constexpr uint32_t type = 1;
  static constexpr uint32_t v_signed_int = 2;
  static constexpr uint32_t v_unsigned_int = 3;
  static constexpr uint32_t v_octets = 5;
  static constexpr uint32_t v_double = 6;
  static constexpr uint32_t v_float = 7;
  static constexpr uint32_t v_bool = 8;
  static constexpr uint32_t v_string = 9;
  static constexpr uint32_t v_position = 10;
  static constexpr uint32_t v_identifier = 11;
};

struct Octets {
  static constexpr uint32_t value = 1;
  static constexpr uint32_t content_type = 2;
};

struct String {
  static constexpr uint32_t value = 1;
  static constexpr uint32_t collation = 2;
};

struct Object {
  static constexpr uint32_t fld = 1;
};

struct ObjectField {
  static constexpr uint32_t key = 1;
  static constexpr uint32_t value = 2;
};

struct Array {
  static constexpr uint32_t value = 1;
};

struct StmtExecuteOk {
  static constexpr uint32_t server_id = 17;
};

struct AuthenticateContinue {
  static constexpr uint32_t server_id = 3;

  static constexpr uint32_t auth_data = 1;
};

struct AuthenticateOk {
  static constexpr uint32_t server_id = 4;

  static constexpr uint32_t auth_data = 1;
};

struct Frame {
  static constexpr uint32_t server_id = 11;

  static constexpr uint32_t type = 1;
  static constexpr uint32_t scope = 2;
  static constexpr uint32_t payload = 3;
};

struct Warning {
  static constexpr uint32_t level = 1;
  static constexpr uint32_t code = 2;
  static constexpr uint32_t msg = 3;
};

struct SessionVariableChanged {
  static constexpr uint32_t param = 1;
  static constexpr uint32_t value = 2;
};

struct SessionStateChanged {
  static constexpr uint32_t param = 1;
  static constexpr uint32_t value = 2;
};

struct GroupReplicationStateChanged {
  static constexpr uint32_t type = 1;
  static constexpr uint32_t view_id = 2;
};

struct ServerHello {};

struct FetchDoneMoreOutParams {
  static constexpr uint32_t server_id = 18;
};

struct FetchDoneMoreResultsets {
  static constexpr uint32_t server_id = 16;
};

struct FetchDone {
  static constexpr uint32_t server_id = 14;
};

struct FetchSuspended {
  static constexpr uint32_t server_id = 15;
};

struct ColumnMetaData {
  static constexpr uint32_t server_id = 12;

  static constexpr uint32_t type = 1;
  static constexpr uint32_t original_type = 2;
  static constexpr uint32_t name = 3;
  static constexpr uint32_t original_name = 4;
  static constexpr uint32_t table = 5;
  static constexpr uint32_t original_table = 6;
  static constexpr uint32_t schema = 7;
  static constexpr uint32_t catalog = 8;
  static constexpr uint32_t collation = 9;
  static constexpr uint32_t fractional_digits = 10;
  static constexpr uint32_t length = 11;
  static constexpr uint32_t flags = 12;
  static constexpr uint32_t content_type = 13;
  static constexpr uint32_t original_flags = 14;
};

struct Row {
  static constexpr uint32_t server_id = 13;

  static constexpr uint32_t field = 1;
};

struct TokenDone {
  static constexpr uint32_t server_id = 19;

  static constexpr uint32_t token_left = 1;
};

struct ResultTSO {
  static constexpr uint32_t server_id = 20;

  static constexpr uint32_t error_no = 1;
  static constexpr uint32_t ts = 2;
};

struct Chunk {
  static constexpr uint32_t server_id = 21;

  static constexpr uint32_t row_count = 1;
  static constexpr uint32_t columns = 2;
};

struct Column {
  static constexpr uint32_t null_bitmap = 1;
  static constexpr uint32_t fixed_size_column = 2;
  static constexpr uint32_t variable_size_column = 3;
};

struct ColumnData {
  static constexpr uint32_t value = 1;
};

struct GetFileInfoOK {
  static constexpr uint32_t server_id = 22;
};
struct TransferFileDataOK {
  static constexpr uint32_t server_id = 23;
};
struct FileManageOK {
  static constexpr uint32_t server_id = 24;
};

}  // namespace tags

}  // namespace protocol
}  // namespace polarx_rpc
