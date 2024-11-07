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

#include <cstdint>
#include <string>
#include <vector>

#include "decimal.h"
#include "m_ctype.h"
#include "my_compiler.h"
#include "mysql/service_command.h"

#include "../common_define.h"
#include "../global_defines.h"
#include "../utility/error.h"

namespace polarx_rpc {

class CcommandDelegate {
  NO_COPY(CcommandDelegate)

 public:
  struct info_t final {
    uint64_t affected_rows{0};
    uint64_t last_insert_id{0};
    uint32_t num_warnings{0};
    std::string message;
    uint32_t server_status{0};
  };

  struct field_type_t final {
    enum_field_types type;
    unsigned int flags;
  };

  using field_types_t = std::vector<field_type_t>;

 public:
  CcommandDelegate() = default;
  virtual ~CcommandDelegate() = default;

  CcommandDelegate(CcommandDelegate &&) = default;
  CcommandDelegate &operator=(CcommandDelegate &&) = default;

  inline err_t get_error() const {
    if (sql_errno_ == 0)
      return err_t::Success();
    else
      return {static_cast<int>(sql_errno_), err_msg_, sql_state_};
  }

  inline const info_t &get_info() const { return info_; }
  inline void set_field_types(const field_types_t &field_types) {
    field_types_ = field_types;
  }
  inline const field_types_t &get_field_types() const { return field_types_; }

  inline bool killed() const { return killed_; }
  inline bool got_eof() const { return got_eof_; }

  virtual void reset() {
    info_ = info_t();
    sql_errno_ = 0;
    killed_ = false;
    streaming_metadata_ = false;
    field_types_.clear();
    got_eof_ = false;
  }

  static inline const st_command_service_cbs *callbacks() {
    static st_command_service_cbs cbs = {
        &CcommandDelegate::call_start_result_metadata,
        &CcommandDelegate::call_field_metadata,
        &CcommandDelegate::call_end_result_metadata,
        &CcommandDelegate::call_start_row,
        &CcommandDelegate::call_end_row,
        &CcommandDelegate::call_abort_row,
        &CcommandDelegate::call_get_client_capabilities,
        &CcommandDelegate::call_get_null,
        &CcommandDelegate::call_get_integer,
        &CcommandDelegate::call_get_longlong,
        &CcommandDelegate::call_get_decimal,
        &CcommandDelegate::call_get_double,
        &CcommandDelegate::call_get_date,
        &CcommandDelegate::call_get_time,
        &CcommandDelegate::call_get_datetime,
        &CcommandDelegate::call_get_string,
        &CcommandDelegate::call_handle_ok,
        &CcommandDelegate::call_handle_error,
        &CcommandDelegate::call_shutdown,
#ifdef MYSQL8PLUS
        nullptr};
#else
    };
#endif
    return &cbs;
  }

  virtual enum cs_text_or_binary representation() const = 0;

 protected:
  info_t info_;
  field_types_t field_types_;
  uint sql_errno_ = 0;
  std::string err_msg_;
  std::string sql_state_;

  bool killed_ = false;
  bool streaming_metadata_ = false;
  bool got_eof_ = false;

 public:
  /*** Getting metadata ***/
  /*
    Indicates beginning of metadata for the result set

    @param num_cols Number of fields being sent
    @param flags    Flags to alter the metadata sending
    @param resultcs Charset of the result set

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int start_result_metadata(
      uint num_cols, uint flags MY_ATTRIBUTE((unused)),
      const CHARSET_INFO *resultcs MY_ATTRIBUTE((unused))) {
    field_types_.clear();

    /*
      std::vector often reserves memory using following
      allocation function:

          f(n)=2^n

      where "n" is number of inserted elements and f(n) describes number
      of total elements that current memory block can hold.
      Thus if user does "push_back", "n" times then following number of
      allocations will occur:

           |Number of push_back |Allocations |Reserved |
           |--------------------|------------|---------|
           |0                   |0           |0        |
           |1                   |1           |1        |
           |2                   |2           |2        |
           |3                   |3           |4        |
           |4                   |3           |4        |
           |5                   |4           |8        |
           |6                   |4           |8        |

      Following reservation should give little boost for
      resultsets with 3+ columns.
    */
    field_types_.reserve(num_cols);
    return 0;
  }

  /*
    Field metadata is provided via this callback

    @param field   Field's metadata (see field.h)
    @param charset Field's charset

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int field_metadata(struct st_send_field *field,
                             const CHARSET_INFO *charset
                                 MY_ATTRIBUTE((unused))) {
    field_type_t type = {field->type, field->flags};
    field_types_.push_back(type);

    return 0;
  }

  /*
    Indicates end of metadata for the result set

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int end_result_metadata(uint server_status MY_ATTRIBUTE((unused)),
                                  uint warn_count MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Indicates the beginning of a new row in the result set/metadata

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int start_row() { return 0; }

  /*
   Indicates the end of the current row in the result set/metadata

   @returns
   true  an error occurred, server will abort the command
   false ok
  */
  virtual int end_row() { return 0; }

  /*
    An error occurred during execution

    @details This callback indicates that an error occurreded during command
    execution and the partial row should be dropped. Server will raise error
    and return.

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual void abort_row() {}

  /*
    Return client's capabilities (see mysql_com.h, CLIENT_*)

    @return Bitmap of client's capabilities
  */
  virtual ulong get_client_capabilities() { return 0; }

  /****** Getting data ******/
  /*
    Receive NULL value from server

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_null() { return 0; }

  /*
    Get TINY/SHORT/LONG value from server

    @param value         Value received

    @note In order to know which type exactly was received, the plugin must
    track the metadata that was sent just prior to the result set.

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_integer(longlong value MY_ATTRIBUTE((unused))) { return 0; }

  /*
    Get LONGLONG value from server

    @param value         Value received
    @param unsigned_flag true <=> value is unsigned

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_longlong(longlong value MY_ATTRIBUTE((unused)),
                           uint unsigned_flag MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Receive DECIMAL value from server

    @param value Value received

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_decimal(const decimal_t *value MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Get FLOAT/DOUBLE from server

    @param value    Value received
    @param decimals Number of decimals

    @note In order to know which type exactly was received, the plugin must
    track the metadata that was sent just prior to the result set.

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_double(double value MY_ATTRIBUTE((unused)),
                         uint32 decimals MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Get DATE value from server

    @param value    Value received

    @returns
    true  an error occurred during storing, server will abort the command
    false ok
  */
  virtual int get_date(const MYSQL_TIME *value MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Get TIME value from server

    @param value    Value received
    @param decimals Number of decimals

    @returns
    true  an error occurred during storing, server will abort the command
    false ok
  */
  virtual int get_time(const MYSQL_TIME *value MY_ATTRIBUTE((unused)),
                       uint decimals MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Get DATETIME value from server

    @param value    Value received
    @param decimals Number of decimals

    @returns
    true  an error occurred during storing, server will abort the command
    false ok
  */
  virtual int get_datetime(const MYSQL_TIME *value MY_ATTRIBUTE((unused)),
                           uint decimals MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /*
    Get STRING value from server

    @param value   Value received
    @param length  Value's length
    @param valuecs Value's charset

    @returns
    true  an error occurred, server will abort the command
    false ok
  */
  virtual int get_string(const char *const value MY_ATTRIBUTE((unused)),
                         size_t length MY_ATTRIBUTE((unused)),
                         const CHARSET_INFO *const valuecs
                             MY_ATTRIBUTE((unused))) {
    return 0;
  }

  /****** Getting execution status ******/
  /*
    Command ended with success

    @param server_status        Status of server (see mysql_com.h,
    SERVER_STATUS_*)
    @param statement_warn_count Number of warnings thrown during execution
    @param affected_rows        Number of rows affected by the command
    @param last_insert_id       Last insert id being assigned during execution
    @param message              A message from server
  */
  virtual void handle_ok(uint32_t server_status, uint32_t statement_warn_count,
                         uint64_t affected_rows, uint64_t last_insert_id,
                         const char *const message) {
    info_.server_status = server_status;
    info_.num_warnings = statement_warn_count;
    info_.affected_rows = affected_rows;
    info_.last_insert_id = last_insert_id;
    info_.message = message ? message : "";
  }

  /*
   Command ended with ERROR

   @param sql_errno Error code
   @param err_msg   Error message
   @param sqlstate  SQL state correspongin to the error code
  */
  virtual void handle_error(uint sql_errno, const char *const err_msg,
                            const char *const sql_state) {
    sql_errno_ = sql_errno;
    err_msg_ = err_msg ? err_msg : "";
    sql_state_ = sql_state ? sql_state : "";
  }

  /*
   Session was shutdown while command was running

  */
  virtual void shutdown(int flag MY_ATTRIBUTE((unused))) { killed_ = true; }

 private:
  static int call_start_result_metadata(void *ctx, uint num_cols, uint flags,
                                        const CHARSET_INFO *resultcs) {
    auto self = static_cast<CcommandDelegate *>(ctx);
    self->streaming_metadata_ = true;
    return self->start_result_metadata(num_cols, flags, resultcs);
  }

  static int call_field_metadata(void *ctx, struct st_send_field *field,
                                 const CHARSET_INFO *charset) {
    return static_cast<CcommandDelegate *>(ctx)->field_metadata(field, charset);
  }

  static int call_end_result_metadata(void *ctx, uint server_status,
                                      uint warn_count) {
    auto self = static_cast<CcommandDelegate *>(ctx);
    auto tmp = self->end_result_metadata(server_status, warn_count);
    self->streaming_metadata_ = false;
    return tmp;
  }

  static int call_start_row(void *ctx) {
    auto self = static_cast<CcommandDelegate *>(ctx);
    if (self->streaming_metadata_) return false;
    return self->start_row();
  }

  static int call_end_row(void *ctx) {
    auto self = static_cast<CcommandDelegate *>(ctx);
    if (self->streaming_metadata_) return false;
    return self->end_row();
  }

  static void call_abort_row(void *ctx) {
    static_cast<CcommandDelegate *>(ctx)->abort_row();
  }

  static ulong call_get_client_capabilities(void *ctx) {
    return static_cast<CcommandDelegate *>(ctx)->get_client_capabilities();
  }

  static int call_get_null(void *ctx) {
    return static_cast<CcommandDelegate *>(ctx)->get_null();
  }

  static int call_get_integer(void *ctx, longlong value) {
    return static_cast<CcommandDelegate *>(ctx)->get_integer(value);
  }

  static int call_get_longlong(void *ctx, longlong value, uint unsigned_flag) {
    return static_cast<CcommandDelegate *>(ctx)->get_longlong(value,
                                                              unsigned_flag);
  }

  static int call_get_decimal(void *ctx, const decimal_t *value) {
    return static_cast<CcommandDelegate *>(ctx)->get_decimal(value);
  }

  static int call_get_double(void *ctx, double value, uint32 decimals) {
    return static_cast<CcommandDelegate *>(ctx)->get_double(value, decimals);
  }

  static int call_get_date(void *ctx, const MYSQL_TIME *value) {
    return static_cast<CcommandDelegate *>(ctx)->get_date(value);
  }

  static int call_get_time(void *ctx, const MYSQL_TIME *value, uint decimals) {
    return static_cast<CcommandDelegate *>(ctx)->get_time(value, decimals);
  }

  static int call_get_datetime(void *ctx, const MYSQL_TIME *value,
                               uint decimals) {
    return static_cast<CcommandDelegate *>(ctx)->get_datetime(value, decimals);
  }

  static int call_get_string(void *ctx, const char *const value, size_t length,
                             const CHARSET_INFO *const valuecs) {
    return static_cast<CcommandDelegate *>(ctx)->get_string(value, length,
                                                            valuecs);
  }

  static void call_handle_ok(void *ctx, uint server_status,
                             uint statement_warn_count, ulonglong affected_rows,
                             ulonglong last_insert_id,
                             const char *const message) {
    auto context = static_cast<CcommandDelegate *>(ctx);

    if (!context->got_eof_) {
      context->got_eof_ = !(
          server_status & (SERVER_MORE_RESULTS_EXISTS | SERVER_PS_OUT_PARAMS));
    }

    context->handle_ok(server_status, statement_warn_count, affected_rows,
                       last_insert_id, message);
  }

  static void call_handle_error(void *ctx, uint sql_errno,
                                const char *const err_msg,
                                const char *const sqlstate) {
    static_cast<CcommandDelegate *>(ctx)->handle_error(sql_errno, err_msg,
                                                       sqlstate);
  }

  static void call_shutdown(void *ctx, int flag) {
    static_cast<CcommandDelegate *>(ctx)->shutdown(flag);
  }
};

}  // namespace polarx_rpc
