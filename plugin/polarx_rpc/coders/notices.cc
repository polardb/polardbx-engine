//
// Created by zzy on 2022/9/5.
//

#include <string>
#include <vector>

#include "callback_command_delegate.h"
#include "protocol_fwd.h"

#include "notices.h"

namespace polarx_rpc {

namespace {

class CwarningResultset final {
 private:
  using Row = CcallbackCommandDelegate::Row_data;
  using Field = CcallbackCommandDelegate::Field_value;
  using Field_list = std::vector<Field *>;

  CpolarxEncoder &encoder_;
  const bool skip_single_error_;
  CcallbackCommandDelegate delegate_;

  Row row_;
  std::string last_error_;
  uint32_t num_errors_{0u};

  using Warning = ::PolarXRPC::Notice::Warning;

  Row *start_row() {
    row_.clear();
    return &row_;
  }

  static inline Warning::Level get_warning_level(const std::string &level) {
    static const char *const ERROR_STRING = "Error";
    static const char *const WARNING_STRING = "Warning";
    if (level == WARNING_STRING) return Warning::WARNING;
    if (level == ERROR_STRING) return Warning::ERROR;
    return Warning::NOTE;
  }

  bool end_row(Row *row) {
    if (!last_error_.empty()) {
      encoder_.message_encoder().encode_notice(
          ::PolarXRPC::Notice::Frame_Type_WARNING,
          ::PolarXRPC::Notice::Frame_Scope_LOCAL, last_error_);
      last_error_.clear();
    }

    Field_list &fields = row->fields;
    if (fields.size() != 3) return false;

    const Warning::Level level = get_warning_level(*fields[0]->value.v_string);

    Warning warning;
    warning.set_level(level);
    warning.set_code(
        static_cast<google::protobuf::uint32>(fields[1]->value.v_long));
    warning.set_msg(*fields[2]->value.v_string);

    std::string data;
    warning.SerializeToString(&data);

    if (level == Warning::ERROR) {
      ++num_errors_;
      if (skip_single_error_ && (num_errors_ <= 1)) {
        last_error_ = data;
        return true;
      }
    }

    encoder_.message_encoder().encode_notice(
        ::PolarXRPC::Notice::Frame_Type_WARNING,
        ::PolarXRPC::Notice::Frame_Scope_LOCAL, data);
    return true;
  }

 public:
  CwarningResultset(CpolarxEncoder &encoder, const bool skip_single_error)
      : encoder_(encoder),
        skip_single_error_(skip_single_error),
        delegate_(std::bind(&CwarningResultset::start_row, this),
                  std::bind(&CwarningResultset::end_row, this,
                            std::placeholders::_1)) {}

  inline CcommandDelegate &delegate() { return delegate_; }
};

}  // namespace

err_t send_warnings(CsessionBase &session, CpolarxEncoder &encoder,
                    bool skip_single_error) {
  static const std::string q = "SHOW WARNINGS";
  CwarningResultset resultset(encoder, skip_single_error);
  // send warnings as notices
  return session.execute_sql(q.data(), q.length(), resultset.delegate());
}

}  // namespace polarx_rpc
