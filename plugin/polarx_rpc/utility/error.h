//
// Created by zzy on 2022/7/27.
//

#pragma once

#include <cstdarg>
#include <cstdio>
#include <string>
#include <utility>
#include <sstream>

#include "my_sys.h"
#include "mysqld_error.h"

#include "../coders/protocol_fwd.h"

namespace polarx_rpc {

struct err_t final {
  static const int MAX_MESSAGE_LENGTH = 1024;

  int error;
  std::string message;
  std::string sql_state;
  enum Severity {
    OK = 0,
    ERROR = 1,
    FATAL = 2,
  } severity;

  err_t() : error(0), severity(OK) {}

  err_t(int e, std::string m, std::string state = "HY000", Severity sev = ERROR)
      : error(e),
        message(std::move(m)),
        sql_state(std::move(state)),
        severity(sev) {}

  err_t(int e, std::string state, Severity sev, const char *fmt, va_list args)
      : error(e), sql_state(std::move(state)), severity(sev) {
    char buffer[MAX_MESSAGE_LENGTH];
    std::snprintf(buffer, sizeof(buffer), fmt, args);
    message.assign(buffer);
  }

  explicit inline operator bool() const { return error != 0; }

  inline PolarXRPC::Error_Severity get_protocol_severity() const {
    return FATAL == severity ? PolarXRPC::Error::FATAL
                             : PolarXRPC::Error::ERROR;
  }

  static inline err_t Success(const char *msg, ...) {
    va_list ap;
    va_start(ap, msg);
    err_t tmp(0, "", err_t::OK, msg, ap);
    va_end(ap);
    return tmp;
  }

  static inline err_t Success() { return err_t(); }

  static inline err_t SQLError(const int error_code, ...) {
    va_list ap;
    va_start(ap, error_code);
    const auto format = my_get_err_msg(error_code);
    err_t tmp(error_code, "");
    if (nullptr != format)
      tmp = err_t(error_code, "HY000", err_t::ERROR, format, ap);
    va_end(ap);
    return tmp;
  }

  static inline err_t SQLError_access_denied() {
    return err_t(ER_ACCESS_DENIED_ERROR, "Invalid user or password");
  }

  static inline err_t Error(int e, const char *msg, ...) {
    va_list ap;
    va_start(ap, msg);
    err_t tmp(e, "HY000", err_t::ERROR, msg, ap);
    va_end(ap);
    return tmp;
  }

  static inline err_t Fatal(int e, const char *msg, ...) {
    va_list ap;
    va_start(ap, msg);
    err_t tmp(e, "HY000", err_t::FATAL, msg, ap);
    va_end(ap);
    return tmp;
  }

  static inline err_t Fatal(const err_t &err) {
    err_t error(err);
    error.severity = err_t::FATAL;
    return error;
  }
};

}  // namespace polarx_rpc
