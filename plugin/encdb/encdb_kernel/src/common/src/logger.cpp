/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "logger.h"
#include <cstdarg>
#include <cstdio>
#include <iostream>
#include <string>
#include "EncDBConfig.h"
#include "storage.h"

bool encdb_elog(int elevel, const char *filename, int lineno,
                const char *funcname, const char *fmt, ...) {
  const char *logHeaderFmt = "-- encdb log -- %s,%d,%s: ";

  int reqSize;
  va_list va;
  va_start(va, fmt);
  reqSize = snprintf(NULL, 0, logHeaderFmt, filename, lineno, funcname);
  reqSize += vsnprintf(NULL, 0, fmt, va);
  va_end(va);
  std::string buf(reqSize + 1, '\0');
  va_start(va, fmt);
  reqSize =
      snprintf(&buf[0], buf.size(), logHeaderFmt, filename, lineno, funcname);
  vsnprintf(&buf[reqSize], buf.size() - reqSize, fmt, va);
  va_end(va);

  // buf is a fully formatted message that may embed user data containing
  // '%' (e.g. a rule name inside a failed SQL text).  It must be passed as
  // an argument, never as the format string, or vsnprintf inside
  // encdb_log_message would consume nonexistent varargs and crash.
  encdb_log_message(static_cast<LogLevel>(elevel), "%s", buf.c_str());

  return true;
}
