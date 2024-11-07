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


extern const char *error_message_for_error_log(int mysql_errno);

#include "sql/consensus/consensus_err.h"


extern "C" {

typedef enum {
  EASY_LOG_OFF = 1,
  EASY_LOG_FATAL,
  EASY_LOG_ERROR,
  EASY_LOG_WARN,
  EASY_LOG_INFO,
  EASY_LOG_DEBUG,
  EASY_LOG_TRACE,
  EASY_LOG_ALL
} easy_log_level_t;

extern int easy_vsnprintf(char *buf, size_t size, const char *fmt, va_list args);

typedef void (*easy_log_print_pt)(easy_log_level_t log_level, const char *message);
typedef void (*easy_log_format_pt)(int level, const char *file, int line, const char *function, const char *fmt, ...);

void easy_log_print_default(easy_log_level_t log_level, const char *message);
void easy_log_format_default(int level, const char *file, int line,
                             const char *function, const char *fmt, ...);
easy_log_print_pt easy_log_print = easy_log_print_default;
easy_log_format_pt easy_log_format = easy_log_format_default;
easy_log_level_t easy_log_level = EASY_LOG_WARN;

/**
 * 加上日志
 */
void easy_log_format_default(int level, const char *file, int line,
                             const char *function, const char *fmt, ...) {
  char buffer[LOG_BUFF_MAX];
  va_list args;
  va_start(args, fmt);
  auto len = easy_vsnprintf(buffer, sizeof(buffer), fmt, args);
  va_end(args);

  while (buffer[len - 1] == '\n') len--;
  buffer[len] = '\0';

  easy_log_print(static_cast<easy_log_level_t>(level), buffer);
}

void raft_log_print(easy_log_level_t log_level, const char *message) {
  switch(log_level) {
    case EASY_LOG_OFF:
      xp::system(ER_XP_PROTO) << message;
      break;
    case EASY_LOG_FATAL:
      xp::fatal(ER_XP_PROTO) << message;
      break;
    case EASY_LOG_ERROR:
      xp::error(ER_XP_PROTO) << message;
      break;
    case EASY_LOG_WARN:
      xp::warn(ER_XP_PROTO) << message;
      break;
    case EASY_LOG_INFO:
    case EASY_LOG_DEBUG:
    case EASY_LOG_TRACE:
      xp::info(ER_XP_PROTO) << message;
      break;
    case EASY_LOG_ALL:
      xp::system(ER_XP_PROTO) << message;
    default:
      break;
  }
}

void easy_log_print_default(easy_log_level_t log_level, const char *message) {
  raft_log_print(log_level, message);
}

}

