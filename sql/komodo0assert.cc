/*****************************************************************************

Copyright (c) 2013, 2025, Alibaba and/or its affiliates. All Rights Reserved.

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

/** @file sql/komodo0assert.cc
  Komodo assertion like ut0dbg

 Created 2025-12-01 by Jiyang.zhang
 *******************************************************/

#include <stdlib.h>
#include <thread>
#include <string>
#include <sstream>

#include "komodo0assert.h"
#include "my_dbug.h"
#include "my_sys.h"
#include "mysql/components/services/log_builtins.h"
#include "mysqld_error.h"

#ifndef UNIV_HOTBACKUP
#include "sql/log.h"
#endif /* !UNIV_HOTBACKUP */

namespace komodo {
static std::string to_string(std::thread::id thread_id) {
  try {
    std::stringstream ss;
    /* The documentation for this says "May throw implementation-defined
    exceptions." - we catch anything that might be thrown here. */
    ss << thread_id;
    return ss.str();
  } catch (...) {
    return "invalid_thread_id";
  }
}

/** Report a failed assertion.
@param[in] expr The failed assertion
@param[in] file Source file containing the assertion
@param[in] line Line number of the assertion */
[[noreturn]] void ut_dbg_assertion_failed(const char *expr, const char *file,
                                          uint64_t line) {
  auto filename = base_name(file);

  if (filename == nullptr) {
    filename = "null";
  }

#if !defined(UNIV_HOTBACKUP) && !defined(UNIV_NO_ERR_MSGS)
  LogErr(ERROR_LEVEL, ER_KOMODO_ASSERTION, filename, line,
         ((expr != nullptr) ? ":" : ""), ((expr != nullptr) ? expr : ""),
         to_string(std::this_thread::get_id()).c_str());

  flush_error_log_messages();

#else  /* !UNIV_HOTBACKUP && !defined(UNIV_NO_ERR_MSGS) */

  fprintf(stderr,
          "Komodo: Assertion failure: %s:" UINT64PF
          "%s%s\n"
          "Komodo: thread %s",
          filename, line, expr != nullptr ? ":" : "",
          expr != nullptr ? expr : "",
          to_string(std::this_thread::get_id()).c_str());
#endif /* !UNIV_HOTBACKUP */

  fflush(stderr);
  fflush(stdout);
  my_abort();
}
}  // namespace komodo
