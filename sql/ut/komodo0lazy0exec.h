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

/** @file sql/ut/komodo0lazy0exec.h


  Created 2025-06-20 by Jiyang.zhang
 *******************************************************/

#ifndef komodo0lazy0exec_h
#define komodo0lazy0exec_h

#include <chrono>
#include <cstdint>
#include <functional>

namespace komodo {

class Simple_timer {
 public:
  Simple_timer() : m_time_point() {}

  void update() { m_time_point = std::chrono::steady_clock::now(); }

  std::chrono::seconds since_last_update() const {
    auto now = std::chrono::steady_clock::now();
    return std::chrono::duration_cast<std::chrono::seconds>(now - m_time_point);
  }

 private:
  std::chrono::steady_clock::time_point m_time_point;
};

class Lazy_executor {
 public:
  Lazy_executor(const std::chrono::seconds interval)
      : m_internal(interval), m_timer(), m_first(true) {}

  bool try_run(const std::function<void()> &functor) {
    if (m_first) {
      functor();
      m_timer.update();
      m_first = false;
      return true;
    } else if (m_timer.since_last_update() > m_internal) {
      functor();
      m_timer.update();
      return true;
    } else {
      return false;
    }
  }

  void reset() { m_first = true; }

 private:
  std::chrono::seconds m_internal;

  Simple_timer m_timer;

  bool m_first;
};

}  // namespace komodo

#endif
