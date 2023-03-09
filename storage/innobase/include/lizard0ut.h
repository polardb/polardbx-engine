/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */


/** @file include/lizard0ut.h
 Lizard utility

 Created 2020-06-04 by Jianwei.zhao
 *******************************************************/

#ifndef lizard0ut_h
#define lizard0ut_h

#include <atomic>
#include <vector>
#include <ctime>    // std::time_t ...

namespace lizard {

template <typename Element_type, typename Object_type, std::size_t Prealloc>
class Partition {
 public:
  using Container = std::vector<Element_type *>;

  explicit Partition();
  virtual ~Partition();

  bool insert(Object_type object);
  bool exist(Object_type object);

 private:
  std::size_t m_size;
  std::atomic_ullong m_counter;
  Container m_parts;
};

class Simple_timer {
 public:
  Simple_timer() : m_ts(0) {}

  void update() { m_ts = std::time(0); }

  uint64_t since_last_update() const {
    std::time_t now = std::time(0);
    return now - m_ts;
  }

 private:
  std::time_t m_ts;
};

class Lazy_printer {
 public:
  Lazy_printer(const uint32_t interval_secs)
      : m_internal_secs(interval_secs), m_timer(), m_first(true) {}

  bool print(const char *msg);
  void reset();

 private:
  /** log printing interval */
  const uint32_t m_internal_secs;

  /** Timer */
  Simple_timer m_timer;

  /** First time to print, no take interval into consideration. */
  bool m_first;
};

}  // namespace lizard

#endif
