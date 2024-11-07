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
// Created by zzy on 2022/8/17.
//

#pragma once

#include <cstdint>
#include <thread>
#include <type_traits>

#include "../common_define.h"

namespace polarx_rpc {

class Crandom {
  NO_COPY_MOVE(Crandom)

 private:
  enum : uint32_t {
    M = 2147483647L  // 2^31-1
  };

  enum : uint64_t {
    A = 16807  // bits 14, 8, 7, 5, 2, 1, 0
  };

  uint32_t seed_;

  static uint32_t good_seed(uint32_t s) { return (s & M) != 0 ? (s & M) : 1; }

 public:
  enum : uint32_t { kMaxNext = M };

  explicit Crandom(uint32_t s) : seed_(good_seed(s)) {}

  uint32_t next() {
    uint64_t product = seed_ * A;
    seed_ = static_cast<uint32_t>((product >> 31u) + (product & M));
    if (seed_ > M) seed_ -= M;
    return seed_;
  }

  static Crandom *get_instance() {
    static thread_local Crandom *tls_instance = nullptr;
    static thread_local std::aligned_storage<sizeof(Crandom)>::type
        tls_instance_bytes;

    auto rv = tls_instance;
    if (UNLIKELY(rv == nullptr)) {
      auto seed = std::hash<std::thread::id>()(std::this_thread::get_id());
      rv = new (&tls_instance_bytes) Crandom(seed);
      tls_instance = rv;
    }
    return rv;
  }
};

}  // namespace polarx_rpc
