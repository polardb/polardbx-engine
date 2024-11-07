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
// Created by zzy on 2022/8/29.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <limits>

#include "../common_define.h"

namespace polarx_rpc {

class Cnumber final {
  NO_CONSTRUCTOR(Cnumber)
  NO_COPY_MOVE(Cnumber)

 public:
  static inline uint32_t next_power_of_two_u32(uint32_t v) {
    --v;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    ++v;
    return v;
  }

  static inline uint64_t next_power_of_two_u64(uint64_t v) {
    --v;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    v |= v >> 32;
    ++v;
    return v;
  }

  static inline size_t next_power_of_two_sz(size_t v) {
#if UINTPTR_MAX == UINT32_MAX || SIZE_MAX == UINT32_MAX
    return next_power_of_two_u32(v);
#elif UINTPTR_MAX == UINT64_MAX || SIZE_MAX == UINT64_MAX
    return next_power_of_two_u64(v);
#else
#error "bad size_t"
#endif
  }
};

}  // namespace polarx_rpc
