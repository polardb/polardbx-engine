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
