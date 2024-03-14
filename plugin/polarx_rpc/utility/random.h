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
