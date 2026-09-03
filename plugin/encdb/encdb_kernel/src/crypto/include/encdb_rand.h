/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: read random bytes
 */
#pragma once
#include "bytes.h"

namespace encdb {

class Rand : public EncdbBaseObj {
 public:
  /**
   * @brief Read random bytes into \c buf
   * @param [out]out          random bytes
   */
  static void rand(bytes_t &rnd) noexcept;
  static bytes_t rand(uint32_t len) noexcept;
  static void rand(uint8_t *data, uint32_t len) noexcept;
  static uint64_t rand() noexcept;
};  // Rand

}  // namespace encdb
