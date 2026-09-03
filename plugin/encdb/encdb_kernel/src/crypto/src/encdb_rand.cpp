/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: read random bytes
 */
#include "encdb_rand.h"
#include <cstdint>
#include <cstring>
#include <random>
#include "encdb_crypto_provider.h"

namespace encdb {

void Rand::rand(bytes_t &rnd) noexcept { encdb_rand_bytes(rnd); }
bytes_t Rand::rand(uint32_t len) noexcept {
  bytes_t rnd(len);
  Rand::rand(rnd);
  return rnd;
}
void Rand::rand(uint8_t *data, uint32_t len) noexcept {
  bytes_t rnd(data, len);
  Rand::rand(rnd);
}
uint64_t Rand::rand() noexcept {
  uint64_t rnd;
  Rand::rand(reinterpret_cast<uint8_t *>(&rnd), sizeof(rnd));
  return rnd;
}

}  // namespace encdb
