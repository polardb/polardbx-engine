/**
 * Copyright (c) 2019-2023 Alibaba Damo DBLab. All rights reserved.
 *
 * @desc: stream cipher based on symmetric cryptographic
 */
#include "encdb_symmetric_stream.h"
#include <cassert>
#include <cstdint>
#include <memory>
#include <mutex>
#include "concurrent_limited_cache.hpp"
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"
#include "encdb_rand.h"
#include "encdb_symmetric.h"

namespace encdb {

static void xorBytes(bytes_t &out, const bytes_t &left, const bytes_t &right) {
  assert(left.size() == right.size());

  // XOR in block
  using block_t = uint64_t;

  block_t *p_out = reinterpret_cast<block_t *>(out.data());
  const block_t *p_left = reinterpret_cast<const block_t *>(left.data());
  const block_t *p_right = reinterpret_cast<const block_t *>(right.data());

  uint32_t count = left.size() / sizeof(block_t);
  for (uint32_t i = 0; i < count; i++) {
    p_out[i] = p_left[i] ^ p_right[i];
  }

  // XOR remaining bytes
  uint32_t offset = count * sizeof(block_t);
  while (offset < left.size()) {
    out.data()[offset] = left.data()[offset] ^ right.data()[offset];
    offset++;
  }
}

CtrStreamCryptor::CtrStreamCryptor(const Symmetric::Alg &alg,
                                   const bytes_t &key,
                                   const Symmetric::Flags &flags)
    : alg_(alg),
      key_(key.data(), key.size(), false),
      flags_(flags),
      block_size_(Symmetric::get_block_size(alg_)) {
  if (alg != Symmetric::Alg::AES_128_CTR &&
      alg != Symmetric::Alg::SM4_128_CTR) {
    throw NoSuchAlgorithmException("Do NOT support stream mode for " +
                                   to_string(alg));
  }
  if (flags & Symmetric::Flags::DET) {
    throw NoSuchAlgorithmException("Do NOT support stream mode in DET mode");
  }

  counter_ = 0;
  capacity_ = 0;
}

void CtrStreamCryptor::encrypt(bytes_t &out, const bytes_t &in, bytes_t &iv) {
  std::unique_lock<std::mutex> lock(mutex_);
  uint32_t blocks = (in.size() - 1) / block_size_ + 1;
  assert(blocks <= capacity_);

  // check capacity
  if (counter_ + blocks > capacity_) {
    resetKeyStream();
  }

  // do encrypt, note that remaining bytes in key_stream will be discarded
  xorBytes(out, in,
           bytes_t(stream_.data() + counter_ * block_size_, in.size()));

  // record iv
  iv.set(nonce_.data(), nonce_.size(), false);
  ctr128IncIv(iv, counter_);

  // update key stream state
  counter_ += blocks;
}

// This function triggers compiler warning because its parameters are not used
// void CtrStreamCryptor::decrypt(bytes_t &out, const bytes_t &in) {
//   // TODO: (lhr) boost decryption. maybe we can cache key stream according to
//   // nonce
//   throw NoSuchAlgorithmException("Do NOT support stream mode decryption for "
//   +
//                                  to_string(alg_));
// }

void CtrStreamCryptor::resetKeyStream() {
  bytes_t tag, aad;
  // use random bytes as nonce
  nonce_.resize(static_cast<uint32_t>(Symmetric::Params::CTR_IV_SIZE));
  encdb_rand_bytes(nonce_);
  stream_.resize(Symmetric::KEY_STREAM_SIZE);
  stream_.fill(0);  // reset zero bytes as input to generate key stream
  encdb_symmetric_encrypt(stream_, alg_, key_, nonce_, stream_, tag, aad,
                          flags_);
  capacity_ = stream_.size() / block_size_;
  counter_ = 0;
}

/* increment counter (128-bit int) by blocks, little-endian */
void CtrStreamCryptor::ctr128IncIv(bytes_t &iv, uint32_t blocks) {
  uint32_t n = static_cast<uint32_t>(Symmetric::Params::CTR_IV_SIZE),
           c = blocks;
  assert(iv.size() >= n);

  do {
    --n;
    c += iv.data()[n];
    iv.data()[n] = (uint8_t)c;
    c >>= 8;
  } while (n);
}

// since C++ 20, we can use lambda function to declare hash function for map
// using CryptorKey = std::tuple<uint8_t, bytes_t>;
// auto hash_cryptor_key = [](const CryptorKey &ck) {
//     std::size_t s_alg = std::hash<uint8_t>()(std::get<0>(ck));
//     std::size_t s_key = std::hash<bytes_t>()(std::get<1>(ck));
//     return s_alg ^ s_key;
// };
// auto id = std::make_tuple(static_cast<uint8_t>(alg), key);
// struct CryptorKeyHash {
//     template <typename CryptorKey>
//     std::size_t operator()(const CryptorKey& t) const {
//         std::size_t s_alg = std::hash<Symmetric::Alg>()(std::get<0>(t));
//         std::size_t s_key = std::hash<bytes_t>()(std::get<1>(t));
//         return s_alg ^ s_key;
//     }
// };
using CryptorKey = std::size_t;
CryptorKey getFastHashId(const Symmetric::Alg &alg, const bytes_t &key) {
  std::size_t s_alg = std::hash<uint64_t>()(static_cast<uint64_t>(alg));
  std::size_t s_key = std::hash<bytes_t>()(key);
  return s_alg ^ s_key;
}

CryptorHandle StreamCipherCache::getCryptor(const Symmetric::Alg &alg,
                                            const bytes_t &key,
                                            const Symmetric::Flags &flags) {
  thread_local LimitedCache<CryptorKey, CryptorHandle> cryptor_cache(
      MAX_CACHE_SIZE);

  auto id = getFastHashId(alg, key);
  if (!cryptor_cache.exists(id)) {
    auto handle = std::shared_ptr<CtrStreamCryptor>(
        new CtrStreamCryptor(alg, key, flags));
    cryptor_cache.put(id, std::move(handle));
  }
  return cryptor_cache.get(id);
}

}  // namespace encdb
