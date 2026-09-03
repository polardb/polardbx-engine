/**
 * Copyright (c) 2019-2023 Alibaba Damo DBLab. All rights reserved.
 *
 * @desc: stream cipher based on symmetric cryptographic
 */
#pragma once
#include <cstdint>
#include <mutex>
#include "encdb_symmetric.h"

namespace encdb {

/**
 * @brief Stream cryptor based on symmetric encryption using ctr-stream-mode.
 *        Note that the output result is still normal CTR cipher with updated
 * IV.
 */
class CtrStreamCryptor : public EncdbBaseObj {
 public:
  explicit CtrStreamCryptor(const Symmetric::Alg &alg, const bytes_t &key,
                            const Symmetric::Flags &flags);

  /**
   * @param out [out]
   * @param in  [in]
   * @param iv  [out]
   */
  void encrypt(bytes_t &out, const bytes_t &in, bytes_t &iv);
  // void decrypt(bytes_t &out, const bytes_t &in);

  CtrStreamCryptor(const CtrStreamCryptor &) = delete;
  CtrStreamCryptor() = delete;
  void operator=(const CtrStreamCryptor &) = delete;

 private:
  void resetKeyStream();

  /* increment counter (128-bit int) by blocks, little-endian */
  static void ctr128IncIv(bytes_t &iv, uint32_t blocks);

 private:
  const Symmetric::Alg alg_;
  const bytes_t key_;
  const Symmetric::Flags flags_;
  const uint32_t block_size_;

  // iv = nonce + counter, where counter <= KEY_STREAM_SIZE / block_size
  bytes_t nonce_;
  uint32_t counter_;
  uint32_t capacity_;
  bytes_t stream_;

  std::mutex mutex_;
};

using CryptorHandle = std::shared_ptr<CtrStreamCryptor>;
class StreamCipherCache : public EncdbBaseObj {
 public:
  StreamCipherCache(const StreamCipherCache &) = delete;
  StreamCipherCache() = delete;
  void operator=(const StreamCipherCache &) = delete;

  static CryptorHandle getCryptor(const Symmetric::Alg &alg, const bytes_t &key,
                                  const Symmetric::Flags &flags);

 private:
  static const uint32_t MAX_CACHE_SIZE = 100;
};

}  // namespace encdb
