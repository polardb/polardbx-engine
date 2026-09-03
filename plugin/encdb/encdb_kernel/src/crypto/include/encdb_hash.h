/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: hash related, including digest, hmac, kdf
 */
#pragma once
#include <cstdint>
#include <string>
#include "bytes.h"

namespace encdb {

// hash algorithm, including digest, mac, kdf
class Hash : public EncdbBaseObj {
 public:
  enum class Alg : uint8_t {
    SHA256,
    SM3,

    __COUNT__  // last element, for counter purpose
  };
  enum class Params : uint32_t {
    SM3_HASH_SIZE = 32,
    SHA256_HASH_SIZE = 32,
    MAX_HASH_SIZE = 32,
  };

  /**
   * @brief digest \c msg data using \c alg algorithm
   * @param [out]out          digest result
   * @param [in]alg           algorithm used to do hash
   * @param [in]msg           message data
   */
  static void hash(bytes_t &out, Alg alg, const bytes_t &msg);
  static bytes_t hash(Alg alg, const bytes_t &msg);
  /**
   * @brief derive key from \c secret , \c salt , \c aad using \c alg algorithm
   * @param [out]out          kdf result, with size specified in \c out.size()
   * @param [in]alg           algorithm used to do hash
   * @param [in]secret        secret used
   * @param [in]salt          random salt data
   * @param [in]aad           user-specified additional info. NOTE: \c aad
   * should be not longer than 1024 (limited by OpenSSL).
   */
  static void hkdf(bytes_t &out, Alg alg, const bytes_t &secret,
                   const bytes_t &salt, const bytes_t &aad);
  static bytes_t hkdf(uint32_t length, Alg alg, const bytes_t &secret,
                      const bytes_t &salt, const bytes_t &aad);
  /**
   * @brief compute hmac of \c data with \c key using \c alg algorithm
   * @param [out]out          digest result
   * @param [in]alg           algorithm used to do hash
   * @param [in]key           key used
   * @param [in]data          data to be MACed
   */
  static void hmac(bytes_t &out, Alg alg, const bytes_t &key,
                   const bytes_t &data);
  static bytes_t hmac(Alg alg, const bytes_t &key, const bytes_t &data);
  static uint32_t size(Hash::Alg alg);
};  // Hash

/**
 * @brief show algorithem name
 */
const std::string to_string(Hash::Alg alg) noexcept;

}  // namespace encdb
