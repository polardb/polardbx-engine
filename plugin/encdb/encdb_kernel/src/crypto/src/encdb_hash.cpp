/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: hash related, including digest, hmac, kdf
 */
#include "encdb_hash.h"
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"

namespace encdb {

/**
 * @brief digest \c msg data using \c alg algorithm
 * @param [out]out          digest result
 * @param [in]alg           algorithm used to do hash
 * @param [in]msg           message data
 */
void Hash::hash(bytes_t &out, Alg alg, const bytes_t &msg) {
  encdb_hash(out, alg, msg);
}
bytes_t Hash::hash(Alg alg, const bytes_t &msg) {
  bytes_t out(static_cast<uint32_t>(Hash::Params::MAX_HASH_SIZE));
  Hash::hash(out, alg, msg);
  return out;
}

/**
 * @brief derive key from \c secret , \c salt , \c aad using \c alg algorithm
 * @param [out]out          kdf result
 * @param [in]alg           algorithm used to do hash
 * @param [in]secret        secret used
 * @param [in]salt          random salt data
 * @param [in]aad           user-specified additional info. NOTE: \c aad should
 * be not longer than 1024 (limited by OpenSSL).
 */
void Hash::hkdf(bytes_t &out, Alg alg, const bytes_t &secret,
                const bytes_t &salt, const bytes_t &aad) {
  encdb_hkdf(out, alg, secret, salt, aad);
}
bytes_t Hash::hkdf(uint32_t length, Alg alg, const bytes_t &secret,
                   const bytes_t &salt, const bytes_t &aad) {
  bytes_t out(length);
  Hash::hkdf(out, alg, secret, salt, aad);
  return out;
}

/**
 * @brief compute hmac of \c data with \c key using \c alg algorithm
 * @param [out]out          digest result
 * @param [in]alg           algorithm used to do hash
 * @param [in]key           key used
 * @param [in]data          data to be MACed
 */
void Hash::hmac(bytes_t &out, Alg alg, const bytes_t &key,
                const bytes_t &data) {
  encdb_hmac(out, alg, key, data);
}
bytes_t Hash::hmac(Alg alg, const bytes_t &key, const bytes_t &data) {
  bytes_t out(static_cast<uint32_t>(Hash::Params::MAX_HASH_SIZE));
  Hash::hmac(out, alg, key, data);
  return out;
}

const std::string to_string(Hash::Alg alg) noexcept {
  std::string str;
  switch (alg) {
    case Hash::Alg::SHA256:
      str = "SHA256";
      break;
    case Hash::Alg::SM3:
      str = "SM3";
      break;
    default:
      str = "ALG_NOT_SUPPORT";
      break;
  }
  return str;
}

uint32_t Hash::size(Hash::Alg alg) {
  uint32_t size = 0;
  switch (alg) {
    case Hash::Alg::SHA256:
      size = 32;
      break;
    case Hash::Alg::SM3:
      size = 32;
      break;
    default:
      size = 0;
      break;
  }
  return size;
}

}  // namespace encdb
