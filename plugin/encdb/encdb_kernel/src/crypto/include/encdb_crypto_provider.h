/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: declaration primitive interfaces for crypto provider
 */
#pragma once
#include <string>
#include "encdb_asymmetric.h"
#include "encdb_cipher_types.h"
#include "encdb_hash.h"
#include "encdb_symmetric.h"

// TODO(lhr): change to class

namespace encdb {

/**
 * @brief crypto operation type
 */
enum class CryptoOpType : uint32_t {
  ENCRYPT = 0,
  DECRYPT = 1,
  VERIFY = 2,
  SIGN = 3,
};

// random generator
void encdb_rand_bytes(bytes_t &rnd);

/**
 * @brief digest \c msg data using \c alg algorithm
 * @param [out]out          digest result
 * @param [in]alg           algorithm used to do hash
 * @param [in]msg           message data
 */
uint32_t encdb_hash(bytes_t &digest, Hash::Alg alg, const bytes_t &msg);
/**
 * @brief derive key from \c secret , \c salt , \c aad using \c alg algorithm
 * @param [out]out          kdf result
 * @param [in]alg           algorithm used to do hash
 * @param [in]secret        secret used
 * @param [in]salt          random salt data
 * @param [in]aad           user-specified additional info
 */
uint32_t encdb_hkdf(bytes_t &out, Hash::Alg alg, const bytes_t &secret,
                    const bytes_t &salt, const bytes_t &aad);
/**
 * @brief compute hmac of \c data with \c key using \c alg algorithm
 * @param [out]out          digest result
 * @param [in]alg           algorithm used to do hash
 * @param [in]key           key used
 * @param [in]data          data to be MACed
 */
uint32_t encdb_hmac(bytes_t &out, Hash::Alg alg, const bytes_t &key,
                    const bytes_t &data);

/**
 * @param flags enable crypto cache by default (for performance purpose)
 * @return real size of \c ciphertext text data
 */
uint32_t encdb_symmetric_encrypt(
    bytes_t &ciphertext, Symmetric::Alg alg, const bytes_t &key,
    const bytes_t &iv, const bytes_t &plaintext, bytes_t &tag,
    const bytes_t &aad,
    const Symmetric::Flags flags = Symmetric::Flags::USE_CRYPTO_CACHE);
/**
 * @param flags control flags
 *      By default USE_CRYPTO_CACHE is set to enable crypto cache for
 * performance purpose. When PARTIAL_CRYPT is set, partial encryption/decryption
 * is enable, and the caller is responsible for prepare right IV and ciphertext.
 * Note that:
 *          1. if authentication tag exists, it will not be checked, thus the
 * integrity of partial data is not guaranteed.
 *          2. input the position of \c ciphertext should be aligned to block
 * size and be multiple times of block size when NO_PADDING is set.
 * @return real size of \c plaintext data
 */
uint32_t encdb_symmetric_decrypt(
    bytes_t &plaintext, Symmetric::Alg alg, const bytes_t &key,
    const bytes_t &iv, const bytes_t &ciphertext, const bytes_t &tag,
    const bytes_t &aad,
    const Symmetric::Flags flags = Symmetric::Flags::USE_CRYPTO_CACHE);

/**
 * @return real size of \c ciphertext text data
 */
uint32_t encdb_asymmetric_encrypt(bytes_t &ciphertext, Asymmetric::Alg alg,
                                  const std::string &key_string,
                                  const bytes_t &plaintext);
/**
 * @return real size of \c plaintext data
 */
uint32_t encdb_asymmetric_decrypt(bytes_t &plaintext, Asymmetric::Alg alg,
                                  const std::string &key_string,
                                  const bytes_t &ciphertext);
/**
 * @brief sign the input (typically data digest) using private key with given
 * algorithm. This does not hash the data to be signed, and therefore is
 * normally used to sign digests.
 */
uint32_t encdb_asymmetric_sign(bytes_t &sig, Asymmetric::Alg alg,
                               const std::string &key_string,
                               const bytes_t &in);
/**
 * @brief verify the input (typically data digest) and signature using public
 * key with given algorithm. This does not hash the data to be signed, and
 * therefore is normally used to sign digests.
 */
bool encdb_asymmetric_verify(Asymmetric::Alg alg, const std::string &key_string,
                             const bytes_t &sig, const bytes_t &in);

/**
 * @brief generate pkey pair pem string according to given algorithm
 * @param [in]alg               Asymmetric::Alg
 * @param [out]prikeyPemStr     private key pem string
 * @param [out]pubkeyPemSt      public key pem string
 * @param [opt]keySizeInBits    specify asymmetric key size in bits, only for
 * RSA
 * @return total size of public and private key pem string size
 */
uint32_t encdb_asymmetric_generate_key_pair(std::string &prikeyPemStr,
                                            std::string &pubkeyPemStr,
                                            Asymmetric::Alg alg,
                                            uint32_t keySizeInBits = 0);

}  // namespace encdb
