/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: asymmectric cryptographic
 */
#pragma once
#include <cstdint>
#include <string>
#include "bytes.h"
#include "encdb_hash.h"
#include "encdb_symmetric.h"

namespace encdb {

// asymmetric encryption algorithm
class Asymmetric : public EncdbBaseObj {
 public:
  enum class Alg : uint8_t {
    RSA,
    RSA_WITH_SHA256,  // always adopt SHA256 when doing RSA encrypting/signing
                      // by default
    SM2,

    __COUNT__  // last element, for counter purpose
  };
  enum class Params : uint32_t {
    SM2_PUBLIC_KEY_SIZE = 65,
    SM2_PRIVATE_KEY_SIZE = 32,  // 04 || X || Y
  };

  /**
   * @brief generate pkey pair according to given algorithm
   * @param [in]alg               Asymmetric::Alg
   * @param [out]prikey           private key, either raw bytes or pem string
   * @param [out]pubkey           public key, either raw bytes or pem string
   * @param [opt]keySizeInBits    specify asymmetric key size in bits, only for
   * RSA
   */
  static void generateKeyPair(std::string &prikey_string,
                              std::string &pubkey_string, Alg alg,
                              uint32_t keySizeInBits = 0);
  /**
   * @brief encrypt \c in data with given \c pubkey using \c alg algorithm
   * @param [out]out          encrypted result, in the format specified by
   * algorithm
   * @param [in]alg           algorithm used to do encrypt
   *                          Warning: for RSA, length of \c in should be
   * shorter than key size (several bytes)
   * @param [in]pubkey        public key used to do encrypt, either raw bytes or
   * pem string
   * @param [in]in            data to be encrypted
   */
  static void encrypt(bytes_t &out, Alg alg, const std::string &pubkey_string,
                      const bytes_t &in);
  static bytes_t encrypt(Alg alg, const std::string &pubkey_string,
                         const bytes_t &in);
  /**
   * @brief decrypt \c in data with given \c prikey using \c alg algorithm
   * @param [out]out          decrypted result, i.e., plaintext
   * @param [in]alg           algorithm used to do decrypt
   * @param [in]prikey        private key used to do decrypt, either raw bytes
   * or pem string
   * @param [in]in            cipher data to be decrypted, in the format
   * specified by algorithm
   */
  static void decrypt(bytes_t &out, Alg alg, const std::string &prikey_string,
                      const bytes_t &in);
  static bytes_t decrypt(Alg alg, const std::string &prikey_string,
                         const bytes_t &in);

  /**
   * @brief sign the input data using private key with given signature
   * algorithm. This API does not hash the input data to be signed, and
   * therefore is normally used to sign digests
   *
   * @param alg signature algorithm
   * @param prikey_string private key used to sign the input, in pem string
   * format
   * @param in data to be signed
   * @return bytes_t signature
   */
  static void sign(bytes_t &sig, Alg alg, const std::string &prikey_string,
                   const bytes_t &in);
  static bytes_t sign(Alg alg, const std::string &prikey_string,
                      const bytes_t &in);
  /**
   * @brief verify the signature and input data using public key with given
   * signature algorithm. This API does not hash the input data to be signed,
   * and therefore is normally used to sign digests
   *
   * @param alg signature algorithm
   * @param pubkey_string private key used to sign the input, in pem string
   * format
   * @param sig signature to be verified
   * @param in data to be verified
   * @return true/false
   */
  static bool verify(Alg alg, const std::string &pubkey_string,
                     const bytes_t &sig, const bytes_t &in);
};  // Asymmetric

/**
 * @brief show algorithem name
 */
const std::string to_string(Asymmetric::Alg alg) noexcept;

}  // namespace encdb
