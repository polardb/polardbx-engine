/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: symmectric cryptographic
 */
#pragma once
#include <array>
#include <cstdint>
#include <string>
#include "bytes.h"
#include "encdb_crypto_exception.h"

namespace encdb {

// symmetric encryption algorithm
class Symmetric : public EncdbBaseObj {
 public:
  // Symmetric algorithms that supports
  enum class Alg : uint8_t {
    AES_128_GCM = 0,
    AES_128_ECB,
    AES_128_CTR,
    AES_128_CBC,

    SM4_128_CBC,
    SM4_128_ECB,
    SM4_128_CTR,
    SM4_128_GCM,

    __COUNT__  // last element, for counter purpose
  };
  static inline Alg to_alg(uint8_t i) {
    if (i >= static_cast<uint8_t>(Alg::__COUNT__)) {
      throw InvalidAlgorithmParameterException(
          "Symmetric::Alg cannot be converted from " + std::to_string(i));
    }
    return static_cast<Alg>(i);
  }
  // Algorithm parameters
  enum class Params : uint32_t {
    AES_BLOCK_SIZE = 16,
    SM4_BLOCK_SIZE = 16,
    MAX_BLOCK_SIZE = 16,

    AES_128_KEY_SIZE = 16,
    SM4_128_KEY_SIZE = 16,
    MAX_KEY_SIZE = 16,

    GCM_IV_SIZE = 12,
    CBC_IV_SIZE = 16,
    CTR_IV_SIZE = 16,
    ECB_IV_SIZE = 0,
    MAX_IV_SIZE = 16,

    GCM_TAG_SIZE = 16,
  };
  static uint32_t get_key_size(Alg alg);
  static uint32_t get_block_size(Alg alg);
  // Recommended size for CTR stream mode
  static const uint32_t KEY_STREAM_SIZE = 1 << 18;  // 256KB
  static const uint32_t MIN_BATCH_SIZE = 1 << 11;   // 2KB

  // clang-format off
    enum Flags : uint32_t {
        NONE             = 0x00000000,
        NO_PADDING       = 0x00000001,
        USE_CRYPTO_CACHE = 0x00000002,
        PARTIAL_CRYPT    = 0x00000004,
        STREAM_MODE      = 0x00000008,  // extend block cipher to stream cipher, typically adopting CTR mode
        DET              = 0x00000010,  // encrypt with fixed iv; disabled by default, i.e., encrypt with random iv.

        // FIXME: it seems EVP_CIPHER_CTX_copy in openssl 3.0.9 error, so we disable USE_CRYPTO_CACHE.
        DEFAULT          = STREAM_MODE | USE_CRYPTO_CACHE,
    };
  // clang-format on

  /**
   * @brief encrypt \c in data with given \c key and \c iv using \c alg
   * algorithm
   * @param [out]out          encrypted result, in the format specified by
   * algorithm
   * @param [in]alg           algorithm used to do encrypt
   * @param [in]key           key used to do encrypt
   * @param [in]iv            iv used to do encrypt, IV will be truncated if IV
   * is too long
   * @param [in]in            data to be encrypted
   * @param flags             control flags
   *      When PARTIAL_CRYPT is set, partial encryption/decryption is enable,
   * and the caller is responsible for prepare right IV. Note that PARTIAL_CRYPT
   * will not check the authentication tag, thus the integrity of partial data
   * is not guaranteed.
   */
  static void encrypt(bytes_t &out, Alg alg, const bytes_t &key,
                      const bytes_t &iv, const bytes_t &in,
                      const Flags &flags = Flags::DEFAULT);
  static bytes_t encrypt(Alg alg, const bytes_t &key, const bytes_t &iv,
                         const bytes_t &in,
                         const Flags &flags = Flags::DEFAULT);
  /**
   * @brief encrypt \c in data with given \c key and IV deriving from \c in
   * (i.e., DET mode) using \c alg algorithm
   * @param [out]out          encrypted result, in the format specified by
   * algorithm
   * @param [in]alg           algorithm used to do encrypt
   * @param [in]key           key used to do encrypt
   * @param [in]in            data to be encrypted
   * @param flags             control flags
   *      When PARTIAL_CRYPT is set, partial encryption/decryption is enable,
   * and the caller is responsible for prepare right IV. Note that PARTIAL_CRYPT
   * will not check the authentication tag, thus the integrity of partial data
   * is not guaranteed.
   */
  static void encrypt(bytes_t &out, Alg alg, const bytes_t &key,
                      const bytes_t &in, const Flags &flags = Flags::DEFAULT);
  static bytes_t encrypt(Alg alg, const bytes_t &key, const bytes_t &in,
                         const Flags &flags = Flags::DEFAULT);
  /**
   * @brief decrypt \c in data with given \c key using \c alg algorithm
   * @param [out]out          decrypted result, i.e., plaintext
   * @param [in]alg           algorithm used to do decrypt
   * @param [in]key           key used to do decrypt
   * @param [in]iv            iv used to do decrypt
   * @param [in]in            cipher data to be decrypted, in the format
   * specified by algorithm
   * @param flags             control flags
   *      When PARTIAL_CRYPT is set, partial encryption/decryption is enable,
   * and the caller is responsible for prepare right IV. Note that PARTIAL_CRYPT
   * will not check the authentication tag, thus the integrity of partial data
   * is not guaranteed.
   */
  static void decrypt(bytes_t &out, Alg alg, const bytes_t &key,
                      const bytes_t &in, const Flags &flags = Flags::DEFAULT);
  static bytes_t decrypt(Alg alg, const bytes_t &key, const bytes_t &in,
                         const Flags &flags = Flags::DEFAULT);

  /**
   * @brief Calculate the exactly required cipher size to encrypt data of length
   * \c length using \c alg
   * @param [in]alg           algorithm used to form the cipher
   * @param [in]length        plaintext length
   * @return                  exactly required cipher size
   */
  static uint32_t size(uint32_t length, Symmetric::Alg alg);
  /**
   * @brief Calculate the max required cipher size to encrypt data of length \c
   * length
   * @param [in]length        plaintext length
   * @return                  max required cipher size
   */
  static uint32_t size(uint32_t length) noexcept;

  /**
   * @brief Calculate the closest plaintext size of encrypt data \c cipherBlob
   * @param [in]alg           algorithm used to form the cipher
   * @param [in]cipherBlob    encrypted cipher blob
   * @return                  closest plaintext size
   */
  static uint32_t dataSize(Alg alg, const bytes_t &cipherBlob);

  /**
   * @brief Return required IV size for a given Symmetric Alg.
   * @param [in]alg
   */
  static uint32_t ivSize(Alg alg);

  /**
   * @brief Return required key size for a given Symmetric Alg.
   * @param [in]alg
   */
  static uint32_t keySize(Alg alg);
};  // Symmetric

#pragma pack(1)
/*
 * sizeof(array<uint8_t, Size>) is NOT always equal to Size
 * - depends on specified implementation
 * - for std::array:
 *      - sizeof(array<uint8_t, Size>) = Size, when Size > 0;
 *      - sizeof(array<uint8_t, Size>) = 1, when Size = 0;
 *
 * std::array<uint8_t, ivSize> iv;
 * uint8_t iv[static_cast<uint32_t>(ivSize)];
 */

struct CBCCipher {
  std::array<uint8_t, static_cast<uint32_t>(Symmetric::Params::CBC_IV_SIZE)> iv;
  uint8_t data[0];
};

struct CTRCipher {
  std::array<uint8_t, static_cast<uint32_t>(Symmetric::Params::CTR_IV_SIZE)> iv;
  uint8_t data[0];
};

struct GCMCipher {
  std::array<uint8_t, static_cast<uint32_t>(Symmetric::Params::GCM_IV_SIZE)> iv;
  std::array<uint8_t, static_cast<uint32_t>(Symmetric::Params::GCM_TAG_SIZE)>
      tag;
  uint8_t data[0];
};

struct ECBCipher {
  uint8_t data[0];
};
#pragma pack()

union CipherBlob {
  CBCCipher cbc;
  CTRCipher ctr;
  GCMCipher gcm;
  ECBCipher ecb;
};

/**
 * @brief show algorithem name
 */
const std::string to_string(Symmetric::Alg alg) noexcept;

Symmetric::Alg to_symmetric_alg(const std::string &str);

}  // namespace encdb
