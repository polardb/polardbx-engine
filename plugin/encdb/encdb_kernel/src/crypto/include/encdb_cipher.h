/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: declare EncDB cipher
 */
#pragma once
#include <array>
#include <cstddef>
#include <cstdint>
#include <string>
#include "encdb_asymmetric.h"
#include "encdb_cipher_suite.h"
#include "encdb_cipher_types.h"
#include "encdb_common_exception.h"
#include "encdb_hash.h"
#include "encdb_symmetric.h"

namespace encdb {

enum class CipherFormatVersion : uint8_t {
  VERSION_0 = static_cast<uint8_t>(0b00000000),
  VERSION_64 = static_cast<uint8_t>(0b01000000)
};

enum class Params : uint32_t {
  SALT_SIZE = static_cast<uint32_t>(Symmetric::Params::MAX_BLOCK_SIZE),
  MAC_SIZE = static_cast<uint32_t>(Hash::Params::MAX_HASH_SIZE),
  DEFAULT_MEK_SIZE = static_cast<uint32_t>(Symmetric::Params::MAX_KEY_SIZE),

  DEFAULT_RSA_KEY_SIZE_IN_BITS =
      3072,  // matched with that in SGX and other popular apps

  CHECK_CODE_SIZE = 1,  // for both plaintext and ciphertext check code

  DEFAULT_POLICY = static_cast<uint32_t>(encdb::CCPolicy::DEFAULT),
  DEFAULT_FLAGS = static_cast<uint32_t>(encdb::CCFlags::RND),

  MAX_KEY_PAIR_SIZE = 2048,
  MAX_SEALED_MEKINFO_SIZE =
      1024  // FIXME(yx): once the structure mek_info is changed, maybe change
            // this value accordingly
};

class KeyExchange : public EncdbBaseObj {
 public:
  /**
   * @brief seal \c in data into envelope with given \c pubkey using \c suite
   * algorithm, following:
   *      - encrypting \c in with ephemeral symmetric key,
   *      - and then encrypting the ephemeral key with \c pubkey
   * @param [out]out          envelope blob, in the format specified by
   * algorithm
   * @param [in]suite         cipher suite used to do encrypt
   * @param [in]pubkey        public key used to do encrypt, either raw bytes or
   * pem string
   * @param [in]in            data to be encrypted
   */
  static bytes_t seal(const CipherSuite &suite,
                      const std::string &pubkey_string, const bytes_t &in);
  /**
   * @brief open envelope \c in with given \c prikey using \c suite algorithm
   *      - decrypting the ephemeral symmetric key with \c prikey
   *      - and then decrypting the encrypted data with the ephemeral key
   * @param [out]out          decrypted result, i.e., plaintext
   * @param [in]suite         cipher suite used to do decrypt
   * @param [in]prikey        private key used to do decrypt, either raw bytes
   * or pem string
   * @param [in]in            envelope blob to be decrypted, in the format
   * specified by algorithm
   */
  static void open(bytes_t &out, const CipherSuite &suite,
                   const std::string &prikey_string, const bytes_t &in);
  static bytes_t open(const CipherSuite &suite,
                      const std::string &prikey_string, const bytes_t &in);
};
#if 0  // NOT_IN_USE
class KeyCipher : public EncdbBaseObj {
 public:
  static bytes_t encrypt(const Symmetric::Alg &alg, const DekId &dek_id,
                         const bytes_t &plaintext_key, const bytes_t &key);
  static bytes_t decrypt(DekId &dek_id, const bytes_t &ciphertext_key,
                         const bytes_t &key);
  /**
   * @brief compute KeyCipher size for given \c key_length and \c alg
   *
   * @param key_length    plaintext key length, including dekid || dek
   * @param alg           algorithm used to encrypt plaintext key
   * @return expected size
   */
  static uint32_t size(uint32_t key_length, Symmetric::Alg alg) noexcept;
  static uint32_t size(uint32_t key_length) noexcept;
};
#endif
// delegate cipher blob bytes
class Cipher : public EncdbBaseObj {
 public:
  // for output cipher
  explicit Cipher(bytes_t &bytes, bool check = false);
  // for input cipher, always const
  explicit Cipher(const bytes_t &bytes, bool check = false);

  // return delegated cipher bytes_t
  void setBytes(bytes_t &&bytes) noexcept;

  // Get cipher total size or data, including cipher header
  uint32_t size() const noexcept;
  uint32_t headerSize() const noexcept;
  const uint8_t *data() const noexcept;
  uint8_t *data() noexcept;

  uint8_t getDataType() const noexcept;
  void setDataType(uint8_t type);

  /**
   * @brief Calculate max required size for plaintext of \c length using \c alg
   * to encrypt it if exists
   * @param [in]length        plaintext length
   * @param [opt]alg          algorithm used to encrypt plaintext
   */
  static uint32_t size(uint32_t length, Symmetric::Alg alg) noexcept;
  static uint32_t size(uint32_t length) noexcept;
  static uint32_t size(uint32_t n, uint32_t total_len) noexcept;

  static uint32_t encrypt(bytes_t &out, Symmetric::Alg alg, CCFlags flags,
                          const bytes_t &key, const bytes_t &in,
                          const bytes_t &nonce);
  /**
   * @brief encrypt \c in data of type set using \c setDataType with algorithm,
   * flags and key specified by \c cc
   */
  uint32_t encrypt(const CipherContext &cc, const bytes_t &in);

  /**
   * @brief similar to above one, but algo is specified with param.
   */
  uint32_t encrypt(const CipherContext &cc, Symmetric::Alg alg,
                   const bytes_t &in);

  /**
   * @brief decrypt \c in data with given \c key using \c alg algorithm
   * @param [out]out          decrypted result, i.e., plaintext
   * @param [in]alg           algorithm used to do decrypt
   * @param [in]key           key used to do decrypt
   * @param [in]in            encdb cipher data to be decrypted
   * @return decrypted data size, excluding plaintext integrity check code
   */
  static uint32_t decrypt(bytes_t &out, uint8_t &type, const bytes_t &key,
                          const bytes_t &in);
  static bytes_t decrypt(uint8_t &type, const bytes_t &key, const bytes_t &in);
  /**
   * @brief decrypt current cipher using algorithm and key specified by CtxID,
   * which should be previously set using \c setCtxID decrypted result is set to
   * \c in data
   */
  uint32_t decrypt(const CipherContext &cc, bytes_t &out) const;
  bytes_t decrypt(const CipherContext &cc) const;

  static std::array<uint8_t, static_cast<uint32_t>(Params::CHECK_CODE_SIZE)>
  CalcCheckCode(const uint8_t *data, uint32_t len);

 private:
  // total bytes including cipher header and cipher blob, typically used for
  // input cipher
  bytes_t bytes_;
};

class Signature : public EncdbBaseObj {
 public:
  /**
   * @brief Sign the digest of input data \c in using the given signature
   * algorithm. The hash algorithm selected: RSA - SHA256; SM2 - SM3
   *
   * @param alg asymmetric signature algorithm
   * @param prikey_string private key pem string
   * @param in input data of any length, the digest of it computing from related
   * hash algorithm will be signed
   * @return bytes_t signature blob, including signatureAlgorithm and
   * signagtureValue
   */
  static bytes_t sign(const Asymmetric::Alg &alg,
                      const std::string &prikey_string, const bytes_t &in);
  /**
   * @brief Verify the signature of input data
   *
   * @param pubkey_string public key pem string
   * @param sig_blob signature blob, including signatureAlgorithm and
   * signagtureValue
   * @param in input data of any length
   * @return true - signature match; otherwise false - mismatch
   */
  static bool verify(const std::string &pubkey_string, const bytes_t &sig_blob,
                     const bytes_t &in);

  /**
   * @brief sign input data \c in with \c key using given hmac algorithm \c alg
   *
   * @param alg hash algorithm used to do hmac
   * @return hmac result
   */
  static bytes_t sign_hmac(const Hash::Alg &alg, const bytes_t &key,
                           const bytes_t &in);
  /**
   * @brief verify the hmac \c mac_blob of the input data \c in
   *
   * @param alg hash algorithm used to do hmac
   * @return true - signature match; otherwise false - mismatch
   */
  static bool verify_hmac(const bytes_t &key, const bytes_t &mac_blob,
                          const bytes_t &in);
};

}  // namespace encdb
