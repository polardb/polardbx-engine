/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: asymmetric cryptographic
 */
#include "encdb_asymmetric.h"
#include <iostream>
#include <string>
#include "encdb_crypto_exception.h"
#include "encdb_crypto_provider.h"
#include "encdb_rand.h"
#include "helper.hpp"

namespace encdb {

const std::string to_string(Asymmetric::Alg alg) noexcept {
  std::string str;
  switch (alg) {
    case Asymmetric::Alg::SM2:
      str = "SM2";
      break;
    case Asymmetric::Alg::RSA_WITH_SHA256:
    case Asymmetric::Alg::RSA:
      str = "RSA";
      break;
    default:
      str = "ALG_NOT_SUPPORT";
      break;
  }
  return str;
}

/**
 * @brief generate pkey pair pem string according to given algorithm
 * @param [in]alg               Asymmetric::Alg
 * @param [out]prikey_string    private key pem string
 * @param [out]pubkeyPemSt      public key pem string
 * @param [opt]keySizeInBits    specify asymmetric key size in bits, only for
 * RSA
 */
void Asymmetric::generateKeyPair(std::string &prikey_string,
                                 std::string &pubkey_string, Alg alg,
                                 uint32_t keySizeInBits) {
  encdb_asymmetric_generate_key_pair(prikey_string, pubkey_string, alg,
                                     keySizeInBits);
}
/**
 * @brief encrypt \c in data with given \c pubkey using \c alg algorithm
 * @param [out]out          encrypted result, in the format specified by
 * algorithm
 * @param [in]alg           algorithm used to do encrypt
 *                          Warning: for RSA, length of \c in should be shorter
 * than key size (several bytes)
 * @param [in]pubkey        public key used to do encrypt, either raw bytes or
 * pem string
 * @param [in]in            data to be encrypted
 */
void Asymmetric::encrypt(bytes_t &out, Alg alg,
                         const std::string &pubkey_string, const bytes_t &in) {
  encdb_asymmetric_encrypt(out, alg, pubkey_string, in);
}
bytes_t Asymmetric::encrypt(Alg alg, const std::string &pubkey_string,
                            const bytes_t &in) {
  bytes_t out;
  // FIXME(lhr): avoid using try...catch to detect required output size, since
  // exception handling is time-consuming
  try {
    Asymmetric::encrypt(out, alg, pubkey_string, in);
  } catch (const encdb::ResultOutOfMemoryException &e) {
    out.resize(e.get_expected_length());
    Asymmetric::encrypt(out, alg, pubkey_string, in);
  }
  return out;
}
/**
 * @brief decrypt \c in data with given \c prikey using \c alg algorithm
 * @param [out]out          decrypted result, i.e., plaintext
 * @param [in]alg           algorithm used to do decrypt
 * @param [in]prikey        private key used to do decrypt, either raw bytes or
 * pem string
 * @param [in]in            cipher data to be decrypted, in the format specified
 * by algorithm
 */
void Asymmetric::decrypt(bytes_t &out, Alg alg,
                         const std::string &prikey_string, const bytes_t &in) {
  encdb_asymmetric_decrypt(out, alg, prikey_string, in);
}
bytes_t Asymmetric::decrypt(Alg alg, const std::string &prikey_string,
                            const bytes_t &in) {
  bytes_t out(in.size());
  Asymmetric::decrypt(out, alg, prikey_string, in);
  return out;
}

/**
 * @brief sign the input data using private key with given signature algorithm.
 * This API does not hash the input data to be signed, and therefore is normally
 * used to sign digests
 *
 * @param alg signature algorithm
 * @param prikey_string private key used to sign the input, in pem string format
 * @param in data to be signed
 * @return bytes_t signature
 */
void Asymmetric::sign(bytes_t &sig, Alg alg, const std::string &prikey_string,
                      const bytes_t &in) {
  encdb_asymmetric_sign(sig, alg, prikey_string, in);
}
bytes_t Asymmetric::sign(Alg alg, const std::string &prikey_string,
                         const bytes_t &in) {
  bytes_t sig;
  // FIXME(lhr): avoid using try...catch to detect required output size, since
  // exception handling is time-consuming
  try {
    Asymmetric::sign(sig, alg, prikey_string, in);
  } catch (const encdb::ResultOutOfMemoryException &e) {
    sig.resize(e.get_expected_length());
    Asymmetric::sign(sig, alg, prikey_string, in);
  }
  return sig;
}
/**
 * @brief verify the signature and input data using public key with given
 * signature algorithm. This API does not hash the input data to be signed, and
 * therefore is normally used to sign digests
 *
 * @param alg signature algorithm
 * @param pubkey_string private key used to sign the input, in pem string format
 * @param sig signature to be verified
 * @param in data to be verified
 * @return true/false
 */
bool Asymmetric::verify(Alg alg, const std::string &pubkey_string,
                        const bytes_t &sig, const bytes_t &in) {
  return encdb_asymmetric_verify(alg, pubkey_string, sig, in);
}

}  // namespace encdb
