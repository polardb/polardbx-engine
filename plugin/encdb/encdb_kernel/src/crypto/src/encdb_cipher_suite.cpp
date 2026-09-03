/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: cipher suite used in EncDB
 */
#include "encdb_cipher_suite.h"
#include <cstdint>
#include <string>
#include "bytes.h"
#include "encdb_asymmetric.h"
#include "encdb_crypto_exception.h"
#include "encdb_hash.h"
#include "encdb_symmetric.h"

namespace encdb {

const CipherSuite CipherSuite::SM2_WITH_SM4_128_CBC_SM3 = {
    Asymmetric::Alg::SM2, Symmetric::Alg::SM4_128_CBC, Hash::Alg::SM3};
const CipherSuite CipherSuite::RSA_WITH_AES_128_CBC_SHA256 = {
    Asymmetric::Alg::RSA_WITH_SHA256, Symmetric::Alg::AES_128_CBC,
    Hash::Alg::SHA256};
const CipherSuite CipherSuite::SM2_WITH_SM4_128_GCM_SM3 = {
    Asymmetric::Alg::SM2, Symmetric::Alg::SM4_128_GCM, Hash::Alg::SM3};
const CipherSuite CipherSuite::RSA_WITH_AES_128_GCM_SHA256 = {
    Asymmetric::Alg::RSA_WITH_SHA256, Symmetric::Alg::AES_128_GCM,
    Hash::Alg::SHA256};

Asymmetric::Alg CipherSuite::get_asymm() const noexcept { return asymm_alg; }
Symmetric::Alg CipherSuite::get_symm() const noexcept { return symm_alg; }
Hash::Alg CipherSuite::get_hash() const noexcept { return hash_alg; }

const std::string CipherSuite::to_string() const noexcept {
  return encdb::to_string(asymm_alg) + "_WITH_" + encdb::to_string(symm_alg) +
         "_" + encdb::to_string(hash_alg);
}
const CipherSuite CipherSuite::to_cipher_suite(
    const std::string &cipher_suite_str) {
  if (cipher_suite_str == "RSA_WITH_AES_128_CBC_SHA256") {
    return CipherSuite::RSA_WITH_AES_128_CBC_SHA256;
  } else if (cipher_suite_str == "SM2_WITH_SM4_128_CBC_SM3") {
    return CipherSuite::SM2_WITH_SM4_128_CBC_SM3;
  } else if (cipher_suite_str == "RSA_WITH_AES_128_GCM_SHA256") {
    return CipherSuite::RSA_WITH_AES_128_GCM_SHA256;
  } else if (cipher_suite_str == "SM2_WITH_SM4_128_GCM_SM3") {
    return CipherSuite::SM2_WITH_SM4_128_GCM_SM3;
  } else {
    throw NoSuchCipherSuiteException(cipher_suite_str);
  }
}

bool CipherSuite::operator==(const CipherSuite &other) const noexcept {
  return (this->asymm_alg == other.asymm_alg) &&
         (this->symm_alg == other.symm_alg) &&
         (this->hash_alg == other.hash_alg);
}
bool CipherSuite::operator!=(const CipherSuite &other) const noexcept {
  return !(this->operator==(other));
}

}  // namespace encdb
