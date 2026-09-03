/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: cipher suite used in EncDB
 */
#pragma once
#include <cstdint>
#include <string>
#include "bytes.h"
#include "encdb_asymmetric.h"
#include "encdb_hash.h"
#include "encdb_symmetric.h"

namespace encdb {

struct CipherSuite {
  Asymmetric::Alg asymm_alg;
  Symmetric::Alg symm_alg;
  Hash::Alg hash_alg;

  static const CipherSuite SM2_WITH_SM4_128_CBC_SM3;
  static const CipherSuite RSA_WITH_AES_128_CBC_SHA256;
  static const CipherSuite SM2_WITH_SM4_128_GCM_SM3;
  static const CipherSuite RSA_WITH_AES_128_GCM_SHA256;

  Asymmetric::Alg get_asymm() const noexcept;
  Symmetric::Alg get_symm() const noexcept;
  Hash::Alg get_hash() const noexcept;

  const std::string to_string() const noexcept;
  static const CipherSuite to_cipher_suite(const std::string &cipher_suite_str);

  bool operator==(const CipherSuite &other) const noexcept;
  bool operator!=(const CipherSuite &other) const noexcept;
};

}  // namespace encdb
