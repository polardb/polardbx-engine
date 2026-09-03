/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: types for cipher
 */
#pragma once
#include <cstddef>
#include <cstdint>
#include <functional>
#include "bytes.h"
#include "defs.h"
#include "encdb_cipher_suite.h"
#include "encdb_common_exception.h"
#include "encdb_symmetric.h"

namespace encdb {

typedef uint64_t CtxId;
typedef uint64_t MekId;
typedef uint64_t DekId;
const uint32_t NONCE_LENGTH = 8;

enum class CCPolicy : uint32_t {
  DEFAULT = 0,
};

enum class CCFlags : uint32_t {
  RND = 0x00000000,
  DET = 0x00000001,
};

struct CipherContext {
  std::string username;
  CCFlags flags = CCFlags::RND;  // default RND
  CCPolicy policy;
  Symmetric::Alg symmAlg;  // Algo used in symmetric encryption
  Hash::Alg hashAlg;       // Hash algo used in DEK derivation: HKDF(MEK, nonce)
  uint8_t nonce[NONCE_LENGTH];
  bytes_t dek;  // derived from nonce using KDF(MEK, nonce)
  bytes_t mek;
  CipherContext() {}
  CipherContext(const encdb::CipherContext &cc) {
    this->username = cc.username;
    this->flags = cc.flags;
    this->policy = cc.policy;
    this->symmAlg = cc.symmAlg;
    this->hashAlg = cc.hashAlg;
    memcpy(this->nonce, cc.nonce, NONCE_LENGTH);
    this->dek = std::move(bytes_t(cc.dek.data(), cc.dek.size(), false));
    this->mek = std::move(bytes_t(cc.mek.data(), cc.mek.size(), false));
  }
  const CipherContext &operator=(const CipherContext &cc) {
    if (this == &cc) {
      return *this;
    }
    this->username = cc.username;
    this->flags = cc.flags;
    this->policy = cc.policy;
    this->symmAlg = cc.symmAlg;
    this->hashAlg = cc.hashAlg;
    memcpy(this->nonce, cc.nonce, NONCE_LENGTH);
    this->dek = std::move(bytes_t(cc.dek.data(), cc.dek.size(), false));
    this->mek = std::move(bytes_t(cc.mek.data(), cc.mek.size(), false));
    return *this;
  }
};

bool operator==(const CipherContext &lhs, const CipherContext &rhs);

CCPolicy to_cc_policy(const std::string &str);
CCFlags to_cc_flags(const std::string &str);
std::string to_string(const CCPolicy &policy);
std::string to_string(const CCFlags &flags);

}  // namespace encdb
