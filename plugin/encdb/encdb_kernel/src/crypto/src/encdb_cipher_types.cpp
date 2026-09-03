/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: types for cipher
 */
#include "encdb_cipher_types.h"
#include <cstddef>
#include <cstdint>
#include <functional>
#include "bytes.h"
#include "defs.h"
#include "encdb_asymmetric.h"
#include "encdb_crypto_exception.h"
#include "encdb_hash.h"
#include "encdb_rand.h"
#include "encdb_symmetric.h"
#include "helper.hpp"
#include "json.hpp"

namespace encdb {
bool operator==(const CipherContext &lhs, const CipherContext &rhs) {
  // clang-format off
    if (lhs.symmAlg != rhs.symmAlg || lhs.dek != rhs.dek || lhs.flags != rhs.flags || lhs.mek != rhs.mek ||
        lhs.nonce != rhs.nonce || lhs.policy != rhs.policy || lhs.username != rhs.username) {
        return false;
    } else {
        return true;
    }
  // clang-format on
}

CCPolicy to_cc_policy(const std::string &str) {
  if (str == "DEFAULT") {
    return CCPolicy::DEFAULT;
  } else {
    throw NoSuchCCPolicyException(str);
  }
}

CCFlags to_cc_flags(const std::string &str) {
  if (str == "RND") {
    return CCFlags::RND;
  } else if (str == "DET") {
    return CCFlags::DET;
  } else {
    throw NoSuchCCFlagsException(str);
  }
}

std::string to_string(const CCPolicy &policy) {
  switch (policy) {
    case CCPolicy::DEFAULT:
      return "DEFAULT";
    default:
      throw NoSuchCCPolicyException("no such CCPolicy");
  }
}

std::string to_string(const CCFlags &flags) {
  switch (flags) {
    case CCFlags::RND:
      return "RND";
    case CCFlags::DET:
      return "DET";
    default:
      throw NoSuchCCFlagsException("no such CCFlags");
  }
}

}  // namespace encdb
