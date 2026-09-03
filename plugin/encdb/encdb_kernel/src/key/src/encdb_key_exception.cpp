/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include <cstdio>
#include <string>

#include "encdb_cipher_types.h"
#include "encdb_error.h"
#include "encdb_key_exception.h"

namespace encdb {

EncdbKeyException::EncdbKeyException(EncdbStatus status, const std::string &msg)
    : EncdbException(status, msg) {}
EncdbKeyException::EncdbKeyException()
    : EncdbKeyException(ENCDB_EXCEPTION_KEY, "encdb key exception") {}

EncdbMekMissException::EncdbMekMissException(MekId mek_id)
    : EncdbKeyException(ENCDB_EXCEPTION_MEK_MISS,
                        "missing mek: " + std::to_string(mek_id)),
      mek_id_(mek_id) {}
MekId EncdbMekMissException::get_missing_mek_id() const noexcept {
  return this->mek_id_;
}

EncdbEnclaveKeyPairMissException::EncdbEnclaveKeyPairMissException()
    : EncdbKeyException(ENCDB_EXCEPTION_ENCLAVE_KEY_PAIR_MISS,
                        "missing enclave public key") {}

EncdbCcMissException::EncdbCcMissException(const std::string &uname)
    : EncdbKeyException(ENCDB_EXCEPTION_CC_MISS,
                        "missing cipher context for user '" + uname +
                            "': MEK_PROVISION has not been performed on this"
                            " connection"),
      user_name_(uname) {}
std::string EncdbCcMissException::get_missing_user_name() const noexcept {
  return this->user_name_;
}

EncdbImportEncRuleException::EncdbImportEncRuleException(const std::string &str)
    : EncdbKeyException(ENCDB_EXCEPTION_IMPORT_ENC_RULE_FAILED, str) {}

EncdbRAException::EncdbRAException(const std::string &error_msg)
    : EncdbKeyException(ENCDB_REMOTE_ATTESTATION_EXCEPTION, error_msg) {}

}  // namespace encdb
