/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "enc_aux.h"
#include "bytes.h"
#include "encdb_cipher.h"
#include "encdb_common_exception.h"
#include "encdb_key_exception.h"
#include "encdb_rules.h"
#include "encdb_sens_rule.h"
#include "storage.h"
#include "encdb_symmetric.h"
#include "key_mgmt.h"
#include "logger.h"
namespace encdb {
bytes_t decrypt(uint8_t &type, const Cipher &ops, const std::string &username) {
  type = ops.getDataType();
  bytes_t result;
  CipherContext cc;
  try {
    cc = getCC(username);
  } catch (const EncdbCcMissException &e) {
    ENCDB_LOG(LL_DEBUG, "%s for user %s", e.what(),
              e.get_missing_user_name().c_str());
    throw;
  }
  try {
    result = ops.decrypt(cc);
  } catch (const EncdbException &e) {
    ENCDB_LOG(LL_DEBUG, "%s", e.what());
    throw;
  }
  return result;
}

bytes_t decrypt(const Cipher &ops, const std::string &username) {
  uint8_t type;
  return decrypt(type, ops, username);
}

const CipherContext &getCCWithAccsType(const std::string &username,
                                       UserAccess accs) {
  try {
    std::string uname;
    if (accs == UserAccess::RESTRICTED_ACCESS) {
      uname = username;
    } else {
#ifdef DEBUG
      ENCDB_LOG(LL_WARNING,
                "Error: user %s does not seem to have RESTRICTED_ACCESS",
                username.c_str());
#endif
      uname = DUMMY_USERNAME;
    }
    return getCC(uname);
  } catch (const EncdbCcMissException &ccmiss) {
    // RESTRICTED_ACCESS user without MEK_PROVISION on this connection.
    if (encdb_plugin_cc_miss_error()) {
      // Strict mode: propagate error to client.
      ENCDB_LOG(LL_WARNING, "[EncDB] %s", ccmiss.what());
      throw;
    }
    // Default: fall back to dummy CC (ephemeral key) — user gets unreadable
    // ciphertext rather than an error, consistent with CN behavior.
    ENCDB_LOG(LL_WARNING, "[EncDB] %s, encrypting with ephemeral key",
              ccmiss.what());
    return getCC(DUMMY_USERNAME);
  }
}

uint32_t encrypt(Cipher &ops, const bytes_t &in, const std::string &username,
                 UserAccess accs) {
  return ops.encrypt(getCCWithAccsType(username, accs), in);
}

uint32_t encrypt(Cipher &ops, const bytes_t &in, const std::string &username,
                 UserAccess accs, Symmetric::Alg alg) {
  return ops.encrypt(getCCWithAccsType(username, accs), alg, in);
}

uint32_t encdb_get_precise_cipher_size(const std::string &username,
                                       uint32_t in_size) {
  Symmetric::Alg alg;
  UserAccess ua = hook::encdbGetUserAccess(username);
  try {
    alg = getCCWithAccsType(username, ua).symmAlg;
  } catch (const EncdbCcMissException &ccmiss) {
    alg = getCC(DUMMY_USERNAME).symmAlg;
  }
  return Cipher::size(in_size, alg);
}

uint32_t encdb_get_precise_cipher_size(uint32_t in_size, uint8_t alg) {
  return Cipher::size(in_size, Symmetric::to_alg(alg));
}

EncdbStatus encdb_untrusted_encrypt(const bytes_t &input, uint8_t type,
                                    const std::string &username,
                                    bytes_t &output, UserAccess accs) {
  if (!output) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  try {
    Cipher out_cipher(output);
    out_cipher.setDataType(type);
    uint32_t out_size = encrypt(out_cipher, input, username, accs);
    output.resize(out_size);
  } catch (const EncdbException &e) {
    ENCDB_LOG(LL_ERROR, "%s", e.what());
    return e.get_encdb_status();
  }
  return ENCDB_STATUS_OK;
}

EncdbStatus encdb_untrusted_encrypt(const bytes_t &input, uint8_t type,
                                    const std::string &username,
                                    bytes_t &output, UserAccess accs,
                                    uint8_t algo) {
  if (!output) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  try {
    Cipher out_cipher(output);
    out_cipher.setDataType(type);
    uint32_t out_size =
        encrypt(out_cipher, input, username, accs, Symmetric::to_alg(algo));
    output.resize(out_size);
  } catch (const EncdbException &e) {
    ENCDB_LOG(LL_ERROR, "%s", e.what());
    return e.get_encdb_status();
  }
  return ENCDB_STATUS_OK;
}

EncdbStatus encdb_untrusted_decrypt(const bytes_t &input,
                                    const std::string &username,
                                    bytes_t &output, uint8_t &type) {
  if (!input || !output) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  if (username == DUMMY_USERNAME) {
    return ENCDB_EXCEPTION_ACCESS_DENIED;
  }
  try {
    output = decrypt(type, Cipher(input, true), username);
  } catch (const EncdbException &e) {
    ENCDB_LOG(LL_DEBUG,
              "decrypt failed. Maybe you have a invalid cipher or you don't "
              "have a valid MEK.");
    return e.get_encdb_status();
  }
  return ENCDB_STATUS_OK;
}

}  // namespace encdb
