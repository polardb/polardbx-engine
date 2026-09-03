/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: implementation for untrusted key management
 */
#pragma once

#include <stddef.h>
#include <stdint.h>
#include <string>
#include "EncDBConfig.h"
#include "encdb_cipher_types.h"
#include "encdb_error.h"
#include "storage.h"

// MySQL usernames cannot have '@', so we use it as dummy username
#define DUMMY_USERNAME "@@@@@@@@@@@@"

namespace encdb {

void setDefaultEncryptionAlgorithm(std::string algorithm);

// SERVER_INFO_GET
CipherSuite getDefaultCipherSuite();
std::string getPublicKey();
std::string getPublicKeyHash();

// CC Operation
const CipherContext &getCC(const std::string &);
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
// MEK_UPDATE
EncdbStatus encdbUpdateMek(const std::string &, const std::string &);
// MEK_PROVISION
EncdbStatus encdbRecvMek(const std::string &, const std::string &);
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
// MEK_UPDATE
EncdbStatus encdbUpdateMek(const std::string &, const std::string &,
                           const std::string &, std::string &);
// MEK_PROVISION
EncdbStatus encdbRecvMek(const std::string &, const std::string &,
                         const std::string &, std::string &);
#endif

// MEK_IMPORT_FROM_KMS
void encdbImportMekFromKms(const std::string &msgIn);
// MEK_DETACH_KMS
void encdbExitKmsMode();

// Remote Attestation
const std::string getRaQuote(const std::string &user_data_json_str);
const std::string verifyRaQuote(const std::string &quote_str,
                                const std::string &user_data_json_str,
                                const std::string &enclave_data_json_str);

}  // namespace encdb
