/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: definition for message request type
 */
#pragma once

namespace encdb {

enum class EnclaveRequestType {
  // ENCLAVE INTERNAL REQUEST
  ENCLAVE_EXPORT_MEK = 0x90,
  ENCLAVE_GEN_KEY_PAIR = 0x91,
};

enum class RequestType {
  // Server info
  SERVER_INFO_GET = 0x00,   // request server info
  SERVER_GET_NONCE = 0x01,  // request a new server nonce

  // Remote attestation
  RA_CHALLENGE = 0x10,
  RA_01 = 0x11,  // always combine message 0 and message 1
  RA_2 = 0x12,
  RA_3 = 0x13,
  RA_4 = 0x14,
  RA_GET_PUBLIC_KEY = 0x15,
  RA_GET_QUOTE = 0x16,  // generate RA quote
  RA_VERIFY_QUOTE = 0x17,

  // MEK
  MEK_PROVISION = 0x30,
  MEK_UPDATE = 0x31,
  MEK_IMPORT_FROM_KMS = 0x32,  // register or rotate MEK via KMS
  MEK_DETACH_KMS      = 0x33,  // detach from KMS mode, delete all KMS metadata

  // DEK
  DEK_GENERATE = 0x50,          // generate DEK for client in enclave
  DEK_INSERT = 0x51,            // insert DEK from local, should be
                                // encrypted in specified format
  DEK_UPDATE = 0x52,            // update DEK info
  DEK_GET_BY_NAME = 0x53,       // get DEK by keyname
  DEK_GET_BY_ID = 0x54,         // get DEK by ctxid
  DEK_GET_NEXT_DEK_ID = 0x55,   // allocate a new dek entry and get latest dekid
  DEK_UPDATE_ATTRIBUTE = 0x56,  // update DEK CC attributes info
  DEK_UPDATE_ALGORITHM = 0x57,  // update DEK CC algorithm info
  DEK_COPY_KEYNAME = 0x58,      // dek copy keyname
  KEYNAME_GENERATE = 0x59,      // generate keyname
  KEYNAME_SEARCH = 0x5A,        // search keyname

  // BCL
  BCL_ISSUE = 0x70,     // issue or update BCL
  BCL_GET = 0x71,       // get BCL, including related BRL
  BCL_REVOKE = 0x72,    // issue BRL to revoke BCL
  BCL_REGISTER = 0x73,  // register user public key

  // ENC_RULE
  ENC_RULE_IMPORT = 0xA0,  // import encryption rule
};

}  // namespace encdb
