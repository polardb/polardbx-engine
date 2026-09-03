/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include "bytes.h"
#include "db_operation.h"
#include "encdb_cipher.h"
#include "encdb_common_exception.h"
#include "encdb_common_exception.h"  // for VersionMismatchException
#include "encdb_key_exception.h"
#include "encdb_req_types.h"
#include "encdb_rules.h"
#include "encdb_version.h"
#include "json.hpp"
#include "key_mgmt.h"
#include "logger.h"
#include "storage.h"

using encdb::bytes_t;

// show more info flag
char debug = 0;
char verbose = 0;
// Global variables
#define ERR_MSG_BUF_SIZE 1024

/**
 * @brief check the least client version for current server (#EDB-636).
 *
 * @throw VersionMismatchException - 不满足，抛异常断开连接
 * @return true  - 严格match
 * @return false - 满足但不是最新的，给个提示
 */
bool encdb_validate_client_version(const std::string &client_version_str) {
  using namespace encdb;
  thread_local Version min_client_version(CLIENT_MIN_VERSION);
  thread_local Version recommended_client_version(CLIENT_RECOMMENDED_VERSION);

  Version client_version(client_version_str);
  if (recommended_client_version == client_version) {
    return true;
  } else if (min_client_version < client_version ||
             min_client_version == client_version) {
    return false;
  } else {
    throw encdb::VersionMismatchException(
        "Client version is " + client_version_str +
        ", required minimum client version is " + CLIENT_MIN_VERSION);
  }
}

json::JSON encdb_get_server_info() {
  // No DB access should be done. Return.
  json::JSON server_info_json;
  server_info_json["tee_type"] = "MOCK";
  server_info_json["cipher_suite"] = encdb::getDefaultCipherSuite().to_string();
  server_info_json["public_key"] = encdb::getPublicKey();
  server_info_json["public_key_hash"] = encdb::getPublicKeyHash();
  server_info_json["version"] = ENCDB_VERSION;
  // KMS mode: append metadata for encjdbc to retrieve encrypted MEK
  encdb::KmsMetaRecord kms_rec;
  if (encdb_plugin_kms_meta_read_latest(&kms_rec) == 0) {
    server_info_json["kms_mode"] = true;
    server_info_json["kms_enc_mek"] = kms_rec.enc_mek;
    server_info_json["kms_region"] = kms_rec.region;
    server_info_json["kms_key_id"] = kms_rec.key_id;
    server_info_json["kms_mek_version"] = kms_rec.version;
  }
  return server_info_json;
}

#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
EncdbStatus encdb_process_msg_internal(std::string &msgOut,
                                       const std::string &msgIn,
                                       const std::string &username) {
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
EncdbStatus encdb_process_msg_internal(std::string &msgOut,
                                       const std::string &msgIn,
                                       const std::string &username,
                                       const std::string &old_mac,
                                       std::string &new_mac) {
#endif
  EncdbStatus retCode = ENCDB_STATUS_OK;
  std::string response_str;
  char errMsgBuf[ERR_MSG_BUF_SIZE];

  try {
    json::JSON request_json = json::JSON::Load(msgIn);
    encdb::RequestType request_type =
        static_cast<encdb::RequestType>(request_json["request_type"].ToInt());

    switch (request_type) {
      case encdb::RequestType::SERVER_INFO_GET: {
        std::string client_version = request_json["version"].ToString();
        if (client_version.empty()) {
          ENCDB_LOG(LL_WARNING,
                    "Client version is missing (possibly you are using a "
                    "deprecated version). Recommended version is %s.\n",
                    CLIENT_RECOMMENDED_VERSION);
        } else if (!encdb_validate_client_version(client_version)) {
          ENCDB_LOG(LL_WARNING,
                    "Client version is %s. Recommended version is %s.\n",
                    client_version.c_str(), CLIENT_RECOMMENDED_VERSION);
        }

        json::JSON server_info_json;
        server_info_json["server_info"] = encdb_get_server_info();
        response_str = server_info_json.dump();
      } break;
      // MEK_PROVISION requires username to map MEK
#if ENCDB_DB_TYPE == POLARDB_MYSQL
      case encdb::RequestType::MEK_PROVISION: {
        encdb::encdbRecvMek(msgIn, username, old_mac, new_mac);
      } break;
      case encdb::RequestType::MEK_UPDATE: {
        encdb::encdbUpdateMek(msgIn, username, old_mac, new_mac);
      } break;
#else  // NO_DB or RDS_MYSQL
      case encdb::RequestType::MEK_PROVISION: {
        encdb::encdbRecvMek(msgIn, username);
      } break;
      case encdb::RequestType::MEK_UPDATE: {
        encdb::encdbUpdateMek(msgIn, username);
      } break;
#endif
      case encdb::RequestType::RA_GET_QUOTE: {
        // return quote and public key
        std::string user_data = request_json["user_data"].ToString();
        if (user_data.empty()) {
          /*
           * some client tests have not provided this parameter yet
           * use this value to pass the tests
           * this user_data will be decoded to 32 bytes
           */
          user_data = "{}";
        }
        response_str = encdb::getRaQuote(user_data);
      } break;
      case encdb::RequestType::RA_VERIFY_QUOTE: {
        std::string quote_str = request_json["quote"].ToString();
        std::string user_data_json_str = request_json["user_data"].ToString();
        std::string enclave_data_json_str =
            request_json["enclave_data"].ToString();

        json::JSON response_json;
        response_json["envelope"] = encdb::verifyRaQuote(
            quote_str, user_data_json_str, enclave_data_json_str);
        response_str = response_json.dump();
      } break;
      case encdb::RequestType::MEK_IMPORT_FROM_KMS: {
        encdb::encdbImportMekFromKms(msgIn);
      } break;
      case encdb::RequestType::MEK_DETACH_KMS: {
        encdb::encdbExitKmsMode();
      } break;
      default:
        throw encdb::EncdbUnknownMsgTypeException(
            "unknown request_type=" +
            std::to_string(static_cast<int>(request_type)));
    }
  } catch (const encdb::VersionMismatchException &e) {
    // show error message and terminate the connection
    snprintf(errMsgBuf, ERR_MSG_BUF_SIZE,
             "Client version does NOT match -- %s\n", e.what());
    response_str.assign(errMsgBuf);
    retCode = e.get_encdb_status();
    ENCDB_LOG(LL_ERROR, "Client version does NOT match -- %s\n", e.what());
  } catch (const encdb::EncdbException &e) {
    retCode = e.get_encdb_status();
    snprintf(errMsgBuf, ERR_MSG_BUF_SIZE,
             "process request message error (0x%x): %s\n", retCode, e.what());
    response_str.assign(errMsgBuf);
    ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n", retCode,
              e.what());
  }

  msgOut.assign(response_str);

  return retCode;
}
