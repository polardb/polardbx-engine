/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: AbstractCipher implementation for MySQL proxy
 */
#include "encdb.h"
#include <memory>
#include <string>
#include "EncDBConfig.h"
#include "concurrent_limited_cache.hpp"
#include "enc_aux.h"
#include "encdb_cipher.h"
#include "encdb_common_exception.h"
#include "encdb_rules.h"
#include "encdb_uts.h"
#include "helper.hpp"
#include "json.hpp"
#include "key_mgmt.h"
#include "logger.h"

#define START_ENCDB_ERR_CATCH try {
#define END_ENCDB_ERR_CATCH                                           \
  }                                                                   \
  catch (const encdb::EncdbException &e) {                            \
    ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n", \
              e.get_encdb_status(), e.what());                        \
  }

#if ENCDB_DB_TYPE == POLARDB_MYSQL
// TODO(yx): In TDX trust model we do not need different MEKs for different
// users for now. So whatever actual username is,
//           we use a GLOBAL_USERNAME "encdb" to do
//           encryption/decryption/key-management operations
#define GLOBAL_USERNAME "encdb"
#endif

namespace encdb {

EncdbStatus EncdbCipher::encrypt(const std::string &uname,
                                 uint8_t *base64_cipher,
                                 uint32_t *base64_cipher_len,
                                 const uint8_t *plaintext,
                                 uint32_t plaintext_len, uint8_t type) {
  START_ENCDB_ERR_CATCH
  if (IS_NULL(base64_cipher) || IS_NULL(base64_cipher_len) ||
      IS_NULL(plaintext)) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  UserAccess accs = hook::encdbGetUserAccess(uname);
  // Ensure enough memory is allocated, no more check will be done later.
  uint32_t expected_size = expectedCipherSize(uname, plaintext_len),
           given_size = *base64_cipher_len;
  if (given_size != expected_size) {
    ENCDB_LOG(LL_ERROR, "expected %d bytes for cipher, but got %d",
              expected_size, given_size);
    return ERROR_NOT_ENOUGH_MEMORY_FROM_PROXY;
  }
  bytes_t ret_data(*base64_cipher_len);
  bytes_t in(plaintext, plaintext_len);
  EncdbStatus status = encdb_untrusted_encrypt(in, type,
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
                                               uname,
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
                                               GLOBAL_USERNAME,
#endif
                                               ret_data, accs);
  if (!ENCDB_STATUS_IS_OK(status)) {
    return status;
  }
  std::string base64_ret = Utils::base64_encode(ret_data);
  memcpy(base64_cipher, base64_ret.data(), base64_ret.size());
  *base64_cipher_len = base64_ret.size();
  return ENCDB_STATUS_OK;
  END_ENCDB_ERR_CATCH
  return ENCDB_EXCEPTION;
}

EncdbStatus EncdbCipher::encrypt(const std::string &uname,
                                 uint8_t *base64_cipher,
                                 uint32_t *base64_cipher_len,
                                 const uint8_t *plaintext,
                                 uint32_t plaintext_len, uint8_t type,
                                 uint8_t alg) {
  if (IS_NULL(base64_cipher) || IS_NULL(base64_cipher_len) ||
      IS_NULL(plaintext)) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  UserAccess accs = hook::encdbGetUserAccess(uname);
  // Ensure enough memory is allocated, no more check will be done later.
  if (*base64_cipher_len < expectedCipherSize(plaintext_len, alg)) {
    return ERROR_NOT_ENOUGH_MEMORY_FROM_PROXY;
  }
  bytes_t ret_data(*base64_cipher_len);
  bytes_t in(plaintext, plaintext_len);
  EncdbStatus status = encdb_untrusted_encrypt(in, type,
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
                                               uname,
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
                                               GLOBAL_USERNAME,
#endif
                                               ret_data, accs, alg);
  if (!ENCDB_STATUS_IS_OK(status)) {
    return status;
  }
  std::string base64_ret = Utils::base64_encode(ret_data);
  memcpy(base64_cipher, base64_ret.data(), base64_ret.size());
  *base64_cipher_len = base64_ret.size();
  return ENCDB_STATUS_OK;
}

EncdbStatus EncdbCipher::decrypt(const std::string &uname, uint8_t *plaintext,
                                 uint32_t *plaintext_len, uint8_t *type,
                                 const uint8_t *base64_cipher,
                                 uint32_t base64_cipher_len) {
  if (IS_NULL(plaintext) || IS_NULL(plaintext_len) || IS_NULL(base64_cipher)) {
    return ENCDB_INVALID_PARAMETER_EXCEPTION;
  }
  if (base64_cipher_len == 0 || base64_cipher_len % 4 != 0) {
    return ENCDB_EXCEPTION_INVALID_DATABASE_DATA;
  }
  if (*plaintext_len < base64_cipher_len) {
    return ERROR_NOT_ENOUGH_MEMORY_FROM_PROXY;
  }
  std::string base64_cipher_str(reinterpret_cast<const char *>(base64_cipher),
                                base64_cipher_len);
  bytes_t cipher = Utils::base64_decode_to_bytes(base64_cipher_str);
  bytes_t res_plaintext(cipher.size());
  EncdbStatus status = encdb_untrusted_decrypt(cipher,
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
                                               uname,
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
                                               GLOBAL_USERNAME,
#endif
                                               res_plaintext, *type);
  if (!ENCDB_STATUS_IS_OK(status)) {
    return status;
  }
  *plaintext_len = res_plaintext.size();
  memcpy(plaintext, res_plaintext.data(), res_plaintext.size());
  return ENCDB_STATUS_OK;
}

uint32_t EncdbCipher::expectedCipherSize(const std::string &uname,
                                         uint32_t length) {
  START_ENCDB_ERR_CATCH
  return Utils::Base64EncodedLen(encdb_get_precise_cipher_size(uname, length));
  END_ENCDB_ERR_CATCH
  return 0;
}

uint32_t EncdbCipher::expectedCipherSize(uint32_t length, uint8_t alg) {
  return Utils::Base64EncodedLen(encdb_get_precise_cipher_size(length, alg));
}

uint32_t EncdbCipher::expectedCipherSize(uint32_t n, uint32_t total_length) {
  return Utils::Base64EncodedLen(Cipher::size(n, total_length) * n) +
         n;  // The last n compensates for error in base64 encoding
}

void KeyStore::processKeyRequest(const std::string &msg,
                                 const std::string &username,
                                 const std::string &, std::string &,
                                 std::string &ret) {
  std::string msgOut;
  EncdbStatus status = encdb_process_msg_internal(msgOut, msg,
#if ENCDB_DB_TYPE == RDS_MYSQL || ENCDB_DB_TYPE == NO_DB
                                                  username);
#elif ENCDB_DB_TYPE == POLARDB_MYSQL
                                                  GLOBAL_USERNAME, old_mac,
                                                  new_mac);
#endif
  json::JSON retObj;
  retObj["status"] = static_cast<long>(status);
  retObj["body"] = msgOut;
  ret.assign(retObj.dump());
  Utils::base64_inplace_encode(ret);
}

bool KeyStore::whetherNeedEncryption(const std::string &database,
                                     const std::string &table,
                                     const std::string &column,
                                     const std::string &username) {
  // database encdb is only operatable with plugin encdb
  START_ENCDB_ERR_CATCH
  if (database == "encdb" /* encdb internal tables */ ||
      database == "" /* For SQLs like SHOW VARIABLES */
      || database == "information_schema" || database == "mysql" ||
      database == "sys" || database == "__recycle_bin__" ||
      database == "performance_schema" /* MySQL sys tables*/) {
    return false;
  }
  if (hook::encdbGetUserAccess(username) == UserAccess::FULL_ACCESS) {
    return false;
  }
  return hook::encdbNeedEncryption(database, table, column);
  END_ENCDB_ERR_CATCH
  return true;
}

std::string KeyStore::ruleExport(std::string &err_msg) {
  try {
    err_msg.clear();
    return udf::encdbExportEncRules();
  } catch (const encdb::EncdbException &e) {
    ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n",
              e.get_encdb_status(), e.what());
    err_msg.assign(e.what());
  }
  return "null";
}

std::string KeyStore::userAuthExport(std::string &err_msg) {
  try {
    err_msg.clear();
    return udf::encdbExportUserAuths();
  } catch (const encdb::EncdbException &e) {
    ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n",
              e.get_encdb_status(), e.what());
    err_msg.assign(e.what());
  }
  return "null";
}
int KeyStore::ruleSpreadInsertOrCreateFrom(
    const std::string &src_db, const std::string &src_tbl,
    const std::string &src_col, const std::string &dst_db,
    const std::string &dst_tbl, const std::string &dst_col, bool dst_tmp) {
  START_ENCDB_ERR_CATCH
  return hook::encdbRuleSpreadInsertOrCreateFrom(
      src_db, src_tbl, src_col, dst_db, dst_tbl, dst_col, dst_tmp);
  END_ENCDB_ERR_CATCH
  return -1;
}

int KeyStore::ruleSpreadRenameTable(const std::string &src_db,
                                    const std::string &src_tbl,
                                    const std::string &dst_db,
                                    const std::string &dst_tbl, bool tmp) {
  START_ENCDB_ERR_CATCH
  return hook::encdbRuleSpreadRenameTable(src_db, src_tbl, dst_db, dst_tbl,
                                          tmp);
  END_ENCDB_ERR_CATCH
  return -1;
}

int KeyStore::ruleSpreadRenameColumn(const std::string &src_db,
                                     const std::string &src_tbl,
                                     const std::string &src_col,
                                     const std::string &dst_col, bool tmp) {
  START_ENCDB_ERR_CATCH
  return hook::encdbRuleSpreadRenameColumn(src_db, src_tbl, src_col, dst_col,
                                           tmp);
  END_ENCDB_ERR_CATCH
  return -1;
}

void KeyStore::reloadAllRules() {
  START_ENCDB_ERR_CATCH
  return hook::encdbReloadAllEncRules();
  END_ENCDB_ERR_CATCH
}

std::string KeyStore::ruleOp(std::string &err_msg, const std::string &op,
                             const std::string &param) {
  try {
    err_msg.clear();
    if (op == "add") {
      udf::encdbAddOrUpdateEncRules(param);
    } else if (op == "update") {
      udf::encdbAddOrUpdateEncRules(param);
    } else if (op == "delete") {
      udf::encdbDeleteEncRules(param);
    } else if (op == "grant") {
      udf::encdbSetUserAuths(param);
    } else {
      throw EncdbUnsupportedOperationException(op + " not supported");
    }
    return "OK";
  } catch (const encdb::EncdbException &e) {
    ENCDB_LOG(LL_ERROR, "process request message error (0x%x): %s\n",
              e.get_encdb_status(), e.what());
    err_msg.assign(e.what());
  }
  return "FAILED";
}

void KeyStore::setDefaultUserAuths(const std::string &rights) {
  udf::encdbSetDefaultUserAuths(rights);
}

void KeyStore::setDefaultAlgorithm(const std::string &algorithm) {
  setDefaultEncryptionAlgorithm(algorithm);
}

bool ForTestOnly::isResponseOK(const std::string &base64_str) {
  bytes_t bytes = Utils::base64_decode_to_bytes(base64_str);
  std::string str(reinterpret_cast<const char *>(bytes.data()), bytes.size());
  json::JSON ret_json = json::JSON::Load(str);
  return ret_json["status"].ToInt() == 0;
}

std::string ForTestOnly::generateMekProvisionMsgFromServerInfo(
    const std::string &base64_server_info, const std::string &hex_mek,
    const std::string &algo) {
  START_ENCDB_ERR_CATCH
  bytes_t server_info_bytes = Utils::base64_decode_to_bytes(base64_server_info);
  std::string server_info(
      reinterpret_cast<const char *>(server_info_bytes.data()),
      server_info_bytes.size());
  bytes_t mek = Utils::hexstring2encdbbytes(hex_mek);
  json::JSON ret_json = json::JSON::Load(server_info);
  std::string ret_body_str = ret_json["body"].ToString();
  json::JSON ret_body_json = json::JSON::Load(ret_body_str);
  std::string server_cipher_suite =
      ret_body_json["server_info"]["cipher_suite"].ToString();
  std::string server_puk =
      ret_body_json["server_info"]["public_key"].ToString();
  std::string server_puk_hash =
      ret_body_json["server_info"]["public_key_hash"].ToString();
  json::JSON request_json, envelope_json;
  request_json["request_type"] = 48;  // MEK_PROVISION
  request_json["cipher_suite"] = server_cipher_suite;
  request_json["public_key_hash"] = server_puk_hash;
  request_json["algorithm"] = algo;
  envelope_json["mek"] = Utils::base64_encode(mek);
  envelope_json["expire"] = 1;
  std::string envelope_str = envelope_json.dump();
  bytes_t envelope_bytes(reinterpret_cast<const uint8_t *>(envelope_str.data()),
                         envelope_str.size());
  CipherSuite ciphersuite = CipherSuite::to_cipher_suite(server_cipher_suite);
  bytes_t sealed_envelope =
      KeyExchange::seal(ciphersuite, server_puk, envelope_bytes);
  std::string sealed_envelope_base64 = Utils::base64_encode(sealed_envelope);
  request_json["envelope"] = sealed_envelope_base64;

  std::string request_str = request_json.dump();
  return request_str;
  END_ENCDB_ERR_CATCH
  return "";
}

std::string ForTestOnly::generateMekUpdateMsgFromServerInfo(
    const std::string &base64_server_info, const std::string &old_hex_mek,
    const std::string &new_hex_mek) {
  bytes_t server_info_bytes = Utils::base64_decode_to_bytes(base64_server_info);
  std::string server_info(
      reinterpret_cast<const char *>(server_info_bytes.data()),
      server_info_bytes.size());
  bytes_t old_mek = Utils::hexstring2encdbbytes(old_hex_mek);
  bytes_t new_mek = Utils::hexstring2encdbbytes(new_hex_mek);
  json::JSON ret_json = json::JSON::Load(server_info);
  std::string ret_body_str = ret_json["body"].ToString();
  json::JSON ret_body_json = json::JSON::Load(ret_body_str);
  std::string server_cipher_suite =
      ret_body_json["server_info"]["cipher_suite"].ToString();
  std::string server_puk =
      ret_body_json["server_info"]["public_key"].ToString();
  std::string server_puk_hash =
      ret_body_json["server_info"]["public_key_hash"].ToString();
  json::JSON request_json, envelope_json;
  request_json["request_type"] = 49;  // MEK_UPDATE
  request_json["cipher_suite"] = server_cipher_suite;
  request_json["public_key_hash"] = server_puk_hash;
  envelope_json["old_mek"] = Utils::base64_encode(old_mek);
  envelope_json["mek"] = Utils::base64_encode(new_mek);
  envelope_json["expire"] = 1;
  std::string envelope_str = envelope_json.dump();
  bytes_t envelope_bytes(reinterpret_cast<const uint8_t *>(envelope_str.data()),
                         envelope_str.size());
  CipherSuite ciphersuite = CipherSuite::to_cipher_suite(server_cipher_suite);
  bytes_t sealed_envelope =
      KeyExchange::seal(ciphersuite, server_puk, envelope_bytes);
  std::string sealed_envelope_base64 = Utils::base64_encode(sealed_envelope);
  request_json["envelope"] = sealed_envelope_base64;

  std::string request_str = request_json.dump();
  return request_str;
}

}  // namespace encdb
