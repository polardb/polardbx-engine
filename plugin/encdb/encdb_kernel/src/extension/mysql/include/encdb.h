/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: APIs provided to MySQL proxy.
 */
#pragma once
#include <stdint.h>
#include <memory>
#include <string>
#include "encdb_error.h"

namespace encdb {

class EncdbCipher {
 public:
  /**
   * @brief Encrypt input plaintext
   *
   * @param cipher [out] output cipher blob
   * @param cipher_len [in/out] input init length of cipher buffer (typically
   * cipherSize( \c plaintext_len) ), and output real size of returned cipher
   * buffer
   * @param plaintext [in] input plaintext that will be encrypted
   * @param plaintext_len [in] length of plaintext
   * @param type [in] type of data (enum_field_types)
   * @return EncdbStatus 0 on success
   */
  static EncdbStatus encrypt(const std::string &uname, uint8_t *cipher,
                             uint32_t *cipher_len, const uint8_t *plaintext,
                             uint32_t plaintext_len, uint8_t type);

  static EncdbStatus encrypt(const std::string &uname, uint8_t *cipher,
                             uint32_t *cipher_len, const uint8_t *plaintext,
                             uint32_t plaintext_len, uint8_t type, uint8_t alg);
  /**
   * @brief Decrypt input cipher
   *
   * @param plaintext [out] output plaintext blob
   * @param plaintext_len [in/out] input init length of plaintext buffer
   * (typically \c cipher_len ), and output real size of returned plaintext
   * buffer
   * @param type [out] type of data (enum_field_types)
   * @param cipher [in] input ciphertext that will be decrypted
   * @param cipher_len [in] length of ciphertext
   * @return EncdbStatus 0 on success
   */
  static EncdbStatus decrypt(const std::string &uname, uint8_t *plaintext,
                             uint32_t *plaintext_len, uint8_t *type,
                             const uint8_t *cipher, uint32_t cipher_len);
  /**
   * @brief Compute required cipher length for input plaintext length.
   *
   * @param length [in] length of plaintext
   * @return uint32_t upper bound of expected cipher length
   */
  static uint32_t expectedCipherSize(const std::string &uname, uint32_t length);
  static uint32_t expectedCipherSize(uint32_t length, uint8_t alg);
  /**
   * @brief Compute required (upper bound) cipher length for \p n plaintext
   * objects with a total length of \p total_length
   *
   * @param n [in] number of plaintext objects
   * @param total_length [in] total size of plaintext objects
   * @return required total cipher size
   */
  static uint32_t expectedCipherSize(uint32_t n, uint32_t total_length);
};

class KeyStore {
 public:
  /**
   * @brief A universal API provided by EncDB. Can support many kinds of key
   * management operations, like key-provision or rule setting.
   *
   * @param msg [in] A json string, containing data used for key management
   * operations.
   * @param username [in]
   * @param old_mac  [in] User's existing mac in Proxy's storage. If none, just
   * pass "".
   * @param new_mac  [out] If MEK is updated by EncDB, a new_mac will be
   * generated. Proxy need to cache new_mac if it's a non-empty string/
   * @param reyt     [out] Output message
   * @return A base 64 encoded string, whose decoded content is actually a json
   * string, in below format:
   * {
   *   "status": xxx,
   *   "body": "xxx"
   * }
   */
  static void processKeyRequest(const std::string &msg,
                                const std::string &username,
                                const std::string &old_mac,
                                std::string &new_mac, std::string &ret);
  /**
   * @brief Return if a field (consists of user, database, table and column)
   * requires encryption.
   *
   * @param username [in]
   * @param database [in]
   * @param table    [in]
   * @param column   [in]
   * @return bool
   * @warning this interface should only be used with RDS_MYSQL
   */
  static bool whetherNeedEncryption(const std::string &database,
                                    const std::string &table,
                                    const std::string &column,
                                    const std::string &username);
  /**
   * @brief dump all in-memory rules into a string.
   */
  static std::string ruleExport(std::string &err_msg);
  static std::string userAuthExport(std::string &err_msg);
  static int ruleSpreadInsertOrCreateFrom(
      const std::string &src_db, const std::string &src_tbl,
      const std::string &src_col, const std::string &dst_db,
      const std::string &dst_tbl, const std::string &dst_col, bool dst_tmp);
  static int ruleSpreadRenameTable(const std::string &src_db,
                                   const std::string &src_tbl,
                                   const std::string &dst_db,
                                   const std::string &dst_tbl, bool tmp);
  static int ruleSpreadRenameColumn(const std::string &src_db,
                                    const std::string &src_tbl,
                                    const std::string &src_col,
                                    const std::string &dst_col, bool tmp);
  static void reloadAllRules();
  static std::string ruleOp(std::string &err_msg, const std::string &op,
                            const std::string &param);

  static void setDefaultUserAuths(const std::string &rights);

  static void setDefaultAlgorithm(const std::string &algorithm);
};

// This class should be used for test only.
class ForTestOnly {
 public:
  /**
   * @brief Check if a response string returned by KeyStore::processKeyRequest
   * is OK.
   *
   * @param str [in] A string returned by KeyStore::processKeyRequest
   * @return boolean, true for OK
   */
  static bool isResponseOK(const std::string &str);
  /**
   * @brief Generate a Request string that can be used to do MEK_PROVISION with
   * KeyStore::processKeyRequest, using a server_info generated from
   * KeyStore::processKeyRequest
   *
   * @param server_info [in] can be generated from KeyStore::processKeyRequest,
   * with request: {"request_type":0, "version":"0.0.1"}
   * @param mek         [in] A hex string (length 32). User's own secret key.
   * @return A string that can be used to do MEK_PROVISION with
   * KeyStore::processKeyRequest
   */
  static std::string generateMekProvisionMsgFromServerInfo(
      const std::string &server_info, const std::string &hex_mek,
      const std::string &algo = "SM4_128_CBC");
  /**
   * @brief Generate a Request string that can be used to do MEK_UPDATE with
   * KeyStore::processKeyRequest, using a server_info generated from
   * KeyStore::processKeyRequest
   *
   * @param server_info [in] can be generated from KeyStore::processKeyRequest,
   * with request: {"request_type":0, "version":"0.0.1"}
   * @param old_hex_mek [in] A hex string (length 32). User's old secret key.
   * @param new_hex_mek [in] A hex string (length 32). User's new secret key.
   * @return A string than can be used to do MEK_UPDATE with
   * KeyStore::processKeyRequest
   */
  static std::string generateMekUpdateMsgFromServerInfo(
      const std::string &server_info, const std::string &old_hex_mek,
      const std::string &new_hex_mek);
};

}  // namespace encdb
