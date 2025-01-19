//
// Created by 王俊岳 on 2025/4/15.
//

#include "../global_defines.h"
#ifdef MYSQL8
#include "my_inttypes.h"
#else
#include "my_global.h"
#endif
#include "mysql_com.h"
#include "password.h"
#include "sha1.h"

#include <openssl/sha.h>
#include <stdexcept>
#include "sha2_verification.h"

#include <sql/auth/i_sha2_password.h>

#include <iomanip>

namespace polarx_rpc {
std::string toHexString(const unsigned char *hash, std::size_t length) {
  std::ostringstream oss;
  for (std::size_t i = 0; i < length; i++) {
    oss << std::hex << std::setw(2) << std::setfill('0') << (int)hash[i];
  }
  return oss.str();
}

bool Sha2_verification::verify_authentication_string(
    const std::string &user, const std::string &host,
    const std::string &client_string, const std::string &db_string) const {
  // db_string = authentication_string, need to extract hashedpassword from
  // authentication_string as db_hash
  std::string digest = extract_digest_from_authentication_string(db_string);
  if (digest.empty()) return false;

  if (client_string.empty()) return db_string.empty();

  if (db_string.empty()) return false;

  uint8 db_hash[SCRAMBLE_LENGTH + 1] = {0};
  uint8 user_hash[SCRAMBLE_LENGTH + 1] = {0};
  // digest需要做处理再传入check_scramble
  // 对digest做两次SHA1，前面加一个*再传入get_salt_from_password
  uint8 hash1[SHA1_HASH_SIZE + 1];
  uint8 hash2[SHA1_HASH_SIZE + 1];

  /* Stage 1: hash password */
  compute_sha1_hash(hash1, digest.c_str(),
                    sha2_password::STORED_SHA256_DIGEST_LENGTH);

  /* Stage 2 : hash first stage's output. */
  compute_sha1_hash(hash2, (const char *)hash1, SHA1_HASH_SIZE);

  // 转换为十六进制字符串
  std::string hexResult = toHexString(hash2, SHA1_HASH_SIZE);

  // 在最前面添加一个星号
  std::string finalResult = "*" + hexResult;

  ::get_salt_from_password(db_hash, finalResult.c_str());
  ::get_salt_from_password(user_hash, client_string.c_str());
  return 0 ==
         ::check_scramble((const uchar *)user_hash, k_salt.c_str(), db_hash);
}

void log_error(const std::string &message) {
  DBUG_PRINT("error", ("%s",message.c_str()));
}

std::string Sha2_verification::extract_digest_from_authentication_string(
    const std::string &db_string) {
  // 定义分隔符的位置
  std::string::size_type delimiter = 0;
  // $A$005${salt}{digest}
  const int required_delimiters = 3;

  // 查找所有需要的分隔符位置
  for (int i = 0; i < required_delimiters; ++i) {
    delimiter = db_string.find(sha2_password::DELIMITER, delimiter);
    if (delimiter == std::string::npos) {
      log_error("authentication_string is not in expected format.");
      return "";
    }
    delimiter += 1;  // 移动到下一个字符位置
  }

  // 计算 digest 的起始位置和长度
  const std::string::size_type start_pos =
      delimiter + sha2_password::SALT_LENGTH;
  if (start_pos > db_string.size()) {
    log_error("authentication_string is not in expected format.");
    return "";
  }

  try {
    return db_string.substr(start_pos, std::string::npos);
  } catch (const std::out_of_range &e) {
    log_error("authentication_string is not in expected format.");
    return "";
  }
}

const std::string Sha2_verification::extract_iter_and_salt_from_authentication_string(
    const std::string &authentication_string) {
  std::string iter_and_salt;

  try {
    // 初始化变量
    std::string::size_type pos = 0;

    // 解析 Digest Type
    std::string::size_type delimiter =
        authentication_string.find(sha2_password::DELIMITER, pos);
    if (delimiter == std::string::npos ||
        delimiter + sha2_password::DIGEST_INFO_LENGTH >=
            authentication_string.size()) {
      log_error("Digest string is not in expected format.");
      return "";
    }
    std::string digest_type_info = authentication_string.substr(
        delimiter + 1, sha2_password::DIGEST_INFO_LENGTH);
    if (digest_type_info != "A") {
      log_error(
          "Digest string is not in expected format. Missing digest type "
          "information.");
      return "";
    }

    // 更新位置
    pos = delimiter + 1 + sha2_password::DIGEST_INFO_LENGTH;

    // 解析 Iteration + salt
    delimiter = authentication_string.find(sha2_password::DELIMITER, pos);
    if (delimiter == std::string::npos ||
        delimiter + sha2_password::ITERATION_LENGTH + 1 +
                sha2_password::SALT_LENGTH >=
            authentication_string.size()) {
      log_error(
          "Digest string is not in expected format. Missing iteration count "
          "information.");
      return "";
    }
    // 更新位置
    pos = delimiter + 1;
    iter_and_salt = authentication_string.substr(
        pos, sha2_password::ITERATION_LENGTH + 1 + sha2_password::SALT_LENGTH);
    if (iter_and_salt.length() !=
        sha2_password::ITERATION_LENGTH + 1 + sha2_password::SALT_LENGTH) {
      log_error(
          "Digest string is not in expected format. Invalid salt "
          "information.");
      return "";
        }
  } catch (const std::exception &e) {
    log_error("Error during parsing: " + std::string(e.what()));
    return "";
  }

  return iter_and_salt;
}
}  // namespace polarx_rpc
