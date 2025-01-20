//
// Created by 王俊岳 on 2025/4/15.
//

#pragma once

#include <string>

#include "challenge_response_verification.h"

namespace polarx_rpc {

class Sha2_verification : public Challenge_response_verification {
 public:
  bool verify_authentication_string(
      const std::string &user, const std::string &host,
      const std::string &client_string,
      const std::string &db_string) const override;
  const std::string extract_iter_and_salt_from_authentication_string(
      const std::string &authentication_string);

 private:
  static std::string extract_digest_from_authentication_string(
      const std::string &db_string);
};

}  // namespace polarx_rpc