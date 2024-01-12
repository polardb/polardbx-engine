#pragma once

#include <string>

namespace polarx_rpc {
class Identifier {
 public:
  Identifier(const std::string &identifier) : identifier_(identifier) {}
  Identifier(std::string &&identifier)
      : identifier_(std::forward<std::string>(identifier)) {}

  std::string identifier_;
};
}  // namespace polarx_rpc
