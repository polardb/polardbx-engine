#pragma once

#include <string>

namespace polarx_rpc {
class Identifier {
public:
  Identifier(const std::string &identifier) : identifier_(identifier) {}

  std::string identifier_;
};
}
