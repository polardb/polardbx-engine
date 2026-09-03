/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: Version used in EncDB.
 */
#pragma once
#include <string>
#include "memory.hpp"

namespace encdb {

class Version : public EncdbBaseObj {
  int major, minor, patch;

 public:
  Version(const std::string &version_str);
  bool operator<(const Version &other) const noexcept;
  bool operator==(const Version &other) const noexcept;
};

}  // namespace encdb
