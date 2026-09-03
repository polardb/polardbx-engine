/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once
#include <ctime>
#include <string>
#include "memory.hpp"

namespace encdb {

class Timer : public EncdbBaseObj {
 public:
  static std::string Now();
};

}  // namespace encdb
