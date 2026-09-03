/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once
#include <ctime>
#include <string>
#include "memory.hpp"

class Timestamp : public encdb::EncdbBaseObj {
 public:
  // Default constructor, should not be called explicitly
  Timestamp() {}
  // Build Timestamp from a string in format YYYY-MM-DD hh:mm:ss
  Timestamp(const std::string &, bool);
  bool operator>(const Timestamp &) const;
  bool operator!=(const Timestamp &) const;
  bool operator==(const Timestamp &) const;
  std::string to_string() const;

 private:
  int year, month, day, hour, minute, second;
};
const Timestamp MYSQL_MIN_TIME("1000-01-01 00:00:00", false);
const Timestamp MYSQL_MAX_TIME("9999-12-31 23:59:59", false);
