/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <exception>
#include <string>
#include "encdb_error.h"
#include "memory.hpp"

namespace encdb {

// base class for encdb exception
class EncdbException : public std::exception {
 public:
  EncdbException(EncdbStatus, const std::string &);
  virtual ~EncdbException();
  virtual const char *what(void) const noexcept override;
  virtual EncdbStatus get_encdb_status(void) const noexcept;

 protected:
  EncdbStatus error_code_;
  std::string error_msg_;
};

class ResultOutOfMemoryException : public EncdbException {
 public:
  ResultOutOfMemoryException(std::string msg, uint32_t len);
  uint32_t get_expected_length() const noexcept;

 protected:
  uint32_t expected_len_;
};

class OutOfMemoryException : public EncdbException {
 public:
  OutOfMemoryException(std::string msg);
};

class EncdbInvalidParameterException : public EncdbException {
 public:
  EncdbInvalidParameterException(const std::string & = "invalid parameter");
};

class EncdbUnsupportedOperationException : public EncdbException {
 public:
  EncdbUnsupportedOperationException(std::string error_msg);
};

class EncdbUnknownMsgTypeException : public EncdbException {
 public:
  EncdbUnknownMsgTypeException(const std::string &error_msg);
};

class VersionMismatchException : public EncdbException {
 public:
  VersionMismatchException(const std::string &msg);
};

class InvalidTextRepresentationException : public EncdbException {
 public:
  InvalidTextRepresentationException(const std::string &msg);
};

class EncdbInternalSQLException : public EncdbException {
 public:
  EncdbInternalSQLException(const std::string &msg);
};

class InvalidSensitiveAlgoException : public EncdbException {
 public:
  InvalidSensitiveAlgoException(const std::string &msg);
};

class InvalidMaskAlgoException : public EncdbException {
 public:
  InvalidMaskAlgoException(const std::string &msg);
};

class InvalidJsonDataException : public EncdbException {
 public:
  InvalidJsonDataException(const std::string &msg);
};

class JsonKeyMissException : public EncdbException {
 public:
  JsonKeyMissException(const std::string &msg);
};

class InvalidValueReadFromDatabaseException : public EncdbException {
 public:
  InvalidValueReadFromDatabaseException(const std::string &msg);
};

class CacheMissException : public EncdbException {
 public:
  CacheMissException(const std::string &msg);
};

class InvalidTimestampException : public EncdbException {
 public:
  InvalidTimestampException(const std::string &msg);
};

}  // namespace encdb
