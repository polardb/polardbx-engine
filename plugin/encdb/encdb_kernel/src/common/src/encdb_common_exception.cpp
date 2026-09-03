/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#include "encdb_common_exception.h"
#include <cstring>
#include <string>

namespace encdb {

// class EncdbException
EncdbException::EncdbException(EncdbStatus status, const std::string &msg)
    : error_code_(status), error_msg_(msg) {}

EncdbException::~EncdbException() {}

const char *EncdbException::what(void) const noexcept {
  return error_msg_.data();
}

EncdbStatus EncdbException::get_encdb_status(void) const noexcept {
  return error_code_;
}

ResultOutOfMemoryException::ResultOutOfMemoryException(std::string msg,
                                                       uint32_t len)
    : EncdbException(ERROR_RESULT_OUT_OF_MEMORY, msg), expected_len_(len) {}

uint32_t ResultOutOfMemoryException::get_expected_length() const noexcept {
  return expected_len_;
}

OutOfMemoryException::OutOfMemoryException(std::string msg)
    : EncdbException(ERROR_OUT_OF_MEMORY, msg) {}

EncdbInvalidParameterException::EncdbInvalidParameterException(
    const std::string &msg)
    : EncdbException(ENCDB_INVALID_PARAMETER_EXCEPTION, msg) {}

EncdbUnsupportedOperationException::EncdbUnsupportedOperationException(
    std::string error_msg)
    : EncdbException(ENCDB_UNSUPPORTED_OPERATION_EXCEPTION, error_msg) {}

VersionMismatchException::VersionMismatchException(const std::string &msg)
    : EncdbException(ENCDB_VERSION_MISMATCH_EXCEPTION, msg) {}

InvalidTextRepresentationException::InvalidTextRepresentationException(
    const std::string &msg)
    : EncdbException(ENCDB_INVALID_TEXT_REPRESENTATION, msg) {}

EncdbUnknownMsgTypeException::EncdbUnknownMsgTypeException(
    const std::string &error_msg)
    : EncdbException(ENCDB_EXCEPTION_UNKNOWN_MSG_TYPE, error_msg) {}

EncdbInternalSQLException::EncdbInternalSQLException(
    const std::string &error_msg)
    : EncdbException(ENCDB_EXCEPTION_INTERNAL_SQL_ERROR, error_msg) {}

InvalidSensitiveAlgoException::InvalidSensitiveAlgoException(
    const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_NO_SUCH_SENSITIVE_ALGO, msg) {}

InvalidMaskAlgoException::InvalidMaskAlgoException(const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_NO_SUCH_MASK_ALGO, msg) {}

InvalidJsonDataException::InvalidJsonDataException(const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_INVALID_JSON_DATA, msg) {}

JsonKeyMissException::JsonKeyMissException(const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_JSON_KEY_MISS, msg) {}

InvalidValueReadFromDatabaseException::InvalidValueReadFromDatabaseException(
    const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_INVALID_DATABASE_DATA, msg) {}

CacheMissException::CacheMissException(const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_CACHE_MISS, msg) {}

InvalidTimestampException::InvalidTimestampException(const std::string &msg)
    : EncdbException(ENCDB_EXCEPTION_INVALID_TIMESTAMP, msg) {}
}  // namespace encdb
