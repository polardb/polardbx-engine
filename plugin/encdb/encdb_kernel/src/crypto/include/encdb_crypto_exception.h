/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: declaration for crypto exception
 */
#pragma once
#include <string>
#include "encdb_common_exception.h"

namespace encdb {

class CryptoException : public EncdbException {
 public:
  CryptoException(EncdbStatus code, std::string msg);
};

class NoSuchAlgorithmException : public CryptoException {
 public:
  explicit NoSuchAlgorithmException(std::string msg);
};

class NoSuchCCFlagsException : public CryptoException {
 public:
  explicit NoSuchCCFlagsException(std::string msg);
};

class NoSuchCipherSuiteException : public CryptoException {
 public:
  explicit NoSuchCipherSuiteException(std::string msg);
};

class NoSuchCCPolicyException : public CryptoException {
 public:
  explicit NoSuchCCPolicyException(std::string msg);
};

class InvalidAlgorithmParameterException : public CryptoException {
 public:
  explicit InvalidAlgorithmParameterException(std::string msg);
};

class InvalidKeyException : public CryptoException {
 public:
  explicit InvalidKeyException(std::string msg);
};

class MacMismatchException : public CryptoException {
 public:
  explicit MacMismatchException(std::string msg);
};

class EncdbOldMekMissException : public CryptoException {
 public:
  explicit EncdbOldMekMissException(std::string msg);
};

class BadPaddingException : public CryptoException {
 public:
  explicit BadPaddingException(std::string msg);
};

class BadCipherException : public CryptoException {
 public:
  explicit BadCipherException(std::string msg);
};

}  // namespace encdb
