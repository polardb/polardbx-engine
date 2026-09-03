/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <string>
#include <utility>
#include "encdb_cipher_types.h"
#include "encdb_common_exception.h"

namespace encdb {

// base class for encdb key exceptions
class EncdbKeyException : public EncdbException {
 public:
  explicit EncdbKeyException(EncdbStatus, const std::string &);
  EncdbKeyException();
};

class EncdbMekMissException : public EncdbKeyException {
 public:
  EncdbMekMissException(MekId);
  virtual MekId get_missing_mek_id(void) const noexcept;

 protected:
  const MekId mek_id_;
};

class EncdbCcMissException : public EncdbKeyException {
  const std::string user_name_;

 public:
  EncdbCcMissException(const std::string &);
  virtual std::string get_missing_user_name(void) const noexcept;
};

class EncdbEnclaveKeyPairMissException : public EncdbKeyException {
 public:
  EncdbEnclaveKeyPairMissException();
};

class EncdbImportEncRuleException : public EncdbKeyException {
 public:
  EncdbImportEncRuleException(const std::string &);
};

class EncdbRAException : public EncdbKeyException {
 public:
  EncdbRAException(const std::string &);
};

}  // namespace encdb
