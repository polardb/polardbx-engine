#pragma once

#include <string>
#include "encdb_encode.h"
#include "mask.h"
#include "mysql.hh"

#include "bytes.h"
#include "encdb_encode_types.h"

namespace encdb {

/*
 * Only support text protocol
 */
class EncdbEncode : public Encode {
 public:
  EncdbEncode(const std::string &name, const std::string &params);
  ~EncdbEncode();
  bool is_variadic_encoding();
  bool need_encrypting();
  uint32_t get_encode_size(uint32_t length, const ENCODE_PARA &para) const;
  encode_ret variadic_encode(ENCODE_VALUE &v, const ENCODE_PARA &para,
                             uint8_t *result, uint32_t *result_len);

 private:
  bool encode_bytes(const bytes_t &b, const ENCODE_PARA &para, uint8_t *result,
                    uint32_t *result_len, enum_field_types type) const;
  std::string alg_params_;
  EncodeBasicAlgo enc_alg_;
  EncodeBasicAlgo mask_alg_;
};

}  // namespace encdb
