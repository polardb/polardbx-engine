#include "encdb_encode.h"
#include "concurrent_limited_cache.hpp"
#include "encdb_common_exception.h"
#include "encdb_encode_impl.h"

#include "encdb_encode_types.h"

Encode::Encode(const std::string &name) { m_name = name; }
Encode::~Encode() {}
uint32_t Encode::get_encode_size(uint32_t len, const ENCODE_PARA &para) const {
  return len;
}
encode_ret Encode::variadic_encode(ENCODE_VALUE &v, const ENCODE_PARA &para,
                                   uint8_t *result, uint32_t *result_len) {
  return ENCODE_SUCC;
}
encode_ret Encode::encode(ENCODE_VALUE &v, const ENCODE_PARA &para) {
  return ENCODE_SUCC;
}

SEncode create_encdb_encoder(const std::string &name,
                             const std::string &param) {
  return std::make_shared<encdb::EncdbEncode>(name, param);
}
