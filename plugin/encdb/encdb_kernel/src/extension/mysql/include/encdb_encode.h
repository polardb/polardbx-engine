/*
 * This file is ported from PolarDB Proxy (or MaxScale).
 * The code standard might be inconsistent with SecureGW requirements, but we
 * ignore here.
 */
#pragma once
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#define ALGO_CONNECTOR "-with-"

typedef enum { ENCODE_SUCC, ENCODE_TYPE_NOT_SUPPORTED, ENCODE_FAIL } encode_ret;
typedef struct {
  std::string hint_name;  // algorithm name specified with SQL's hint
  std::string user;       // username
  uint32_t field_num;     // number of fields that need encoding
} ENCODE_PARA;
typedef struct {
  bool is_binary_result;  // is binary protocol, or text protocol
  uint16_t type;          // mysql data type
  uint16_t flag;          // mysql data flag
  uint8_t *pData;         // data itself
} ENCODE_VALUE;

class Encode;
typedef std::shared_ptr<Encode> SEncode;

class Encode {
 public:
  Encode(const std::string &name);
  virtual ~Encode();
  virtual bool is_variadic_encoding() = 0;
  virtual bool need_encrypting() = 0;
  virtual uint32_t get_encode_size(uint32_t length,
                                   const ENCODE_PARA &para) const;
  virtual encode_ret variadic_encode(ENCODE_VALUE &v, const ENCODE_PARA &para,
                                     uint8_t *result, uint32_t *result_len);
  virtual encode_ret encode(ENCODE_VALUE &v, const ENCODE_PARA &para);
  static SEncode create(const std::string &name, const std::string &params);

 protected:
  std::string m_name;
};
SEncode create_encdb_encoder(const std::string &name,
                             const std::string &params);
