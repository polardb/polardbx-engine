/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 *
 * @desc: definition for enc types
 */
#pragma once
#include <cstdint>
#include <string>

namespace encdb {

typedef enum {
  // Encrypt algos
  ENCRYPT_AES_GCM = 0,
  ENCRYPT_AES_ECB,
  ENCRYPT_AES_CTR,
  ENCRYPT_AES_CBC,
  ENCRYPT_SM4_CBC,
  ENCRYPT_SM4_ECB,
  ENCRYPT_SM4_CTR,
  ENCRYPT_SM4_GCM,
  // Mask algo
  MASK_FIX_POS,
  __INVALIDALGO__
} EncodeBasicAlgo;

typedef enum { ENCRYPT, MASK, __INVALIDCLASS__ } EncodeAlgoClass;

inline std::string to_string(EncodeBasicAlgo alg) {
  switch (alg) {
    case EncodeBasicAlgo::ENCRYPT_AES_CBC:
      return "aes-128-cbc";
    case EncodeBasicAlgo::ENCRYPT_AES_CTR:
      return "aes-128-ctr";
    case EncodeBasicAlgo::ENCRYPT_AES_ECB:
      return "aes-128-ecb";
    case EncodeBasicAlgo::ENCRYPT_AES_GCM:
      return "aes-128-gcm";
    case EncodeBasicAlgo::ENCRYPT_SM4_CBC:
      return "sm4-128-cbc";
    case EncodeBasicAlgo::ENCRYPT_SM4_CTR:
      return "sm4-128-ctr";
    case EncodeBasicAlgo::ENCRYPT_SM4_ECB:
      return "sm4-128-ecb";
    case EncodeBasicAlgo::ENCRYPT_SM4_GCM:
      return "sm4-128-gcm";
    case EncodeBasicAlgo::MASK_FIX_POS:
      return "fix-pos-mask";
    default:
      return "";
  }
}
inline EncodeBasicAlgo from_string(const std::string &str) {
  if (str == "aes-128-cbc") {
    return EncodeBasicAlgo::ENCRYPT_AES_CBC;
  } else if (str == "aes-128-ctr") {
    return EncodeBasicAlgo::ENCRYPT_AES_CTR;
  } else if (str == "aes-128-ecb") {
    return EncodeBasicAlgo::ENCRYPT_AES_ECB;
  } else if (str == "aes-128-gcm") {
    return EncodeBasicAlgo::ENCRYPT_AES_GCM;
  } else if (str == "sm4-128-cbc") {
    return EncodeBasicAlgo::ENCRYPT_SM4_CBC;
  } else if (str == "sm4-128-ctr") {
    return EncodeBasicAlgo::ENCRYPT_SM4_CTR;
  } else if (str == "sm4-128-ecb") {
    return EncodeBasicAlgo::ENCRYPT_SM4_ECB;
  } else if (str == "sm4-128-gcm") {
    return EncodeBasicAlgo::ENCRYPT_SM4_GCM;
  } else if (str == "fix-pos-mask") {
    return EncodeBasicAlgo::MASK_FIX_POS;
  } else {
    return EncodeBasicAlgo::__INVALIDALGO__;
  }
}
inline EncodeAlgoClass get_encode_class(EncodeBasicAlgo alg) {
  switch (alg) {
    case EncodeBasicAlgo::ENCRYPT_AES_CBC:
    case EncodeBasicAlgo::ENCRYPT_AES_CTR:
    case EncodeBasicAlgo::ENCRYPT_AES_ECB:
    case EncodeBasicAlgo::ENCRYPT_AES_GCM:
    case EncodeBasicAlgo::ENCRYPT_SM4_CBC:
    case EncodeBasicAlgo::ENCRYPT_SM4_CTR:
    case EncodeBasicAlgo::ENCRYPT_SM4_ECB:
    case EncodeBasicAlgo::ENCRYPT_SM4_GCM:
      return EncodeAlgoClass::ENCRYPT;
    case EncodeBasicAlgo::MASK_FIX_POS:
      return EncodeAlgoClass::MASK;
    default:
      return EncodeAlgoClass::__INVALIDCLASS__;
  }
}

}  // namespace encdb
