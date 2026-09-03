/**
 * Copyright (c) 2019-2023 Alibaba DAMO DBLab. All rights reserved.
 */
#pragma once

#include <array>
#include <cassert>
#include <cstdarg>
#include <string>
#include <vector>
#include "bytes.h"
#include "encdb_common_exception.h"
#include "memory.hpp"

namespace encdb {

class Utils : public EncdbBaseObj {
 public:
  static std::string byte2hexstring(const char *pSrc, size_t srcLen) {
    const char _hextable[] = "0123456789abcdef";
    std::string str = "";

    for (size_t i = 0; i < srcLen; ++i) {
      str += _hextable[(pSrc[i] >> 4) & 0xf];
      str += _hextable[pSrc[i] & 0xf];
    }

    return str;
  }

  static std::string byte2hexstring(const uint8_t *pSrc, size_t srcLen) {
    return byte2hexstring((const char *)pSrc, srcLen);
  }

  static std::string byte2hexstring(const std::string &bytes) {
    return byte2hexstring(bytes.data(), bytes.size());
  }

  static std::string byte2hexstring(const std::vector<uint8_t> &bytes) {
    return byte2hexstring((const char *)bytes.data(), bytes.size());
  }

  static std::string byte2hexstring(const encdb::bytes_t &bytes) {
    return byte2hexstring(reinterpret_cast<const char *>(bytes.data()),
                          bytes.size());
  }

  static uint32_t hexstring2byte(const std::string &hexStr, char *dest) {
    assert(0 == hexStr.length() % 2);
    for (size_t i = 0; i < hexStr.length(); i += 2) {
      if (!isxdigit(hexStr[i])) {
        throw InvalidTextRepresentationException("invalid hex digit");
      }

      uint8_t v;
      if (hexStr[i] > '9')
        v = tolower(hexStr[i]) - 'a' + 10;
      else
        v = hexStr[i] - '0';
      dest[i / 2] = v;
      if (hexStr[i + 1] > '9')
        v = tolower(hexStr[i + 1]) - 'a' + 10;
      else
        v = hexStr[i + 1] - '0';
      dest[i / 2] = (dest[i / 2] << 4) | v;
    }

    return hexStr.length() / 2;
  }

  static std::string hexstring2byte(const std::string &hexStr) {
    std::string dest(hexStr.length() / 2, 0x00);
    hexstring2byte(hexStr, const_cast<char *>(dest.data()));
    return dest;
  }

  static encdb::bytes_t hexstring2encdbbytes(const std::string &hexStr) {
    encdb::bytes_t dest(hexStr.length() / 2);
    hexstring2byte(hexStr, reinterpret_cast<char *>(dest.data()));
    return dest;
  }

  static std::string prepare_format_string(const char *fmt, ...)
      __attribute__((format(printf, 1, 2))) {
    va_list args;
    int needed;

    va_start(args, fmt);
    needed = vsnprintf(NULL, 0, fmt, args);
    va_end(args);

    std::string result(needed, '\0');
    va_start(args, fmt);
    needed = vsnprintf(&result[0], result.size() + 1, fmt, args);
    va_end(args);

    return result;
  }

  static std::string to_hex_string(uint64_t value) {
    return prepare_format_string("0x%016lx", value);
  }

  /*
   * base64Decode
   *     to decode the base64 string and put it to the result.
   * Parameters
   *     [IN]  aSrc: the source string coded in base64
   *           srcLen: length of the source string
   *     [OUT] result: point to the decoded string
   * Return Value
   *     int---The length of the decoded string
   */
  static int base64_decode(const unsigned char *aSrc, size_t srcLen,
                           unsigned char *result) {
    // if there nothing to decode, just return
    if (aSrc == nullptr || srcLen == 0) {
      return 0;
    }
    assert(aSrc != nullptr && result != nullptr);
    assert(srcLen % 4 == 0);
    // two reasons will cause the function return 0: 1- The input parameters are
    // wrong, 2- srcLen<4
    static const char index_64[256] = {
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 62, 64, 64, 64, 63, 52, 53, 54, 55, 56, 57,
        58, 59, 60, 61, 64, 64, 64, 64, 64, 64, 64, 0,  1,  2,  3,  4,  5,  6,
        7,  8,  9,  10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24,
        25, 64, 64, 64, 64, 64, 64, 26, 27, 28, 29, 30, 31, 32, 33, 34, 35, 36,
        37, 38, 39, 40, 41, 42, 43, 44, 45, 46, 47, 48, 49, 50, 51, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64, 64,
        64, 64, 64, 64};

    unsigned char ch1 = 0, ch2 = 0, ch3 = 0, ch4 = 0;
    unsigned char *ptr = result;

    for (unsigned int i = 0; i < srcLen; ++i) {
      ch1 = index_64[aSrc[i]];
      if (ch1 == 64) continue;
      ch2 = index_64[aSrc[++i]];
      if (ch2 == 64) continue;
      *(ptr++) = (unsigned char)(ch1 << 2 | ch2 >> 4);
      ch3 = index_64[aSrc[++i]];
      if (aSrc[i] == '=' || ch3 == 64) continue;
      *(ptr++) = (unsigned char)(ch2 << 4 | ch3 >> 2);
      ch4 = index_64[aSrc[++i]];
      if (aSrc[i] == '=' || ch4 == 64) continue;
      *(ptr++) = (unsigned char)(ch3 << 6 | ch4);
    }
    return static_cast<int>(ptr - result);
  }

  static std::vector<uint8_t> base64_decode(std::string base64str) {
    std::vector<uint8_t> result(base64str.size(), '\0');
    int len = base64_decode((uint8_t *)base64str.data(), base64str.size(),
                            result.data());
    result.resize(len);
    return result;
  }

  static encdb::bytes_t base64_decode_to_bytes(std::string base64str) {
    encdb::bytes_t result(base64str.size());
    int len = base64_decode((uint8_t *)base64str.data(), base64str.size(),
                            result.data());
    result.resize(len);
    return result;
  }

  static uint32_t Base64EncodedLen(uint32_t len) { return 4 * ((len + 2) / 3); }

  static void base64_inplace_encode(std::string &str) {
    static const char encoding_table[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
    static const int mod_table[] = {0, 2, 1};
    size_t ret_size = Base64EncodedLen(str.size());
    size_t orig_size = str.size();
    str.resize(ret_size);
    // from tail to head
    int i = orig_size - 1;
    int j = ret_size - 1;
    size_t last_n = (i + 1) % 3;
    uint32_t octet_a = 0;
    uint32_t octet_b = 0;
    uint32_t octet_c = 0;
    uint32_t triple = 0;
    if (last_n == 1) {
      octet_a = (unsigned char)str[i];
      i -= 1;
    } else if (last_n == 2) {
      octet_a = (unsigned char)str[i - 1];
      octet_b = (unsigned char)str[i];
      i -= 2;
    }
    if (last_n != 0) {
      triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;
      str[j - 3] = encoding_table[(triple >> 3 * 6) & 0x3F];
      str[j - 2] = encoding_table[(triple >> 2 * 6) & 0x3F];
      str[j - 1] = encoding_table[(triple >> 1 * 6) & 0x3F];
      str[j] = encoding_table[(triple >> 0 * 6) & 0x3F];
      j -= 4;
      for (int x = 0; x < mod_table[orig_size % 3]; x++) {
        str[ret_size - 1 - x] = '=';
      }
    }
    while (i > 0) {
      octet_a = (unsigned char)str[i - 2];
      octet_b = (unsigned char)str[i - 1];
      octet_c = (unsigned char)str[i];
      i -= 3;
      triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;
      str[j - 3] = encoding_table[(triple >> 3 * 6) & 0x3F];
      str[j - 2] = encoding_table[(triple >> 2 * 6) & 0x3F];
      str[j - 1] = encoding_table[(triple >> 1 * 6) & 0x3F];
      str[j] = encoding_table[(triple >> 0 * 6) & 0x3F];
      j -= 4;
    }
  }

  static std::string base64_encode(const uint8_t *data, size_t size) {
    static const char encoding_table[] = {
        'A', 'B', 'C', 'D', 'E', 'F', 'G', 'H', 'I', 'J', 'K', 'L', 'M',
        'N', 'O', 'P', 'Q', 'R', 'S', 'T', 'U', 'V', 'W', 'X', 'Y', 'Z',
        'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
        'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z',
        '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', '+', '/'};
    static const int mod_table[] = {0, 2, 1};

    size_t outlen = Base64EncodedLen(size);
    std::string encoded_data(outlen, '\0');

    for (size_t i = 0, j = 0; i < size;) {
      uint32_t octet_a = i < size ? (unsigned char)data[i++] : 0;
      uint32_t octet_b = i < size ? (unsigned char)data[i++] : 0;
      uint32_t octet_c = i < size ? (unsigned char)data[i++] : 0;

      uint32_t triple = (octet_a << 0x10) + (octet_b << 0x08) + octet_c;

      encoded_data[j++] = encoding_table[(triple >> 3 * 6) & 0x3F];
      encoded_data[j++] = encoding_table[(triple >> 2 * 6) & 0x3F];
      encoded_data[j++] = encoding_table[(triple >> 1 * 6) & 0x3F];
      encoded_data[j++] = encoding_table[(triple >> 0 * 6) & 0x3F];
    }

    for (int i = 0; i < mod_table[size % 3]; i++) {
      encoded_data[outlen - 1 - i] = '=';
    }

    return encoded_data;
  }

  static std::string base64_encode(const std::vector<uint8_t> &data) {
    return base64_encode(data.data(), data.size());
  }

  static std::string base64_encode(const encdb::bytes_t &data) {
    return base64_encode(data.data(), data.size());
  }

  static std::string toLower(const std::string &str) {
    std::string result(str);
    for (uint64_t i = 0; i < str.length(); i++) {
      result[i] = tolower(str[i]);
    }
    return result;
  }
};
}  // namespace encdb
