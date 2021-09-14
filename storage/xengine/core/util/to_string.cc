/*
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 * http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "to_string.h"
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <ctype.h>

namespace xengine {
namespace util {

int32_t hex_to_str(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size) {
  unsigned const char *p = nullptr;
  int32_t i = 0;
  if (in_data != nullptr && buff != nullptr && buff_size >= data_length * 2) {
    p = (unsigned const char *)in_data;
    for (; i < data_length; i++) {
      sprintf((char *)buff + i * 2, "%02X", *(p + i));
    }
  }
  return i;
}

int32_t str_to_hex(const void *in_data, const int32_t data_length, void *buff,
                   const int32_t buff_size) {
  unsigned const char *p = nullptr;
  unsigned char *o = nullptr;
  unsigned char c;
  int32_t i = 0;
  if (in_data != nullptr && buff != nullptr && buff_size >= data_length / 2) {
    p = (unsigned const char *)in_data;
    o = (unsigned char *)buff;
    c = 0;
    for (i = 0; i < data_length; i++) {
      c = static_cast<unsigned char>(c << 4);
      if (*(p + i) > 'F' || (*(p + i) < 'A' && *(p + i) > '9') ||
          *(p + i) < '0')
        break;
      if (*(p + i) >= 'A')
        c = static_cast<unsigned char>(c + (*(p + i) - 'A' + 10));
      else
        c = static_cast<unsigned char>(c + (*(p + i) - '0'));
      if (i % 2 == 1) {
        *(o + i / 2) = c;
        c = 0;
      }
    }
  }
  return i;
}

inline int char_to_hex(const char &input, char &output) {
  int ret = 0;
  if (isxdigit(input)) {
    if ('a' <= input && 'f' >= input) {
      output = (char)(input - 'a' + 10);
    } else if ('A' <= input && 'F' >= input) {
      output = (char)(input - 'A' + 10);
    } else {
      output = (char)(input - '0');
    }
  } else {
    ret = -1;
  }
  return ret;
}

int unhex(const char *input, int64_t input_len, char *output,
          int64_t output_len, int64_t &write_len) {
  int ret = 0;
  int i = 0;
  int j = 0;
  char value = '0';
  if (nullptr == input || nullptr == output || 0 >= input_len ||
      0 >= output_len) {
    ret = -1;
  } else if (output_len < (input_len / 2 + 1)) {
    ret = -2;
  } else {
    if (input_len % 2 == 1) {
      const char &begin = input[0];
      ret = char_to_hex(begin, value);
      if (0 == ret) {
        output[0] = value;
        i = 1;
        j = 1;
      }
    }
    if (0 == ret) {
      for (; i < input_len && 0 == ret; ++j, i += 2) {
        const char &c1 = input[i];
        const char &c2 = input[i + 1];
        char v1 = 0;
        char v2 = 0;
        ret = char_to_hex(c1, v1);
        if (0 == ret) {
          ret = char_to_hex(c2, v2);
        }
        if (0 == ret) {
          value = (char)(v1 * 16 + v2);
          output[j] = value;
        } else {
          break;
        }
      }  // end for
    }
  }
  if (0 == ret && j != -1) {
    write_len = j;
  } else {
    write_len = 0;
  }
  return ret;
}

////////////////////////////////////////////////////////////////
int databuff_printf(char *buf, const int64_t buf_len, int64_t &pos,
                    const char *fmt, ...) {
  int ret = 0;
  if (nullptr != buf && 0 <= pos && pos <= buf_len) {
    va_list args;
    va_start(args, fmt);
    int len = vsnprintf(buf + pos, buf_len - pos, fmt, args);
    va_end(args);
    if (len < buf_len - pos) {
      pos += len;
    } else {
      pos = buf_len - 1;  // skip '\0' written by vsnprintf
      ret = -2;
    }
  } else {
    ret = -2;
  }
  return ret;
}

}  // end namespace util
}  // end namespace xengine
