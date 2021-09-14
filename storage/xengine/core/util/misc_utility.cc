/************************************************************************
 *
 * Copyright (c) 2020, Alibaba Group Holding Limited
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at

 *   http://www.apache.org/licenses/LICENSE-2.0

 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.

 * $Id:  misc_utility.cc,v 1.0 03/13/2018 09:06:01 PM
 *
 ************************************************************************/

/**
 * @file misc_utility.cc
 * @date 03/13/2018 09:06:02 PM
 * @version 1.0
 * @brief
 *
 **/

#include "misc_utility.h"

namespace xengine
{
namespace util
{
#define MAX_FUNCTION_ADDR_NUM 100
#define MAX_BT_BUF_LENGTH 1024

char *print_array(char *buf, int64_t buf_len, int64_t *array, int64_t array_size)
{
  if (nullptr != buf && buf_len > 0 && nullptr != array) {
    int64_t pos = 0;
    int64_t size = 0;
    for (int64_t i = 0; i < array_size; ++i) {
      if (0 == i) {
        size = snprintf(buf + pos, buf_len - pos, "0x%lx", array[i]);
      } else {
        size = snprintf(buf + pos, buf_len - pos, " 0x%lx", array[i]);
      }
      if (size >= 0 && pos + size < buf_len) {
        pos += size;
      } else {
        COMMON_LOG(WARN, "buf not enough", K(buf_len), K(size));
      }
    }
    buf[pos] = '\0';
  }
  return buf;
}

void bt()
{
/*
  void *addrs[MAX_FUNCTION_ADDR_NUM];
  char buf[MAX_BT_BUF_LENGTH];
  int32_t size = backtrace(addrs, MAX_FUNCTION_ADDR_NUM);

  print_array(buf, MAX_BT_BUF_LENGTH, reinterpret_cast<int64_t *>(addrs), size);
*/
}

} //namespace util
}
