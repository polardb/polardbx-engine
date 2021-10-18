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

 * $Id:  misc_utility.h,v 1.0 03/13/2018 09:06:01 PM
 *
 ************************************************************************/

/**
 * @file misc_utility.h
 * @date 03/13/2018 09:06:02 PM
 * @version 1.0
 * @brief
 *
 **/
#ifndef IS_MISC_UTILIRY_H_
#define IS_MISC_UTILIRY_H_

#include <execinfo.h>
#include "logger/logger.h"

#define MAX_FUNCTION_ADDR_NUM 100
#define MAX_BT_BUF_LENGTH 1024

#define BACKTRACE(level, fmt, ...) {\
  void *bt_addrs[MAX_FUNCTION_ADDR_NUM]; \
  char bt_buf[MAX_BT_BUF_LENGTH]; \
  int32_t bt_size = backtrace(bt_addrs, MAX_FUNCTION_ADDR_NUM); \
  xengine::util::print_array(bt_buf, MAX_BT_BUF_LENGTH, reinterpret_cast<int64_t *>(bt_addrs), bt_size); \
  __COMMON_LOG(level, fmt "BackTrace : %s", ##__VA_ARGS__, bt_buf); \
}\

namespace xengine
{
namespace util
{
//print the element in array to buf
//the memory of buf should been allocate by caller
char *print_array(char *buf, int64_t buf_len, int64_t *array, int64_t array_size);

//print the backtrace to return value
void bt();

}
}
#endif
