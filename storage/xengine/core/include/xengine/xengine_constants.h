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

#ifndef XENGINE_CONSTRANTS_H_
#define XENGINE_CONSTRANTS_H_

#include <stdint.h>
#include "port/likely.h"

namespace xengine {
namespace storage {

// the size of one extent
const int64_t MAX_EXTENT_SIZE = (2 * 1024 * 1024);
const int32_t DATA_BLOCK_SIZE = (16 * 1024);
const int64_t MAX_EXTENT_FILE_SIZE = (1024 * 1024 * 1024);
const int32_t MAX_EXTENT_NUM = (MAX_EXTENT_FILE_SIZE / MAX_EXTENT_SIZE);
const int32_t BITS_PER_BYTE = (8);  // 8 bits of a char
static const int32_t MAX_FILE_PATH_SIZE = 1024;
static const int32_t RESERVED_SIZE = 32; 
static const int64_t MAX_TIER_COUNT = 3;
static const int64_t MAX_LOB_KV_SIZE = 36 * 1024;
}  // storage
}  // xengine

#ifndef UNUSED
#define UNUSED(x) ((void)x)
#endif

#ifndef INT32_MAX
#define INT32_MAX 0x7FFFFFFF
#endif

#define IS_NULL(expr) (UNLIKELY(nullptr == (expr)))
#define ISNULL(expr) (UNLIKELY(nullptr == (expr)))
#define SUCCED(expr) (LIKELY(common::Status::kOk == (ret = (expr))))
#define SUCC(expr) (LIKELY(common::Status::kOk == (ret = (expr))))
#define FAILED(expr) (UNLIKELY(common::Status::kOk != (ret = expr)))
#define FAIL_RETURN(expr)                                                  \
  if (FAILED(expr)) {                                                      \
    fprintf(stderr, "failed on %s:%d, ret=%d\n", __FILE__, __LINE__, ret); \
    return ret;                                                            \
  }

#define FAIL_RETURN_MSG(expr, fmt, ...) \
  if (FAILED(expr)) {                           \
    __XENGINE_LOG(ERROR, fmt, __VA_ARGS__); \
    return ret;                                 \
  }

#define FAIL_RETURN_VOID_MSG(expr, fmt, ...) \
  if (FAILED(expr)) {                                \
    __XENGINE_LOG(ERROR, fmt, __VA_ARGS__);        \
    return;                                          \
  }

#define FAIL_RETURN_MSG_NEW(expr, fmt, ...) \
  if (FAILED(expr)) {                       \
    __XENGINE_LOG(ERROR, fmt, __VA_ARGS__); \
    return ret;                             \
  }
#endif
