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

#pragma once
#include <stdint.h>
#include <cerrno>
#include <cstddef>

namespace xengine {

namespace memory {

class Logger;

class Allocator {
 public:
  virtual ~Allocator() {}

  virtual char* Allocate(size_t bytes) = 0;
  virtual char* AllocateAligned(size_t bytes, size_t huge_page_size = 0) = 0;

  virtual size_t BlockSize() const = 0;
};

class SimpleAllocator {
 public:
  virtual ~SimpleAllocator(){};
  virtual void* alloc(const int64_t sz) = 0;
  virtual void free(void* p) = 0;
  virtual int64_t size() const = 0;
  virtual void set_mod_id(const size_t mod_id) = 0;
};

}  // namespace util
}  // namespace xengine
