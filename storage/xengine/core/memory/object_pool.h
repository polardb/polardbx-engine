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
#ifndef IS_UTIL_OBJECT_POOL
#define IS_UTIL_OBJECT_POOL

#include <list>
#include <mutex>
#include "page_arena.h"
#include "thread_local_store.h"

namespace xengine {
namespace memory {

template <typename T, typename Initializer = DefaultInitializer<T>,
          typename Allocator = ArenaAllocator>
class ObjectPool {
 public:
  ObjectPool();
  explicit ObjectPool(Initializer &initializer);
  explicit ObjectPool(Allocator &allocator);
  ObjectPool(Initializer &initializer, Allocator &allocator);
  ~ObjectPool();
  int init();
  T *alloc();
  void free(T *obj);
  int64_t cached_size() const;
  void set_mod_id(const size_t mod_id) {
    allocator_.set_mod_id(mod_id);
  }
 private:
  typedef std::list<T *> ContainerType;
  std::mutex mutex_;
  DefaultInitializer<T> default_initializer_;
  ArenaAllocator default_allocator_;
  Initializer &initializer_;
  Allocator &allocator_;
  mutable ThreadLocalStore<ContainerType, DefaultInitializer<ContainerType>,
                           Allocator>
      list_;
};

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator>::ObjectPool()
    : default_initializer_(),
      default_allocator_(PageArena<>::DEFAULT_PAGE_SIZE, ModId::kObjectPool),
      initializer_(default_initializer_),
      allocator_(default_allocator_),
      list_(default_allocator_) {
  list_.init();
}

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator>::ObjectPool(Initializer &initializer)
    : default_initializer_(),
      default_allocator_(PageArena<>::DEFAULT_PAGE_SIZE, ModId::kObjectPool),
      initializer_(initializer),
      allocator_(default_allocator_),
      list_(default_allocator_) {
  list_.init();
}

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator>::ObjectPool(Allocator &allocator)
    : default_initializer_(),
      default_allocator_(PageArena<>::DEFAULT_PAGE_SIZE, ModId::kObjectPool),
      initializer_(default_initializer_),
      allocator_(allocator),
      list_(allocator) {
  list_.init();
}

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator>::ObjectPool(Initializer &initializer,
                                                  Allocator &allocator)
    : default_initializer_(),
      default_allocator_(PageArena<>::DEFAULT_PAGE_SIZE, ModId::kObjectPool),
      initializer_(initializer),
      allocator_(allocator),
      list_(allocator) {
  list_.init();
}

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator>::~ObjectPool() {
  if (list_.get() != nullptr && list_.get()->size() > 0) {
    for (T *object : *list_.get()) {
      if (nullptr != object) {
        object->~T();
        allocator_.free(object);
      }
    }
  }
}

template <typename T, typename Initializer, typename Allocator>
int64_t ObjectPool<T, Initializer, Allocator>::cached_size() const {
  int64_t size = 0;
  if (nullptr != list_.get()) {
    size = list_.get()->size();
  }
  return size;
}

template <typename T, typename Initializer, typename Allocator>
T *ObjectPool<T, Initializer, Allocator>::alloc() {
  T *ret = nullptr;
  if (list_.get() != nullptr && list_.get()->size() > 0) {
    ret = list_.get()->front();
    list_.get()->pop_front();
  }
  if (nullptr == ret) {
    std::lock_guard<std::mutex> guard(mutex_);
    void *buffer = allocator_.alloc(sizeof(T));
    if (nullptr != buffer) {
      initializer_(buffer);
      ret = reinterpret_cast<T *>(buffer);
    }
  }
  return ret;
}

template <typename T, typename Initializer, typename Allocator>
void ObjectPool<T, Initializer, Allocator>::free(T *obj) {
  if (list_.get() != nullptr) {
    list_.get()->push_back(obj);
  } else {
    obj->~T();
    allocator_.free(obj);
  }
}

template <typename T, typename Initializer, typename Allocator>
ObjectPool<T, Initializer, Allocator> &get_object_pool() {
  static ObjectPool<T, Initializer, Allocator> resource_pool;
  return resource_pool;
}

#define TS_OBJECT_POOL(type)                                              \
  get_object_pool<type,                                    \
                  DefaultInitializer<type>, \
                  ArenaAllocator>()
#define TS_OP_ALLOC(type)                                                 \
  get_object_pool<type,                                    \
                  DefaultInitializer<type>, \
                  ArenaAllocator>()         \
      .alloc()
#define TS_OP_FREE(type, ptr)                                             \
  get_object_pool<type,                                    \
                  DefaultInitializer<type>, \
                  ArenaAllocator>()         \
      .free(ptr)

#endif
}  // namespace memory
}  // namespace xengine


