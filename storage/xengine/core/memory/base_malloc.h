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
//#ifndef UTIL_BASE_MALLOC_H_
//#define UTIL_BASE_MALLOC_H_

#include "alloc_mgr.h"
#include <memory>
#include <cstddef>

namespace xengine {
namespace memory {

inline void update_mod_id(const void *ptr, const size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  alloc_mgr->update_mod_id(ptr, mod_id);
}

inline void update_hold_size(const int64_t size, const size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  alloc_mgr->update_hold_size(size, mod_id);
}

inline size_t base_malloc_usable_size(void *ptr) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->bmalloc_usable_size(ptr);
}

inline void *base_memalign(const size_t alignment, const int64_t size) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->memalign_alloc(alignment, size, ModId::kDefaultMod);
}

inline void *base_memalign(const size_t alignment, const int64_t size,
                           const size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->memalign_alloc(alignment, size, mod_id);
}

inline void base_memalign_free(void *ptr) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->memlign_free(ptr);
}

inline void *base_malloc(const int64_t size) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->balloc(size, ModId::kDefaultMod);
}

inline void *base_malloc(const int64_t size, const size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->balloc(size, mod_id);
}

inline void base_free(void *ptr, bool update = true) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  alloc_mgr->bfree(ptr, update);
}

inline void *base_realloc(void *ptr, const int64_t size) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->brealloc(ptr, size, ModId::kDefaultMod);
}

inline void *base_realloc(void *ptr, const int64_t size, size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->brealloc(ptr, size, mod_id);
}

inline void *base_alloc_chunk(const  int64_t size, const size_t mod_id) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->balloc_chunk(size, mod_id);
}

inline void base_free_chunk(void *ptr) {
  AllocMgr *alloc_mgr = AllocMgr::get_instance();
  return alloc_mgr->bfree_chunk(ptr);
}

template <typename Tp> struct ptr_delete {
  /// Default constructor
  constexpr ptr_delete() noexcept = default;
  void operator()(Tp *ptr) const {
    static_assert(sizeof(Tp) > 0, "can't delete pointer to incomplete type");

    base_free(ptr);
  }
};

template <typename Tp>
struct ptr_destruct {
  // Default constructor
  constexpr ptr_destruct() noexcept = default;
  void operator()(Tp *ptr) const
  {
    static_assert(sizeof(Tp) > 0, "can't delete pointer to incomplete type");

    ptr->~Tp();
  }
};

template <typename Tp>
struct ptr_destruct_delete {
  constexpr ptr_destruct_delete() noexcept = default;
  void operator()(Tp *ptr) const
  {
    static_assert(sizeof(Tp) > 0, "can't delete pointer to incomplete type");
    ptr->~Tp();
    base_memalign_free(ptr);
  }
};

} // namespace memory
} // namespace xengine

#define MOD_NEW_OBJECT(MOD, T, ...)                                            \
  ({                                                                           \
    T *__ret = nullptr;                                                        \
    void *__buf = xengine::memory::base_memalign(alignof(std::max_align_t), sizeof(T), MOD);\
    if (nullptr != __buf) {                                                    \
      __ret = new (__buf) T(__VA_ARGS__);                                      \
    }                                                                          \
    __ret;                                                                     \
  })

#define MOD_DELETE_OBJECT(T, ptr)                                              \
  do {                                                                         \
    if (nullptr != ptr) {                                                      \
      ptr->~T();                                                               \
      xengine::memory::base_memalign_free(ptr);                                \
      ptr = nullptr;                                                           \
    }                                                                          \
  } while (0)

#define MOD_DEFINE_UNIQUE_PTR(PTR, MOD, T, ...)                                \
  std::unique_ptr<T, ptr_delete<T>> PTR(                                   \
      new (xengine::memory::base_malloc(sizeof(T), MOD)) T(__VA_ARGS__));

#define PLACEMENT_NEW(T, ALLOC, ...)                  \
  ({                                                  \
    T* __ret = nullptr;                               \
    void* __buf = (ALLOC).AllocateAligned(sizeof(T)); \
    if (nullptr != __buf) {                           \
      __ret = new (__buf) T(__VA_ARGS__);             \
    }                                                 \
    __ret;                                            \
  })

#define PLACEMENT_DELETE(T, ALLOC, ptr) \
  do {                                  \
    if (nullptr != ptr) {               \
      ptr->~T();                        \
      ptr = nullptr;                    \
    }                                   \
  } while (0)

#define ALLOC_OBJECT(T, ALLOC, ...)            \
  ({                                        \
    T* __ret = nullptr;                     \
    void* __buf = (ALLOC).alloc(sizeof(T)); \
    if (nullptr != __buf) {                 \
      __ret = new (__buf) T(__VA_ARGS__);   \
    }                                       \
    __ret;                                  \
  })

#define FREE_OBJECT(T, ALLOC, ptr)                 \
  ({                                               \
    if (nullptr != (ptr)) {                        \
      ptr->~T();                                   \
      (ALLOC).free((ptr));                         \
      ptr = nullptr;                               \
    }                                              \
   })

#define COMMON_NEW(MOD, T, ALLOC, ...)      \
  ({                                        \
    T* __common_ret = nullptr;              \
    if (nullptr != ALLOC) {                 \
      __common_ret = ALLOC_OBJECT(T, *ALLOC, __VA_ARGS__); \
    } else {                                \
      __common_ret = MOD_NEW_OBJECT(MOD, T, __VA_ARGS__);  \
    }                                       \
    __common_ret;                           \
  })

#define COMMON_DELETE(MOD, T, ALLOC, ...)      \
  ({                                           \
    if (nullptr != ALLOC) {                    \
      FREE_OBJECT(T, *ALLOC, __VA_ARGS__);     \
    } else {                                   \
      MOD_DELETE_OBJECT(MOD, T, __VA_ARGS__);  \
    }                                          \
  })

#define ALLOC_ARRAY(ALLOC, type, size)             \
  ( (type*) (ALLOC).alloc(sizeof(type) * (size)))

#define FREE_ARRAY(ALLOC, ptr)                    \
  ({                                              \
    if (nullptr != (ptr)) {                       \
      (ALLOC).free((void*) (ptr))                 \
      (ptr) = nullptr;                            \
    }                                             \
   })
//#endif /* UTIL_BASE_MALLOC_H_ */
