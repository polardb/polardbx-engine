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
#ifndef _IS_STL_ADAPT_ALLOCATOR_H_
#define _IS_STL_ADAPT_ALLOCATOR_H_ 1

#include <stdint.h>
#include <memory>
#include <bits/move.h>
#include "base_malloc.h"
#include "page_arena.h"

#define XDB_USE_NOEXCEPT 
#define XDB_NOEXCEPT

namespace xengine {
namespace memory {

/**
 *  @brief  An allocator that use for replace std::allocator
 *
 *  This is precisely the allocator defined in the C++ Standard.
 *     
 *  @tparam  Tp  Type of allocated object.
 */
template <typename Tp, int Mod = 0, typename AllocatorT = WrapAllocator> class stl_adapt_allocator {
public:
  typedef std::size_t size_type;
  typedef std::ptrdiff_t difference_type;
  typedef Tp *pointer;
  typedef const Tp *const_pointer;
  typedef Tp &reference;
  typedef const Tp &const_reference;
  typedef Tp value_type;
private:
  AllocatorT stl_alloc_;
public:
  template <typename Tp1> struct rebind { typedef stl_adapt_allocator<Tp1, Mod, AllocatorT> other; };

#if __cplusplus >= 201103L
  // _GLIBCXX_RESOLVE_LIB_DEFECTS
  // 2103. propagate_on_container_move_assignment
  typedef std::true_type propagate_on_container_move_assignment;
#endif

  stl_adapt_allocator()
      : stl_alloc_(Mod) XDB_USE_NOEXCEPT {}
  stl_adapt_allocator(AllocatorT alloc)
      :stl_alloc_(alloc) XDB_USE_NOEXCEPT {}

  stl_adapt_allocator(const stl_adapt_allocator &) XDB_USE_NOEXCEPT {}

  template <typename Tp1, int Mod1>
  stl_adapt_allocator(const stl_adapt_allocator<Tp1, Mod1, AllocatorT> &) XDB_USE_NOEXCEPT {}


  ~stl_adapt_allocator() XDB_USE_NOEXCEPT {}

  pointer address(reference x) const XDB_NOEXCEPT {
    return std::addressof(x);
  }

  const_pointer address(const_reference x) const XDB_NOEXCEPT {
    return std::addressof(x);
  }

  // NB: n is permitted to be 0.  The C++ standard says nothing
  // about what the return value is when n == 0.
  pointer allocate(size_type n, const void * = 0) {
    if (n > this->max_size())
      return nullptr;
    void *p = stl_alloc_.alloc(n * sizeof(Tp));
    return static_cast<Tp *>(p);
  }

  // p is not permitted to be a null pointer.
  void deallocate(pointer p, size_type) { 
    stl_alloc_.free(p);
  }

  size_type max_size() const XDB_USE_NOEXCEPT {
    return std::size_t(-1) / sizeof(Tp);
  }

#if __cplusplus >= 201103L
  template <typename Up, typename... Args>
  void construct(Up *p, Args &&... args) {
    ::new ((void *)p) Up(std::forward<Args>(args)...);
  }

  template <typename Up> void destroy(Up *p) { 
    p->~Up(); 
  }
#else
  // _GLIBCXX_RESOLVE_LIB_DEFECTS
  // 402. wrong new expression in [some_] allocator::construct
  void construct(pointer p, const Tp &val) {
    ::new ((void *)p) Tp(val);
  }

  void destroy(pointer p) { p->~Tp(); }
#endif
};

template <typename Tp, int Mod = 0>
inline bool operator==(const stl_adapt_allocator<Tp> &, const stl_adapt_allocator<Tp, Mod> &) {
  return true;
}

// todo add compare allocator
template <typename Tp, int Mod = 0>
inline bool operator==(const stl_adapt_allocator<Tp> &, const stl_adapt_allocator<Tp> &) {
  return true;
}

template <typename Tp1, typename Tp2, int Mod = 0>
inline bool operator==(const stl_adapt_allocator<Tp1, Mod> &, const stl_adapt_allocator<Tp2, Mod> &) {
  return true;
}

template <typename Tp, int Mod = 0>
inline bool operator!=(const stl_adapt_allocator<Tp> &, const stl_adapt_allocator<Tp, Mod> &) {
  return false;
}

template <typename Tp1, typename Tp2, int Mod = 0>
inline bool operator!=(const stl_adapt_allocator<Tp1, Mod> &, const stl_adapt_allocator<Tp2, Mod> &) {
  return true;
}

typedef std::basic_string<char, std::char_traits<char>,
    const stl_adapt_allocator<char>> xstring;

} // namespace memory
}  // namespace xengine

#endif
