/*****************************************************************************

Copyright (c) 2023, 2024, Alibaba and/or its affiliates. All Rights Reserved.

This program is free software; you can redistribute it and/or modify it under
the terms of the GNU General Public License, version 2.0, as published by the
Free Software Foundation.

This program is also distributed with certain software (including but not
limited to OpenSSL) that is licensed under separate terms, as designated in a
particular file or component or in included license documentation. The authors
of MySQL hereby grant you an additional permission to link the program and
your derivative works with the separately licensed software that they have
included with MySQL.

This program is distributed in the hope that it will be useful, but WITHOUT
ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS
FOR A PARTICULAR PURPOSE. See the GNU General Public License, version 2.0,
for more details.

You should have received a copy of the GNU General Public License along with
this program; if not, write to the Free Software Foundation, Inc.,
51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA

*****************************************************************************/


//
// Created by zzy on 2022/7/6.
//

#pragma once

//
// Disable/default class copy & move.
//

#include <cstddef>
#include <utility>

#define NO_CONSTRUCTOR(_class) \
 public:                       \
  _class() = delete;           \
                               \
 private:

#define NO_COPY_MOVE(_class)                         \
 public:                                             \
  _class(const _class &another) = delete;            \
  _class(_class &&another) = delete;                 \
  _class &operator=(const _class &another) = delete; \
  _class &operator=(_class &&another) = delete;      \
                                                     \
 private:

#define NO_COPY(_class)                              \
 public:                                             \
  _class(const _class &another) = delete;            \
  _class &operator=(const _class &another) = delete; \
                                                     \
 private:

#define NO_MOVE(_class)                         \
 public:                                        \
  _class(_class &&another) = delete;            \
  _class &operator=(_class &&another) = delete; \
                                                \
 private:

#define DEFAULT_COPY_MOVE(_class)                     \
 public:                                              \
  _class(const _class &another) = default;            \
  _class(_class &&another) = default;                 \
  _class &operator=(const _class &another) = default; \
  _class &operator=(_class &&another) = default;      \
                                                      \
 private:

#define DEFAULT_COPY(_class)                          \
 public:                                              \
  _class(const _class &another) = default;            \
  _class &operator=(const _class &another) = default; \
                                                      \
 private:

#define DEFAULT_MOVE(_class)                     \
 public:                                         \
  _class(_class &&another) = default;            \
  _class &operator=(_class &&another) = default; \
                                                 \
 private:

//
// Platform.
//

#if defined(__x86_64__) || defined(_M_X64)
#define IS_X64 1
#else
#define IS_X64 0
#endif

#if defined(__arm__)
#define IS_ARM 1
#else
#define IS_ARM 0
#endif

#if defined(__aarch64__)
#define IS_AARCH64 1
#else
#define IS_AARCH64 0
#endif

#if defined(__powerpc64__)
#define IS_PPC64 1
#else
#define IS_PPC64 0
#endif

#if defined(mips) || defined(__mips__) || defined(__mips)
#define IS_MIPS 1
#else
#define IS_MIPS 0
#endif

#if defined(__hppa__)
#define IS_RISCV 1
#else
#define IS_RISCV 0
#endif

#if defined(__s390x__)
#define IS_S390X 1
#else
#define IS_S390X 0
#endif

//
// Align.
//

/**
 * Notes:
 * Starting from Intel's Sandy Bridge, spatial prefetcher is now pulling pairs
 * of 64-byte cache lines at a time, so we have to align to 128 bytes rather
 * than 64. ARM's big.LITTLE architecture has asymmetric cores and "big" cores
 * have 128-byte cache line size. powerpc64 has 128-byte cache line size. arm,
 * mips, mips64, and riscv64 have 32-byte cache line size.
 */

#if IS_X64
static constexpr size_t CACHE_LINE_SIZE = 64;
static constexpr size_t CACHE_LINE_FETCH_ALIGN = 2 * CACHE_LINE_SIZE;
#elif IS_AARCH64 || IS_PPC64
static constexpr size_t CACHE_LINE_SIZE = 128;
static constexpr size_t CACHE_LINE_FETCH_ALIGN = CACHE_LINE_SIZE;
#elif IS_ARM || IS_MIPS || IS_RISCV
static constexpr size_t CACHE_LINE_SIZE = 32;
static constexpr size_t CACHE_LINE_FETCH_ALIGN = CACHE_LINE_SIZE;
#elif IS_S390X
static constexpr size_t CACHE_LINE_SIZE = 256;
static constexpr size_t CACHE_LINE_FETCH_ALIGN = CACHE_LINE_SIZE;
#else
static constexpr size_t CACHE_LINE_SIZE = 64;
static constexpr size_t CACHE_LINE_FETCH_ALIGN = CACHE_LINE_SIZE;
#endif

#if defined(_MSC_VER)

#define ALIGNED_(x) __declspec(align(x))
#define UNUSED

#elif defined(__GNUC__)

#define ALIGNED_(x) __attribute__((aligned(x)))
#define UNUSED_ __attribute__((unused))

#else
#error "Unknown compiler."
#endif

template <class T>
#if IS_X64
struct ALIGNED_(128) cache_padded_t final {  /// hard code here for GCC 4.x
#else
struct ALIGNED_(CACHE_LINE_FETCH_ALIGN) cache_padded_t final {
#endif
  T v;

  template <class... Args>
  explicit cache_padded_t(Args &&...args) : v(std::forward<Args>(args)...) {}

  inline const T &operator()() const { return v; }

  inline T &operator()() { return v; }
};

#define ALIGNED_TYPE(t, x) typedef t ALIGNED_(x)

/* Usage
 *
 * #pragma pack(push, 8)
 * ALIGNED_TYPE(struct, 16) tag_xxx {
 * } xxx_t;
 * #pragma pack(pop)
 *
 */

// test cache padded
static_assert(0 == sizeof(cache_padded_t<int>) % CACHE_LINE_FETCH_ALIGN,
              "Bad cache_padded_t");

//
// Likely.
//

#if defined(__GNUC__) && __GNUC__ >= 4
#define LIKELY(x) (__builtin_expect(!!(x), 1))
#define UNLIKELY(x) (__builtin_expect(!!(x), 0))
#else
#define LIKELY(x) (x)
#define UNLIKELY(x) (x)
#endif
