/* Copyright (c) 2018, 2021, Alibaba and/or its affiliates. All rights reserved.
   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.
   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL/PolarDB-X Engine hereby grant you an
   additional permission to link the program and your derivative works with the
   separately licensed software that they have included with
   MySQL/PolarDB-X Engine.
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.
   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include "../service/udf.h"

#include <assert.h>
#include <string.h>
#include <string>

#if defined(_WIN32) || defined(_WIN64) || defined(__WIN32__) || defined(WIN32)
#define DLLEXP __declspec(dllexport)
#else
#define DLLEXP
#endif

//-----------------------------------------------------------------------------
// Platform-specific functions and macros

// Microsoft Visual Studio

#if defined(_MSC_VER) && (_MSC_VER < 1600)

typedef unsigned char uint8_t;
typedef unsigned int uint32_t;
typedef unsigned __int64 uint64_t;

// Other compilers

#else  // defined(_MSC_VER)

#include <stdint.h>

#endif  // !defined(_MSC_VER)

//-----------------------------------------------------------------------------

#if defined(_MSC_VER)

#define FORCE_INLINE __forceinline

#include <stdlib.h>

#define ROTL32(x, y) _rotl(x, y)
#define ROTL64(x, y) _rotl64(x, y)

#define BIG_CONSTANT(x) (x)

// Other compilers

#else  // defined(_MSC_VER)

#define FORCE_INLINE inline __attribute__((always_inline))
typedef void (*HASH_FUNC)(const void *, int, uint32_t, void *);

namespace gx {

static inline uint32_t rotl32(uint32_t x, int8_t r) {
  return (x << r) | (x >> (32 - r));
}

static inline uint64_t rotl64(uint64_t x, int8_t r) {
  return (x << r) | (x >> (64 - r));
}

#define ROTL32(x, y) rotl32(x, y)
#define ROTL64(x, y) rotl64(x, y)

#define BIG_CONSTANT(x) (x##LLU)

#endif  // !defined(_MSC_VER)

enum enum_hash_method { MURMUR3 = 0, XXHASH = 1 };

static const bool is_hash_result_64[] = {false, true};

static const char NULL_VALUE[] = {0, 0, 0, 0, 0, 0, 0, 0,
                                  0, 0, 0, 0, 0, 0, 0, 0};

static const uint32_t DEFAULT_SEED = 0;

static const uint64_t PRIME64_1 = -7046029288634856825L;
static const uint64_t PRIME64_2 = -4417276706812531889L;
static const uint64_t PRIME64_3 = 1609587929392839161L;
static const uint64_t PRIME64_4 = -8796714831421723037L;
static const uint64_t PRIME64_5 = 2870177450012600261L;

static const uint64_t V1 = DEFAULT_SEED + PRIME64_1 + PRIME64_2;
static const uint64_t V2 = DEFAULT_SEED + PRIME64_2;
static const uint64_t V3 = DEFAULT_SEED;
static const uint64_t V4 = DEFAULT_SEED - PRIME64_1;

#ifdef WORDS_BIGENDIAN
static const bool Is_big_endian = true;
#else
static const bool Is_big_endian = false;
#endif

//-----------------------------------------------------------------------------
// Block read - if your platform needs to do endian-swapping or can only
// handle aligned reads, do the conversion here

static FORCE_INLINE uint32_t getblock32(const uint32_t *p, int i) {
  return p[i];
}

static FORCE_INLINE uint64_t getblock64(const uint64_t *p, int i) {
  return p[i];
}

//-----------------------------------------------------------------------------
// Finalization mix - force all bits of a hash block to avalanche

static FORCE_INLINE uint32_t fmix32(uint32_t h) {
  h ^= h >> 16;
  h *= 0x85ebca6b;
  h ^= h >> 13;
  h *= 0xc2b2ae35;
  h ^= h >> 16;

  return h;
}

//----------

static FORCE_INLINE uint64_t fmix64(uint64_t k) {
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xff51afd7ed558ccd);
  k ^= k >> 33;
  k *= BIG_CONSTANT(0xc4ceb9fe1a85ec53);
  k ^= k >> 33;

  return k;
}

static FORCE_INLINE uint64_t mix(uint64_t current, uint64_t value) {
  return ROTL64(current + value * PRIME64_2, 31) * PRIME64_1;
}

static FORCE_INLINE uint64_t hmix(uint64_t hash, uint64_t value) {
  uint64_t temp = hash ^ mix(0, value);
  return temp * PRIME64_1 + PRIME64_4;
}

static FORCE_INLINE uint64_t get_mixed_hash(uint64_t v1, uint64_t v2,
                                            uint64_t v3, uint64_t v4) {
  uint64_t hash =
      ROTL64(v1, 1) + ROTL64(v2, 7) + ROTL64(v3, 12) + ROTL64(v4, 18);

  hash = hmix(hash, v1);
  hash = hmix(hash, v2);
  hash = hmix(hash, v3);
  hash = hmix(hash, v4);

  return hash;
}

static FORCE_INLINE uint64_t mixForTail(uint64_t hash, uint64_t value) {
  uint64_t temp = hash ^ mix(0, value);
  return ROTL64(temp, 27) * PRIME64_1 + PRIME64_4;
}

static FORCE_INLINE uint64_t processTail(uint64_t hash, uint32_t value) {
  uint64_t temp = hash ^ (value * PRIME64_1);
  return ROTL64(temp, 23) * PRIME64_2 + PRIME64_3;
}

static FORCE_INLINE uint64_t processTail(uint64_t hash, uint8_t value) {
  uint64_t temp = hash ^ (value * PRIME64_5);
  return ROTL64(temp, 11) * PRIME64_1;
}

//-----------------------------------------------------------------------------

void MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out);
void MurmurHash3_x86_128(const void *key, int len, uint32_t seed, void *out);
void MurmurHash3_x64_128(const void *key, int len, uint32_t seed, void *out);
void XxHash_x64_64(const void *key, int len, uint32_t seed, void *out);
longlong bloomfilter(UDF_INIT *initid, UDF_ARGS *args, char *is_null,
                     char *error);

bool bloomfilter_init(UDF_INIT *initid, UDF_ARGS *args, char *message);
void bloomfilter_deinit(UDF_INIT *initid);
void bloomfilter_udf(gs::udf::Udf_definition *def);

static const char *null_safe_value_ptr(char *origin_value_ptr,
                                       char *value_buffer, int length,
                                       bool reverse) {
  if (origin_value_ptr == nullptr) {
    return NULL_VALUE;
  } else if (reverse) {
    for (int i = 0; i < length; i++) {
      value_buffer[length - 1 - i] = origin_value_ptr[i];
    }
    return value_buffer;
  } else {
    return origin_value_ptr;
  }
}

void MurmurHash3_x86_32(const void *key, int len, uint32_t seed, void *out) {
  const uint8_t *data = (const uint8_t *)key;
  const int nblocks = len / 4;

  uint32_t h1 = seed;

  const uint32_t c1 = 0xcc9e2d51;
  const uint32_t c2 = 0x1b873593;

  //----------
  // body

  const uint32_t *blocks = (const uint32_t *)(data + nblocks * 4);

  for (int i = -nblocks; i; i++) {
    uint32_t k1 = getblock32(blocks, i);

    k1 *= c1;
    k1 = ROTL32(k1, 15);
    k1 *= c2;

    h1 ^= k1;
    h1 = ROTL32(h1, 13);
    h1 = h1 * 5 + 0xe6546b64;
  }

  //----------
  // tail

  const uint8_t *tail = (const uint8_t *)(data + nblocks * 4);

  uint32_t k1 = 0;

  switch (len & 3) {
    case 3:
      k1 ^= tail[2] << 16;
      __attribute__((fallthrough));
    case 2:
      k1 ^= tail[1] << 8;
      __attribute__((fallthrough));
    case 1:
      k1 ^= tail[0];
      k1 *= c1;
      k1 = ROTL32(k1, 15);
      k1 *= c2;
      h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len;

  h1 = fmix32(h1);

  *(uint32_t *)out = h1;
}

//-----------------------------------------------------------------------------

void MurmurHash3_x86_128(const void *key, const int len, uint32_t seed,
                         void *out) {
  const uint8_t *data = (const uint8_t *)key;
  const int nblocks = len / 16;

  uint32_t h1 = seed;
  uint32_t h2 = seed;
  uint32_t h3 = seed;
  uint32_t h4 = seed;

  const uint32_t c1 = 0x239b961b;
  const uint32_t c2 = 0xab0e9789;
  const uint32_t c3 = 0x38b34ae5;
  const uint32_t c4 = 0xa1e38b93;

  //----------
  // body

  const uint32_t *blocks = (const uint32_t *)(data + nblocks * 16);

  for (int i = -nblocks; i; i++) {
    uint32_t k1 = getblock32(blocks, i * 4 + 0);
    uint32_t k2 = getblock32(blocks, i * 4 + 1);
    uint32_t k3 = getblock32(blocks, i * 4 + 2);
    uint32_t k4 = getblock32(blocks, i * 4 + 3);

    k1 *= c1;
    k1 = ROTL32(k1, 15);
    k1 *= c2;
    h1 ^= k1;

    h1 = ROTL32(h1, 19);
    h1 += h2;
    h1 = h1 * 5 + 0x561ccd1b;

    k2 *= c2;
    k2 = ROTL32(k2, 16);
    k2 *= c3;
    h2 ^= k2;

    h2 = ROTL32(h2, 17);
    h2 += h3;
    h2 = h2 * 5 + 0x0bcaa747;

    k3 *= c3;
    k3 = ROTL32(k3, 17);
    k3 *= c4;
    h3 ^= k3;

    h3 = ROTL32(h3, 15);
    h3 += h4;
    h3 = h3 * 5 + 0x96cd1c35;

    k4 *= c4;
    k4 = ROTL32(k4, 18);
    k4 *= c1;
    h4 ^= k4;

    h4 = ROTL32(h4, 13);
    h4 += h1;
    h4 = h4 * 5 + 0x32ac3b17;
  }

  //----------
  // tail

  const uint8_t *tail = (const uint8_t *)(data + nblocks * 16);

  uint32_t k1 = 0;
  uint32_t k2 = 0;
  uint32_t k3 = 0;
  uint32_t k4 = 0;

  switch (len & 15) {
    case 15:
      k4 ^= tail[14] << 16;
      __attribute__((fallthrough));
    case 14:
      k4 ^= tail[13] << 8;
      __attribute__((fallthrough));
    case 13:
      k4 ^= tail[12] << 0;
      k4 *= c4;
      k4 = ROTL32(k4, 18);
      k4 *= c1;
      h4 ^= k4;
      __attribute__((fallthrough));

    case 12:
      k3 ^= tail[11] << 24;
      __attribute__((fallthrough));
    case 11:
      k3 ^= tail[10] << 16;
      __attribute__((fallthrough));
    case 10:
      k3 ^= tail[9] << 8;
      __attribute__((fallthrough));
    case 9:
      k3 ^= tail[8] << 0;
      k3 *= c3;
      k3 = ROTL32(k3, 17);
      k3 *= c4;
      h3 ^= k3;
      __attribute__((fallthrough));

    case 8:
      k2 ^= tail[7] << 24;
      __attribute__((fallthrough));
    case 7:
      k2 ^= tail[6] << 16;
      __attribute__((fallthrough));
    case 6:
      k2 ^= tail[5] << 8;
      __attribute__((fallthrough));
    case 5:
      k2 ^= tail[4] << 0;
      k2 *= c2;
      k2 = ROTL32(k2, 16);
      k2 *= c3;
      h2 ^= k2;
      __attribute__((fallthrough));

    case 4:
      k1 ^= tail[3] << 24;
      __attribute__((fallthrough));
    case 3:
      k1 ^= tail[2] << 16;
      __attribute__((fallthrough));
    case 2:
      k1 ^= tail[1] << 8;
      __attribute__((fallthrough));
    case 1:
      k1 ^= tail[0] << 0;
      k1 *= c1;
      k1 = ROTL32(k1, 15);
      k1 *= c2;
      h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len;
  h2 ^= len;
  h3 ^= len;
  h4 ^= len;

  h1 += h2;
  h1 += h3;
  h1 += h4;
  h2 += h1;
  h3 += h1;
  h4 += h1;

  h1 = fmix32(h1);
  h2 = fmix32(h2);
  h3 = fmix32(h3);
  h4 = fmix32(h4);

  h1 += h2;
  h1 += h3;
  h1 += h4;
  h2 += h1;
  h3 += h1;
  h4 += h1;

  ((uint32_t *)out)[0] = h1;
  ((uint32_t *)out)[1] = h2;
  ((uint32_t *)out)[2] = h3;
  ((uint32_t *)out)[3] = h4;
}

//-----------------------------------------------------------------------------

void MurmurHash3_x64_128(const void *key, const int len, const uint32_t seed,
                         void *out) {
  const uint8_t *data = (const uint8_t *)key;
  const int nblocks = len / 16;

  uint64_t h1 = seed;
  uint64_t h2 = seed;

  const uint64_t c1 = BIG_CONSTANT(0x87c37b91114253d5);
  const uint64_t c2 = BIG_CONSTANT(0x4cf5ad432745937f);

  //----------
  // body

  const uint64_t *blocks = (const uint64_t *)(data);

  for (int i = 0; i < nblocks; i++) {
    uint64_t k1 = getblock64(blocks, i * 2 + 0);
    uint64_t k2 = getblock64(blocks, i * 2 + 1);

    k1 *= c1;
    k1 = ROTL64(k1, 31);
    k1 *= c2;
    h1 ^= k1;

    h1 = ROTL64(h1, 27);
    h1 += h2;
    h1 = h1 * 5 + 0x52dce729;

    k2 *= c2;
    k2 = ROTL64(k2, 33);
    k2 *= c1;
    h2 ^= k2;

    h2 = ROTL64(h2, 31);
    h2 += h1;
    h2 = h2 * 5 + 0x38495ab5;
  }

  //----------
  // tail

  const uint8_t *tail = (const uint8_t *)(data + nblocks * 16);

  uint64_t k1 = 0;
  uint64_t k2 = 0;

  switch (len & 15) {
    case 15:
      k2 ^= ((uint64_t)tail[14]) << 48;
      __attribute__((fallthrough));
    case 14:
      k2 ^= ((uint64_t)tail[13]) << 40;
      __attribute__((fallthrough));
    case 13:
      k2 ^= ((uint64_t)tail[12]) << 32;
      __attribute__((fallthrough));
    case 12:
      k2 ^= ((uint64_t)tail[11]) << 24;
      __attribute__((fallthrough));
    case 11:
      k2 ^= ((uint64_t)tail[10]) << 16;
      __attribute__((fallthrough));
    case 10:
      k2 ^= ((uint64_t)tail[9]) << 8;
      __attribute__((fallthrough));
    case 9:
      k2 ^= ((uint64_t)tail[8]) << 0;
      k2 *= c2;
      k2 = ROTL64(k2, 33);
      k2 *= c1;
      h2 ^= k2;
      __attribute__((fallthrough));

    case 8:
      k1 ^= ((uint64_t)tail[7]) << 56;
      __attribute__((fallthrough));
    case 7:
      k1 ^= ((uint64_t)tail[6]) << 48;
      __attribute__((fallthrough));
    case 6:
      k1 ^= ((uint64_t)tail[5]) << 40;
      __attribute__((fallthrough));
    case 5:
      k1 ^= ((uint64_t)tail[4]) << 32;
      __attribute__((fallthrough));
    case 4:
      k1 ^= ((uint64_t)tail[3]) << 24;
      __attribute__((fallthrough));
    case 3:
      k1 ^= ((uint64_t)tail[2]) << 16;
      __attribute__((fallthrough));
    case 2:
      k1 ^= ((uint64_t)tail[1]) << 8;
      __attribute__((fallthrough));
    case 1:
      k1 ^= ((uint64_t)tail[0]) << 0;
      k1 *= c1;
      k1 = ROTL64(k1, 31);
      k1 *= c2;
      h1 ^= k1;
  };

  //----------
  // finalization

  h1 ^= len;
  h2 ^= len;

  h1 += h2;
  h2 += h1;

  h1 = fmix64(h1);
  h2 = fmix64(h2);

  h1 += h2;
  h2 += h1;

  ((uint64_t *)out)[0] = h1;
  ((uint64_t *)out)[1] = h2;
}

void XxHash_x64_64(const void *key, const int len, uint32_t seed, void *out) {
  const int BLOCK_SIZE = 32;
  uint64_t hash = len;
  const uint64_t *blocks = (const uint64_t *)(key);
  uint64_t *tail_start = reinterpret_cast<uint64_t *>(const_cast<void *>(key));

  //----------
  // body
  if (len > BLOCK_SIZE) {
    const int nblocks = len / BLOCK_SIZE;
    tail_start += nblocks;

    uint64_t k1 = V1, k2 = V2, k3 = V3, k4 = V4;
    for (int i = 0; i < nblocks; i++) {
      k1 = mix(k1, getblock64(blocks, i * 4));
      k2 = mix(k2, getblock64(blocks, i * 4 + 1));
      k3 = mix(k3, getblock64(blocks, i * 4 + 2));
      k4 = mix(k4, getblock64(blocks, i * 4 + 3));
    }
    hash += get_mixed_hash(k1, k2, k3, k4);
  } else {
    hash += (seed + PRIME64_5);
  }

  //----------
  // tail

  int bytes_left = len % BLOCK_SIZE;
  uint8 *tail_by_int8 = (uint8 *)(tail_start);
  // process by 8-byte blocks
  while (bytes_left >= 8) {
    hash = mixForTail(hash, getblock64((uint64_t *)tail_by_int8, 0));
    tail_by_int8 += 8;
    bytes_left -= 8;
  }
  // process by 4-byte block
  if (bytes_left >= 4) {
    hash = processTail(hash, getblock32((uint32_t *)tail_by_int8, 0));
    tail_by_int8 += 4;
    bytes_left -= 4;
  }
  // process by 1 byte
  for (int i = 0; i < bytes_left; ++i) {
    hash = processTail(hash, tail_by_int8[i]);
  }

  // final hash
  hash ^= hash >> 33;
  hash *= PRIME64_2;
  hash ^= hash >> 29;
  hash *= PRIME64_3;
  hash ^= hash >> 32;

  ((uint64_t *)out)[0] = hash;
}

HASH_FUNC get_hash_func(int hash_enum) {
  switch (hash_enum) {
    case (int)MURMUR3:
      return MurmurHash3_x86_128;
    case (int)XXHASH:
      return XxHash_x64_64;
    default:
      return nullptr;
  }
}

}  // namespace gx

/**
 * BloomFilter only supports one field now
 */
longlong bloomfilter(UDF_INIT *, UDF_ARGS *args, char *, char *) {
  char *bitmap_ = args->args[0];
  long long total_bits_ = *((long long *)args->args[1]);
  long long numFunc = *((long long *)args->args[2]);
  unsigned char hash_res[16];

  char tmp_value_buffer[sizeof(double)];

  gs::udf::udf_counter.bloomfilter_counter++;

  HASH_FUNC hash_func = gx::MurmurHash3_x64_128;
  bool is_result_64 = false;
  if (args->arg_count == 5) {
    int hash_func_enum = (*((int *)args->args[4]));
    hash_func = gx::get_hash_func(hash_func_enum);
    is_result_64 = gx::is_hash_result_64[hash_func_enum];
  }

  // The possible type values are STRING_RESULT, INT_RESULT, and REAL_RESULT.
  switch (args->arg_type[3]) {
    case INT_RESULT:
      // For arguments of types INT_RESULT or REAL_RESULT, lengths still
      // contains the maximum length of the argument (as for the initialisation
      // function). For an argument of type INT_RESULT, you must cast
      // args->args[i] to a long long value :
      hash_func(gx::null_safe_value_ptr(args->args[3], tmp_value_buffer,
                                        sizeof(long long), gx::Is_big_endian),
                sizeof(long long), gx::DEFAULT_SEED, &hash_res);
      break;
    case DECIMAL_RESULT:
    case STRING_RESULT:
      // For each invocation of the main function, lengths contains the actual
      // lengths of any string arguments that are passed for the row currently
      // being processed An argument of type STRING_RESULT is given as a string
      // pointer plus a length, to allow handling of binary data or data of
      // arbitrary length. The string contents are available as args->args[i]
      // and the string length is args->lengths[i]. You should not assume that
      // strings are null-terminated.

      hash_func(gx::null_safe_value_ptr(args->args[3], tmp_value_buffer,
                                        args->lengths[3], false),
                args->lengths[3], gx::DEFAULT_SEED, &hash_res);
      break;
    case REAL_RESULT:
      // For an argument of type REAL_RESULT, you must cast args->args[i] to a
      // double value:
      hash_func(gx::null_safe_value_ptr(args->args[3], tmp_value_buffer,
                                        sizeof(double), gx::Is_big_endian),
                sizeof(double), gx::DEFAULT_SEED, &hash_res);
      break;
    default:
      return 0;
  }

  if (!is_result_64) {
    // for 128 bit
    int64_t hash1 = *(reinterpret_cast<int64_t *>(hash_res));
    int64_t hash2 = *(reinterpret_cast<int64_t *>(hash_res + 8));

    int64_t combined_hash = hash1;

    for (int64_t i = 0; i < numFunc; ++i) {
      int64_t bit_index = (combined_hash & INT64_MAX) % total_bits_;
      int64_t byte_index = bit_index / 8;
      char byte_mask = 1 << (bit_index % 8);
      if (!(bitmap_[byte_index] & byte_mask)) {
        return 0;
      }
      combined_hash += hash2;
    }
    return 1;
  } else {
    // for 64 bit
    int64_t hash = *(reinterpret_cast<int64_t *>(hash_res));

    int32_t hash1 = (int32_t)hash;
    int32_t hash2 = (int32_t)(hash >> 32);
    int32_t combined_hash = hash1 + hash2;

    for (int64_t i = 0; i < numFunc; ++i) {
      if (combined_hash < 0) {
        combined_hash &= INT32_MAX;
      }
      int32_t bit_index = combined_hash % total_bits_;
      int32_t byte_index = bit_index / 8;
      char byte_mask = 1 << (bit_index % 8);
      if (!(bitmap_[byte_index] & byte_mask)) {
        // 只要有一次不匹配 就不存在
        return 0;
      }
      combined_hash += hash2;
    }
    return 1;
  }
}

bool bloomfilter_init(UDF_INIT *initid MY_ATTRIBUTE((unused)), UDF_ARGS *args,
                      char *message) {
  if (args->arg_count != 4 && args->arg_count != 5) {
    strcpy(message, "bloomfilter requires 4 arguments (5 args for xxhash)");
    return 1;
  }

  if (args->arg_type[0] != STRING_RESULT) {
    strcpy(message, "bloomfilter requires 1 blob/string argument");
    return 1;
  }

  if (args->arg_type[3] != REAL_RESULT && args->arg_type[3] != INT_RESULT &&
      args->arg_type[3] != DECIMAL_RESULT &&
      args->arg_type[3] != STRING_RESULT) {
    strcpy(message, "illegal datatype");
    return 1;
  }

  // Ask mysql to coerce decimal value to double
  if (args->arg_type[3] == DECIMAL_RESULT) {
    args->arg_type[3] = REAL_RESULT;
  }

  if (args->arg_count == 5) {
    if (args->arg_type[4] != INT_RESULT) {
      strcpy(message, "illegal hash method version");
      return 1;
    }
    if (gx::get_hash_func(*((int *)args->args[4])) == nullptr) {
      strcpy(message, "not supported hash method");
      return 1;
    }
  }

  strcpy(message, "bloomfilter inited!");
  return 0;
}

void bloomfilter_deinit(UDF_INIT *initid MY_ATTRIBUTE((unused))) {}

void bloomfilter_udf(gs::udf::Udf_definition *def) {
  def->m_name = const_cast<char *>("bloomfilter");
  def->m_result = INT_RESULT;
  def->m_type = UDFTYPE_FUNCTION;
  def->m_func_init = (Udf_func_init)bloomfilter_init;
  def->m_func_deinit = (Udf_func_deinit)bloomfilter_deinit;
  def->m_func = (Udf_func_any)bloomfilter;
}
