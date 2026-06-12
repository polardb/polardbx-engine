/* Copyright (c) 2020, 2022, Oracle and/or its affiliates.

   This program is free software; you can redistribute it and/or modify
   it under the terms of the GNU General Public License, version 2.0,
   as published by the Free Software Foundation.

   This program is also distributed with certain software (including
   but not limited to OpenSSL) that is licensed under separate terms,
   as designated in a particular file or component or in included license
   documentation.  The authors of MySQL hereby grant you an additional
   permission to link the program and your derivative works with the
   separately licensed software that they have included with MySQL.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License, version 2.0, for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin St, Fifth Floor, Boston, MA 02110-1301  USA */

#include <algorithm>
#include <array>
#include <vector>

#include <gtest/gtest.h>

#include "unittest/gunit/benchmark.h"
#include "unittest/gunit/mysys_util.h"

#include "my_checksum.h"
// Unit tests for my_checksum function implemented with zlib and hardware
// intrinsics where supported.
using namespace mycrc32;

namespace mysys_my_checksum {

#if defined(__x86_64__) || defined(__amd64__) || defined(_M_X64)
extern "C" {
int has_crc32_x86_avx(void);
int has_crc32_x86_avx512(void);
unsigned long crc32_z_impl_x86_avx(unsigned long crc,
                                   const unsigned char FAR *buf, z_size_t len);
#if defined(__GNUC__) && (__GNUC__ >= 8)
unsigned long crc32_z_impl_x86_avx512(unsigned long crc,
                                      const unsigned char FAR *buf, z_size_t len)
    __attribute__((weak));
#endif
}
#endif

std::uint32_t VerifyChecksumFuncs(const unsigned char *buf,
                                  std::size_t length) {
  std::uint32_t crc_seed = 0xbadcafe;
  std::uint32_t expected_crc = crc32_z(crc_seed, buf, length);

  EXPECT_EQ(expected_crc, my_checksum(crc_seed, buf, length));
  EXPECT_EQ(expected_crc, PunnedCrc32<std::uint64_t>(crc_seed, buf, length));
  return expected_crc;
}

std::uint32_t TableCrc32Reference(std::uint32_t crc_seed,
                                  const unsigned char *buf,
                                  std::size_t length) {
  const z_crc_t FAR *table = get_crc_table();
  std::uint32_t crc = (~crc_seed) & 0xffffffffU;
  for (std::size_t i = 0; i < length; ++i) {
    crc = (crc >> 8) ^ static_cast<std::uint32_t>(table[(crc ^ buf[i]) & 0xffU]);
  }
  return crc ^ 0xffffffffU;
}

#if defined(__x86_64__) || defined(__amd64__) || defined(_M_X64)
void VerifyAgainstTableAndZlib(
    const std::vector<unsigned char> &data, std::size_t offset, std::size_t len,
    std::uint32_t seed,
    unsigned long (*impl)(unsigned long, const unsigned char FAR *, z_size_t)) {
  const unsigned char *ptr = data.data() + offset;
  const std::uint32_t table_crc = TableCrc32Reference(seed, ptr, len);
  const std::uint32_t z_crc = crc32_z(seed, ptr, len);
  const std::uint32_t simd_crc = impl(seed, ptr, len);

  EXPECT_EQ(table_crc, z_crc)
      << "offset=" << offset << " seed=" << seed << " len=" << len;
  EXPECT_EQ(table_crc, simd_crc)
      << "offset=" << offset << " seed=" << seed << " len=" << len;
}
#endif

TEST(MysysMyChecksum, EmptyBuffer) {
  unsigned char b[1] = {'0'};
  EXPECT_EQ(0xbadcafe, VerifyChecksumFuncs(b, 0));
}

TEST(MysysMyChecksum, TenBytesZero) {
  unsigned char b[10] = {'0', '0', '0', '0', '0', '0', '0', '0', '0', '0'};
  EXPECT_EQ(272755629U, VerifyChecksumFuncs(b, 10));
}

TEST(MysysMyChecksum, TenBytesFF) {
  unsigned char b[10] = {0xff, 0xff, 0xff, 0xff, 0xff,
                         0xff, 0xff, 0xff, 0xff, 0xff};
  EXPECT_EQ(533143559U, VerifyChecksumFuncs(b, 10));
}

TEST(MysysMyChecksum, ThirtyOneBytes) {
  alignas(alignof(std::int64_t)) unsigned char b[] = {
      0xff, 0xee, 0xdd, 0xcc, 0xbb, 0xaa, 0x99, 0x88, 0x77, 0x66, 0x55,
      0x44, 0x33, 0x22, 0x11, 0x00, 0x10, 0x20, 0x30, 0x40, 0x50, 0x60,
      0x70, 0x80, 0x90, 0xa0, 0xb0, 0xc0, 0xd0, 0xe0, 0xf0};
  EXPECT_EQ(2359828439U, VerifyChecksumFuncs(b, sizeof(b)));
  EXPECT_EQ(1093230115U, VerifyChecksumFuncs(b + 1, sizeof(b) - 1));
  EXPECT_EQ(3891498923U, VerifyChecksumFuncs(b + 4, sizeof(b) - 4));
  EXPECT_EQ(561217492U, VerifyChecksumFuncs(b + 7, sizeof(b) - 7));
}

TEST(MysysMyChecksum, IntegerCrc32_8bit) {
  unsigned char b = 0xba;
  std::uint32_t crc = 0xff;
  std::uint32_t zres = crc32_z(~crc, &b, 1U);
  EXPECT_EQ(IntegerCrc32(crc, b), ~zres);
}

TEST(MysysMyChecksum, IntegerCrc32_16bit) {
  unsigned char value_bytes[] = {0xaa, 0xbb};

  std::uint32_t crc = 0xbadcafe;
  std::uint32_t zres = crc32_z(~crc, value_bytes, sizeof(value_bytes));

  std::uint16_t value;
  memcpy(&value, value_bytes, sizeof(value_bytes));

  EXPECT_EQ(IntegerCrc32(crc, value), ~zres);
}

TEST(MysysMyChecksum, IntegerCrc32_32bit) {
  unsigned char value_bytes[] = {0xaa, 0xbb, 0xcc, 0xdd};
  std::uint32_t crc = 0xbadcafe;
  std::uint32_t zres = crc32_z(~crc, value_bytes, sizeof(value_bytes));
  std::uint32_t value;
  memcpy(&value, value_bytes, sizeof(value_bytes));
  EXPECT_EQ(IntegerCrc32(crc, value), ~zres);
}

TEST(MysysMyChecksum, IntegerCrc32_double_32bit) {
  unsigned char value_bytes[] = {0x99, 0x11, 0xaa, 0xbb,
                                 0xcc, 0xdd, 0xee, 0xff};
  std::uint32_t crc = 0xbadcafe;
  std::uint32_t zres = crc32_z(~crc, value_bytes, sizeof(value_bytes));
  std::uint32_t value1, value2;
  memcpy(&value1, value_bytes, sizeof(value1));
  memcpy(&value2, value_bytes + sizeof(value1), sizeof(value2));

  std::uint32_t crc1 = IntegerCrc32(crc, value1);
  EXPECT_EQ(IntegerCrc32(crc1, value2), ~zres);
}

TEST(MysysMyChecksum, IntegerCrc32_64bit) {
  unsigned char value_bytes[] = {0x11, 0x22, 0x33, 0x44,
                                 0x55, 0x66, 0x77, 0x88};
  std::uint64_t value;
  memcpy(&value, value_bytes, sizeof(value));
  std::uint32_t crc = 0xbadcafe;
  std::uint32_t zres = crc32_z(~crc, value_bytes, sizeof(value_bytes));
  EXPECT_EQ(IntegerCrc32(crc, value), ~zres);
}

#if defined(__x86_64__) || defined(__amd64__) || defined(_M_X64)
TEST(MysysMyChecksum, Crc32ZMatchesAvxImplementationWhenSupported) {
  if (!has_crc32_x86_avx()) {
    GTEST_SKIP() << "CPU/OS does not support AVX+PCLMUL CRC32";
  }

  std::vector<unsigned char> data(8192 + 257);
  unsigned char v = 0x5a;
  std::generate(data.begin(), data.end(), [&] { return ++v; });

  const std::array<std::size_t, 22> lengths = {
      0,    1,    2,    3,    7,    8,    9,    15,   16,   31,   32,
      63,   64,   65,   127,  128,  255,  256,  511,  4096, 7777, 8192};
  const std::array<std::uint32_t, 4> seeds = {0u, 1u, 0xbadcafeu, 0xdeadcafeu};
  const std::array<std::size_t, 4> offsets = {0, 1, 3, 7};

  for (std::uint32_t seed : seeds) {
    for (std::size_t offset : offsets) {
      for (std::size_t len : lengths) {
        VerifyAgainstTableAndZlib(data, offset, len, seed, crc32_z_impl_x86_avx);
      }
    }
  }
}

TEST(MysysMyChecksum, Crc32ZDispatchesToAvxImplementationWhenSupported) {
  if (!has_crc32_x86_avx()) {
    GTEST_SKIP() << "CPU/OS does not support AVX+PCLMUL CRC32";
  }
#if defined(__GNUC__) && (__GNUC__ >= 8)
  if (crc32_z_impl_x86_avx512 != nullptr && has_crc32_x86_avx512()) {
    GTEST_SKIP() << "crc32_z uses AVX512 path on this CPU";
  }
#endif

  alignas(alignof(std::uint64_t)) unsigned char buf[256];
  unsigned char v = 0x3c;
  std::generate(buf, buf + sizeof(buf), [&] { return v += 0x11; });

  const std::array<std::size_t, 6> lengths = {0, 16, 64, 127, 255, 256};
  const std::array<std::uint32_t, 3> seeds = {0u, 0xbadcafeu, 0xffffffffu};
  const std::array<std::size_t, 3> offsets = {0, 3, 7};

  for (std::uint32_t seed : seeds) {
    for (std::size_t offset : offsets) {
      for (std::size_t len : lengths) {
        const unsigned char *ptr = buf + offset;
        const std::uint32_t z_crc = crc32_z(seed, ptr, len);
        const std::uint32_t avx_crc =
            crc32_z_impl_x86_avx(seed, ptr, len);
        EXPECT_EQ(avx_crc, z_crc)
            << "offset=" << offset << " seed=" << seed << " len=" << len;
      }
    }
  }
}

TEST(MysysMyChecksum, ShortLengthWithNonZeroSeedMatchesReference) {
  if (!has_crc32_x86_avx()) {
    GTEST_SKIP() << "CPU/OS does not support AVX+PCLMUL CRC32";
  }

  std::vector<unsigned char> data(32);
  unsigned char v = 0xa0;
  std::generate(data.begin(), data.end(), [&] { return ++v; });

  const std::array<std::size_t, 4> lengths = {12, 13, 14, 15};
  const std::uint32_t seed = 0xbadcafeu;
  const std::array<std::size_t, 4> offsets = {0, 1, 3, 7};

  for (std::size_t offset : offsets) {
    for (std::size_t len : lengths) {
      const unsigned char *ptr = data.data() + offset;
      VerifyAgainstTableAndZlib(data, offset, len, seed,
                                crc32_z_impl_x86_avx);
      EXPECT_EQ(crc32_z(seed, ptr, len),
                crc32_z_impl_x86_avx(seed, ptr, len))
          << "offset=" << offset << " len=" << len;
    }
  }
}

TEST(MysysMyChecksum, SixteenByteBoundaryWithVariousOffsets) {
  if (!has_crc32_x86_avx()) {
    GTEST_SKIP() << "CPU/OS does not support AVX+PCLMUL CRC32";
  }

  std::vector<unsigned char> data(48);
  unsigned char v = 0x11;
  std::generate(data.begin(), data.end(), [&] { return v += 7; });

  constexpr std::size_t len = 16;
  const std::array<std::uint32_t, 3> seeds = {0u, 0xbadcafeu, 0xffffffffu};
  const std::array<std::size_t, 5> offsets = {0, 1, 5, 11, 15};

  for (std::uint32_t seed : seeds) {
    for (std::size_t offset : offsets) {
      VerifyAgainstTableAndZlib(data, offset, len, seed,
                                crc32_z_impl_x86_avx);
      const unsigned char *ptr = data.data() + offset;
      EXPECT_EQ(crc32_z(seed, ptr, len),
                crc32_z_impl_x86_avx(seed, ptr, len))
          << "offset=" << offset << " seed=" << seed;
    }
  }
}

#if defined(__GNUC__) && (__GNUC__ >= 8)
TEST(MysysMyChecksum, Crc32ZDispatchesToAvx512ImplementationWhenSupported) {
  if (crc32_z_impl_x86_avx512 == nullptr) {
    GTEST_SKIP() << "AVX512 crc32 implementation is not linked in this build";
  }
  if (!has_crc32_x86_avx512()) {
    GTEST_SKIP() << "CPU/OS does not support AVX-512 VPCLMUL CRC32";
  }

  std::vector<unsigned char> data(4096 + 16);
  unsigned char v = 0x7e;
  std::generate(data.begin(), data.end(), [&] { return v += 0x09; });

  const std::array<std::size_t, 6> lengths = {0, 16, 256, 512, 1024, 4096};
  const std::array<std::uint32_t, 3> seeds = {0u, 0x10203040u, 0xffffffffu};
  const std::array<std::size_t, 3> offsets = {0, 5, 11};

  for (std::uint32_t seed : seeds) {
    for (std::size_t offset : offsets) {
      for (std::size_t len : lengths) {
        if (offset + len > data.size()) continue;
        const unsigned char *ptr = data.data() + offset;
        const std::uint32_t z_crc = crc32_z(seed, ptr, len);
        const std::uint32_t avx512_crc =
            crc32_z_impl_x86_avx512(seed, ptr, len);
        EXPECT_EQ(avx512_crc, z_crc)
            << "offset=" << offset << " seed=" << seed << " len=" << len;
      }
    }
  }
}

TEST(MysysMyChecksum, Crc32ZMatchesAvx512ImplementationWhenSupported) {
  if (crc32_z_impl_x86_avx512 == nullptr) {
    GTEST_SKIP() << "AVX512 crc32 implementation is not linked in this build";
  }
  if (!has_crc32_x86_avx512()) {
    GTEST_SKIP() << "CPU/OS does not support AVX-512 VPCLMUL CRC32";
  }

  std::vector<unsigned char> data(16384 + 129);
  unsigned char v = 0xa5;
  std::generate(data.begin(), data.end(), [&] { return v += 17; });

  const std::array<std::size_t, 23> lengths = {
      0,    1,     2,    3,    7,    8,    9,    15,   16,   31,   32,  63,
      64,   65,    127,  128,  255,  256,  511,  512,  4095, 8192, 16384};
  const std::array<std::uint32_t, 4> seeds = {0u, 0x10203040u, 0xbadcafeu,
                                              0xffffffffu};
  const std::array<std::size_t, 4> offsets = {0, 1, 5, 11};

  for (std::uint32_t seed : seeds) {
    for (std::size_t offset : offsets) {
      for (std::size_t len : lengths) {
        VerifyAgainstTableAndZlib(data, offset, len, seed,
                                  crc32_z_impl_x86_avx512);
      }
    }
  }
}
#endif
#endif

static volatile std::uint32_t do_not_optimize = 0;
// 50k buffer, 8-byte using crc32_z directly
static void BM_crc32_z_50k(size_t num_iterations) {
  StopBenchmarkTiming();

  alignas(alignof(std::uint64_t)) unsigned char buf[50000];
  unsigned char v = 0xda;
  std::generate(buf, buf + sizeof(buf), [&] { return ++v; });

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    crc = crc32_z(crc, buf, sizeof(buf) - 1);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_crc32_z_50k)

// 50k buffer, 8-byte using my_checksum (will use intrinsics on ARM)
//
static void BM_my_checksum_50k(size_t num_iterations) {
  StopBenchmarkTiming();

  alignas(alignof(std::uint64_t)) unsigned char buf[50000];
  unsigned char v = 0xda;
  std::generate(buf, buf + sizeof(buf), [&] { return ++v; });

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    crc = my_checksum(crc, buf, sizeof(buf) - 1);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_my_checksum_50k)

#ifdef HAVE_ARMV8_CRC32_INTRINSIC

// Baseline 8 bit integer using crc32_z
static void BM_crc32_z_8bit(size_t num_iterations) {
  StopBenchmarkTiming();

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    std::uint8_t b = static_cast<std::uint8_t>(i);
    crc = crc32_z(crc, &b, 1);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_crc32_z_8bit)

// 8-bit integer using intrinsic wrapper overload
static void BM_IntegerCrc32_8bit(size_t num_iterations) {
  StopBenchmarkTiming();

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    crc = IntegerCrc32(crc, static_cast<std::uint8_t>(i));
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_IntegerCrc32_8bit)

// Baseline 64-bit integer suing crc32_z
static void BM_crc32_z_64bit(size_t num_iterations) {
  StopBenchmarkTiming();

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    unsigned char buf[8];
    memcpy(buf, &i, sizeof(i));
    crc = crc32_z(crc, buf, 8);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_crc32_z_64bit)

// 64-bit integer using intrinsic wrapper overload
static void BM_IntegerCrc32_64bit(size_t num_iterations) {
  StopBenchmarkTiming();

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    std::uint64_t v = i;
    crc = IntegerCrc32(crc, v);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_IntegerCrc32_64bit)

// PunnedCrc32 algo 50k with 8-byte slices
static void BM_PunnedCrc32_50k(size_t num_iterations) {
  StopBenchmarkTiming();

  alignas(alignof(std::uint64_t)) unsigned char buf[50000];
  unsigned char v = 0xda;
  std::generate(buf, buf + sizeof(buf), [&] { return ++v; });

  std::uint32_t crc = 0xdeadcafe;
  StartBenchmarkTiming();

  for (size_t i = 0; i < num_iterations; ++i) {
    crc = PunnedCrc32<std::uint64_t>(crc, buf, sizeof(buf) - 1);
  }

  StopBenchmarkTiming();
  do_not_optimize = crc;
}
BENCHMARK(BM_PunnedCrc32_50k)

#endif /* HAVE_ARMV8_CRC32_INTRINSIC */

}  // namespace mysys_my_checksum
