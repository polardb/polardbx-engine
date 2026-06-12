/* Copyright (c) 2026, Chengdu Haiguang IC Design Co., Ltd. All rights reserved. */

#include "zutil.h"

#define ZLIB_X86_64_CRC32_PCLMUL 1

/* Check GCC version and only apply vpclmulqdq target attribute when compiler is GCC 8 or later */
#if defined(__GNUC__) && (__GNUC__ >= 8)
#  define HAVE_VPCLMULQDQ_ATTRIBUTE 1
#endif

int has_crc32_x86_avx(void);
/* Runtime check for crc32_z_impl_x86_avx512: AVX-512 state + AVX512F/VL + VPCLMULQDQ. */
int has_crc32_x86_avx512(void);

/* SIMD optimized variants of crc32_z */
unsigned long
crc32_z_impl_x86_avx(unsigned long crc, const unsigned char FAR* buf,
    z_size_t len); /* pclmul */
#ifdef HAVE_VPCLMULQDQ_ATTRIBUTE
unsigned long
crc32_z_impl_x86_avx512(unsigned long crc, const unsigned char FAR* buf,
    z_size_t len); /* vpclmulqdq */
#endif