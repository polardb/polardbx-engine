/* Copyright (c) 2026, Chengdu Haiguang IC Design Co., Ltd. All rights reserved. */

#include <stdint.h>

static inline uint32_t cpuid_leaf1_ecx(void)
{
    uint32_t eax, ebx, ecx, edx;

    eax = 0x01;
    ecx = 0x00;
    __asm__ volatile (
            "cpuid"
            : "+a"(eax), "=b"(ebx), "+c"(ecx), "=d"(edx)
            :
            : "memory"
    );

    return ecx;
}

static inline int has_osxsave(void)
{
    return (cpuid_leaf1_ecx() >> 27) & 0x01;
}

// check XCR0 Bits
static inline uint64_t xgetbv(uint32_t ecx)
{
    uint32_t eax, edx;
    __asm__ volatile (
            "xgetbv"
            : "=a"(eax), "=d"(edx)
            : "c"(ecx)
            : "memory"
    );

    return ((uint64_t)edx << 32) | eax;
}

static inline int xcr0_ymm_enabled(void)
{
    uint64_t xcr0 = xgetbv(0);

    // Check XCR0[2:1] == '11'(YMM state enabled)
    return ((xcr0 >> 1) & 0x03) == 0x03;
}

static inline int xcr0_zmm_enabled(void) {
    uint64_t xcr0 = xgetbv(0);

    // Check XCR0[7:5] == '111'(ZMM + OPMASK state enabled)
    return ((xcr0 >> 5) & 0x07) == 0x07;
}

/* Runtime check for crc32_z_impl_x86_avx: AVX + OS YMM state + PCLMULQDQ. */
int has_crc32_x86_avx(void) {
    uint32_t ecx;

    ecx = cpuid_leaf1_ecx();
    if (((ecx >> 27) & 0x01) == 0) /* OSXSAVE */
        return 0;
    if (((ecx >> 28) & 0x01) == 0) /* AVX */
        return 0;
    if (((ecx >> 1) & 0x01) == 0) /* PCLMULQDQ */
        return 0;
    if (!xcr0_ymm_enabled())
        return 0;
    return 1;
}

int has_crc32_x86_avx512(void)
{
    uint32_t eax, ebx, ecx, edx;

    if (!has_osxsave())
         return 0;
    if (!xcr0_ymm_enabled())
         return 0;

    if (!xcr0_zmm_enabled())
        return 0;

    eax = 0x07;
    ecx = 0x00;
    __asm__ volatile (
            "cpuid"
            : "+a"(eax), "=b"(ebx), "+c"(ecx), "=d"(edx)
            :
            : "memory"
    );

    /* AVX512F, AVX512VL, and VPCLMULQDQ required by crc32_z_impl_x86_avx512. */
    return ((ebx >> 16) & 0x01) && ((ebx >> 31) & 0x01) && ((ecx >> 10) & 0x01);
}
