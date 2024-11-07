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


#ifndef EASY_LOCK_ATOMIC_H_
#define EASY_LOCK_ATOMIC_H_

#include <easy_define.h>
#include <stdint.h>
#include <sched.h>

/**
 * 原子操作
 */

EASY_CPP_START

#define EASY_SMP_LOCK               "lock;"
#define easy_atomic_set(v,i)        ((v) = (i))
typedef volatile int32_t            easy_atomic32_t;

// 32bit
static __inline__ void easy_atomic32_add(easy_atomic32_t *v, int i)
{
#if defined __x86_64__
    __asm__ __volatile__(
        EASY_SMP_LOCK "addl %1,%0"
        : "=m" ((*v)) : "r" (i), "m" ((*v)));
#elif defined __aarch64__
    __sync_fetch_and_add(v, i);
#endif
}
static __inline__ int32_t easy_atomic32_add_return(easy_atomic32_t *value, int32_t diff)
{
#if defined __x86_64__
    int32_t                 old = diff;
    __asm__ volatile (
        EASY_SMP_LOCK "xaddl %0, %1"
        :"+r" (diff), "+m" (*value) : : "memory");
    return diff + old;
#elif defined __aarch64__
    return __sync_add_and_fetch(value, diff); 
#endif
}
static __inline__ void easy_atomic32_inc(easy_atomic32_t *v)
{
#if defined __x86_64__
    __asm__ __volatile__(EASY_SMP_LOCK "incl %0" : "=m" (*v) :"m" (*v));
#elif defined __aarch64__
    __sync_fetch_and_add(v, 1);
#endif
}
static __inline__ void easy_atomic32_dec(easy_atomic32_t *v)
{
#if defined __x86_64__
    __asm__ __volatile__(EASY_SMP_LOCK "decl %0" : "=m" (*v) :"m" (*v));
#elif defined __aarch64__
    __sync_fetch_and_sub(v, 1);
#endif
}

// 64bit
#if __WORDSIZE == 64
typedef volatile int64_t easy_atomic_t;
static __inline__ void easy_atomic_add(easy_atomic_t *v, int64_t i)
{
#if defined __x86_64__
    __asm__ __volatile__(
        EASY_SMP_LOCK "addq %1,%0"
        : "=m" ((*v)) : "r" (i), "m" ((*v)));
#elif defined __aarch64__
    __sync_fetch_and_add(v, i);
#endif
}
static __inline__ int64_t easy_atomic_add_return(easy_atomic_t *value, int64_t i)
{
#if defined __x86_64__
    int64_t                 __i = i;
    __asm__ __volatile__(
        EASY_SMP_LOCK "xaddq %0, %1;"
        :"=r"(i)
        :"m"(*value), "0"(i));
    return i + __i;
#elif defined __aarch64__
    return __sync_add_and_fetch(value, i) ;  //arm
#endif
}
static __inline__ int64_t easy_atomic_cmp_set(easy_atomic_t *lock, int64_t old, int64_t set)
{
#if defined __x86_64__
    uint8_t                 res;
    __asm__ volatile (
        EASY_SMP_LOCK "cmpxchgq %3, %1; sete %0"
        : "=a" (res) : "m" (*lock), "a" (old), "r" (set) : "cc", "memory");
    return res;
#elif defined __aarch64__
    return __sync_bool_compare_and_swap(lock, old, set);  //arm
#endif
}
static __inline__ void easy_atomic_inc(easy_atomic_t *v)
{
#if defined __x86_64__
    __asm__ __volatile__(EASY_SMP_LOCK "incq %0" : "=m" (*v) :"m" (*v));
#elif defined __aarch64__
    __sync_fetch_and_add(v, 1);
#endif
}
static __inline__ void easy_atomic_dec(easy_atomic_t *v)
{
#if defined __x86_64__
    __asm__ __volatile__(EASY_SMP_LOCK "decq %0" : "=m" (*v) :"m" (*v));
#elif defined __aarch64__
    __sync_fetch_and_sub(v, 1);
#endif
}
#else
typedef volatile int32_t easy_atomic_t;
#define easy_atomic_add(v,i) easy_atomic32_add(v,i)
#define easy_atomic_add_return(v,diff) easy_atomic32_add_return(v,diff)
#define easy_atomic_inc(v) easy_atomic32_inc(v)
#define easy_atomic_dec(v) easy_atomic32_dec(v)
static __inline__ int32_t easy_atomic_cmp_set(easy_atomic_t *lock, int32_t old, int32_t set)
{
#if defined __x86_64__
    uint8_t                 res;
    __asm__ volatile (
        EASY_SMP_LOCK "cmpxchgl %3, %1; sete %0"
        : "=a" (res) : "m" (*lock), "a" (old), "r" (set) : "cc", "memory");
    return res;
#elif defined __aarch64__
    return __sync_bool_compare_and_swap(lock, old, set);
#endif
}
#endif

#if defined __x86_64__
#define easy_unlock(lock)   {__asm__ ("" ::: "memory"); *(lock) = 0;}
#elif defined __aarch64__
#define easy_unlock(lock)   {__asm__ __volatile ("dsb sy" ::: "memory"); *(lock) = 0;}
#endif

#if defined __x86_64__
#define easy_mfence() {__asm__ __volatile ("mfence" ::: "memory");}
#elif defined __aarch64__
#define easy_mfence() {__asm__ __volatile ("dsb sy" ::: "memory");}  //for ARM
#endif

#ifndef EASY_SPIN_USE_SYS

#define easy_spin_t easy_atomic_t
#define EASY_SPIN_INITER (0)
#define easy_trylock(lock)  (*(lock) == 0 && easy_atomic_cmp_set(lock, 0, 1))
#define easy_spin_unlock easy_unlock
static __inline__ void easy_spin_lock(easy_spin_t *lock)
{
    int                     i, n;

    for ( ; ; ) {
        if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
            return;
        }

        for (n = 1; n < 1024; n <<= 1) {

            for (i = 0; i < n; i++) {
#if defined __x86_64__
                __asm__ (".byte 0xf3, 0x90");
#elif defined __aarch64__
                __asm__ ("dsb ish" ::: "memory");
#endif
            }

            if (*lock == 0 && easy_atomic_cmp_set(lock, 0, 1)) {
                return;
            }
        }

        sched_yield();
    }
}
#else
#include <pthread.h>
#define easy_spin_t pthread_spinlock_t
#define EASY_SPIN_INITER (1)
static __inline__ int easy_trylock(easy_spin_t *lock)
{
  return pthread_spin_trylock(lock);
}
static __inline__ void easy_spin_lock(easy_spin_t *lock)
{
  pthread_spin_lock(lock);
}
static __inline__ void easy_spin_unlock(easy_spin_t *lock)
{
  pthread_spin_unlock(lock);
}
#endif

static __inline__ void easy_clear_bit(unsigned long nr, volatile void *addr)
{
    volatile int8_t                  *m = ((volatile int8_t *) addr) + (nr >> 3);
    *m &= (int8_t)(~(1 << (nr & 7)));
}
static __inline__ void easy_set_bit(unsigned long nr, volatile void *addr)
{
    volatile int8_t                  *m = ((volatile int8_t *) addr) + (nr >> 3);
    *m |= (int8_t)(1 << (nr & 7));
}

typedef struct easy_spinrwlock_t {
    easy_atomic_t           ref_cnt;
    easy_atomic_t           wait_write;
} easy_spinrwlock_t;
#define EASY_SPINRWLOCK_INITIALIZER {0, 0}
static __inline__ int easy_spinrwlock_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 <= oldv
                        && 0 == lock->wait_write) {
                    easy_atomic_t           newv = oldv + 1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }
#if defined __x86_64__
                asm("pause");
#elif defined __aarch64__
                asm("dsb ish" ::: "memory");
#endif
                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        int                     cond = 1;
        easy_atomic_inc(&lock->wait_write);

        while (cond) {
            int                     loop = 1;

            do {
                easy_atomic_t           oldv = lock->ref_cnt;

                if (0 == oldv) {
                    easy_atomic_t           newv = -1;

                    if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                        cond = 0;
                        break;
                    }
                }
#if defined __x86_64__
                asm("pause");
#elif defined __aarch64__
                asm("dsb ish" ::: "memory");
#endif
                loop <<= 1;
            } while (loop < 1024);

            sched_yield();
        }

        easy_atomic_dec(&lock->wait_write);
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_rdlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 <= oldv
                && 0 == lock->wait_write) {
            easy_atomic_t           newv = oldv + 1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_try_wrlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        ret = EASY_AGAIN;
        easy_atomic_t           oldv = lock->ref_cnt;

        if (0 == oldv) {
            easy_atomic_t           newv = -1;

            if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                ret = EASY_OK;
            }
        }
    }

    return ret;
}
static __inline__ int easy_spinrwlock_unlock(easy_spinrwlock_t *lock)
{
    int                     ret = EASY_OK;

    if (NULL == lock) {
        ret = EASY_ERROR;
    } else {
        while (1) {
            easy_atomic_t           oldv = lock->ref_cnt;

            if (-1 == oldv) {
                easy_atomic_t           newv = 0;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else if (0 < oldv) {
                easy_atomic_t           newv = oldv - 1;

                if (easy_atomic_cmp_set(&lock->ref_cnt, oldv, newv)) {
                    break;
                }
            } else {
                ret = EASY_ERROR;
                break;
            }
        }
    }

    return ret;
}

EASY_CPP_END

#endif
