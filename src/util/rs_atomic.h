/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#ifndef RS_ATOMIC_H__
#define RS_ATOMIC_H__

// C/C++-portable atomic field declaration and relaxed load/store helpers, for
// fields shared between the main thread and background workers in headers that
// are also compiled as C++ (unit tests).
#ifdef __cplusplus
#include <atomic>
#define RS_Atomic(T) std::atomic<T>
#define RS_AtomicBoolLoadRelaxed(p) (((std::atomic<bool> *)(p))->load(std::memory_order_relaxed))
#define RS_AtomicBoolStoreRelaxed(p, v) \
  (((std::atomic<bool> *)(p))->store((v), std::memory_order_relaxed))
#define RS_AtomicIntLoadRelaxed(p) (((std::atomic<int> *)(p))->load(std::memory_order_relaxed))
#define RS_AtomicIntStoreRelaxed(p, v) \
  (((std::atomic<int> *)(p))->store((v), std::memory_order_relaxed))
#define RS_AtomicUintLoadRelaxed(p) \
  (((std::atomic<unsigned> *)(p))->load(std::memory_order_relaxed))
#define RS_AtomicUintFetchOrRelaxed(p, v) \
  (((std::atomic<unsigned> *)(p))->fetch_or((v), std::memory_order_relaxed))
#define RS_AtomicUintFetchAndRelaxed(p, v) \
  (((std::atomic<unsigned> *)(p))->fetch_and((v), std::memory_order_relaxed))
#else
#define RS_Atomic(T) _Atomic(T)
#define RS_AtomicBoolLoadRelaxed(p) __atomic_load_n((bool *)(p), __ATOMIC_RELAXED)
#define RS_AtomicBoolStoreRelaxed(p, v) __atomic_store_n((bool *)(p), (v), __ATOMIC_RELAXED)
#define RS_AtomicIntLoadRelaxed(p) __atomic_load_n((int *)(p), __ATOMIC_RELAXED)
#define RS_AtomicIntStoreRelaxed(p, v) __atomic_store_n((int *)(p), (v), __ATOMIC_RELAXED)
#define RS_AtomicUintLoadRelaxed(p) __atomic_load_n((unsigned *)(p), __ATOMIC_RELAXED)
#define RS_AtomicUintFetchOrRelaxed(p, v) __atomic_fetch_or((unsigned *)(p), (v), __ATOMIC_RELAXED)
#define RS_AtomicUintFetchAndRelaxed(p, v) \
  __atomic_fetch_and((unsigned *)(p), (v), __ATOMIC_RELAXED)
#endif

#include <stdint.h>

// Relaxed atomic add/sub whose previous value is discarded.
//
// On AArch64 with LSE enabled at compile time (__ARM_FEATURE_ATOMICS), emit the
// store-only ST<op> form: an LDADD whose destination is XZR. The core does not
// need the old value back, so it may execute the add as a "far atomic" in the
// interconnect instead of pulling the cache line into L1. No compiler emits this
// form for a discarded __atomic_fetch_add result (GCC and clang both keep a dead
// destination register), so it has to be inline asm. Elsewhere it is a plain
// relaxed fetch_add.
//
// `p` must point to a naturally aligned 1/2/4/8-byte integer. `v` is converted
// to the pointee width, so a negated value wraps correctly for subtraction.
#if defined(__aarch64__) && defined(__ARM_FEATURE_ATOMICS)
#define RS_AtomicAddRelaxedNoRet(p, v)                                                        \
  do {                                                                                        \
    if (sizeof(*(p)) == 8) {                                                                  \
      __asm__ volatile("stadd %x1, %0" : "+Q"(*(p)) : "r"((uint64_t)(v)) : "memory");         \
    } else if (sizeof(*(p)) == 4) {                                                           \
      __asm__ volatile("stadd %w1, %0" : "+Q"(*(p)) : "r"((uint32_t)(v)) : "memory");         \
    } else if (sizeof(*(p)) == 2) {                                                           \
      __asm__ volatile("staddh %w1, %0" : "+Q"(*(p)) : "r"((uint32_t)(uint16_t)(v)) : "memory"); \
    } else {                                                                                  \
      __asm__ volatile("staddb %w1, %0" : "+Q"(*(p)) : "r"((uint32_t)(uint8_t)(v)) : "memory");  \
    }                                                                                         \
  } while (0)
#else
#define RS_AtomicAddRelaxedNoRet(p, v) ((void)__atomic_fetch_add((p), (v), __ATOMIC_RELAXED))
#endif
#define RS_AtomicSubRelaxedNoRet(p, v) RS_AtomicAddRelaxedNoRet((p), -(v))

#endif  // RS_ATOMIC_H__
