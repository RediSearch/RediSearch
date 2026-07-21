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
#else
#define RS_Atomic(T) _Atomic(T)
#define RS_AtomicBoolLoadRelaxed(p) __atomic_load_n((bool *)(p), __ATOMIC_RELAXED)
#define RS_AtomicBoolStoreRelaxed(p, v) __atomic_store_n((bool *)(p), (v), __ATOMIC_RELAXED)
#define RS_AtomicIntLoadRelaxed(p) __atomic_load_n((int *)(p), __ATOMIC_RELAXED)
#define RS_AtomicIntStoreRelaxed(p, v) __atomic_store_n((int *)(p), (v), __ATOMIC_RELAXED)
#endif

#endif  // RS_ATOMIC_H__
