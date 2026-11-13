/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
 */
#pragma once

#include <cstring>
#include <type_traits>

// Alignment-safe load of a trivially-copyable T from an arbitrary byte address.
// Use when accessing fields whose alignment is not guaranteed by the layout
// (e.g. FP32 metadata that follows a uint8_t / float16 payload of dynamic length).
// Compilers reliably lower this to a single load on architectures that allow
// unaligned access; on strict-alignment targets it expands to a safe byte copy.
template <typename T>
static inline T load_unaligned(const void *ptr) {
    static_assert(std::is_trivially_copyable_v<T>,
                  "load_unaligned requires a trivially-copyable T");
    T value;
    std::memcpy(&value, ptr, sizeof(T));
    return value;
}

#if defined(__GNUC__) || defined(__clang__)
#define PORTABLE_ALIGN16 __attribute__((aligned(16)))
#define PORTABLE_ALIGN32 __attribute__((aligned(32)))
#define PORTABLE_ALIGN64 __attribute__((aligned(64)))
#elif defined(_MSC_VER)
#define PORTABLE_ALIGN16 __declspec(align(16))
#define PORTABLE_ALIGN32 __declspec(align(32))
#define PORTABLE_ALIGN64 __declspec(align(64))
#endif

#define PORTABLE_ALIGN PORTABLE_ALIGN64

// TODO: relax the above alignment requirements according to the CPU architecture
#ifndef PORTABLE_ALIGN
#if defined(__AVX512F__)
#define PORTABLE_ALIGN PORTABLE_ALIGN64
#elif defined(__AVX__)
#define PORTABLE_ALIGN PORTABLE_ALIGN32
#elif defined(__SSE__)
#define PORTABLE_ALIGN PORTABLE_ALIGN16
#else
#define PORTABLE_ALIGN
#endif
#endif // ifndef PORTABLE_ALIGN
