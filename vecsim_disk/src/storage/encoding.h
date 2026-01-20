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
#include <cstdint>

namespace encoding {

// Detect endianness at compile time
// Can be overridden with -DFORCE_BIG_ENDIAN_TEST=1 for testing
#if defined(FORCE_BIG_ENDIAN_TEST)
// Force big-endian mode for testing (compile with -DFORCE_BIG_ENDIAN_TEST=1)
constexpr bool kIsLittleEndian = false;

#else
// Auto-detect endianness
#if __BYTE_ORDER__ == __ORDER_LITTLE_ENDIAN__
constexpr bool kIsLittleEndian = true;
#elif __BYTE_ORDER__ == __ORDER_BIG_ENDIAN__
constexpr bool kIsLittleEndian = false;
#else
constexpr bool kIsLittleEndian = true;
#endif

#endif

// Helper to get the appropriate byte swap function based on size
// Uses reinterpret_cast to preserve bit patterns for all types
template <typename T>
inline T ByteSwap(T value) noexcept {
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8, "ByteSwap only supports 2, 4, or 8 byte types");
    if constexpr (sizeof(T) == 2) {
        uint16_t swapped = __builtin_bswap16(*reinterpret_cast<uint16_t*>(&value));
        return *reinterpret_cast<T*>(&swapped);
    } else if constexpr (sizeof(T) == 4) {
        uint32_t swapped = __builtin_bswap32(*reinterpret_cast<uint32_t*>(&value));
        return *reinterpret_cast<T*>(&swapped);
    } else if constexpr (sizeof(T) == 8) {
        uint64_t swapped = __builtin_bswap64(*reinterpret_cast<uint64_t*>(&value));
        return *reinterpret_cast<T*>(&swapped);
    }
    return value;
}

// Little-endian encoders/decoders (no-op on little-endian systems)
template <typename T>
inline void EncodeFixedLE(char* buf, const T val) noexcept {
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "EncodeFixedLE only supports 2, 4, or 8 byte types");
    T value = val;
    if constexpr (!kIsLittleEndian) {
        value = ByteSwap(value);
    }
    std::memcpy(buf, &value, sizeof(T)); // Compiler optimizes to single store
}

template <typename T>
inline T DecodeFixedLE(const char* ptr) noexcept {
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "DecodeFixedLE only supports 2, 4, or 8 byte types");
    T value;
    std::memcpy(&value, ptr, sizeof(T)); // Compiler optimizes to single load
    if constexpr (!kIsLittleEndian) {
        value = ByteSwap(value);
    }
    return value;
}

// Big-endian encoders/decoders (no-op on big-endian systems)
template <typename T>
inline void EncodeFixedBE(char* buf, const T val) noexcept {
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "EncodeFixedBE only supports 2, 4, or 8 byte types");
    T value = val;
    if constexpr (kIsLittleEndian) {
        value = ByteSwap(value);
    }
    std::memcpy(buf, &value, sizeof(T)); // Compiler optimizes to single store
}

template <typename T>
inline T DecodeFixedBE(const char* ptr) noexcept {
    static_assert(sizeof(T) == 2 || sizeof(T) == 4 || sizeof(T) == 8,
                  "DecodeFixedBE only supports 2, 4, or 8 byte types");
    T value;
    std::memcpy(&value, ptr, sizeof(T)); // Compiler optimizes to single load
    if constexpr (kIsLittleEndian) {
        value = ByteSwap(value);
    }
    return value;
}

} // namespace encoding
