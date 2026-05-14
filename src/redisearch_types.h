/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

// Fundamental type aliases used across both C code and Rust-generated headers.
// Extracted from redisearch.h to break circular include dependencies:
// generated headers need these types, but redisearch.h includes generated headers.

#pragma once

#include <stdint.h>

typedef uint64_t t_docId;

// Used to identify any field index within the spec, not just textual fields
typedef uint16_t t_fieldIndex;

#if (defined(__x86_64__) || defined(__aarch64__) || defined(__arm64__)) && !defined(RS_NO_U128)
/* 64 bit architectures use 128 bit field masks and up to 128 fields */
typedef __uint128_t t_fieldMask;
#define RS_FIELDMASK_ALL (((__uint128_t)1 << 127) - (__uint128_t)1 + ((__uint128_t)1 << 127))
#else
/* 32 bit architectures use 64 bits and 64 fields only */
typedef uint64_t t_fieldMask;
#define RS_FIELDMASK_ALL 0xFFFFFFFFFFFFFFFF
#endif
