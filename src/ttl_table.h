/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/
#pragma once

// Public header for the `TimeToLiveTable`. The implementation lives in the
// Rust crate `ttl_table_ffi`; this file owns the `FieldExpiration` type
// definition (consumed by C/C++ callers and by bindgen for `ffi::FieldExpiration`)
// and then re-exports the cbindgen-generated function prototypes.

#include "redisearch.h"  // t_fieldIndex, t_expirationTimePoint

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FieldExpiration {
  t_fieldIndex index;
  t_expirationTimePoint point;
} FieldExpiration;

#ifdef __cplusplus
}
#endif

#include "redisearch_rs/headers/ttl_table.h"
