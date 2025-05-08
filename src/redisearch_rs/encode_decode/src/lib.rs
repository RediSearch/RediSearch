/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod buffer;
pub mod varint;
pub use buffer::*;

/// Rust implementation of `t_fieldMask` from `redisearch.h`
#[cfg(any(target_arch = "x86_64", target_arch = "aarch64"))]
pub type FieldMask = u128;
#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
pub type FieldMask = u64;
