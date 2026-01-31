/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! WyHash hasher wrapper for HyperLogLog.

use std::hash::Hasher;

/// WyHash hasher wrapper (truncated to 32 bits).
///
/// WyHash is a fast, high-quality hash function with excellent distribution
/// properties. This wrapper adapts it for use with HyperLogLog by truncating
/// the 64-bit output to 32 bits.
pub struct WyHasher(wyhash::WyHash);

impl Default for WyHasher {
    fn default() -> Self {
        Self(wyhash::WyHash::with_seed(0))
    }
}

impl std::hash::Hasher for WyHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for WyHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish() as u32
    }
}
