/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! 32-bit and 64-bit [FNV-1a hashing] functions.
//!
//! These are implemented manually here as the popular [fnv] crate does not
//! include a 32-bit version of the hash, which uses different parameters,
//! and is not just a 32-bit truncation of the 64-bit hashing algorithm.
//!
//! [FNV-1a hashing]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-1a
//! [fnv]: https://docs.rs/fnv

use std::hash::Hasher;

/// A 32-bit FNV-1a hasher.
pub struct Fnv32(u32);

impl Fnv32 {
    /// The 32-bit [FNV-1 prime].
    ///
    /// [FNV-1 prime]:http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    pub(crate) const PRIME: u32 = 0x1000193;

    /// The 32-bit [FNV-1 offset basis].
    ///
    /// [FNV-1 prime]:http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    pub(crate) const OFFSET_BASIS: u32 = 0x811c9dc5;
}

/// A `Fnv32` initialized with a [32-bit FNV-1 offset basis].
///
/// [32-bit FNV-1 offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
impl Default for Fnv32 {
    #[inline]
    fn default() -> Fnv32 {
        Fnv32(Self::OFFSET_BASIS)
    }
}

impl Fnv32 {
    /// Creates an `Fnv32` with a given [offset basis].
    ///
    /// [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    #[inline]
    #[must_use]
    pub fn with_offset_basis(offset_basis: u32) -> Fnv32 {
        Fnv32(offset_basis)
    }
}

impl Hasher for Fnv32 {
    #[inline]
    fn finish(&self) -> u64 {
        self.0 as u64
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let Fnv32(mut hash) = *self;

        for &byte in bytes {
            hash ^= byte as u32;
            hash = hash.wrapping_mul(Self::PRIME);
        }

        *self = Fnv32(hash);
    }
}

/// A 64-bit FNV-1a hasher.
pub struct Fnv64(u64);

impl Fnv64 {
    /// The 64-bit [FNV-1 prime].
    ///
    /// [FNV-1 prime]:http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    const PRIME: u64 = 0x100000001b3;

    /// The 64-bit [FNV-1 offset basis].
    ///
    /// [FNV-1 prime]:http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    const OFFSET_BASIS: u64 = 0xcbf29ce484222325;
}

/// A `Fnv64` initialized with a [64-bit FNV-1 offset basis].
///
/// [64-bit FNV-1 offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
impl Default for Fnv64 {
    #[inline]
    fn default() -> Fnv64 {
        Fnv64(Self::OFFSET_BASIS)
    }
}

impl Fnv64 {
    /// Creates an `Fnv64` with a given [offset basis].
    ///
    /// [offset basis]: http://www.isthe.com/chongo/tech/comp/fnv/#FNV-param
    #[inline]
    #[must_use]
    pub fn with_offset_basis(offset_basis: u64) -> Fnv64 {
        Fnv64(offset_basis)
    }
}

impl Hasher for Fnv64 {
    #[inline]
    fn finish(&self) -> u64 {
        self.0
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        let Fnv64(mut hash) = *self;

        for &byte in bytes {
            hash ^= byte as u64;
            hash = hash.wrapping_mul(Self::PRIME);
        }

        *self = Fnv64(hash);
    }
}
