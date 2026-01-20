/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// FNV-32a hasher with a C-compatible seed.
///
/// This hasher uses the same seed as the original C HLL implementation to ensure
/// hash compatibility when interoperating with C code.
pub struct CFnvHasher(fnv::Fnv32);

impl Default for CFnvHasher {
    fn default() -> Self {
        // The seed used in the original C HLL implementation.
        Self(fnv::Fnv32::with_offset_basis(0x5f61767a))
    }
}

impl std::hash::Hasher for CFnvHasher {
    #[inline]
    fn finish(&self) -> u64 {
        self.0.finish()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.write(bytes);
    }
}

impl hash32::Hasher for CFnvHasher {
    #[inline]
    fn finish32(&self) -> u32 {
        self.0.finish32()
    }
}
