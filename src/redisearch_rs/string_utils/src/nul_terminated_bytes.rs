/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! An owned byte buffer with a trailing NUL.

use std::fmt;
use std::ops::Deref;

/// An owned, heap-allocated bytes buffer with a trailing NUL byte.
///
/// The trailing NUL exists for C-ABI compatibility, preventing out-of-bounds
/// reads and undefined behavior when passing the buffer's raw pointer to C functions
/// that expect a nul-terminated string.
///
/// Note that if the buffer contains interior NUL bytes, C functions will treat
/// the first NUL as the end of the string and truncate the payload early. This
/// can lead to logic bugs, but it remains memory safe.
#[derive(PartialEq, Eq)]
pub struct NulTerminatedBytes(Box<[u8]>);

impl NulTerminatedBytes {
    /// Reconstruct a buffer previously leaked by [`Self::into_raw_parts`].
    ///
    /// # Safety
    ///
    /// 1. `ptr` and `len` must be the exact pair returned by a prior
    ///    [`Self::into_raw_parts`] call whose buffer has not since been freed or
    ///    reconstructed. Passing a mismatched length, a pointer from a different
    ///    allocation, or a pair that was already reclaimed is undefined behaviour.
    pub unsafe fn from_raw_parts(ptr: *mut u8, len: usize) -> Self {
        // `into_raw_parts` leaked a `len + 1`-byte boxed slice (payload plus the
        // NUL), so rebuild the fat pointer with that same length for the box to
        // deallocate the whole allocation. `len` originates from a real
        // allocation, so `len + 1` cannot overflow.
        let ptr = std::ptr::slice_from_raw_parts_mut(ptr, len + 1);

        // SAFETY: caller guarantees (1).
        Self(unsafe { Box::from_raw(ptr) })
    }

    /// Consumes the buffer, transferring ownership to the caller as a raw pointer to
    /// its first byte plus the payload length (excluding the trailing NUL).
    ///
    /// The caller must eventually reclaim it with [`Self::from_raw_parts`] to avoid
    /// leaking. The length is returned alongside the pointer because it cannot
    /// be recovered from the pointer alone.
    pub fn into_raw_parts(self) -> (*mut u8, usize) {
        let len = self.len();
        (Box::into_raw(self.0).cast::<u8>(), len)
    }

    /// Buffer length excluding the trailing NUL.
    pub fn len(&self) -> usize {
        self.0.len() - 1
    }

    /// Whether the buffer is empty, i.e. the buffer is just the trailing NUL.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// The buffer's bytes excluding the trailing NUL.
    pub fn as_bytes(&self) -> &[u8] {
        &self.0[..self.len()]
    }

    /// The full buffer including the trailing NUL byte.
    pub fn as_bytes_with_nul(&self) -> &[u8] {
        &self.0
    }
}

/// Copies `payload` into a fresh `len + 1`-byte buffer, appending a single trailing NUL.
impl From<&[u8]> for NulTerminatedBytes {
    fn from(payload: &[u8]) -> Self {
        let mut data = Vec::with_capacity(payload.len() + 1);
        data.extend_from_slice(payload);
        data.push(0);

        Self(data.into_boxed_slice())
    }
}

/// Convenience for byte-string literals (`b"..."`), which are `&[u8; N]` and so
/// don't coerce to `&[u8]` during `From`/`into` resolution.
impl<const N: usize> From<&[u8; N]> for NulTerminatedBytes {
    fn from(payload: &[u8; N]) -> Self {
        Self::from(payload.as_slice())
    }
}

/// Takes ownership of `payload`, appending a single trailing NUL byte.
impl From<Vec<u8>> for NulTerminatedBytes {
    fn from(mut payload: Vec<u8>) -> Self {
        payload.push(0);

        Self(payload.into_boxed_slice())
    }
}

/// Dereferences to the payload bytes, *excluding* the trailing NUL — the same
/// view as [`as_bytes`](NulTerminatedBytes::as_bytes).
impl Deref for NulTerminatedBytes {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        self.as_bytes()
    }
}

/// Renders the payload (excluding the trailing NUL) as a string, lossily
/// replacing any non-UTF-8 bytes — the payload is usually text.
impl fmt::Debug for NulTerminatedBytes {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&String::from_utf8_lossy(self.as_bytes()), f)
    }
}
