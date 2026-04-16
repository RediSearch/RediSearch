/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use ffi::{sds, sdscatlen};
use std::io::Write;

/// A [`std::io::Write`] adapter for Redis SDS (Simple Dynamic Strings) which allows
/// appending data to an existing SDS string via the C-side `sdscatlen` function.
///
/// # Invariant
///
/// `sds` is a [valid], non-null SDS string allocated by the C SDS library.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
pub struct SdsWriter {
    sds: sds,
}

impl SdsWriter {
    /// Creates a new `SdsWriter` wrapping the given SDS string.
    ///
    /// # Safety
    ///
    /// `sds` must be a [valid], non-null SDS string allocated by the C SDS library.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub const unsafe fn new(sds: sds) -> Self {
        Self { sds }
    }

    /// Convert the SdsWriter back into an SDS string.
    pub const fn into_sds(self) -> sds {
        self.sds
    }
}

impl Write for SdsWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        // `sdscatlen` may reallocate and returns the (possibly new) valid SDS pointer.
        // SAFETY: `self.sds` points to a valid SDS string and `buf` is a valid byte slice.
        self.sds = unsafe { sdscatlen(self.sds, buf.as_ptr().cast(), buf.len()) };
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
