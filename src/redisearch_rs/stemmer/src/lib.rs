/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Byte-oriented indexing expansions for backend-provided stems.

#![warn(
    clippy::unwrap_used,
    clippy::dbg_macro,
    clippy::todo,
    clippy::unimplemented,
    clippy::print_stdout,
    clippy::panic,
    clippy::unnecessary_safety_comment,
    clippy::ptr_as_ptr,
    clippy::borrow_as_ptr,
    clippy::ref_as_ptr,
    clippy::indexing_slicing
)]

/// Distinguishes indexed stems from literal terms.
pub const STEM_PREFIX: u8 = b'+';

/// Reusable storage for one indexing expansion, borrowed until the next update.
#[derive(Default)]
pub struct StemBuffer {
    bytes: Vec<u8>,
}

impl StemBuffer {
    /// Return [`STEM_PREFIX`], stem bytes and a trailing NUL, or suppress unchanged stems.
    /// The indexing length excludes the terminator; inputs contain no appended terminator.
    /// Equal-length inputs compare ASCII case-insensitively up to the first matching NUL.
    pub fn update(&mut self, original: &[u8], stem: &[u8]) -> Option<&[u8]> {
        if original.eq_ignore_ascii_case(stem)
            || (original.len() == stem.len()
                && original
                    .iter()
                    .zip(stem)
                    .find(|&(&a, &b)| a == 0 || !a.eq_ignore_ascii_case(&b))
                    .is_none_or(|(a, b)| a == b))
        {
            return None;
        }
        self.bytes.clear();
        self.bytes.push(STEM_PREFIX);
        self.bytes.extend_from_slice(stem);
        self.bytes.push(0);
        Some(&self.bytes)
    }
}
