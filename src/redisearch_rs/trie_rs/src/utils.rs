/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

/// A version of `std`'s `strip_prefix` that's built on top of [`memchr::arch::is_prefix`].
#[inline(always)]
pub(crate) fn strip_prefix<'a>(haystack: &'a [u8], prefix: &[u8]) -> Option<&'a [u8]> {
    if !memchr::arch::all::is_prefix(haystack, prefix) {
        None
    } else {
        Some(&haystack[prefix.len()..])
    }
}

#[inline(always)]
/// Returns the length of the longest common prefix between `a` and `b`, along with the ordering of the first element
/// that differs between `a` and `b`.
///
/// It returns `None` if either slice is a prefix of the other.
pub(crate) fn longest_common_prefix(a: &[u8], b: &[u8]) -> Option<(usize, std::cmp::Ordering)> {
    let min_len = std::cmp::min(a.len(), b.len());

    // Process chunks of 8 bytes at a time
    let mut i = 0;
    while i + 8 <= min_len {
        let a_chunk = u64::from_ne_bytes(a[i..i + 8].try_into().unwrap());
        let b_chunk = u64::from_ne_bytes(b[i..i + 8].try_into().unwrap());

        if a_chunk != b_chunk {
            // Find the first differing byte
            let xor = a_chunk ^ b_chunk;
            let diff_pos = (xor.trailing_zeros() / 8) as usize;
            i += diff_pos;
            return Some((i, a[i].cmp(&b[i])));
        }

        i += 8;
    }

    // Process remaining bytes individually
    while i < min_len {
        if a[i] != b[i] {
            return Some((i, a[i].cmp(&b[i])));
        }
        i += 1;
    }

    None
}
