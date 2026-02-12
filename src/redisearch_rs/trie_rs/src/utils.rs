/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::arch::aarch64::*;
use std::cmp;

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
pub(crate) fn longest_common_prefix_fallback(
    a: &[u8],
    b: &[u8],
) -> Option<(usize, std::cmp::Ordering)> {
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

#[inline(always)]
pub(crate) fn longest_common_prefix(
    input_a: &[u8],
    input_b: &[u8],
) -> Option<(usize, std::cmp::Ordering)> {
    let len = std::cmp::min(input_a.len(), input_b.len());

    // if len <= uint8x16x4_t::SIMD_WIDTH {
    // longest_common_prefix_fallback(input_a, input_b)
    //     // input_a.iter().zip(input_b.iter()).position(|(a, b)| a != b)
    // } else {
    longest_common_prefix_memchr::<uint8x16x4_t>(input_a.as_ptr(), input_b.as_ptr(), len)
        .map(|i| (i, input_a[i].cmp(&input_b[i])))
    // }
}

#[inline(always)]
pub(crate) fn longest_common_prefix_memchr<V: Vector>(
    a: *const u8,
    b: *const u8,
    len: usize,
) -> Option<usize> {
    unsafe {
        debug_assert!(V::SIMD_WIDTH <= 64, "vector cannot be bigger than 64 bytes");

        let mut i = 0;
        while i + V::SIMD_WIDTH <= len {
            let a = V::load_unaligned(a.add(i));
            let b = V::load_unaligned(b.add(i));

            let ab = a.cmpeq(b);

            let pos = ab.movemask();

            if pos < V::SIMD_WIDTH {
                i += pos;
                return Some(i);
            }

            i += V::SIMD_WIDTH;
        }

        // Process remaining bytes individually
        while i < len {
            if a.add(i).read() != b.add(i).read() {
                return Some(i);
            }
            i += 1;
        }

        None
    }
}

#[inline(always)]
unsafe fn search_chunk<V: Vector>(a: *const u8, b: *const u8) -> Option<usize> {
    unsafe {
        let a = V::load_unaligned(a);
        let b = V::load_unaligned(b);
        let pos = a.cmpeq(b).movemask();
        if pos < V::SIMD_WIDTH { Some(pos) } else { None }
    }
}

trait Vector: Copy + core::fmt::Debug {
    const SIMD_WIDTH: usize;
    /// The bits that must be zero in order for a `*const u8` pointer to be
    /// correctly aligned to read vector values.
    const ALIGN: usize;

    /// Read a vector-size number of bytes from the given pointer. The pointer
    /// does not need to be aligned.
    ///
    /// # Safety
    ///
    /// Callers must guarantee that at least `BYTES` bytes are readable from
    /// `data`.
    unsafe fn load_unaligned(data: *const u8) -> Self;
    /// _mm_cmpeq_epi8 or _mm256_cmpeq_epi8
    unsafe fn cmpeq(self, vector2: Self) -> Self;
    /// _mm_movemask_epi8 or _mm256_movemask_epi8
    unsafe fn movemask(self) -> usize;
}

impl Vector for uint8x16x4_t {
    const SIMD_WIDTH: usize = 64;
    const ALIGN: usize = Self::SIMD_WIDTH - 1;

    #[inline(always)]
    unsafe fn load_unaligned(data: *const u8) -> Self {
        unsafe { vld4q_u8(data) }
    }

    #[inline(always)]
    unsafe fn movemask(self) -> usize {
        unsafe {
            let uint8x16x4_t(a, b, c, d) = self;

            let ab = vsriq_n_u8(b, a, 1);
            let cd = vsriq_n_u8(d, c, 1);
            let abcd = vsriq_n_u8(cd, ab, 2);
            let abcd = vsriq_n_u8(abcd, abcd, 4);

            let asu16s = vreinterpretq_u16_u8(abcd);
            let mask = vshrn_n_u16(asu16s, 4);
            let asu64 = vreinterpret_u64_u8(mask);
            let scalar64 = vget_lane_u64(asu64, 0);

            scalar64.trailing_ones() as usize
        }
    }

    #[inline(always)]
    unsafe fn cmpeq(self, vector2: Self) -> Self {
        unsafe {
            let a = vceqq_u8(self.0, vector2.0);
            let b = vceqq_u8(self.1, vector2.1);
            let c = vceqq_u8(self.2, vector2.2);
            let d = vceqq_u8(self.3, vector2.3);

            Self(a, b, c, d)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        // fn impl_equality(a in ".{2,512}", b in ".{2,512}") {
        #[test]
        fn impl_equality(a: String, b: String) {
            let expected = longest_common_prefix(a.as_bytes(), b.as_bytes()).map(|(idx, _)| idx);
            let found = a.as_bytes().iter().zip(b.as_bytes().iter()).position(|(a, b)| a != b);

                // longest_common_prefix_memchr::<uint8x16x4_t>(a.as_bytes(), b.as_bytes());

            prop_assert_eq!(expected, found);
        }
    }
}
