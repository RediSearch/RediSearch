use std::ffi::c_char;

/// Returns the index of the first occurrence of `target` in `slice`, or `None` if not found.
#[inline(always)]
pub(crate) fn memchr_c_char(target: c_char, slice: &[c_char]) -> Option<usize> {
    memchr::memchr(target as u8, to_u8_slice(slice))
}

/// A version of `std`'s `strip_prefix` that's built on top of [`memchr::arch::is_prefix`].
#[inline(always)]
pub(crate) fn strip_prefix<'a>(haystack: &'a [c_char], prefix: &[c_char]) -> Option<&'a [c_char]> {
    if !memchr::arch::all::is_prefix(to_u8_slice(haystack), to_u8_slice(prefix)) {
        None
    } else {
        Some(&haystack[prefix.len()..])
    }
}

#[inline(always)]
/// Returns the length of the longest common prefix between `a` and `b`, along with the ordering of the first element
/// that differs between `a` and `b`.
pub(crate) fn longest_common_prefix(
    a: &[c_char],
    b: &[c_char],
) -> Option<(usize, std::cmp::Ordering)> {
    let min_len = std::cmp::min(a.len(), b.len());

    // Create byte slices for faster comparison
    let a_bytes = to_u8_slice(a);
    let b_bytes = to_u8_slice(b);

    // Process chunks of 8 bytes at a time
    let mut i = 0;
    while i + 8 <= min_len {
        let a_chunk = u64::from_ne_bytes(a_bytes[i..i + 8].try_into().unwrap());
        let b_chunk = u64::from_ne_bytes(b_bytes[i..i + 8].try_into().unwrap());

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

/// Re-interpret a slice of `c_char` as a slice of `u8`.
/// This is equivalent to casting each `c_char` element to a `u8`.
///
/// This function is useful when working with APIs that expect `u8` slices.
fn to_u8_slice(slice: &[c_char]) -> &[u8] {
    // SAFETY:
    // `c_char` is an alias for either a `u8` or an `i8`.
    // In both cases, the memory layout is identical to `u8` and all `c_char`
    // values are valid `u8` values.
    unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) }
}
