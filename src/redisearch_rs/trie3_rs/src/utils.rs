use std::ffi::c_char;

pub(crate) trait ToCCharArray<const N: usize> {
    /// Convenience method to convert a byte array to a C-compatible character array.
    fn c_chars(self) -> [c_char; N];
}

impl<const N: usize> ToCCharArray<N> for [u8; N] {
    #![allow(dead_code)]
    fn c_chars(self) -> [c_char; N] {
        self.map(|b| b as c_char)
    }
}

/// Convenience method to convert a `c_char` array into a `String`,
/// dropping non-UTF-8 characters along the way.
pub(crate) fn to_string_lossy(label: &[c_char]) -> String {
    let slice = label.iter().map(|&c| c as u8).collect::<Vec<_>>();
    String::from_utf8_lossy(&slice).into_owned()
}

/// Returns the index of the first occurrence of `target` in `slice`, or `None` if not found.
///
/// # Implementation notes
///
/// `c_char` is either a signed or unsigned 8-bit integer. In both cases, it can be safely reinterpreted
/// as a `u8`, thus allowing us to leverage [`memchr::memchr`] directly.
pub(crate) fn memchr_c_char(target: c_char, slice: &[c_char]) -> Option<usize> {
    let target_u8 = target as u8;
    let slice_u8 = unsafe { std::slice::from_raw_parts(slice.as_ptr() as *const u8, slice.len()) };
    memchr::memchr(target_u8, slice_u8)
}

/// A version of `std`'s `strip_prefix` that's built on top of [`memchr::arch::is_prefix`].
pub(crate) fn strip_prefix<'a>(haystack: &'a [c_char], prefix: &[c_char]) -> Option<&'a [c_char]> {
    let haystack_u8 =
        unsafe { std::slice::from_raw_parts(haystack.as_ptr() as *const u8, haystack.len()) };
    let prefix_u8 =
        unsafe { std::slice::from_raw_parts(prefix.as_ptr() as *const u8, prefix.len()) };
    if !memchr::arch::all::is_prefix(haystack_u8, prefix_u8) {
        None
    } else {
        Some(&haystack[prefix.len()..])
    }
}
