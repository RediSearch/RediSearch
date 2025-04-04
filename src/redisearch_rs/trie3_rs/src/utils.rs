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
