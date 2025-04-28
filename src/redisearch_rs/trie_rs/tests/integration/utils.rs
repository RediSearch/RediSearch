use std::ffi::c_char;

pub trait ToCCharVec {
    /// Convenience method to convert a byte array to a C-compatible character array.
    fn c_chars(self) -> Vec<c_char>;
}

impl<const N: usize> ToCCharVec for [u8; N] {
    fn c_chars(self) -> Vec<c_char> {
        self.map(|b| b as c_char).to_vec()
    }
}

impl ToCCharVec for &str {
    fn c_chars(self) -> Vec<c_char> {
        self.as_bytes().iter().map(|b| *b as c_char).collect()
    }
}

#[macro_export]
macro_rules! c_chars_vec {
    ($($x:expr),*) => {
        vec![$($x.c_chars()),*]
    };
}
