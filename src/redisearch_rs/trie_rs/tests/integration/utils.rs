/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

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
