/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Debug formatting for [`RsValue`] with optional obfuscation.
//!
//! Provides [`DebugFormatter`], a wrapper that implements [`Debug`] for [`RsValue`],
//! with support for obfuscating sensitive data via C-side obfuscation functions.

use crate::RsValue;
use ffi::{Obfuscate_Number, Obfuscate_Text};
use std::{
    ffi::CStr,
    fmt::{self, Debug},
};

/// A wrapper around an [`RsValue`] reference that implements [`Debug`] with
/// optional obfuscation of string and numeric values.
///
/// When `obfuscate` is `true`, string and numeric values are replaced with
/// obfuscated representations using the C-side `Obfuscate_Text` and
/// `Obfuscate_Number` functions. Composite types (arrays, maps) recursively
/// obfuscate their elements.
pub struct DebugFormatter<'a> {
    pub(crate) value: &'a RsValue,
    pub(crate) obfuscate: bool,
}

impl<'a> Debug for DebugFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_text(f: &mut fmt::Formatter<'_>, text: &[u8], obfuscate: bool) -> fmt::Result {
            if obfuscate {
                write!(f, "\"{}\"", obfuscate_text(text))
            } else if let Ok(s) = std::str::from_utf8(text) {
                write!(f, "\"{s}\"")
            } else {
                f.write_str("<non-utf8-data>")
            }
        }

        match self.value {
            RsValue::Undefined => f.write_str("<Undefined>"),
            RsValue::Null => f.write_str("NULL"),
            RsValue::Number(num) => {
                if self.obfuscate {
                    f.write_str(obfuscate_number(*num))
                } else {
                    let mut buf = [0; 32];
                    let n = crate::util::num_to_str(*num, &mut buf);
                    let s = std::str::from_utf8(&buf[0..n]).unwrap();
                    f.write_str(s)
                }
            }
            RsValue::String(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            RsValue::RedisString(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            RsValue::Array(array) => {
                let entries = array
                    .iter()
                    .map(|item| item.value().debug_formatter(self.obfuscate));
                f.debug_list().entries(entries).finish()
            }
            RsValue::Map(map) => {
                let entries = map.iter().map(|(key, value)| {
                    (
                        key.value().debug_formatter(self.obfuscate),
                        value.value().debug_formatter(self.obfuscate),
                    )
                });
                f.debug_map().entries(entries).finish()
            }
            RsValue::Ref(ref_value) => ref_value.value().debug_formatter(self.obfuscate).fmt(f),
            RsValue::Trio(trio) => trio.left().value().debug_formatter(self.obfuscate).fmt(f),
        }
    }
}

/// Returns a static string representation of the obfuscated number.
fn obfuscate_number(number: f64) -> &'static str {
    // SAFETY: `Obfuscate_Number` is a C function that returns a pointer to a
    // static null-terminated string.
    let obfuscated = unsafe { Obfuscate_Number(number) };
    // SAFETY: The returned pointer is a valid, null-terminated, static C string.
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}

/// Returns a static string representation of the obfuscated text.
fn obfuscate_text(text: &[u8]) -> &'static str {
    // SAFETY: `Obfuscate_Text` expects a `*const c_char` pointer. `text` is a
    // valid byte slice, and the function returns a pointer to a static
    // null-terminated string.
    let obfuscated = unsafe { Obfuscate_Text(text.as_ptr().cast()) };
    // SAFETY: The returned pointer is a valid, null-terminated, static C string.
    unsafe { CStr::from_ptr(obfuscated) }.to_str().unwrap()
}
