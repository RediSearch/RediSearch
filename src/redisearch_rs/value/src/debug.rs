/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Debug formatting for [`Value`] with optional obfuscation.
//!
//! Provides [`DebugFormatter`], a wrapper that implements [`Debug`] for [`Value`],
//! with support for obfuscating sensitive data via C-side obfuscation functions.

use crate::Value;
use std::{
    fmt::{self, Debug},
    str,
};
use string_utils::obfuscation::{obfuscate_number, obfuscate_text};

/// A wrapper around a [`Value`] reference that implements [`Debug`] with
/// optional obfuscation of string and numeric values.
///
/// When `obfuscate` is `true`, string and numeric values are replaced with
/// obfuscated representations using [`obfuscate_text`] and [`obfuscate_number`].
/// Composite types (arrays, maps) recursively obfuscate their elements.
pub struct DebugFormatter<'a> {
    pub(crate) value: &'a Value,
    pub(crate) obfuscate: bool,
}

impl<'a> Debug for DebugFormatter<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn fmt_text(f: &mut fmt::Formatter<'_>, text: &[u8], obfuscate: bool) -> fmt::Result {
            if obfuscate {
                write!(f, "\"{}\"", obfuscate_text(text))
            } else if let Ok(s) = str::from_utf8(text) {
                write!(f, "\"{s}\"")
            } else {
                f.write_str("<non-utf8-data>")
            }
        }

        match self.value {
            Value::Undefined => f.write_str("<Undefined>"),
            Value::Null => f.write_str("NULL"),
            Value::Number(num) => {
                if self.obfuscate {
                    f.write_str(obfuscate_number(*num))
                } else {
                    let mut buf = [0; 32];
                    let n = crate::util::num_to_str(*num, &mut buf);
                    let s = str::from_utf8(&buf[0..n]).unwrap();
                    f.write_str(s)
                }
            }
            Value::String(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            Value::RedisString(str) => fmt_text(f, str.as_bytes(), self.obfuscate),
            Value::Array(array) => {
                let entries = array
                    .iter()
                    .map(|item| item.debug_formatter(self.obfuscate));
                f.debug_list().entries(entries).finish()
            }
            Value::Map(map) => {
                let entries = map.iter().map(|(key, value)| {
                    (
                        key.debug_formatter(self.obfuscate),
                        value.debug_formatter(self.obfuscate),
                    )
                });
                f.debug_map().entries(entries).finish()
            }
            Value::Ref(ref_value) => ref_value.debug_formatter(self.obfuscate).fmt(f),
            Value::Trio(trio) => trio.left().debug_formatter(self.obfuscate).fmt(f),
        }
    }
}
