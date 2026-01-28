/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Value types for captured Redis replies.

use std::fmt;

/// Represents a captured Redis reply value.
#[derive(Clone, PartialEq)]
pub enum ReplyValue {
    /// A 64-bit signed integer reply.
    LongLong(i64),
    /// A double-precision floating point reply.
    Double(f64),
    /// A simple string reply.
    SimpleString(String),
    /// An array reply containing zero or more values.
    Array(Vec<ReplyValue>),
    /// A map reply containing key-value pairs.
    Map(Vec<(ReplyValue, ReplyValue)>),
}

/// Maximum length for compact (single-line) collection formatting.
pub(super) const COMPACT_COLLECTION_MAX_LEN: usize = 70;

impl ReplyValue {
    /// Returns a compact single-line representation of the value.
    pub(super) fn format_compact(&self) -> String {
        match self {
            ReplyValue::LongLong(n) => format!("{n}"),
            ReplyValue::Double(d) => format!("{d}"),
            ReplyValue::SimpleString(s) => format!("{s:?}"),
            ReplyValue::Array(elements) => {
                let inner: Vec<String> = elements.iter().map(|e| e.format_compact()).collect();
                format!("[{}]", inner.join(", "))
            }
            ReplyValue::Map(pairs) => {
                let inner: Vec<String> = pairs
                    .iter()
                    .map(|(k, v)| format!("{}: {}", k.format_compact(), v.format_compact()))
                    .collect();
                format!("{{{}}}", inner.join(", "))
            }
        }
    }
}

impl fmt::Debug for ReplyValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Use a helper struct for indented formatting of nested structures
        struct IndentedFormatter<'a, 'b> {
            f: &'a mut fmt::Formatter<'b>,
            indent: usize,
        }

        impl IndentedFormatter<'_, '_> {
            fn write_indent(&mut self) -> fmt::Result {
                for _ in 0..self.indent {
                    write!(self.f, "  ")?;
                }
                Ok(())
            }

            fn format_value(&mut self, value: &ReplyValue) -> fmt::Result {
                match value {
                    ReplyValue::LongLong(n) => write!(self.f, "{n}"),
                    ReplyValue::Double(d) => write!(self.f, "{d}"),
                    ReplyValue::SimpleString(s) => write!(self.f, "{s:?}"),
                    ReplyValue::Array(elements) => {
                        if elements.is_empty() {
                            write!(self.f, "[]")
                        } else {
                            // Try compact formatting first
                            let compact = value.format_compact();
                            if compact.len() <= COMPACT_COLLECTION_MAX_LEN {
                                write!(self.f, "{compact}")
                            } else {
                                writeln!(self.f, "[")?;
                                self.indent += 1;
                                for (i, elem) in elements.iter().enumerate() {
                                    self.write_indent()?;
                                    self.format_value(elem)?;
                                    if i < elements.len() - 1 {
                                        write!(self.f, ",")?;
                                    }
                                    writeln!(self.f)?;
                                }
                                self.indent -= 1;
                                self.write_indent()?;
                                write!(self.f, "]")
                            }
                        }
                    }
                    ReplyValue::Map(pairs) => {
                        if pairs.is_empty() {
                            write!(self.f, "{{}}")
                        } else {
                            // Try compact formatting first
                            let compact = value.format_compact();
                            if compact.len() <= COMPACT_COLLECTION_MAX_LEN {
                                write!(self.f, "{compact}")
                            } else {
                                writeln!(self.f, "{{")?;
                                self.indent += 1;
                                for (i, (key, val)) in pairs.iter().enumerate() {
                                    self.write_indent()?;
                                    self.format_value(key)?;
                                    write!(self.f, ": ")?;
                                    self.format_value(val)?;
                                    if i < pairs.len() - 1 {
                                        write!(self.f, ",")?;
                                    }
                                    writeln!(self.f)?;
                                }
                                self.indent -= 1;
                                self.write_indent()?;
                                write!(self.f, "}}")
                            }
                        }
                    }
                }
            }
        }

        let mut formatter = IndentedFormatter { f, indent: 0 };
        formatter.format_value(self)
    }
}
