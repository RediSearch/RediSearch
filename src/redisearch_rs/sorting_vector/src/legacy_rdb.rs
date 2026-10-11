/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding of the sorting vectors embedded in pre-2.0 doc-table RDB payloads.
//!
//! Only the load direction exists: since 2.0, sorting vectors are rebuilt from the
//! keyspace rather than persisted.

use std::io;

use rdb_io::RdbIO;
use value::SharedValue;

use crate::{RS_SORTABLES_MAX, RSSortingVector};

/// The tag of a number element; see [`RSSortingVector::load_legacy_rdb`]. The tags
/// are the numbering of the in-memory value type enum, as the C loader this replaces
/// matched them.
const TAG_NUMBER: u64 = 1;
/// The tag of a string element; see [`RSSortingVector::load_legacy_rdb`].
const TAG_STRING: u64 = 2;

impl RSSortingVector {
    /// Decodes a sorting vector stored by a pre-2.0 doc table.
    ///
    /// The format is an unsigned length, then per element an unsigned tag and its
    /// payload. [`TAG_NUMBER`] is followed by a double. [`TAG_STRING`] is followed by
    /// a buffer holding the string's bytes and a NUL terminator; an empty buffer
    /// decodes as null. Any other tag has no payload and decodes as null.
    ///
    /// A length outside `1..=`[`RS_SORTABLES_MAX`] yields an empty vector, and no
    /// element is read.
    ///
    /// # Errors
    ///
    /// Returns the error of the first failed read; the elements decoded so far are
    /// dropped.
    pub fn load_legacy_rdb(io: &mut impl RdbIO) -> io::Result<Self> {
        let len = io.read_u64()?;
        let Some(len) = usize::try_from(len)
            .ok()
            .filter(|len| (1..=RS_SORTABLES_MAX).contains(len))
        else {
            return Ok(Self::empty());
        };

        let mut vector = Self::new(len);
        for value in vector.iter_mut() {
            match io.read_u64()? {
                TAG_NUMBER => *value = SharedValue::new_num(io.read_f64()?),
                TAG_STRING => {
                    let buffer = io.read_buffer()?;
                    if !buffer.is_empty() {
                        *value = SharedValue::new_string(legacy_string(&buffer).to_vec());
                    }
                }
                _ => {}
            }
        }
        Ok(vector)
    }
}

/// Returns the string content of a legacy string element.
///
/// 1.x saved each string with its NUL terminator, and the C loader read the rest
/// back as a C string, so an interior NUL ends the string.
fn legacy_string(buffer: &[u8]) -> &[u8] {
    let content = &buffer[..buffer.len().saturating_sub(1)];
    match content.iter().position(|&byte| byte == 0) {
        Some(nul) => &content[..nul],
        None => content,
    }
}
