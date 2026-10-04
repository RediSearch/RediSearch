/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Decoding of the byte offsets embedded in pre-2.0 doc-table RDB payloads.
//!
//! Only the load direction exists: since 2.0, byte offsets are rebuilt from the keyspace
//! rather than persisted.

use crate::{ByteOffsets, FieldTokens};

/// A legacy byte-offsets blob ended before the data it describes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("truncated legacy byte offsets")]
pub struct TruncatedError;

impl ByteOffsets {
    /// Decodes byte offsets stored by a pre-2.0 doc table.
    ///
    /// The format is the one the 1.x `RSByteOffsets_Serialize` wrote: a `u8` field
    /// count; per field a `u8` id and the big-endian `u32` positions of its first and
    /// last tokens; a big-endian `u32` length; and that many bytes of offsets, encoded
    /// as [`ByteOffsets::take_offsets`] stores them. Any bytes after those are ignored.
    /// A field whose last position precedes its first one owns no offsets.
    ///
    /// # Errors
    ///
    /// Returns [`TruncatedError`] if `bytes` ends before the data it describes.
    pub fn load_legacy(mut bytes: &[u8]) -> Result<Self, TruncatedError> {
        let [num_fields] = take(&mut bytes)?;
        let fields = (0..num_fields)
            .map(|_| {
                let [field_id] = take(&mut bytes)?;
                let first = u32::from_be_bytes(take(&mut bytes)?);
                let last = u32::from_be_bytes(take(&mut bytes)?);
                let offsets = (u64::from(last) + 1).saturating_sub(u64::from(first));
                Ok(FieldTokens {
                    field_id: field_id.into(),
                    first,
                    offsets: u32::try_from(offsets).unwrap_or(u32::MAX),
                })
            })
            .collect::<Result<_, _>>()?;
        let len = u32::from_be_bytes(take(&mut bytes)?);
        let encoded = usize::try_from(len)
            .ok()
            .and_then(|len| bytes.get(..len))
            .ok_or(TruncatedError)?;
        Ok(Self {
            fields,
            encoded: encoded.into(),
        })
    }
}

/// Splits the first `N` bytes off `bytes`.
fn take<const N: usize>(bytes: &mut &[u8]) -> Result<[u8; N], TruncatedError> {
    let (head, rest) = bytes.split_first_chunk().ok_or(TruncatedError)?;
    *bytes = rest;
    Ok(*head)
}
