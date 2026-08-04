/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Serialize};

/// The entries of an [`IndexBlock`](super::IndexBlock) that belong to a field carrying a
/// field-level expiration (HFE) for the entry's document, identified by the entry's 0-based
/// ordinal within the block. For tag and numeric indexes that field is the single owning field;
/// for term indexes it is any field in the posting's field mask.
///
/// Readers look an entry up by ordinal to set
/// [`RSIndexResult::has_field_expiration`](index_result::RSIndexResult::has_field_expiration),
/// letting expiration-aware iterators skip the TTL-table lookup for documents that have no field
/// TTL. This set lives outside the block's encoded buffer, leaving the document-id codec
/// untouched, and is serialized along with the block so the bits survive the fork GC.
///
/// The backing bitset reaches only as far as the highest ordinal inserted so far; an ordinal past
/// its end is simply absent, which is what non-expiring entries need.
#[derive(Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(transparent)]
pub(crate) struct ExpirationBits(Box<[u8]>);

impl ExpirationBits {
    /// The set holding `ordinal` alone.
    pub(crate) fn new(ordinal: u16) -> Self {
        let mut bits = Self(vec![0; Self::bytes_for(ordinal)].into_boxed_slice());
        bits.insert(ordinal);
        bits
    }

    /// Add `ordinal` to the set, growing the bitset if it does not reach that far yet.
    pub(crate) fn insert(&mut self, ordinal: u16) {
        let needed = Self::bytes_for(ordinal);
        if self.0.len() < needed {
            let mut grown = vec![0; needed];
            grown[..self.0.len()].copy_from_slice(&self.0);
            self.0 = grown.into_boxed_slice();
        }
        self.0[Self::byte(ordinal)] |= Self::mask(ordinal);
    }

    /// Whether `ordinal` is in the set.
    pub(crate) fn contains(&self, ordinal: u16) -> bool {
        let byte = Self::byte(ordinal);
        byte < self.0.len() && self.0[byte] & Self::mask(ordinal) != 0
    }

    /// The number of heap bytes this set occupies.
    pub(crate) fn mem_usage(&self) -> usize {
        self.0.len()
    }

    /// Index of the byte holding `ordinal`'s bit.
    const fn byte(ordinal: u16) -> usize {
        ordinal as usize / 8
    }

    /// Number of bytes a bitset needs in order to reach `ordinal`.
    const fn bytes_for(ordinal: u16) -> usize {
        Self::byte(ordinal) + 1
    }

    /// Mask selecting `ordinal`'s bit within its byte.
    const fn mask(ordinal: u16) -> u8 {
        1 << (ordinal % 8)
    }
}
