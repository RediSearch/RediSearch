/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use thin_vec::ThinVec;

/// The entries of one [`IndexBlock`](super::IndexBlock) that belong to a field carrying a
/// field-level expiration (HFE), keyed by the entry's 0-based ordinal within the block. For tag
/// and numeric indexes that is the single owning field; for term indexes, any field in the
/// posting's field mask.
///
/// Readers look an entry up by ordinal to set
/// [`RSIndexResult::has_field_expiration`](index_result::RSIndexResult::has_field_expiration),
/// letting expiration-aware iterators skip the TTL-table lookup for documents that have no field
/// TTL. Stored per block in [`InvertedIndex::expiration`](super::InvertedIndex::expiration) and
/// serialized with the GC's repaired blocks, so the bits survive the fork GC.
///
/// The set reaches only as far as the highest ordinal inserted; ordinals past its end are absent.
/// An empty set does not allocate.
#[derive(Debug, Default, Clone, Eq, PartialEq)]
pub(crate) struct ExpirationBits(ThinVec<u8, u16>);

impl ExpirationBits {
    /// How many ordinals one byte of the set holds, one bit each.
    const ORDINALS_PER_BYTE: u16 = u8::BITS as u16;

    /// The empty set. Does not allocate.
    pub(crate) const fn new() -> Self {
        Self(ThinVec::new())
    }

    /// Add `ordinal`, growing the bitset if it does not reach that far yet.
    pub(crate) fn insert(&mut self, ordinal: u16) {
        let byte = Self::byte(ordinal);
        if self.0.len() <= byte {
            self.0.resize(byte + 1, 0);
        }
        self.0.as_mut_slice()[byte] |= Self::mask(ordinal);
    }

    /// The ordinals below `entries`: a copy of the bytes that hold them, with the bits past them
    /// masked off. Trailing bytes with no bit left are not copied, so a prefix that holds no
    /// ordinal does not allocate.
    pub(crate) fn prefix(&self, entries: u16) -> Self {
        let len = usize::from(entries.div_ceil(Self::ORDINALS_PER_BYTE));
        let bytes = &self.0.as_slice()[..len.min(self.0.len())];
        // Only the byte holding ordinal `entries` has bits past the cut, if the set reaches it.
        let cut =
            !entries.is_multiple_of(Self::ORDINALS_PER_BYTE) && bytes.len() > Self::byte(entries);
        let Some((&last, head)) = bytes.split_last() else {
            return Self::new();
        };
        let last = if cut {
            last & (Self::mask(entries) - 1)
        } else {
            last
        };
        let prefix = if last != 0 {
            let mut prefix = ThinVec::from(bytes);
            prefix.as_mut_slice()[bytes.len() - 1] = last;
            prefix
        } else {
            let len = head
                .iter()
                .rposition(|&byte| byte != 0)
                .map_or(0, |i| i + 1);
            ThinVec::from(&head[..len])
        };
        Self(prefix)
    }

    /// Whether the set holds no ordinal.
    pub(crate) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Whether `ordinal` is in the set.
    #[inline(always)]
    pub(crate) fn contains(&self, ordinal: u16) -> bool {
        self.0
            .as_slice()
            .get(Self::byte(ordinal))
            .is_some_and(|byte| byte & Self::mask(ordinal) != 0)
    }

    /// Heap bytes occupied, including the allocation header and reserved capacity. Zero while
    /// empty.
    pub(crate) fn mem_usage(&self) -> usize {
        self.0.mem_usage()
    }

    /// Index of the byte holding `ordinal`'s bit.
    const fn byte(ordinal: u16) -> usize {
        (ordinal / Self::ORDINALS_PER_BYTE) as usize
    }

    /// Mask selecting `ordinal`'s bit within its byte.
    const fn mask(ordinal: u16) -> u8 {
        1 << (ordinal % Self::ORDINALS_PER_BYTE)
    }
}

// `thin_vec` has no `serde` impls. The wire form is a plain byte sequence, matching `[u8]`.
impl Serialize for ExpirationBits {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        self.0.as_slice().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for ExpirationBits {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let bytes = Vec::<u8>::deserialize(deserializer)?;
        let mut bits = ThinVec::with_capacity(bytes.len());
        bits.extend_from_slice(&bytes);
        Ok(Self(bits))
    }
}
