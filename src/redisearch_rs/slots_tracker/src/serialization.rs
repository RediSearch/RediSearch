/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The wire format of a [`SlotRangeArray`], which the coordinator sends to the
//! shards with each internal query.
//!
//! It is the in-memory layout with every field little-endian, whatever the host:
//! an `i32` range count followed by a `u16` start and end per range.

use crate::{SlotRange, SlotRangeArray};

const HEADER_SIZE: usize = size_of::<SlotRangeArray>();
const RANGE_SIZE: usize = size_of::<SlotRange>();

const _: () = assert!(HEADER_SIZE == size_of::<i32>());
const _: () = assert!(RANGE_SIZE == 2 * size_of::<u16>());

impl SlotRangeArray {
    /// The size in bytes of an array holding `num_ranges` ranges.
    pub const fn size_of(num_ranges: usize) -> usize {
        HEADER_SIZE + num_ranges * RANGE_SIZE
    }
}

/// Serializes `ranges` into `out`.
///
/// # Panics
///
/// Panics if `out` is not exactly [`SlotRangeArray::size_of`]`(ranges.len())`
/// bytes long, or if `ranges` has more than [`i32::MAX`] elements.
pub fn serialize_into(ranges: &[SlotRange], out: &mut [u8]) {
    assert_eq!(out.len(), SlotRangeArray::size_of(ranges.len()));
    let num_ranges = i32::try_from(ranges.len()).expect("too many slot ranges");

    let (header, body) = out.split_at_mut(HEADER_SIZE);
    header.copy_from_slice(&num_ranges.to_le_bytes());
    for (chunk, range) in body.chunks_exact_mut(RANGE_SIZE).zip(ranges) {
        chunk[..2].copy_from_slice(&range.start.to_le_bytes());
        chunk[2..].copy_from_slice(&range.end.to_le_bytes());
    }
}

/// Deserializes the ranges in `buf`.
///
/// Returns [`None`] if the range count in the header is negative, or if `buf` is not
/// exactly as long as that count says.
pub fn deserialize(buf: &[u8]) -> Option<impl ExactSizeIterator<Item = SlotRange>> {
    let (header, body) = buf.split_first_chunk::<HEADER_SIZE>()?;
    let num_ranges = usize::try_from(i32::from_le_bytes(*header)).ok()?;
    if num_ranges.checked_mul(RANGE_SIZE) != Some(body.len()) {
        return None;
    }

    Some(body.chunks_exact(RANGE_SIZE).map(|chunk| SlotRange {
        start: u16::from_le_bytes([chunk[0], chunk[1]]),
        end: u16::from_le_bytes([chunk[2], chunk[3]]),
    }))
}
