/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Utilities used only in tests and benchmarks.

use ffi::t_fieldMask;
use query_term::RSQueryTerm;

use crate::{RSIndexResult, RSOffsetSlice};

/// Wrapper around `inverted_index::RSIndexResult` ensuring the offsets
/// pointer used internally stays valid for the duration of the test or bench.
#[derive(Debug)]
pub struct TestTermRecord<'index> {
    pub record: RSIndexResult<'index>,
}

impl<'a> TestTermRecord<'a> {
    /// Create a new `TestTermRecord` with the given parameters.
    pub fn new(doc_id: u64, field_mask: t_fieldMask, freq: u32, offsets: &'a [u8]) -> Self {
        let mut term = RSQueryTerm::new("test", 1, 0);
        term.set_idf(5.0);
        term.set_bm25_idf(10.0);

        let record = RSIndexResult::build_term()
            .borrowed_record(Some(term), RSOffsetSlice::from_slice(offsets))
            .doc_id(doc_id)
            .field_mask(field_mask)
            .frequency(freq)
            .weight(1.0)
            .build();

        Self { record }
    }
}

/// Helper to compare only the fields of a term record that are actually encoded.
/// Only used in tests.
#[derive(Debug)]
pub struct TermRecordCompare<'index>(pub &'index RSIndexResult<'index>);

impl<'a> PartialEq for TermRecordCompare<'a> {
    fn eq(&self, other: &Self) -> bool {
        assert!(self.0.is_term());

        if !(self.0.doc_id == other.0.doc_id
            && self.0.dmd == other.0.dmd
            && self.0.field_mask == other.0.field_mask
            && self.0.freq == other.0.freq
            && self.0.kind() == other.0.kind()
            && self.0.metrics == other.0.metrics)
        {
            return false;
        }

        // do not compare `weight` as it's not encoded

        // SAFETY: we asserted the type above
        let a_term_record = self.0.as_term().unwrap();
        // SAFETY: we checked that other has the same type as self
        let b_term_record = other.0.as_term().unwrap();

        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let a_offsets = a_term_record.offsets();

        // SAFETY: `len` is guaranteed to be a valid length for the data pointer.
        let b_offsets = b_term_record.offsets();

        if a_offsets != b_offsets {
            return false;
        }

        // do not compare `RSTermRecord` as it's not encoded
        true
    }
}

/// Move a block's encoded-entry buffer to a different address, leaving its bytes and the index's
/// `gc_marker` alone — what a write that outgrows the buffer's allocation does to a reader parked
/// on that block.
///
/// Appending cannot stand in for this: whether `reserve_exact` growth moves the buffer or extends
/// it in place is the allocator's choice, so a test that appends until the address changes may
/// never see it change. Here the replacement is built while the original is still alive, so the
/// two cannot share an address whatever the allocator does. The original is then freed, which is
/// what makes a reader's cached pointer genuinely dangle.
///
/// The guarantee is only against the address being replaced here, not against any address a
/// caller sampled earlier: an address freed before this call can be handed back by the allocator.
///
/// Returns the buffer's new base address.
///
/// # Panics
///
/// If `block_idx` is out of range.
pub fn relocate_block_buffer<E: crate::Encoder + crate::DecodedBy>(
    index: &mut crate::InvertedIndex<E>,
    block_idx: usize,
) -> *const u8 {
    let block = &mut index.blocks[block_idx];

    // Hold the original allocation while the replacement is allocated, so they cannot overlap.
    let original = std::mem::take(&mut block.buffer);
    let original_base = original.as_ptr();
    let mut relocated = Vec::with_capacity(original.len().max(1));
    relocated.extend_from_slice(&original);

    let base = relocated.as_ptr();
    block.buffer = relocated;
    drop(original);

    // Guaranteed by construction: `original` was still alive when `relocated` was allocated.
    assert_ne!(
        base, original_base,
        "must not reuse the address it just replaced"
    );
    base
}

/// Give a block's buffer enough spare capacity for `extra` more bytes, so that appends of up to
/// that size grow it in place instead of reallocating.
///
/// The counterpart to [`relocate_block_buffer`]: it makes the *other* outcome of an append — the
/// buffer keeping its address while its length grows — reachable on demand rather than at the
/// allocator's discretion.
///
/// # Panics
///
/// If `block_idx` is out of range.
pub fn reserve_block_buffer<E: crate::Encoder + crate::DecodedBy>(
    index: &mut crate::InvertedIndex<E>,
    block_idx: usize,
    extra: usize,
) -> *const u8 {
    let buffer = &mut index.blocks[block_idx].buffer;
    buffer.reserve(extra);
    buffer.as_ptr()
}
