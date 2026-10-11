/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coordinator's side: decoding shard blocks straight into lookup rows.

use crate::{
    ColumnKind,
    reader::{Block, DecodeError, RowReader},
};
use rlookup::{RLookup, RLookupKey, RLookupKeyFlags, RLookupRow};
use std::{borrow::Cow, ffi::CStr, ptr::NonNull};

/// Decodes one block at a time into lookup rows, for a caller that yields a row per call back into C.
///
/// [`RowBlockDecoder::begin`] resolves each column to a lookup key once; [`RowBlockDecoder::next_row`] writes values
/// by key. Decoded strings are copied out of the block, which the caller keeps until the block ends.
#[cheadergen::config(export, opaque)]
#[derive(Debug, Default)]
pub struct RowBlockDecoder {
    /// Per schema column, the key in [`RowBlockDecoder::begin`]'s lookup, which pins keys so the pointers survive it
    /// growing. `None` drops the column's values.
    keys: Vec<Option<NonNull<RLookupKey<'static>>>>,
    kinds: Vec<ColumnKind>,
    /// The active block's undecoded rows, borrowed under [`RowBlockDecoder::begin`]'s contract. Empty when no block is
    /// active.
    rows: RawBytes,
    active: bool,
}

/// A byte range whose lifetime is guaranteed by contract rather than by the borrow checker.
#[derive(Debug, Clone, Copy)]
struct RawBytes {
    ptr: NonNull<u8>,
    len: usize,
}

impl Default for RawBytes {
    fn default() -> Self {
        Self {
            ptr: NonNull::dangling(),
            len: 0,
        }
    }
}

impl From<&[u8]> for RawBytes {
    fn from(bytes: &[u8]) -> Self {
        Self {
            ptr: NonNull::from(bytes).cast(),
            len: bytes.len(),
        }
    }
}

impl RowBlockDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Makes `block` the active block, ending any previous one, and resolves its columns in `lookup`, creating keys for
    /// names it does not know (`LOAD *` sends columns the plan never named). On error no block is active.
    ///
    /// # Safety
    ///
    /// 1. `block` must stay [valid] for reads, and unmodified, until the block ends: by [`RowBlockDecoder::end`], the
    ///    next call to this method, or dropping the decoder.
    /// 2. `lookup` must outlive, and not be cleaned up before, every [`RowBlockDecoder::next_row`] for this block,
    ///    and no exclusive reference to one of its keys may be live during a call.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn begin(
        &mut self,
        lookup: &mut RLookup<'_>,
        block: &[u8],
    ) -> Result<(), DecodeError> {
        self.end();

        let parsed = Block::parse(block)?;
        self.keys.clear();
        self.keys.extend(
            parsed
                .columns()
                .iter()
                .map(|name| resolve_key(lookup, name)),
        );
        self.kinds.clear();
        self.kinds.extend_from_slice(parsed.kinds());
        self.rows = RawBytes::from(parsed.row_bytes());
        self.active = true;
        Ok(())
    }

    /// True from a successful [`RowBlockDecoder::begin`] until [`RowBlockDecoder::end`] or a row fails to decode.
    pub const fn is_active(&self) -> bool {
        self.active
    }

    pub const fn has_rows(&self) -> bool {
        self.active && self.rows.len > 0
    }

    pub const fn ncols(&self) -> usize {
        self.keys.len()
    }

    /// Decodes the next row into `row`. On error the block ends, and `row` may hold the columns before the malformed
    /// one.
    ///
    /// # Panics
    ///
    /// Panics unless [`RowBlockDecoder::has_rows`].
    ///
    /// # Safety
    ///
    /// 1. The contract of the [`RowBlockDecoder::begin`] call that started the active block must still hold.
    /// 2. `row` must belong to the lookup that call was given.
    pub unsafe fn next_row(&mut self, row: &mut RLookupRow<'_>) -> Result<(), DecodeError> {
        assert!(self.has_rows(), "no row is left in the active block");
        // SAFETY: `rows` is a suffix of the block `begin` was given, still valid and unmodified per (1.).
        let rows = unsafe { std::slice::from_raw_parts(self.rows.ptr.as_ptr(), self.rows.len) };
        let mut reader = RowReader::new(rows, &self.kinds);

        let keys = &self.keys;
        let outcome = reader.read_row(|col, value| {
            if let Some(key) = keys[usize::from(col)] {
                // SAFETY: the key's lookup is alive per (1.), and `RLookup` never moves or frees a live key.
                row.write_key(unsafe { key.as_ref() }, value);
            }
        });

        match outcome {
            Ok(()) => {
                self.rows = RawBytes::from(reader.remaining());
                Ok(())
            }
            Err(error) => {
                self.end();
                Err(error)
            }
        }
    }

    /// Releases the decoder's hold on the active block's bytes.
    pub fn end(&mut self) {
        self.rows = RawBytes::default();
        self.active = false;
    }
}

/// An existing key, else one derived from the index schema, else a new one.
fn resolve_key(lookup: &mut RLookup<'_>, name: &CStr) -> Option<NonNull<RLookupKey<'static>>> {
    // A created key outlives the block the name points into, so it must own its name.
    let owned = || Cow::Owned(name.to_owned());
    lookup
        .get_key_read_ptr(owned(), RLookupKeyFlags::empty())
        .or_else(|| lookup.get_key_write_ptr(owned(), RLookupKeyFlags::empty()))
        .map(NonNull::cast)
}
