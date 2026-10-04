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
use std::{
    borrow::Cow,
    ffi::CStr,
    ptr::NonNull,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};
use value::{SharedBuffer, shared_buffer::Dealloc};

/// How many bytes of earlier blocks a decoder's strings may keep alive before new blocks are decoded with copied
/// strings instead.
///
/// One string that outlives its row (a `GROUPBY` key, a `TOLIST` element, a `SORTBY` heap row, a cursor) keeps its
/// whole block allocated, so without a bound a query keeping one string per block would pin every block. Sharing
/// resumes once pinned blocks are released.
pub const MAX_PINNED_BYTES: usize = 8 << 20;

/// Decodes one block at a time into lookup rows, for a caller that yields a row per call back into C.
///
/// [`RowBlockDecoder::begin`] takes over the block's buffer and resolves each column to a lookup key once;
/// [`RowBlockDecoder::next_row`] writes values by key. Decoded strings borrow from the block, within
/// [`MAX_PINNED_BYTES`].
#[cheadergen::config(export, opaque)]
#[derive(Debug, Default)]
pub struct RowBlockDecoder {
    /// Per schema column, the key in [`RowBlockDecoder::begin`]'s lookup, which pins keys so the pointers survive it
    /// growing. `None` drops the column's values.
    keys: Vec<Option<NonNull<RLookupKey<'static>>>>,
    kinds: Vec<ColumnKind>,
    /// `None` when no block is active, or the active one holds no rows.
    buffer: Option<SharedBuffer>,
    at: usize,
    share: bool,
    active: bool,
    /// Bytes of this decoder's blocks still allocated, active or pinned by strings.
    live_bytes: Arc<AtomicUsize>,
}

impl RowBlockDecoder {
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes over the `len` bytes at `block` as the active block, ending any previous one, and resolves its columns in
    /// `lookup`, creating keys for names it does not know (`LOAD *` sends columns the plan never named). On error no
    /// block is active and the buffer is already released.
    ///
    /// # Safety
    ///
    /// 1. `block` must be [valid] for reads and writes of `len` bytes and owned by the caller, who gives it up: it is
    ///    released, from any thread, with `dealloc(block, len)` once the decoder and its strings are done with it.
    /// 2. `lookup` must outlive, and not be cleaned up before, every [`RowBlockDecoder::next_row`] for this block,
    ///    and no exclusive reference to one of its keys may be live during a call.
    ///
    /// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
    pub unsafe fn begin(
        &mut self,
        lookup: &mut RLookup<'_>,
        block: NonNull<u8>,
        len: usize,
        dealloc: Dealloc,
    ) -> Result<(), DecodeError> {
        self.end();

        // SAFETY: valid and ours per (1.); the borrow ends before `SharedBuffer` writes to the buffer.
        let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), len) };
        let rows_at = match self.parse(lookup, bytes) {
            Ok(rows_at) => rows_at,
            Err(error) => {
                // SAFETY: ours to release per (1.), and unreferenced.
                unsafe { dealloc(block, len) };
                return Err(error);
            }
        };

        // Read before this block is counted: the budget covers earlier blocks only.
        self.share = self.live_bytes.load(Ordering::Relaxed) <= MAX_PINNED_BYTES;
        // SAFETY: ensured by caller (1.); the header the shared buffer overwrites has been read.
        self.buffer =
            unsafe { SharedBuffer::from_raw(block, len, dealloc, Some(self.live_bytes.clone())) };
        if self.buffer.is_none() {
            // Only a column-less schema is too short to share.
            debug_assert_eq!(rows_at, len, "a block holding rows is always shareable");
            // SAFETY: as in the error case above.
            unsafe { dealloc(block, len) };
        }
        self.at = rows_at;
        self.active = true;
        Ok(())
    }

    /// Returns the offset of the first row.
    fn parse(&mut self, lookup: &mut RLookup<'_>, bytes: &[u8]) -> Result<usize, DecodeError> {
        let parsed = Block::parse(bytes)?;
        self.keys.clear();
        self.keys.extend(
            parsed
                .columns()
                .iter()
                .map(|name| resolve_key(lookup, name)),
        );
        self.kinds.clear();
        self.kinds.extend_from_slice(parsed.kinds());
        Ok(bytes.len() - parsed.row_bytes().len())
    }

    /// True from a successful [`RowBlockDecoder::begin`] until [`RowBlockDecoder::end`] or a row fails to decode.
    pub const fn is_active(&self) -> bool {
        self.active
    }

    pub fn has_rows(&self) -> bool {
        self.active
            && self
                .buffer
                .as_ref()
                .is_some_and(|buffer| self.at < buffer.as_bytes().len())
    }

    pub const fn ncols(&self) -> usize {
        self.keys.len()
    }

    pub const fn shares_strings(&self) -> bool {
        self.share
    }

    pub fn live_bytes(&self) -> usize {
        self.live_bytes.load(Ordering::Relaxed)
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
        let buffer = self
            .buffer
            .as_ref()
            .expect("a block with rows has a buffer");
        let rows = &buffer.as_bytes()[self.at..];
        let mut reader = if self.share {
            RowReader::sharing(rows, &self.kinds, buffer)
        } else {
            RowReader::new(rows, &self.kinds)
        };

        let keys = &self.keys;
        let outcome = reader.read_row(|col, value| {
            if let Some(key) = keys[usize::from(col)] {
                // SAFETY: the key's lookup is alive per (1.), and `RLookup` never moves or frees a live key.
                row.write_key(unsafe { key.as_ref() }, value);
            }
        });

        match outcome {
            Ok(()) => {
                self.at = buffer.as_bytes().len() - reader.remaining().len();
                Ok(())
            }
            Err(error) => {
                self.end();
                Err(error)
            }
        }
    }

    /// Releases the decoder's hold on the active block, which is freed once no decoded string borrows from it.
    pub fn end(&mut self) {
        self.buffer = None;
        self.at = 0;
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
