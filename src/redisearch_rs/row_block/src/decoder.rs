/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The coordinator's side of the format: decoding the blocks shards send straight into the
//! coordinator's lookup rows.

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

/// How many bytes of earlier blocks a decoder's values may keep alive before it stops
/// sharing strings with new blocks.
///
/// A decoded string borrows its block instead of copying out of it, so one string that
/// outlives its row — a `GROUPBY` key, a `TOLIST` element, a row held by a `SORTBY` heap or
/// across cursor reads — keeps the whole block allocated. Without a bound, a query keeping
/// one short string per block would pin every block the shards sent. Past this budget a block
/// is decoded with copied strings and freed as soon as its rows are read; sharing resumes
/// once the pinned blocks are released.
///
/// A few dozen blocks of typical size: enough that a query whose rows stream through never
/// gets near it, small enough to be noise next to the replies the coordinator buffers anyway.
pub const MAX_PINNED_BYTES: usize = 8 << 20;

/// Decodes one shard reply's block at a time into lookup rows.
///
/// Built for a caller that cannot hold a borrow across calls — the coordinator's network
/// result processor, which yields one row per call back into C. [`RowBlockDecoder::begin`]
/// takes over the block's buffer, parses the header and schema and resolves every column to
/// a lookup key once, so that [`RowBlockDecoder::next_row`] writes each value by key instead
/// of by name.
///
/// Decoded strings borrow from the block rather than copying out of it, within the limit set
/// by [`MAX_PINNED_BYTES`].
///
/// One decoder serves every block a result processor receives: its tables keep their
/// capacity from block to block.
///
/// Opaque to C, which may only hold a pointer to one and pass it back to the `row_block_ffi`
/// entrypoints.
#[cheadergen::config(export, opaque)]
#[derive(Debug, Default)]
pub struct RowBlockDecoder {
    /// One entry per schema column, in schema order. `None` for a column the lookup refused
    /// to create a key for, whose values are dropped.
    ///
    /// The keys belong to the lookup passed to [`RowBlockDecoder::begin`]. `RLookup` pins each
    /// key individually, so the pointers stay valid as the lookup grows.
    keys: Vec<Option<NonNull<RLookupKey<'static>>>>,
    /// The active block's column kinds, in schema order.
    kinds: Vec<ColumnKind>,
    /// The active block, or `None` when no block is active or the active one is too short to
    /// hold a row. Strings decoded from it hold references of their own.
    buffer: Option<SharedBuffer>,
    /// Offset of the active block's first undecoded row.
    at: usize,
    /// Whether strings borrow from the active block; see [`MAX_PINNED_BYTES`].
    share: bool,
    /// Set by [`RowBlockDecoder::begin`]; cleared by [`RowBlockDecoder::end`] and by a row
    /// that fails to decode.
    active: bool,
    /// Bytes held by this decoder's blocks that are still allocated, whether because a block
    /// is active or because strings decoded from it are alive.
    live_bytes: Arc<AtomicUsize>,
}

impl RowBlockDecoder {
    /// Creates a decoder with no active block.
    pub fn new() -> Self {
        Self::default()
    }

    /// Takes over the `len` bytes at `block`, parses their header and schema, resolving each
    /// column name to a key of `lookup`, and makes them the active block. Any block active
    /// before is ended first.
    ///
    /// A column the lookup does not know yet gets a key created for it, exactly as the RESP
    /// per-row path does when it writes by name: a shard can send columns the coordinator's
    /// plan never named (`LOAD *` and other dynamic keys).
    ///
    /// On error no block is active, and the buffer has already been released.
    ///
    /// # Safety
    ///
    /// 1. `block` must be [valid] for reads and writes of `len` bytes, and owned by the caller,
    ///    who must not access it again: the decoder and the strings it decodes release it
    ///    with `dealloc(block, len)` once the last of them is gone, from any thread.
    /// 2. `lookup` must outlive every [`RowBlockDecoder::next_row`] call made for this block,
    ///    and must not be cleaned up in between, since the decoder holds pointers to its keys.
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

        // SAFETY: the bytes are valid and exclusively ours per (1.); this borrow ends before
        // the buffer is handed to `SharedBuffer`, which writes to it.
        let bytes = unsafe { std::slice::from_raw_parts(block.as_ptr(), len) };
        let rows_at = match self.parse(lookup, bytes) {
            Ok(rows_at) => rows_at,
            Err(error) => {
                // SAFETY: the buffer is ours to release per (1.), and nothing refers to it.
                unsafe { dealloc(block, len) };
                return Err(error);
            }
        };

        // Measured before this block is counted: the budget is about the earlier blocks
        // strings kept alive, not about the block about to be read.
        self.share = self.live_bytes.load(Ordering::Relaxed) <= MAX_PINNED_BYTES;
        // SAFETY: ensured by caller (1.); the header and schema, which the shared buffer
        // overwrites the start of, have been fully read.
        self.buffer =
            unsafe { SharedBuffer::from_raw(block, len, dealloc, Some(self.live_bytes.clone())) };
        if self.buffer.is_none() {
            // Too short to share is too short to hold a row: a schema alone is that small only
            // when it declares no columns.
            debug_assert_eq!(rows_at, len, "a block holding rows is always shareable");
            // SAFETY: as in the error case above.
            unsafe { dealloc(block, len) };
        }
        self.at = rows_at;
        self.active = true;
        Ok(())
    }

    /// Parses `bytes`' header and schema into the decoder's tables, returning the offset of
    /// the first row.
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

    /// Whether a block is active, i.e. [`RowBlockDecoder::begin`] succeeded and the block has
    /// neither been ended nor failed to decode since.
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Whether the active block still holds a row. False when no block is active.
    pub fn has_rows(&self) -> bool {
        self.active
            && self
                .buffer
                .as_ref()
                .is_some_and(|buffer| self.at < buffer.as_bytes().len())
    }

    /// The active block's column count, which every row's presence bitmap spans.
    pub const fn ncols(&self) -> usize {
        self.keys.len()
    }

    /// Whether strings decoded from the active block borrow from it instead of being copied;
    /// see [`MAX_PINNED_BYTES`].
    pub const fn shares_strings(&self) -> bool {
        self.share
    }

    /// Bytes of this decoder's blocks still allocated: the active block's, plus those of
    /// earlier blocks that decoded strings keep alive.
    pub fn live_bytes(&self) -> usize {
        self.live_bytes.load(Ordering::Relaxed)
    }

    /// Decodes the active block's next row, writing each present column into `row` under
    /// the key [`RowBlockDecoder::begin`] resolved for it.
    ///
    /// On error the block is no longer active, and `row` may hold the values of the columns
    /// before the malformed one.
    ///
    /// # Panics
    ///
    /// Panics if no block is active or the active block holds no further row: callers check
    /// [`RowBlockDecoder::has_rows`] first.
    ///
    /// # Safety
    ///
    /// 1. The lookup passed to the [`RowBlockDecoder::begin`] call that started the active
    ///    block must still satisfy that call's contract.
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
                // SAFETY: the key belongs to the lookup `begin` resolved it in, which is still
                // alive per (1.), and `RLookup` never moves or frees a key while it lives.
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

    /// Ends the active block, if any, releasing the decoder's hold on its buffer. The buffer
    /// itself is freed once no decoded string borrows from it either.
    pub fn end(&mut self) {
        self.buffer = None;
        self.at = 0;
        self.active = false;
    }
}

/// The key a schema column's values are written under: an existing key, else one the lookup
/// derives from its index schema, else a freshly created one.
fn resolve_key(lookup: &mut RLookup<'_>, name: &CStr) -> Option<NonNull<RLookupKey<'static>>> {
    // The key outlives the block the name points into, so a key created here must own its
    // name. That costs one allocation per column per block even for keys that already exist,
    // which is noise next to the rows the block carries.
    let owned = || Cow::Owned(name.to_owned());
    lookup
        .get_key_read_ptr(owned(), RLookupKeyFlags::empty())
        .or_else(|| lookup.get_key_write_ptr(owned(), RLookupKeyFlags::empty()))
        .map(NonNull::cast)
}
