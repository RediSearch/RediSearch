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
use std::{borrow::Cow, ffi::CStr, ptr::NonNull};

/// Decodes one shard reply's block at a time into lookup rows.
///
/// Built for a caller that cannot hold a borrow across calls — the coordinator's network
/// result processor, which yields one row per call back into C. [`RowBlockDecoder::begin`]
/// parses the header and schema and resolves every column to a lookup key once, so that
/// [`RowBlockDecoder::next_row`] writes each value by key instead of by name.
///
/// One decoder serves every block a result processor receives: the key table keeps its
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
    /// The active block's undecoded rows, borrowed under [`RowBlockDecoder::begin`]'s
    /// contract. Empty when no block is active.
    rows: RawBytes,
    /// Set by [`RowBlockDecoder::begin`]; cleared by [`RowBlockDecoder::end`] and by a row
    /// that fails to decode.
    active: bool,
}

/// A borrowed byte range whose lifetime is guaranteed by contract rather than by the borrow
/// checker.
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

impl RowBlockDecoder {
    /// Creates a decoder with no active block.
    pub fn new() -> Self {
        Self::default()
    }

    /// Parses `block`'s header and schema, resolving each column name to a key of `lookup`,
    /// and makes it the active block. Any block active before is ended first.
    ///
    /// A column the lookup does not know yet gets a key created for it, exactly as the RESP
    /// per-row path does when it writes by name: a shard can send columns the coordinator's
    /// plan never named (`LOAD *` and other dynamic keys).
    ///
    /// On error no block is active.
    ///
    /// # Safety
    ///
    /// 1. `block` must stay [valid] for reads, and unmodified, until the block is ended:
    ///    by [`RowBlockDecoder::end`], by the next call to this method, or by dropping the
    ///    decoder.
    /// 2. `lookup` must outlive every [`RowBlockDecoder::next_row`] call made for this block,
    ///    and must not be cleaned up in between, since the decoder holds pointers to its keys.
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
        self.kinds.clear();
        self.kinds.extend_from_slice(parsed.kinds());
        self.keys.extend(
            parsed
                .columns()
                .iter()
                .map(|name| resolve_key(lookup, name)),
        );

        let rows = parsed.row_bytes();
        self.rows = RawBytes {
            ptr: NonNull::from(rows).cast(),
            len: rows.len(),
        };
        self.active = true;
        Ok(())
    }

    /// Whether a block is active, i.e. [`RowBlockDecoder::begin`] succeeded and the block has
    /// neither been ended nor failed to decode since.
    pub const fn is_active(&self) -> bool {
        self.active
    }

    /// Whether the active block still holds a row. False when no block is active.
    pub const fn has_rows(&self) -> bool {
        self.active && self.rows.len > 0
    }

    /// The active block's column count, which every row's presence bitmap spans.
    pub const fn ncols(&self) -> usize {
        self.keys.len()
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
    /// 1. The contract of the [`RowBlockDecoder::begin`] call that started the active block
    ///    must still hold.
    pub unsafe fn next_row(&mut self, row: &mut RLookupRow<'_>) -> Result<(), DecodeError> {
        assert!(self.has_rows(), "no row is left in the active block");

        // SAFETY: `rows` was carved out of the block `begin` was handed, which is still valid
        // and unmodified per (1.); the decoder only ever shrinks it from the front.
        let bytes = unsafe { std::slice::from_raw_parts(self.rows.ptr.as_ptr(), self.rows.len) };
        let mut reader = RowReader::new(bytes, &self.kinds);

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
                let rest = reader.remaining();
                self.rows = RawBytes {
                    ptr: NonNull::from(rest).cast(),
                    len: rest.len(),
                };
                Ok(())
            }
            Err(error) => {
                self.end();
                Err(error)
            }
        }
    }

    /// Ends the active block, if any, releasing the decoder's hold on its bytes.
    pub fn end(&mut self) {
        self.rows = RawBytes::default();
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
