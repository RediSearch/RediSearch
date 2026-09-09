/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entrypoint for the [`row_block`] wire format: the encoder used by the shard-side
//! aggregate reply path, and the format constants the coordinator-side decoder
//! (`src/coord/rpnet.c`) parses with.
//!
//! The format itself, including which facts are a contract with that decoder, is documented
//! on the [`row_block`] crate.

use ffi::{
    RedisModule_Reply, SendReplyFlags, SendReplyFlags_SENDREPLY_FLAG_EXPAND,
    SendReplyFlags_SENDREPLY_FLAG_TYPED,
};
use query_flags::{QEFlag, QEFlags};
use rlookup::{OpaqueRLookup, OpaqueRLookupRow, RLookup, RLookupKeyFlags, RLookupRow};
use row_block::{Block, ColumnFilter, RowBlockWriter, Tag, TrioMember};
use std::ffi::{c_char, c_uint};

/// Opens a block's header.
#[cheadergen::config(export, rename = "ROW_BLOCK_MAGIC")]
pub const ROW_BLOCK_MAGIC: u32 = row_block::MAGIC;

/// The format version this build writes and reads.
#[cheadergen::config(export, rename = "ROW_BLOCK_VERSION")]
pub const ROW_BLOCK_VERSION: u8 = row_block::VERSION;

/// Tag for a [`Tag::Number`] value.
#[cheadergen::config(export, rename = "ROW_BLOCK_TAG_NUM")]
pub const ROW_BLOCK_TAG_NUM: u8 = Tag::Number as u8;

/// Tag for a [`Tag::String`] value.
#[cheadergen::config(export, rename = "ROW_BLOCK_TAG_STR")]
pub const ROW_BLOCK_TAG_STR: u8 = Tag::String as u8;

/// Tag for a [`Tag::Null`] value.
#[cheadergen::config(export, rename = "ROW_BLOCK_TAG_NULL")]
pub const ROW_BLOCK_TAG_NULL: u8 = Tag::Null as u8;

/// Tag for a [`Tag::Array`] value.
#[cheadergen::config(export, rename = "ROW_BLOCK_TAG_ARRAY")]
pub const ROW_BLOCK_TAG_ARRAY: u8 = Tag::Array as u8;

/// Tag for a [`Tag::Map`] value.
#[cheadergen::config(export, rename = "ROW_BLOCK_TAG_MAP")]
pub const ROW_BLOCK_TAG_MAP: u8 = Tag::Map as u8;

/// Allocates a writer with no header written yet. Free it with [`RowBlockWriter_Free`].
///
/// One writer is meant to serve every chunk of a request, and every request a thread handles:
/// [`RowBlockWriter_Reset`] keeps the buffer capacity that earlier chunks grew.
#[unsafe(no_mangle)]
pub extern "C" fn RowBlockWriter_New() -> *mut RowBlockWriter {
    Box::into_raw(Box::new(RowBlockWriter::new()))
}

/// Releases a writer and its buffer.
///
/// # Safety
///
/// 1. `w` must be a non-null pointer returned by [`RowBlockWriter_New`] and not freed since.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_Free(w: *mut RowBlockWriter) {
    debug_assert!(!w.is_null(), "RowBlockWriter_Free got a NULL writer");
    // SAFETY: ensured by caller (1.)
    drop(unsafe { Box::from_raw(w) });
}

/// Discards the block, keeping the allocated capacity for the next chunk.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Free`]'s `w`, except that the writer stays usable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_Reset(w: *mut RowBlockWriter) {
    // SAFETY: ensured by caller (1.)
    unsafe { writer_mut(w) }.reset();
}

/// Writes the header and the schema taken from `lk`'s visible keys, and returns how many
/// columns it declares.
///
/// `required_flags` / `exclude_flags` are `RLookup_F` bit sets selecting the same key subset the
/// RESP serializer would emit; see [`ColumnFilter`].
///
/// A zero return means this chunk cannot be encoded and the caller must reply in RESP
/// instead: either the schema has no columns, or it holds a name the format cannot carry.
/// Both leave the writer empty.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Reset`]'s `w`, and no schema may have been written
///    since the writer was created or reset.
/// 2. `lk` must be a non-null pointer to a [valid] `RLookup` that outlives this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_WriteSchema(
    w: *mut RowBlockWriter,
    lk: *const OpaqueRLookup,
    required_flags: u32,
    exclude_flags: u32,
) -> u16 {
    // SAFETY: ensured by caller (1.)
    let writer = unsafe { writer_mut(w) };
    // SAFETY: ensured by caller (2.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lk) }.expect("a non-null RLookup");

    match writer.write_schema(lookup, column_filter(required_flags, exclude_flags)) {
        Ok(ncols) => ncols,
        Err(error) => {
            tracing::warn!(%error, "cannot encode this chunk's schema as a row block");
            0
        }
    }
}

/// Appends one row, reading values for the schema's columns out of `row`.
///
/// Returns false when the row holds a value the format cannot represent, in which case
/// nothing is appended for it: the block still holds exactly the rows written before, so the
/// caller can emit it as is or discard it, but must not treat this row as encoded.
///
/// `req_flags` (a `QEFlags` bit set) and `api_version` select how a row field stored as a trio
/// resolves; see [`TrioMember`].
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Reset`]'s `w`, and a schema declaring at least one
///    column must have been written since the writer was created or reset.
/// 2. `lk` must be a non-null pointer to a [valid] `RLookup` that outlives this call, and the
///    same one the schema was written from.
/// 3. `row` must be a non-null pointer to a [valid] `RLookupRow` that outlives this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_WriteRow(
    w: *mut RowBlockWriter,
    lk: *const OpaqueRLookup,
    row: *const OpaqueRLookupRow,
    req_flags: u32,
    api_version: c_uint,
) -> bool {
    // SAFETY: ensured by caller (1.)
    let writer = unsafe { writer_mut(w) };
    // SAFETY: ensured by caller (2.)
    let lookup = unsafe { RLookup::from_opaque_ptr(lk) }.expect("a non-null RLookup");
    // SAFETY: ensured by caller (3.)
    let row = unsafe { RLookupRow::from_opaque_ptr(row) }.expect("a non-null RLookupRow");

    match writer.write_row(
        lookup,
        row,
        trio_member(query_flags(req_flags), api_version),
    ) {
        Ok(()) => true,
        Err(error) => {
            tracing::debug!(%error, "cannot encode this row as part of a row block");
            false
        }
    }
}

/// Borrows the block built so far, writing its length to `len`.
///
/// The returned pointer stays valid until the next call that appends to or resets `w`.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Free`]'s `w`, except that the writer stays usable.
/// 2. `len` must be a non-null, writable pointer to a `size_t`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_Bytes(
    w: *const RowBlockWriter,
    len: *mut usize,
) -> *const c_char {
    // SAFETY: ensured by caller (1.)
    let bytes = unsafe { writer_ref(w) }.as_bytes();
    // SAFETY: ensured by caller (2.)
    unsafe { len.write(bytes.len()) };
    bytes.as_ptr().cast()
}

/// Emits the rows appended so far as ordinary RESP rows, reporting whether it could.
///
/// Returns `false`, having emitted nothing and leaving `nelem` untouched, when the block does
/// not decode. That cannot happen for a block this process just wrote, so it means the encoder
/// and decoder disagree; the caller's contract is to fail the query rather than reply rows it
/// cannot vouch for. The whole block is decoded before the first row is emitted precisely so
/// that failure is all-or-nothing: `RedisModule_Reply` writes through to the client with no way
/// to retract, so detecting the disagreement half way through would leave a partial reply that
/// can no longer be turned into an error.
///
/// The encoder read backwards, for abandoning a block after rows have already gone into it:
/// those rows exist nowhere else - the pipeline row they came from is long released - and a
/// chunk's reply carries either a block or RESP rows, never both. Each row is emitted as the
/// same name/value map the RESP row serializer produces, so a chunk that falls back is
/// indistinguishable on the wire from one a shard with the format off would have sent.
///
/// Only rows are replayed. The block never carried the per-row extras (id, score, sortkey)
/// that `serializeResult` can add, so a request that asks for those cannot use blocks in the
/// first place.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Bytes`]'s `w`.
/// 2. `reply` must be a non-null pointer to a [valid] `RedisModule_Reply` currently building
///    an array, and must outlive this call.
/// 3. `nelem` must be a non-null, writable pointer to a `size_t`.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_ReplayAsResp(
    w: *const RowBlockWriter,
    reply: *mut RedisModule_Reply,
    req_flags: u32,
    nelem: *mut usize,
) -> bool {
    debug_assert!(
        !reply.is_null(),
        "RowBlockWriter_ReplayAsResp got a NULL reply"
    );
    // SAFETY: ensured by caller (1.)
    let writer = unsafe { writer_ref(w) };
    let flags = send_reply_flags(query_flags(req_flags));

    let block = match Block::parse(writer.as_bytes()) {
        Ok(block) => block,
        Err(error) => {
            tracing::error!(%error, "a row block this build wrote is not one it can read");
            debug_assert!(false, "row block replay failed to parse its own block");
            return false;
        }
    };

    // Decode every row before emitting any of it, so a mid-block error cannot strand a
    // half-written reply. See this function's returns-`false` contract.
    for row in block.rows() {
        if let Err(error) = row {
            tracing::error!(%error, "a row block this build wrote is not one it can read");
            debug_assert!(false, "row block replay failed to decode its own row");
            return false;
        }
    }

    let mut nrows = 0;
    for row in block.rows() {
        // Already proven decodable by the validation pass above.
        let Ok(row) = row else {
            unreachable!("row decoded during validation but not during replay")
        };

        // SAFETY: ensured by caller (2.)
        unsafe { ffi::RedisModule_Reply_Map(reply) };
        for (name, value) in row.fields() {
            // SAFETY: ensured by caller (2.); `name` is borrowed from the writer's buffer,
            // which this function does not touch.
            unsafe {
                ffi::RedisModule_Reply_StringBuffer(reply, name.as_ptr(), name.count_bytes())
            };
            // SAFETY: ensured by caller (2.); `value` is a live `RSValue` owned by `row`.
            unsafe { ffi::RedisModule_Reply_RSValue(reply, value.as_ptr().cast(), flags) };
        }
        // SAFETY: ensured by caller (2.)
        unsafe { ffi::RedisModule_Reply_MapEnd(reply) };
        nrows += 1;
    }

    debug_assert_eq!(
        nrows,
        writer.nrows(),
        "replay must re-emit every row the block held"
    );
    // SAFETY: ensured by caller (3.)
    unsafe { nelem.write(nrows) };
    true
}

/// Reinterprets an `RLookup_F` bit pair as the column predicate.
///
/// # Panics
///
/// Panics if either set carries a bit no `RLookup_F` flag defines.
fn column_filter(required_flags: u32, exclude_flags: u32) -> ColumnFilter {
    ColumnFilter {
        required: RLookupKeyFlags::from_bits(required_flags).expect("a valid RLookup_F bit set"),
        excluded: RLookupKeyFlags::from_bits(exclude_flags).expect("a valid RLookup_F bit set"),
    }
}

/// Reinterprets a `QEFlags` bit set.
///
/// Unknown bits are dropped rather than rejected: only the two flags below are consulted, and
/// a request may legitimately carry flags a given build does not know.
fn query_flags(req_flags: u32) -> QEFlags {
    QEFlags::from_bits_truncate(req_flags)
}

/// The member a row field stored as a trio resolves to, chosen exactly as the RESP row
/// serializer (`RedisModule_Reply_RLookupRow`) chooses it.
fn trio_member(req_flags: QEFlags, api_version: c_uint) -> TrioMember {
    if req_flags.contains(QEFlag::FormatExpand) {
        TrioMember::Right
    } else if api_version >= ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
        TrioMember::Middle
    } else {
        TrioMember::Left
    }
}

/// The `SendReplyFlags` a request's `QEFlags` imply, as `serializeResult` derives them.
fn send_reply_flags(req_flags: QEFlags) -> SendReplyFlags {
    let mut flags: SendReplyFlags = 0;
    if req_flags.contains(QEFlag::Typed) {
        flags |= SendReplyFlags_SENDREPLY_FLAG_TYPED;
    }
    if req_flags.contains(QEFlag::FormatExpand) {
        flags |= SendReplyFlags_SENDREPLY_FLAG_EXPAND;
    }
    flags
}

/// # Safety
///
/// 1. `w` must be a non-null pointer returned by [`RowBlockWriter_New`], not freed since,
///    which outlives `'a`.
unsafe fn writer_ref<'a>(w: *const RowBlockWriter) -> &'a RowBlockWriter {
    debug_assert!(!w.is_null(), "row block writer pointer must not be NULL");
    // SAFETY: ensured by caller (1.)
    unsafe { &*w }
}

/// # Safety
///
/// 1. Same contract as [`writer_ref`]'s `w`, and no other reference to the writer may be live
///    for `'a`.
unsafe fn writer_mut<'a>(w: *mut RowBlockWriter) -> &'a mut RowBlockWriter {
    debug_assert!(!w.is_null(), "row block writer pointer must not be NULL");
    // SAFETY: ensured by caller (1.)
    unsafe { &mut *w }
}
