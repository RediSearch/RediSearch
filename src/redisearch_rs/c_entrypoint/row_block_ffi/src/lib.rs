/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entrypoint for the [`row_block`] format: the shard's encoder and RESP replay.

use ffi::{
    RedisModule_Reply, SendReplyFlags, SendReplyFlags_SENDREPLY_FLAG_EXPAND,
    SendReplyFlags_SENDREPLY_FLAG_TYPED,
};
use query_flags::{QEFlag, QEFlags};
use rlookup::{OpaqueRLookup, OpaqueRLookupRow, RLookup, RLookupKeyFlags, RLookupRow};
use row_block::{Block, ColumnFilter, RowBlockWriter, TrioMember};
use std::ffi::{c_char, c_uint};

/// Free it with [`RowBlockWriter_Free`]; reuse it across chunks with [`RowBlockWriter_Reset`].
#[unsafe(no_mangle)]
pub extern "C" fn RowBlockWriter_New() -> *mut RowBlockWriter {
    Box::into_raw(Box::new(RowBlockWriter::new()))
}

/// # Safety
///
/// 1. `w` must be a non-null pointer returned by [`RowBlockWriter_New`] and not freed since.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_Free(w: *mut RowBlockWriter) {
    debug_assert!(!w.is_null(), "RowBlockWriter_Free got a NULL writer");
    // SAFETY: ensured by caller (1.)
    drop(unsafe { Box::from_raw(w) });
}

/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Free`]'s `w`, except that the writer stays usable.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_Reset(w: *mut RowBlockWriter) {
    // SAFETY: ensured by caller (1.)
    unsafe { writer_mut(w) }.reset();
}

/// See [`RowBlockWriter::write_schema`]; `required_flags` / `exclude_flags` are the `RLookup_F` sets of a
/// [`ColumnFilter`]. Returns 0, and the caller replies in RESP, when the chunk cannot be encoded.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Reset`]'s `w`, and no schema may have been written since the writer was created
///    or reset.
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

/// See [`RowBlockWriter::write_row`]; returns false for a refused row. `req_flags` (`QEFlags`) and `api_version` select
/// the [`TrioMember`].
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Reset`]'s `w`, and a schema declaring at least one column must have been
///    written since the writer was created or reset.
/// 2. `lk` must be a non-null pointer to a [valid] `RLookup` that outlives this call, and the same one the schema was
///    written from.
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

/// The block built so far, valid until the next call that appends to or resets `w`.
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

/// # Safety
///
/// Same contract as [`RowBlockWriter_Bytes`]'s `w`.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_RowCount(w: *const RowBlockWriter) -> usize {
    // SAFETY: ensured by the caller.
    unsafe { writer_ref(w) }.nrows()
}

/// Re-emits the rows appended so far as the RESP rows the row serializer would have produced, for a chunk that has to
/// abandon its block after rows went into it (they exist nowhere else).
///
/// Returns false, having emitted nothing, if the block does not decode, which only a writer bug can cause. The whole
/// block is checked before the first row is emitted, since a reply cannot be retracted.
///
/// # Safety
///
/// 1. Same contract as [`RowBlockWriter_Bytes`]'s `w`.
/// 2. `reply` must be a non-null pointer to a [valid] `RedisModule_Reply` currently building an array, and must outlive
///    this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RowBlockWriter_ReplayAsResp(
    w: *const RowBlockWriter,
    reply: *mut RedisModule_Reply,
    req_flags: u32,
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
            return false;
        }
    };

    for row in block.rows() {
        if let Err(error) = row {
            tracing::error!(%error, "a row block this build wrote is not one it can read");
            return false;
        }
    }

    // SAFETY: ensured by caller (2.).
    let resp3 = unsafe { (*reply).resp3 };
    let mut nrows = 0;
    for row in block.rows() {
        let Ok(row) = row else {
            unreachable!("row decoded during validation but not during replay")
        };

        if resp3 {
            // SAFETY: ensured by caller (2.)
            unsafe { ffi::RedisModule_Reply_Map(reply) };
            // SAFETY: ensured by caller (2.); the key is a static C string.
            unsafe {
                ffi::RedisModule_Reply_StringBuffer_FFI(
                    reply,
                    c"extra_attributes".as_ptr(),
                    c"extra_attributes".count_bytes(),
                )
            };
        }
        // SAFETY: ensured by caller (2.)
        unsafe { ffi::RedisModule_Reply_Map(reply) };
        for (name, value) in row.fields() {
            // SAFETY: ensured by caller (2.); `name` borrows the writer's buffer, untouched here.
            unsafe {
                ffi::RedisModule_Reply_StringBuffer_FFI(reply, name.as_ptr(), name.count_bytes())
            };
            // SAFETY: ensured by caller (2.); `value` is a live `RSValue` owned by `row`.
            unsafe { ffi::RedisModule_Reply_RSValue(reply, value.as_ptr().cast(), flags) };
        }
        // SAFETY: ensured by caller (2.)
        unsafe { ffi::RedisModule_Reply_MapEnd(reply) };
        if resp3 {
            // SAFETY: ensured by caller (2.); the key is a static C string.
            unsafe {
                ffi::RedisModule_Reply_StringBuffer_FFI(
                    reply,
                    c"values".as_ptr(),
                    c"values".count_bytes(),
                )
            };
            // SAFETY: ensured by caller (2.)
            unsafe { ffi::RedisModule_Reply_Array(reply) };
            // SAFETY: ensured by caller (2.)
            unsafe { ffi::RedisModule_Reply_ArrayEnd(reply) };
            // SAFETY: ensured by caller (2.)
            unsafe { ffi::RedisModule_Reply_MapEnd(reply) };
        }
        nrows += 1;
    }

    debug_assert_eq!(
        nrows,
        writer.nrows(),
        "replay must re-emit every row the block held"
    );
    true
}

/// # Panics
///
/// Panics if either set carries a bit no `RLookup_F` flag defines.
fn column_filter(required_flags: u32, exclude_flags: u32) -> ColumnFilter {
    ColumnFilter {
        required: RLookupKeyFlags::from_bits(required_flags).expect("a valid RLookup_F bit set"),
        excluded: RLookupKeyFlags::from_bits(exclude_flags).expect("a valid RLookup_F bit set"),
    }
}

/// Unknown bits are dropped: only a few flags are consulted.
fn query_flags(req_flags: u32) -> QEFlags {
    QEFlags::from_bits_truncate(req_flags)
}

/// Chosen as `RedisModule_Reply_RLookupRow` chooses it.
fn trio_member(req_flags: QEFlags, api_version: c_uint) -> TrioMember {
    if req_flags.contains(QEFlag::FormatExpand) {
        TrioMember::Right
    } else if api_version >= ffi::APIVERSION_RETURN_MULTI_CMP_FIRST {
        TrioMember::Middle
    } else {
        TrioMember::Left
    }
}

/// As `serializeResult` derives them.
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
/// 1. `w` must be a non-null pointer returned by [`RowBlockWriter_New`], not freed since, which outlives `'a`.
unsafe fn writer_ref<'a>(w: *const RowBlockWriter) -> &'a RowBlockWriter {
    debug_assert!(!w.is_null(), "row block writer pointer must not be NULL");
    // SAFETY: ensured by caller (1.)
    unsafe { &*w }
}

/// # Safety
///
/// 1. Same contract as [`writer_ref`]'s `w`, and no other reference to the writer may be live for `'a`.
unsafe fn writer_mut<'a>(w: *mut RowBlockWriter) -> &'a mut RowBlockWriter {
    debug_assert!(!w.is_null(), "row block writer pointer must not be NULL");
    // SAFETY: ensured by caller (1.)
    unsafe { &mut *w }
}
