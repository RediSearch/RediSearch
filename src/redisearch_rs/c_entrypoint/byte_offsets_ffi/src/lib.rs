/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`ByteOffsets`].
//!
//! A map is built while its document is indexed, then handed to the document's
//! metadata, which only reads it. Its iterators borrow it, so the map must outlive them
//! and stay unchanged while they are in use.

use std::{ffi::c_char, slice};

use byte_offsets::{ByteOffsets, FieldByteOffsets, OpaqueFieldByteOffsets};
use varint::VectorWriter;

/// The [`byte_offset`](TokenByteOffset::byte_offset) [`RSByteOffsetIterator_Next`]
/// returns once the iteration is over.
#[cheadergen::config(export)]
pub const RSBYTEOFFSET_EOF: u32 = u32::MAX;

// C discards an iterator by letting it go out of scope, so it must not own anything.
const _: () = assert!(!std::mem::needs_drop::<FieldByteOffsets<'static>>());

/// A token's position and byte offset, as returned by [`RSByteOffsetIterator_Next`].
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TokenByteOffset {
    /// The iterator's position after the call; see [`FieldByteOffsets`].
    pub position: u32,
    /// The offset of the token at [`position`](Self::position), or [`RSBYTEOFFSET_EOF`]
    /// if the iteration is over.
    pub byte_offset: u32,
}

/// Creates an empty [`ByteOffsets`]. Must be freed with [`RSByteOffsets_Free`].
#[unsafe(no_mangle)]
pub extern "C" fn NewByteOffsets() -> *mut ByteOffsets {
    Box::into_raw(Box::new(ByteOffsets::new()))
}

/// Frees a [`ByteOffsets`].
///
/// # Safety
///
/// 1. `offsets` must be a [valid], non-null pointer obtained from [`NewByteOffsets`] or
///    [`RSByteOffsets_LoadLegacy`].
/// 2. Neither `offsets` nor any iterator over it may be used after this call.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsets_Free(offsets: *mut ByteOffsets) {
    debug_assert!(!offsets.is_null(), "offsets must not be null");

    // SAFETY: `offsets` came from `Box::into_raw` (1.) and is not used again (2.).
    drop(unsafe { Box::from_raw(offsets) });
}

/// Reserves room for at least `num_fields` more fields.
///
/// # Safety
///
/// 1. `offsets` must be a [valid], non-null pointer obtained from [`NewByteOffsets`].
/// 2. No other call on `offsets` may run concurrently with this one, and no iterator
///    over it may be in use.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsets_ReserveFields(offsets: *mut ByteOffsets, num_fields: usize) {
    // SAFETY: `offsets` is valid (1.) and nothing else accesses it (2.).
    let offsets = unsafe { offsets.as_mut() }.expect("offsets must not be null");
    offsets.reserve_fields(num_fields);
}

/// Records that the field `field_id` owns the next `num_offsets` byte offsets, those of
/// its tokens at the positions from `first_tok_pos` on; see [`ByteOffsets::add_field`].
///
/// # Safety
///
/// 1. `offsets` must be a [valid], non-null pointer obtained from [`NewByteOffsets`].
/// 2. No other call on `offsets` may run concurrently with this one, and no iterator
///    over it may be in use.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsets_AddField(
    offsets: *mut ByteOffsets,
    field_id: u16,
    first_tok_pos: u32,
    num_offsets: u32,
) {
    // SAFETY: `offsets` is valid (1.) and nothing else accesses it (2.).
    let offsets = unsafe { offsets.as_mut() }.expect("offsets must not be null");
    offsets.add_field(field_id, first_tok_pos, num_offsets);
}

/// Replaces the byte offsets in `offsets` with the ones written to `writer`; see
/// [`ByteOffsets::take_offsets`].
///
/// # Safety
///
/// 1. `offsets` must be a [valid], non-null pointer obtained from [`NewByteOffsets`].
/// 2. No other call on `offsets` may run concurrently with this one, and no iterator
///    over it may be in use.
/// 3. `writer` must be a [valid], non-null pointer to a [`VectorWriter`], and no other
///    call on it may run concurrently with this one.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsets_TakeFromWriter(
    offsets: *mut ByteOffsets,
    writer: *mut VectorWriter,
) {
    // SAFETY: `offsets` is valid (1.) and nothing else accesses it (2.).
    let offsets = unsafe { offsets.as_mut() }.expect("offsets must not be null");
    // SAFETY: `writer` is valid and nothing else accesses it (3.).
    let writer = unsafe { writer.as_mut() }.expect("writer must not be null");
    offsets.take_offsets(writer);
}

/// Decodes byte offsets stored by a pre-2.0 doc table; see [`ByteOffsets::load_legacy`].
///
/// A NULL `data`, which is what `RedisModule_LoadStringBuffer` returns when its read
/// fails, is decoded as no bytes, whatever `len` is.
///
/// Returns NULL if the bytes end before the data they describe. Otherwise the caller owns
/// the result and must free it with [`RSByteOffsets_Free`].
///
/// # Safety
///
/// 1. If `data` is not NULL, it must be [valid] for reads of `len` bytes.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsets_LoadLegacy(
    data: *const c_char,
    len: usize,
) -> *mut ByteOffsets {
    let bytes = if data.is_null() {
        &[]
    } else {
        // SAFETY: `data` is valid for reads of `len` bytes (1.).
        unsafe { slice::from_raw_parts(data.cast::<u8>(), len) }
    };
    ByteOffsets::load_legacy(bytes).map_or(std::ptr::null_mut(), |offsets| {
        Box::into_raw(Box::new(offsets))
    })
}

/// Begins iterating over the byte offsets of the field `field_id`, writing the iterator
/// to `iter`. Returns `false`, and leaves `iter` untouched, if `offsets` has no such
/// field.
///
/// # Safety
///
/// 1. `offsets` must be a [valid], non-null pointer obtained from [`NewByteOffsets`] or
///    [`RSByteOffsets_LoadLegacy`].
/// 2. `iter` must be a [valid], non-null and aligned pointer for writes.
/// 3. `offsets` must remain [valid], and must not be passed to any function that takes
///    it mutably, for as long as the iterator is in use.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffset_Iterate(
    offsets: *const ByteOffsets,
    field_id: u16,
    iter: *mut OpaqueFieldByteOffsets,
) -> bool {
    debug_assert!(!iter.is_null(), "iter must not be null");

    // SAFETY: `offsets` is valid (1.), and stays so while the iterator borrows it (3.).
    let offsets = unsafe { offsets.as_ref() }.expect("offsets must not be null");
    let Some(field_offsets) = offsets.field_offsets(field_id) else {
        return false;
    };
    // SAFETY: `iter` is valid for writes and aligned (2.).
    unsafe { iter.write(field_offsets.into_opaque()) };
    true
}

/// Advances the iterator to the next token of its field; see [`FieldByteOffsets`].
///
/// # Safety
///
/// 1. `iter` must be a [valid], non-null pointer to an iterator written by
///    [`RSByteOffset_Iterate`], whose requirements on the iterated map still hold.
/// 2. No other call on `iter` may run concurrently with this one.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn RSByteOffsetIterator_Next(
    iter: *mut OpaqueFieldByteOffsets,
) -> TokenByteOffset {
    // SAFETY: `iter` holds a `FieldByteOffsets` whose map is still valid (1.), and
    // nothing else accesses it (2.).
    let iter =
        unsafe { FieldByteOffsets::from_opaque_mut_ptr(iter) }.expect("iter must not be null");
    let byte_offset = iter.next().unwrap_or(RSBYTEOFFSET_EOF);
    TokenByteOffset {
        position: iter.token_position(),
        byte_offset,
    }
}
