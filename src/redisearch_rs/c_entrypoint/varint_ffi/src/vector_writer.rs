/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::ptr::NonNull;
use varint::VectorWriter;

/// Create a new [`VectorWriter`] with the given capacity.
///
/// Use [`VVW_Free`] to free the memory allocated for the [`VectorWriter`].
#[unsafe(no_mangle)]
pub extern "C" fn NewVarintVectorWriter(cap: usize) -> *mut VectorWriter {
    Box::into_raw(Box::new(VectorWriter::new(cap)))
}

/// Delta-encode an integer and write it into the vector.
///
/// # Return value
///
/// The varint's actual size, if the operation is successful. 0 in case of failure.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`] and cannot be NULL.
/// 2. The caller must have exclusive access to the [`VectorWriter`] pointed to by `writer`.
#[unsafe(no_mangle)]
pub extern "C" fn VVW_Write(writer: Option<NonNull<VectorWriter>>, value: u32) -> usize {
    let mut writer = writer.unwrap();
    // Safety: The preconditions are met, thanks to safety invariants 1. and 2.
    unsafe { writer.as_mut() }.write(value).unwrap_or(0)
}

/// Get a reference to the underlying byte buffer.
/// It returns a NULL pointer if the writer is NULL.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`]
#[unsafe(no_mangle)]
pub unsafe extern "C" fn VVW_GetByteData(writer: *const VectorWriter) -> *const u8 {
    if writer.is_null() {
        return std::ptr::null();
    }

    // Safety: The preconditions are met, thanks to safety invariant 1.
    unsafe { &*writer }.bytes().as_ptr()
}

/// Get the length of the underlying byte buffer.
/// It returns 0 if the writer is NULL.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn VVW_GetByteLength(writer: *const VectorWriter) -> usize {
    if writer.is_null() {
        return 0;
    }

    // Safety: The preconditions are met, thanks to safety invariant 1.
    unsafe { &*writer }.bytes_len()
}

/// Get the number of encoded values in the writer.
/// It returns 0 if the writer is NULL.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`]
#[unsafe(no_mangle)]
pub const unsafe extern "C" fn VVW_GetCount(writer: *const VectorWriter) -> usize {
    if writer.is_null() {
        return 0;
    }

    // Safety: The preconditions are met, thanks to safety invariant 1.
    unsafe { &*writer }.count()
}

/// Reset the vector writer.
///
/// All encoded values are dropped, but the buffer capacity is preserved.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`] and cannot be NULL.
/// 2. The caller must have exclusive access to the [`VectorWriter`] pointed to by `writer`.
#[unsafe(no_mangle)]
pub extern "C" fn VVW_Reset(writer: Option<NonNull<VectorWriter>>) {
    let mut writer = writer.unwrap();
    // Safety: The preconditions are met, thanks to safety invariants 1. and 2.
    unsafe { writer.as_mut() }.reset()
}

#[unsafe(no_mangle)]
/// Free the memory allocated for the [`VectorWriter`].
///
/// After calling this function, the pointer is invalidated and should not be used.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`] and cannot be NULL.
/// 2. The caller must have exclusive access to the [`VectorWriter`] pointed to by `writer`.
pub extern "C" fn VVW_Free(writer: Option<NonNull<VectorWriter>>) {
    let writer = writer.unwrap();
    // Safety: The pointer is leaked in `NewVectorWriter`, so we can safely drop it here.
    drop(unsafe { Box::from_raw(writer.as_ptr()) });
}

#[unsafe(no_mangle)]
/// Resize the vector, dropping any excess capacity.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`] and cannot be NULL.
/// 2. The caller must have exclusive access to the [`VectorWriter`] pointed to by `writer`.
pub extern "C" fn VVW_Truncate(writer: Option<NonNull<VectorWriter>>) -> usize {
    let writer = writer.unwrap();
    // Safety: The preconditions are met, thanks to safety invariants 1. and 2.
    unsafe { &mut *writer.as_ptr() }.shrink_to_fit()
}

#[unsafe(no_mangle)]
/// Take ownership of the byte buffer stored in the vector.
/// After this call, `len` will be set to the length of the byte buffer while `writer`
/// will be left holding a fresh empty buffer.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid [`VectorWriter`] obtained from [`NewVarintVectorWriter`] and cannot be NULL.
/// 2. The caller must have exclusive access to the [`VectorWriter`] pointed to by `writer`.
/// 3. The caller must have exclusive access to `len`.
pub unsafe extern "C" fn VVW_TakeByteData(
    writer: *mut VectorWriter,
    mut len: NonNull<usize>,
) -> *mut u8 {
    if writer.is_null() {
        return std::ptr::null_mut();
    }

    // Safety: Guaranteed by safety invariant 1. and 2.
    let vector_writer = unsafe { &mut *writer };
    // Safety: Guaranteed by safety invariant 3.
    let len = unsafe { len.as_mut() };
    let mut bytes = vec![];
    std::mem::swap(vector_writer.bytes_mut(), &mut bytes);
    *len = bytes.len();
    let ptr = bytes.as_mut_ptr();
    std::mem::forget(bytes);

    ptr
}
