/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use buffer::{BufferReader, BufferWriter};
use std::ptr::NonNull;
use varint::VarintEncode;

pub type FieldMask = ffi::t_fieldMask;

/// Read a varint-encoded field mask from the given buffer.
///
/// # Panics
///
/// Panics if the buffer doesn't contain a valid varint-encoded field mask.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// 1. `b` must point to a valid `BufferReader` instance and cannot be NULL.
/// 2. The caller must have exclusive access to the buffer reader.
#[unsafe(no_mangle)]
// `improper_ctypes_definitions` would be triggered because of u128 crossing the FFI boundary but
// that's no longer an issue:
// https://blog.rust-lang.org/2024/03/30/i128-layout-update/#compatibility
#[allow(improper_ctypes_definitions)]
pub extern "C" fn ReadVarintFieldMask(b: Option<NonNull<BufferReader>>) -> FieldMask {
    let mut buffer_reader = b.unwrap();
    // Safety: Safe thanks to invariants 1. and 2.
    let buffer_reader = unsafe { buffer_reader.as_mut() };
    varint::read(buffer_reader).unwrap()
}

/// Write a varint-encoded field mask into the given buffer writer.
/// It returns the number of bytes that have been added to the capacity of
/// the underlying buffer.
///
/// # Panics
///
/// Panics if the buffer can't grow its capacity to fit the encoded field mask.
///
/// # Safety
///
/// The following invariants must be upheld when calling this function:
/// 1. `writer` must point to a valid `BufferWriter` instance and cannot be NULL.
/// 2. The caller must have exclusive access to the buffer writer.
#[unsafe(no_mangle)]
// `improper_ctypes_definitions` would be triggered because of u128 crossing the FFI boundary but
// that's no longer an issue:
// https://blog.rust-lang.org/2024/03/30/i128-layout-update/#compatibility
#[allow(improper_ctypes_definitions)]
pub extern "C" fn WriteVarintFieldMask(
    value: FieldMask,
    writer: Option<NonNull<BufferWriter>>,
) -> usize {
    let mut writer = writer.unwrap();
    // Safety: Safe thanks to invariants 1. and 2.
    let writer = unsafe { writer.as_mut() };
    let cap = writer.buffer().capacity();
    value.write_as_varint(&mut *writer).unwrap();
    writer.buffer().capacity() - cap
}
