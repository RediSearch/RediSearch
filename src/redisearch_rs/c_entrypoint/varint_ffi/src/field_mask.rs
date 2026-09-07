/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use buffer::{BufferReader, BufferWriter};
use rqe_core::FieldMask;
use varint::VarintEncode;

/// Read a varint-encoded field mask from the given buffer.
///
/// # Panics
///
/// Panics if the buffer doesn't contain a valid varint-encoded field mask.
///
/// # Safety
/// The following invariants must be upheld when calling this function:
/// 1. `b` must point to a [valid] `BufferReader` instance and cannot be NULL.
/// 2. The caller must have exclusive access to the buffer reader.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn ReadVarintFieldMask(b: *mut BufferReader) -> FieldMask {
    // Safety: Safe thanks to invariants 1. and 2.
    let buffer_reader = unsafe { b.as_mut() }.expect("b must not be NULL");
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
/// 1. `writer` must point to a [valid] `BufferWriter` instance and cannot be NULL.
/// 2. The caller must have exclusive access to the buffer writer.
///
/// [valid]: https://doc.rust-lang.org/std/ptr/index.html#safety
#[unsafe(no_mangle)]
pub unsafe extern "C" fn WriteVarintFieldMask(
    value: FieldMask,
    writer: *mut BufferWriter,
) -> usize {
    // Safety: Safe thanks to invariants 1. and 2.
    let writer = unsafe { writer.as_mut() }.expect("writer must not be NULL");
    let cap = writer.buffer().capacity();
    value.write_as_varint(&mut *writer).unwrap();
    writer.buffer().capacity() - cap
}
