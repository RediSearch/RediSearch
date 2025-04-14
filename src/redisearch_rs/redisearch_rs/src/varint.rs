/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The C API goes here. This part will hopefully all be removed once all users of `varint` API are
//! oxidized.

use std::ptr::NonNull;

use encode_decode::{
    BufferReader, BufferWriter, FieldMask,
    varint::{VectorWriter, read, read_field_mask, write, write_field_mask},
};

#[unsafe(no_mangle)]
extern "C" fn ReadVarint(b: Option<NonNull<BufferReader>>) -> u32 {
    let mut buffer_reader = b.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let buffer_reader = unsafe { buffer_reader.as_mut() };
    read(buffer_reader).unwrap()
}

#[unsafe(no_mangle)]
// `improper_ctypes_definitions` would be triggered because of u128 crossing the FFI boundary but
// that's no longer an issue:
// https://blog.rust-lang.org/2024/03/30/i128-layout-update/#compatibility
#[allow(improper_ctypes_definitions)]
extern "C" fn ReadVarintFieldMask(b: Option<NonNull<BufferReader>>) -> FieldMask {
    let mut buffer_reader = b.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let buffer_reader = unsafe { buffer_reader.as_mut() };
    read_field_mask(buffer_reader).unwrap()
}

#[unsafe(no_mangle)]
extern "C" fn WriteVarint(value: u32, writer: Option<NonNull<BufferWriter>>) -> usize {
    let mut writer = writer.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let writer = unsafe { writer.as_mut() };
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let cap = unsafe { writer.buf.as_ref() }.capacity();
    write(value, &mut *writer).unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let new_cap = unsafe { writer.buf.as_ref() }.capacity();
    new_cap - cap
}

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
extern "C" fn WriteVarintFieldMask(
    value: FieldMask,
    writer: Option<NonNull<BufferWriter>>,
) -> usize {
    let mut writer = writer.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let writer = unsafe { writer.as_mut() };
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let cap = unsafe { writer.buf.as_ref() }.capacity();
    write_field_mask(value, &mut *writer).unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let new_cap = unsafe { writer.buf.as_ref() }.capacity();
    new_cap - cap
}

#[unsafe(no_mangle)]
extern "C" fn NewVarintVectorWriter(cap: usize) -> NonNull<VectorWriter> {
    let vector_writer = Box::into_raw(Box::new(VectorWriter::new(cap)));

    // Safety: The pointer is valid because we just created it.
    unsafe { NonNull::new_unchecked(vector_writer) }
}

/// Write an integer to the vector.
///
/// # Parameters
///
/// `w` a vector writer
/// `i`` the integer we want to write
///
/// # Return value
///
/// The varint's actual size, if the operation is successful. 0 in case of failure.
#[unsafe(no_mangle)]
extern "C" fn VVW_Write(writer: Option<NonNull<VectorWriter>>, value: u32) -> usize {
    let mut writer = writer.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { writer.as_mut() }.write(value).unwrap_or(0)
}

#[unsafe(no_mangle)]
extern "C" fn VVW_GetByteData(writer: *const VectorWriter) -> *const u8 {
    if writer.is_null() {
        return std::ptr::null();
    }

    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { &*writer }.bytes().as_ptr()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_GetByteLength(writer: *const VectorWriter) -> usize {
    if writer.is_null() {
        return 0;
    }

    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { &*writer }.bytes_len()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_GetCount(writer: *const VectorWriter) -> usize {
    if writer.is_null() {
        return 0;
    }

    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { &*writer }.count()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Reset(writer: Option<NonNull<VectorWriter>>) {
    let mut writer = writer.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { writer.as_mut() }.reset()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Free(writer: Option<NonNull<VectorWriter>>) {
    let writer = writer.unwrap();
    // Safety: The pointer is leaked in `NewVarintVectorWriter`, so we can safely drop it here.
    drop(unsafe { Box::from_raw(writer.as_ptr()) });
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Truncate(writer: Option<NonNull<VectorWriter>>) -> usize {
    let writer = writer.unwrap();
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { &mut *writer.as_ptr() }.shrink_to_fit()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_TakeByteData(writer: *mut VectorWriter, mut len: NonNull<usize>) -> *mut u8 {
    if writer.is_null() {
        return std::ptr::null_mut();
    }

    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let vector_writer = unsafe { &mut *writer };
    // Safety: Same here.
    let len = unsafe { len.as_mut() };
    let mut bytes = vec![];
    std::mem::swap(vector_writer.bytes_mut(), &mut bytes);
    *len = bytes.len();
    let ptr = bytes.as_mut_ptr();
    std::mem::forget(bytes);

    ptr
}
