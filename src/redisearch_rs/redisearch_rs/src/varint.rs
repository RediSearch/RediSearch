//! The C API goes here. This part will hopefully all be removed once all users of `varint` API are
//! oxidized.

use std::ptr::NonNull;

use encode_decode::{
    BufferReader, BufferWriter, CBuffer, FieldMask,
    varint::{VectorWriter, read, read_field_mask, write, write_field_mask},
};

#[unsafe(no_mangle)]
extern "C" fn ReadVarint(mut b: NonNull<BufferReader>) -> u32 {
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let buffer_reader = unsafe { b.as_mut() };
    let mut cursor = buffer_reader.as_cursor();
    let val = read(&mut cursor).unwrap();
    buffer_reader.pos = cursor.position() as usize;

    val
}

#[unsafe(no_mangle)]
// `improper_ctypes_definitions` would be triggered because of u128 crossing the FFI boundary but
// that's no longer an issue:
// https://blog.rust-lang.org/2024/03/30/i128-layout-update/#compatibility
#[allow(improper_ctypes_definitions)]
extern "C" fn ReadVarintFieldMask(mut b: NonNull<BufferReader>) -> FieldMask {
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    let buffer_reader = unsafe { b.as_mut() };
    let mut cursor = buffer_reader.as_cursor();
    let val = read_field_mask(&mut cursor).unwrap();
    buffer_reader.pos = cursor.position() as usize;

    val
}

/// Note: This function now returns the number of bytes written to the buffer, not the change in
/// capacity. The change of the buffer capacity is an internal detail and should not be of concern
/// to the caller.
#[unsafe(no_mangle)]
extern "C" fn WriteVarint(value: u32, writer: NonNull<BufferWriter>) -> usize {
    let mut buffer = CBuffer::from(writer);
    let len = write(value, &mut buffer).unwrap();
    buffer.to_buffer_writer(writer);

    len
}

/// See the note above for [`WriteVarint`].
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
extern "C" fn WriteVarintFieldMask(value: FieldMask, writer: NonNull<BufferWriter>) -> usize {
    let mut buffer = CBuffer::from(writer);
    let len = write_field_mask(value, &mut buffer).unwrap();
    buffer.to_buffer_writer(writer);

    len
}

#[unsafe(no_mangle)]
extern "C" fn NewVarintVectorWriter(cap: usize) -> NonNull<VectorWriter> {
    let vector_writer = Box::leak(Box::new(VectorWriter::new(cap)));

    // Safety: The pointer is valid because we just created it.
    unsafe { NonNull::new_unchecked(vector_writer) }
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Write(mut writer: NonNull<VectorWriter>, value: u32) -> usize {
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { writer.as_mut() }.write(value).unwrap()
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
extern "C" fn VVW_Reset(mut writer: NonNull<VectorWriter>) {
    // Safety: The caller is responsible for ensuring that the pointer is valid.
    unsafe { writer.as_mut() }.reset()
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Free(writer: NonNull<VectorWriter>) {
    // Safety: The pointer is leaked in `NewVarintVectorWriter`, so we can safely drop it here.
    drop(unsafe { Box::from_raw(writer.as_ptr()) });
}

#[unsafe(no_mangle)]
extern "C" fn VVW_Truncate(writer: NonNull<VectorWriter>) -> usize {
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
