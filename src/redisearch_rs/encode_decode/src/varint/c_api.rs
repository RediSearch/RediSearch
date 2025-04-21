//! The C API goes here. This part will hopefully all be removed once all users of `varint` API are
//! oxidized.

use super::{read, read_field_mask, write, write_field_mask};
use crate::{
    FieldMask,
    buffer::{BufferReader, BufferWriter},
};

#[unsafe(no_mangle)]
extern "C" fn ReadVarint(b: *mut BufferReader) -> u32 {
    let buffer_reader = unsafe { b.as_mut() }.unwrap();
    let mut cursor = buffer_reader.to_cursor();
    let val = read(&mut cursor).unwrap();
    buffer_reader.pos = cursor.position() as usize;

    val
}

#[unsafe(no_mangle)]
// `improper_ctypes_definitions` would be triggered because of u128 crossing the FFI boundary but
// that's no longer an issue:
// https://blog.rust-lang.org/2024/03/30/i128-layout-update/#compatibility
#[allow(improper_ctypes_definitions)]
extern "C" fn ReadVarintFieldMask(b: *mut BufferReader) -> FieldMask {
    let buffer_reader = unsafe { b.as_mut() }.unwrap();
    let mut cursor = buffer_reader.to_cursor();
    let val = read_field_mask(&mut cursor).unwrap();
    buffer_reader.pos = cursor.position() as usize;

    val
}

/// Note: This function now returns the number of bytes written to the buffer, not the change in
/// capacity. The change of the buffer capacity is an internal detail and should not be of concern
/// to the caller.
#[unsafe(no_mangle)]
extern "C" fn WriteVarint(value: u32, b: *mut BufferWriter) -> usize {
    let buffer_writer = unsafe { b.as_mut() }.unwrap();
    let mut cursor = std::io::Cursor::<Vec<u8>>::from(*buffer_writer);
    let bytes_written = write(value, &mut cursor).unwrap();
    *buffer_writer = cursor.into();

    bytes_written
}

/// See the note above for [`WriteVarint`].
#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
extern "C" fn WriteVarintFieldMask(value: FieldMask, b: *mut BufferWriter) -> usize {
    let buffer_writer = unsafe { b.as_mut() }.unwrap();
    let mut cursor = std::io::Cursor::<Vec<u8>>::from(*buffer_writer);
    let bytes_written = write_field_mask(value, &mut cursor).unwrap();
    *buffer_writer = cursor.into();

    bytes_written
}
