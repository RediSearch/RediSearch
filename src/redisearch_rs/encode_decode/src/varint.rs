use crate::{
    FieldMask,
    buffer::{BufferReader, BufferWriter},
};

use std::{
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
};

//
// The C API goes here. This part will hopefully all be removed once all users of `varint` API are
// oxidized.
//

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

#[unsafe(no_mangle)]
extern "C" fn WriteVarint(value: u32, b: *mut BufferWriter) -> usize {
    let buffer_writer = unsafe { b.as_mut() }.unwrap();
    let mut cursor = io::Cursor::<Vec<u8>>::from(*buffer_writer);
    let cap = cursor.get_ref().capacity();
    write(value, &mut cursor).unwrap();
    let bytes_allocated = cursor.get_ref().capacity() - cap;
    *buffer_writer = cursor.into();

    bytes_allocated
}

#[unsafe(no_mangle)]
#[allow(improper_ctypes_definitions)]
extern "C" fn WriteVarintFieldMask(value: FieldMask, b: *mut BufferWriter) -> usize {
    let buffer_writer = unsafe { b.as_mut() }.unwrap();
    let mut cursor = io::Cursor::<Vec<u8>>::from(*buffer_writer);
    let cap = cursor.get_ref().capacity();
    write_field_mask(value, &mut cursor).unwrap();
    let bytes_allocated = cursor.get_ref().capacity() - cap;
    *buffer_writer = cursor.into();

    bytes_allocated
}

//
// The Rust API goes here. This part will be used by the Rust code and will be used by the C code
// above.
//

/// Read an encoded integer from the given reader.
pub fn read<R>(mut read: R) -> io::Result<u32>
where
    R: Read,
{
    let mut buffer = [0; 1];
    read.read_exact(&mut buffer)?;
    let mut c = buffer[0];

    let mut val = (c & 127) as u32;
    while c >> 7 != 0 {
        val += 1;
        read.read_exact(&mut buffer)?;
        c = buffer[0];
        val = (val << 7) | ((c & 127) as u32);
    }

    Ok(val)
}

/// Same as `read` but reads the value into a `FieldMask`.
// FIXME: The logic is identical to `read` so we can use a macro here to avoid code duplication.
pub fn read_field_mask<R>(mut read: R) -> io::Result<FieldMask>
where
    R: Read,
{
    let mut buffer = [0; 1];
    read.read_exact(&mut buffer)?;
    let mut c = buffer[0];

    let mut val = (c & 127) as FieldMask;
    while c >> 7 != 0 {
        val += 1;
        read.read_exact(&mut buffer)?;
        c = buffer[0];
        val = (val << 7) | ((c & 127) as FieldMask);
    }

    Ok(val)
}

/// Encode an integer into a varint format and write it to the given writer.
pub fn write<W: Write>(value: u32, mut write: W) -> io::Result<usize>
where
    W: Write,
{
    let mut variant = VarintBuf::new();
    let pos = encode(value, &mut variant);
    write.write(&variant[pos..])
}

/// Encode a FieldMask into a varint format and write it to the given writer.
pub fn write_field_mask<W: Write>(value: FieldMask, mut write: W) -> io::Result<usize>
where
    W: Write,
{
    let mut variant = VarintBuf::new();
    let pos = encode_field_mask(value, &mut variant);
    write.write(&variant[pos..])
}

#[inline(always)]
fn encode(mut value: u32, vbuf: &mut VarintBuf) -> usize {
    let mut pos = vbuf.len() - 1;
    vbuf[pos] = (value & 127) as u8;
    value >>= 7;
    while value != 0 {
        pos -= 1;
        value -= 1;
        vbuf[pos] = 128 | ((value & 127) as u8);
        value >>= 7;
    }

    pos
}

// FIXME: The logic is identical to `encode` so we can use a macro here to avoid code duplication.
fn encode_field_mask(mut value: FieldMask, vbuf: &mut VarintBuf) -> usize {
    let mut pos = vbuf.len() - 1;
    vbuf[pos] = (value & 127) as u8;
    value >>= 7;
    while value != 0 {
        pos -= 1;
        value -= 1;
        vbuf[pos] = 128 | ((value & 127) as u8);
        value >>= 7;
    }

    pos
}

#[repr(transparent)]
struct VarintBuf([u8; 24]);

impl VarintBuf {
    fn new() -> Self {
        Self([0; 24])
    }
}

impl Deref for VarintBuf {
    type Target = [u8; 24];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for VarintBuf {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}
