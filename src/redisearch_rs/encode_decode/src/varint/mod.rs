mod c_api;
mod vector_writer;

use crate::FieldMask;

use std::{
    io::{self, Read, Write},
    ops::{Deref, DerefMut},
};

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
pub fn write<W>(value: u32, mut write: W) -> io::Result<usize>
where
    W: Write,
{
    let mut variant = VarintBuf::new();
    let pos = encode(value, &mut variant);
    write.write(&variant[pos..])
}

/// Encode a FieldMask into a varint format and write it to the given writer.
pub fn write_field_mask<W>(value: FieldMask, mut write: W) -> io::Result<usize>
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
