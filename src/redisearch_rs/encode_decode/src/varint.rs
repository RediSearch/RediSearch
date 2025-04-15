use crate::{FieldMask, buffer::BufferReader};

use std::io::{self, Read};

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
