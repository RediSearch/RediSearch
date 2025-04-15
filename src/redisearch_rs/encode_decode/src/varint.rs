use crate::buffer::BufferReader;

use std::io::Read;

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

//
// The Rust API goes here. This part will be used by the Rust code and will be used by the C code
// above.
//

/// Read an encoded integer from the given reader.
pub fn read<R>(mut read: R) -> std::io::Result<u32>
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
