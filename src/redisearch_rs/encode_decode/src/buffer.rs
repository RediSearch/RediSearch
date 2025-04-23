use std::{io::Cursor, mem::ManuallyDrop};

/// Redefines the `Buffer` struct from `buffer.h`
#[repr(C)]
pub(crate) struct Buffer {
    pub data: *mut u8,
    pub cap: usize,
    pub offset: usize,
}

/// Redefines the `BufferReader` struct from `buffer.h`
#[repr(C)]
pub(crate) struct BufferReader {
    pub buf: *const Buffer,
    pub pos: usize,
}

impl BufferReader {
    pub fn as_cursor(&mut self) -> Cursor<&[u8]> {
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &*self.buf };
        // Safety: All invariants of `std::slice::from_raw_parts` should hold here if the C side
        // doesn't do something naughty.
        let buffer_slice = unsafe { std::slice::from_raw_parts(buffer.data, buffer.cap) };
        let mut cursor = Cursor::new(buffer_slice);
        cursor.set_position(self.pos as u64);
        cursor
    }
}

/// Redefines the `BufferWriter` struct from `buffer.h`
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub(crate) struct BufferWriter {
    pub buf: *mut Buffer,
    pub pos: *mut u8,
}

impl From<BufferWriter> for Cursor<Vec<u8>> {
    fn from(writer: BufferWriter) -> Self {
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &mut *writer.buf };
        // Safety: According the docs of `Vec::from_raw_parts`, we shouldn't be doing this but it
        // works and all this is hopefully transient anyway until all users of `BufferWriter` are
        // oxidized and `BufferWriter` can then be dropped along with module.
        let buffer_vec = unsafe { Vec::from_raw_parts(buffer.data, buffer.offset, buffer.cap) };
        // Safety: Both pointers here should be valid, if C side doesn't do something naughty.
        let pos = unsafe { writer.pos.offset_from(buffer.data) };
        let mut cursor = Cursor::new(buffer_vec);
        cursor.set_position(pos as u64);
        cursor
    }
}

impl From<Cursor<Vec<u8>>> for BufferWriter {
    fn from(cursor: Cursor<Vec<u8>>) -> Self {
        let pos = cursor.position() as usize;
        let mut buffer_vec = ManuallyDrop::new(cursor.into_inner());
        let buffer_ptr = buffer_vec.as_mut_ptr();

        BufferWriter {
            buf: Box::into_raw(Box::new(Buffer {
                data: buffer_ptr,
                cap: buffer_vec.capacity(),
                offset: buffer_vec.len(),
            })),
            // Safety: We got both the pointer and the position from the cursor, so we know that
            // the pointer is valid and the position is within the bounds of the buffer.
            pos: unsafe { buffer_ptr.add(pos) },
        }
    }
}
