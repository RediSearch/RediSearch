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
    pub fn to_cursor(&mut self) -> Cursor<&[u8]> {
        let buffer_slice = unsafe {
            let buffer = &*self.buf;
            std::slice::from_raw_parts(buffer.data, buffer.cap)
        };
        let mut cursor = Cursor::new(buffer_slice);
        cursor.set_position(self.pos as u64);
        cursor
    }
}

/// Redefines the `BufferWriter` struct from `buffer.h`
#[repr(C)]
pub(crate) struct BufferWriter {
    pub buf: *mut Buffer,
    pub pos: *mut u8,
}

impl From<BufferWriter> for Cursor<Vec<u8>> {
    fn from(writer: BufferWriter) -> Self {
        let (buffer_vec, pos) = unsafe {
            let buffer = &mut *writer.buf;
            let buffer_vec = Vec::from_raw_parts(buffer.data, buffer.offset, buffer.cap);
            let pos = writer.pos.offset_from(buffer.data);

            (buffer_vec, pos)
        };
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
            pos: unsafe { buffer_ptr.add(pos) },
        }
    }
}
