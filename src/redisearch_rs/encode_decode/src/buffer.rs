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
    pub fn to_cursor(&mut self) -> std::io::Cursor<&[u8]> {
        let buffer_slice = unsafe {
            let buffer = &*self.buf;
            std::slice::from_raw_parts(buffer.data, buffer.cap)
        };
        let mut cursor = std::io::Cursor::new(buffer_slice);
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

impl BufferWriter {
    pub fn into_cursor(self) -> std::io::Cursor<Vec<u8>> {
        let (buffer_vec, pos) = unsafe {
            let buffer = &mut *self.buf;
            let buffer_vec = Vec::from_raw_parts(buffer.data, buffer.offset, buffer.cap);
            let pos = buffer.data.offset_from(self.pos);

            (buffer_vec, pos)
        };
        let mut cursor = std::io::Cursor::new(buffer_vec);
        cursor.set_position(pos as u64);
        cursor
    }
}
