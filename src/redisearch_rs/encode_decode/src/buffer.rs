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
            std::slice::from_raw_parts(buffer.data, buffer.offset)
        };
        let mut cursor = std::io::Cursor::new(buffer_slice);
        cursor.set_position(self.pos as u64);
        cursor
    }
}
