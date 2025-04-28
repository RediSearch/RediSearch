use std::{io::Cursor, ptr::NonNull};

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

pub(crate) struct BufferWriterWrapper {
    writer: NonNull<BufferWriter>,
    cursor: Cursor<Vec<u8>>,
}

impl From<NonNull<BufferWriter>> for BufferWriterWrapper {
    fn from(mut writer: NonNull<BufferWriter>) -> Self {
        // Safety: `writer` is a valid pointer, if C side doesn't do something naughty.
        let writer_mut = unsafe { writer.as_mut() };
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &mut *writer_mut.buf };
        // Safety: According the docs of `Vec::from_raw_parts`, we shouldn't be doing this but it
        // works and all this is hopefully transient anyway until all users of `BufferWriter` are
        // oxidized and `BufferWriter` can then be dropped along with module.
        let buffer_vec = unsafe { Vec::from_raw_parts(buffer.data, buffer.offset, buffer.cap) };
        // Safety: Both pointers here should be valid, if C side doesn't do something naughty.
        let pos = unsafe { writer_mut.pos.offset_from(buffer.data) };
        let mut cursor = Cursor::new(buffer_vec);
        cursor.set_position(pos as u64);

        BufferWriterWrapper { writer, cursor }
    }
}

impl std::io::Write for BufferWriterWrapper {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let bytes_written = self.cursor.write(buf)?;

        // Safety: This was already wetted by `BufferWriterWrapper::from`.
        let writer_mut = unsafe { self.writer.as_mut() };
        // Safety: This was already wetted by `BufferWriterWrapper::from`.
        let buffer = unsafe { &mut *writer_mut.buf };
        let buffer_ptr = self.cursor.get_mut().as_mut_ptr();
        buffer.data = buffer_ptr;
        buffer.cap = self.cursor.get_mut().capacity();
        buffer.offset = self.cursor.get_ref().len();
        let pos = self.cursor.position() as usize;
        writer_mut.pos = unsafe { buffer_ptr.add(pos) };

        Ok(bytes_written)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

impl Drop for BufferWriterWrapper {
    fn drop(&mut self) {
        // The C side is responsible for freeing the buffer, so we need to ensure we don't free it.
        let mut buffer = vec![];
        std::mem::swap(self.cursor.get_mut(), &mut buffer);
        buffer.leak();
    }
}
