use std::{
    ffi::c_void,
    io::Cursor,
    ptr::{NonNull, copy_nonoverlapping},
    slice,
};

use redis_module::RedisModule_Realloc;

/// Redefines the `Buffer` struct from `buffer.h`
///
/// Allocated by C, we never want to free it.
#[repr(C)]
pub struct Buffer {
    data: NonNull<u8>,
    capacity: usize,
    len: usize,
}

/// Redefines the `BufferReader` struct from `buffer.h`
#[repr(C)]
pub struct BufferReader {
    pub buf: NonNull<Buffer>,
    pub pos: usize,
}

impl BufferReader {
    pub fn as_cursor(&mut self) -> Cursor<&[u8]> {
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { self.buf.as_mut() };
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let data = unsafe { buffer.data.as_mut() };
        // Safety: All invariants of `std::slice::from_raw_parts` should hold here if the C side
        // doesn't do something naughty.
        let buffer_slice = unsafe { slice::from_raw_parts(data, buffer.capacity) };
        let mut cursor = Cursor::new(buffer_slice);
        cursor.set_position(self.pos as u64);
        cursor
    }
}

impl Buffer {
    /// Creates a new `Buffer` with the given pointer, length, and capacity.
    ///
    /// # Panics
    ///
    /// Panics if `len` is greater than `capacity`.
    pub fn new(data: NonNull<u8>, len: usize, capacity: usize) -> Self {
        assert!(len <= capacity, "len must not exceed capacity");
        Self {
            data,
            len,
            capacity,
        }
    }

    /// The internal buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        // Safety: `self.ptr` is a valid pointer, if C side gave us one.
        let data = unsafe { self.data.as_ref() };
        // Safety: `self.len` is a valid length, if C side didn't mess up.
        unsafe { slice::from_raw_parts(data, self.len) }
    }

    /// The internal buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: `self.ptr` is a valid pointer, if C side gave us one.
        let data = unsafe { self.data.as_mut() };
        // Safety: `self.len` is a valid length, if C side didn't mess up.
        unsafe { slice::from_raw_parts_mut(data, self.len) }
    }

    /// The length of the buffer.
    pub fn len(&self) -> usize {
        self.len
    }

    /// If the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The capacity of the buffer.
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// The remaining capacity of the buffer.
    pub fn remaining_capacity(&self) -> usize {
        self.capacity - self.len
    }

    /// Advance the buffer by `n` bytes.
    ///
    /// # Panics
    ///
    /// Panics if `n` exceeds the remaining capacity of the buffer.
    pub fn advance(&mut self, n: usize) {
        assert!(n <= self.remaining_capacity());
        self.len += n;
    }
}

/// Redefines the `BufferWriter` struct from `buffer.h`
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferWriter {
    pub buf: NonNull<Buffer>,
    pub cursor: NonNull<u8>,
}

impl std::io::Write for BufferWriter {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { self.buf.as_mut() };
        // Safety: All invariants of `std::slice::from_raw_parts` should hold here if the C side
        // does everything correctly.
        let pos = unsafe { self.cursor.byte_offset_from(buffer.data) } as usize;
        // Check if pos + bytes.len() exceeds the buffer capacity and reallocate if necessary.
        if pos + bytes.len() > buffer.capacity {
            let extra = bytes.len() - (buffer.capacity - pos);
            let new_capacity = buffer.capacity + extra;
            // Safety: Static-mutable requires `unsafe` to be accessed.
            let realloc = unsafe { RedisModule_Realloc }.unwrap();
            // Safety: Calling into C, so its unsafe by definition.
            let new_ptr = unsafe { realloc(buffer.data.as_ptr() as *mut c_void, new_capacity) };
            buffer.data = NonNull::new(new_ptr as *mut u8).ok_or_else(|| {
                std::io::Error::new(
                    std::io::ErrorKind::OutOfMemory,
                    "Failed to reallocate buffer",
                )
            })?;
            buffer.capacity = new_capacity;
            // Safety: All invariants of `std::ptr::NonNull::add` should hold here.
            self.cursor = unsafe { buffer.data.add(pos) };
        }

        // Now copy the bytes into the buffer.

        let src = bytes.as_ptr();
        let dest = self.cursor.as_ptr();
        // Safety:
        // * We just created `src` and `dest` using safe API.
        // * `bytes.len()` is less than capacity - pos.
        unsafe { copy_nonoverlapping(src, dest, bytes.len()) };
        // Update the buffer length.
        buffer.len = pos + bytes.len();
        // Update the position.
        // Safety: All invariants of `std::ptr::NonNull::add` should hold here.
        self.cursor = unsafe { self.cursor.add(bytes.len()) };

        Ok(bytes.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
