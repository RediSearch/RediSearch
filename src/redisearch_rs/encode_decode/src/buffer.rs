use std::{ffi::c_void, io::Cursor, ptr::NonNull, slice};

use redis_module::RedisModule_Realloc;

/// Redefines the `Buffer` struct from `buffer.h`
#[repr(C)]
pub struct Buffer {
    pub data: *mut u8,
    pub cap: usize,
    pub offset: usize,
}

/// Redefines the `BufferReader` struct from `buffer.h`
#[repr(C)]
pub struct BufferReader {
    pub buf: *const Buffer,
    pub pos: usize,
}

impl BufferReader {
    pub fn as_cursor(&mut self) -> Cursor<&[u8]> {
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &*self.buf };
        // Safety: All invariants of `std::slice::from_raw_parts` should hold here if the C side
        // doesn't do something naughty.
        let buffer_slice = unsafe { slice::from_raw_parts(buffer.data, buffer.cap) };
        let mut cursor = Cursor::new(buffer_slice);
        cursor.set_position(self.pos as u64);
        cursor
    }
}

/// Redefines the `BufferWriter` struct from `buffer.h`
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BufferWriter {
    pub buf: *mut Buffer,
    pub pos: *mut u8,
}

/// Allocated by C, we never want to free it.
/// Morally, a `&mut Vec<u8>`.
pub struct CBuffer {
    ptr: *mut u8,
    len: usize,
    capacity: usize,
}

impl CBuffer {
    /// Creates a new `CBuffer` with the given pointer, length, and capacity.
    ///
    /// # Panics
    ///
    /// Panics if `len` is greater than `capacity`.
    pub fn new(ptr: *mut u8, len: usize, capacity: usize) -> Self {
        assert!(len <= capacity, "len must not exceed capacity");
        Self { ptr, len, capacity }
    }

    /// The internal buffer as a slice.
    pub fn as_slice(&self) -> &[u8] {
        // Safety: `self.ptr` is a valid pointer, if C side gave us one.
        unsafe { slice::from_raw_parts(self.ptr, self.len) }
    }

    /// The internal buffer as a mutable slice.
    pub fn as_mut_slice(&mut self) -> &mut [u8] {
        // Safety: `self.ptr` is a valid pointer, if C side gave us one.
        unsafe { slice::from_raw_parts_mut(self.ptr, self.len) }
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

    /// Append a slice to the buffer, growing it if necessary.
    pub fn extend_from_slice(&mut self, additional: &[u8]) {
        let additional_len = additional.len();
        if self.remaining_capacity() < additional_len && !self.try_reserve(additional_len) {
            panic!("Failed to reserve additional capacity");
        }

        let src = additional.as_ptr();
        // Safety:
        // *`self.ptr` is a valid pointer, if C side gave us one.
        // * `self.len` is less than or equal to `self.capacity`.
        let dest = unsafe { self.ptr.add(self.len) };
        // Safety:
        // * We just created `src` from `additional` reference, so it's valid.
        // * `additional_len` is less than `self.remaining_capacity()`.
        unsafe { std::ptr::copy_nonoverlapping(src, dest, additional_len) };
        self.len += additional_len;
    }

    /// Try to reserve additional capacity in the buffer.
    pub fn try_reserve(&mut self, additional: usize) -> bool {
        if self.remaining_capacity() >= additional {
            return true;
        }

        let new_capacity = self.len + additional;
        // Safety: Static-mutable requires `unsafe` to be accessed.
        let realloc = unsafe { RedisModule_Realloc }.unwrap();

        // Safety: Calling into C, so its unsafe by definition.
        let new_ptr = unsafe { realloc(self.ptr as *mut c_void, new_capacity) };
        if new_ptr.is_null() {
            return false;
        }
        self.ptr = new_ptr as *mut u8;
        self.capacity = new_capacity;
        true
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

    /// Hands over the buffer (back) to the C side inside the given [`BufferWriter`].
    // FIXME: Better name please?
    pub fn to_buffer_writer(self, mut writer: NonNull<BufferWriter>) {
        // Safety: `writer` is a valid pointer, if C side doesn't do something naughty.
        let writer_mut = unsafe { writer.as_mut() };
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &mut *writer_mut.buf };
        // Safety: `data` is a valid pointer, if C side doesn't do something naughty.
        buffer.data = self.ptr;
        buffer.cap = self.capacity;
        buffer.offset = self.len;
        // Safety: `pos` is a valid pointer, if C side doesn't do something naughty.
        writer_mut.pos = unsafe { self.ptr.add(self.len) };
    }
}

impl From<NonNull<BufferWriter>> for CBuffer {
    fn from(value: NonNull<BufferWriter>) -> Self {
        // Safety: `value` is a valid pointer, if C side doesn't do something naughty.
        let writer = unsafe { value.as_ref() };
        if writer.buf.is_null() {
            panic!("BufferWriter.buf is null");
        }
        // Safety: `buf` is a valid pointer, if C side doesn't do something naughty.
        let buffer = unsafe { &*writer.buf };
        // Safety: `data` is a valid pointer, if C side doesn't do something naughty.
        let data = unsafe { slice::from_raw_parts(buffer.data, buffer.cap) };
        let ptr = data.as_ptr() as *mut u8;

        Self::new(ptr, buffer.offset, buffer.cap)
    }
}

impl std::io::Write for CBuffer {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.extend_from_slice(buf);
        Ok(buf.len())
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
