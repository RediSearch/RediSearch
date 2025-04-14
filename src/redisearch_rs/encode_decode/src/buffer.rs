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
