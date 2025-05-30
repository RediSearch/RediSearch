#[repr(C)]
pub struct BufferWriter(pub buffer::BufferWriter);

/// Returns a `BufferWriter` that wraps the given `ffi::Buffer`.
///
/// # Safety
///
/// The `buf` pointer must point to a valid `ffi::Buffer` instance and cannot be written to or
/// be invalidaded while the `BufferWriter` is in use.
#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewBufferWriter(buf: *mut ffi::Buffer) -> *mut BufferWriter {
    let b = unsafe { buffer::BufferWriter::from_ffi_buffer(buf) };
    let b = BufferWriter(b);
    let b = Box::new(b);
    Box::into_raw(b)
}
