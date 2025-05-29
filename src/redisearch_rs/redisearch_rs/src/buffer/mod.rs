use std::ptr::NonNull;

use buffer::BufferWriter;

#[repr(C)]
pub struct BufferWriterRS(pub BufferWriter);

#[unsafe(no_mangle)]
pub unsafe extern "C" fn NewBufferWriter_RS(buf: *mut ffi::Buffer) -> *mut BufferWriterRS {
    let b = BufferWriter::from_ffi_buffer(NonNull::new(buf).unwrap());
    let b = BufferWriterRS(b);
    let b = Box::new(b);
    Box::into_raw(b)
}
