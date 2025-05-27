use result_processor::{counter::Counter, ffi};

#[unsafe(no_mangle)]
extern "C" fn RPCounter_New() -> *mut ffi::Header {
    let rp = Box::pin(ffi::ResultProcessor::new(Counter::new()));

    // Safety: TODO
    unsafe { ffi::ResultProcessor::into_ptr(rp) }.cast()
}
