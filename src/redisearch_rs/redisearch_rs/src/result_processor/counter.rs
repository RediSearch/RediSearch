use super::{result_processor_alloc, result_processor_free, result_processor_next};
use result_processor::{ResultProcessorType, counter::Counter};
use std::ptr::NonNull;

#[unsafe(no_mangle)]
extern "C" fn RPCounter_New() -> NonNull<result_processor::Header> {
    let header = result_processor::Header::new(
        ResultProcessorType::Counter,
        result_processor_next::<Counter>,
        result_processor_free::<Counter>,
    );

    let ptr = result_processor_alloc(Counter::new(header));

    // Safety: we just allocated the ptr, it cannot be null
    unsafe { NonNull::new_unchecked(ptr) }.cast::<result_processor::Header>()
}
