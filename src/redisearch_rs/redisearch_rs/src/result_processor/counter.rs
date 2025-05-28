use result_processor::{
    counter::Counter,
    header::{Header, ResultProcessorWrapper},
};

#[unsafe(no_mangle)]
extern "C" fn RPCounter_New() -> *mut Header {
    ResultProcessorWrapper {
        base: Header::for_processor::<Counter>(),
        payload: Counter::new(),
    }
    .into_ptr()
}
