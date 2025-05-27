mod counter;

use result_processor::{ResultProcessor, SearchResult};
use std::{mem::MaybeUninit, ptr::NonNull};

unsafe extern "C" fn result_processor_next<T: ResultProcessor>(
    ptr: NonNull<result_processor::Header>,
    mut out: NonNull<MaybeUninit<SearchResult>>,
) -> i32 {
    let mut me = ptr.cast::<T>();

    let me = unsafe { me.as_mut() };

    match me.next() {
        Ok(Some(res)) => {
            unsafe { out.as_mut() }.write(res);
            return 0;
        }
        Ok(None) => {
            // TODO convert into ret code
            return 1;
        }
        Err(_) => {
            // TODO convert rust error into ret code
            return 1;
        }
    }
}

unsafe extern "C" fn result_processor_free<T: ResultProcessor>(
    ptr: NonNull<result_processor::Header>,
) {
    let me = ptr.cast::<T>();

    // Safety: TODO
    drop(unsafe { Box::from_raw(me.as_ptr()) });
}

fn result_processor_alloc<T: ResultProcessor>(t: T) -> *mut T {
    Box::into_raw(Box::new(t))
}
