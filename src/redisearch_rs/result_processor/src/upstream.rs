use std::{mem::MaybeUninit, ptr::NonNull};

use crate::{SearchResult, header::Header};

/// Just a type that fowards the `ResultProcessor` method to the unsafe `ResultProcessorNext` function
pub struct Upstream {
    pub hdr: NonNull<Header>,
}

impl Upstream {
    pub fn next(&mut self) -> crate::Result<Option<SearchResult>> {
        let next = unsafe { self.hdr.as_mut() }.next;

        let mut res: MaybeUninit<SearchResult> = MaybeUninit::uninit();

        let ret = unsafe { next(NonNull::from(self.hdr), NonNull::from(&mut res)) };

        const RS_RESULT_OK: i32 = 0;

        if ret == RS_RESULT_OK {
            // Safety: next returned `RS_RESULT_OK` and guarantees the ptr is "filled with valid data"
            Ok(Some(unsafe { res.assume_init() }))
        } else {
            todo!("map the return code to rust error")
        }
    }
}
