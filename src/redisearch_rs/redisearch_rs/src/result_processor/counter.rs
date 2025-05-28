/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use result_processor::{counter::Counter, ffi};

#[unsafe(no_mangle)]
pub extern "C" fn RPCounter_New() -> *mut ffi::Header {
    let rp = Box::pin(ffi::ResultProcessor::new(Counter::new()));
    eprintln!("RPCounter_New self_addr={rp:p} self={rp:?}");

    // Safety: TODO
    unsafe { ffi::ResultProcessor::into_ptr(rp) }.cast()
}
