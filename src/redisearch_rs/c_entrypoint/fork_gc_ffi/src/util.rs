/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, ptr};

use fork_gc::reader::RecvFrame;

// Sentinel pointer defined in `src/fork_gc/pipe.c`, compared by pointer
// identity in C callers (e.g. `recvFieldHeader`) to detect end-of-stream
// frames returned through `FGC_recvBuffer`'s `buf` out-parameter.
unsafe extern "C" {
    static RECV_BUFFER_EMPTY: *mut c_void;
}

/// Consume the frame, producing the `(buf, len)` pair that the C
/// `FGC_recvBuffer` API exposes through its out-parameters.
///
/// - [`RecvFrame::Terminator`] → `(terminator, usize::MAX)`. The
///   caller supplies the sentinel pointer that C-side callers
///   (e.g. `recvFieldHeader`) compare by identity to detect
///   end-of-stream.
/// - [`RecvFrame::Empty`] → `(null, 0)`.
/// - [`RecvFrame::Data`] → the payload is copied into a fresh
///   `len + 1`-byte, NUL-terminated allocation via the global
///   allocator (which routes through `RedisModule_Alloc`). Ownership
///   of the pointer is transferred to the caller, which is expected
///   to release it with the Redis module allocator's `rm_free`.
pub(crate) fn frame_into_c_buffer(frame: RecvFrame) -> (*mut c_void, usize) {
    match frame {
        RecvFrame::Terminator => {
            // SAFETY: `RECV_BUFFER_EMPTY` is a static pointer defined in `pipe.c`.
            (unsafe { RECV_BUFFER_EMPTY }, usize::MAX)
        }
        RecvFrame::Empty => (ptr::null_mut(), 0),
        RecvFrame::Data(data) => {
            let len = data.len();
            // Allocate `len + 1` with a trailing NUL so C callers
            // can treat the result as a C string, matching the
            // original `rm_malloc(len + 1); buf[len] = 0;` shape.
            let mut owned = Vec::<u8>::with_capacity(len + 1);
            owned.extend_from_slice(&data);
            owned.push(0);
            let leaked: &'static mut [u8] = Box::leak(owned.into_boxed_slice());
            (leaked.as_mut_ptr().cast(), len)
        }
    }
}
