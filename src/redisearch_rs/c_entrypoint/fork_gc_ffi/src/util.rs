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

/// Consume the frame, producing the `(buf, len)` pair that the C
/// `FGC_recvBuffer` API exposes through its out-parameters.
///
/// - [`RecvFrame::Terminator`] → `(RECV_BUFFER_EMPTY, usize::MAX)`.
/// - [`RecvFrame::Empty`] → `(null, 0)`.
/// - [`RecvFrame::Data`] → leaks the boxed payload, transferring ownership
///   to the caller. The caller is responsible for releasing it with
///   [`super::FGC_freeBuffer`].
pub(crate) fn frame_into_c_buffer(frame: RecvFrame) -> (*mut c_void, usize) {
    match frame {
        RecvFrame::Terminator => {
            // SAFETY: `RECV_BUFFER_EMPTY` is a static pointer defined in `pipe.c`.
            (unsafe { super::RECV_BUFFER_EMPTY }, usize::MAX)
        }
        RecvFrame::Empty => (ptr::null_mut(), 0),
        RecvFrame::Data(data) => {
            let len = data.len();
            let ptr = Box::into_raw(data) as *mut u8;
            (ptr.cast(), len)
        }
    }
}
