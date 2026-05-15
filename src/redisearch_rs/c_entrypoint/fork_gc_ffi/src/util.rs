/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{ffi::c_void, ptr};

use fork_gc::Frame;

/// Consume the frame, producing the `(buf, len)` pair that the C
/// `FGC_recvBuffer` and `recvFieldHeader` API exposes through its
/// out-parameters.
///
/// - [`Frame::Terminator`] → `(RECV_BUFFER_EMPTY, usize::MAX)`.
/// - [`Frame::Empty`] → `(null, 0)`.
/// - [`Frame::Data`] → leaks the boxed payload, transferring ownership
///   to the caller. The caller is responsible for releasing it with
///   [`super::FGC_freeBuffer`].
pub(crate) fn frame_into_c_buffer(frame: Frame<Box<[u8]>>) -> (*mut c_void, usize) {
    match frame {
        Frame::Terminator => {
            // SAFETY: `RECV_BUFFER_EMPTY` is a static pointer defined in `pipe.c`.
            (unsafe { super::RECV_BUFFER_EMPTY }, usize::MAX)
        }
        Frame::Empty => (ptr::null_mut(), 0),
        Frame::Data(data) => {
            let len = data.len();
            let ptr = Box::into_raw(data) as *mut u8;
            (ptr.cast(), len)
        }
    }
}
