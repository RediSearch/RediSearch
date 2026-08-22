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
use nul_terminated_bytes::NulTerminatedBytes;

/// Consume the frame, producing the `(buf, len)` pair that the C
/// `FGC_recvBuffer` and `recvFieldHeader` API exposes through its
/// out-parameters.
///
/// - [`Frame::Terminator`] → `(null, SIZE_MAX)`. Callers detect end-of-stream
///   by checking `*len == SIZE_MAX`.
/// - [`Frame::Empty`] → `(null, 0)`.
/// - [`Frame::Data`] → hands the buffer off to the caller, which is
///   responsible for releasing it with [`super::FGC_freeBuffer`]. The
///   returned length is the payload length excluding the NUL terminator.
pub(crate) fn frame_into_c_buffer(frame: Frame<NulTerminatedBytes>) -> (*mut c_void, usize) {
    match frame {
        Frame::Terminator => (ptr::null_mut(), usize::MAX),
        Frame::Empty => (ptr::null_mut(), 0),
        Frame::Data(data) => {
            let (ptr, len) = data.into_inner().into_raw_parts();
            (ptr.cast(), len)
        }
    }
}
