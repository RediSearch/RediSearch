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
use string_utils::NulTerminatedBytes;

/// Consume the frame, producing the `(buf, len)` pair that the C
/// `FGC_recvBuffer` and `recvFieldHeader` API exposes through its
/// out-parameters.
///
/// - [`Frame::Terminator`] → `(null, SIZE_MAX)`. Callers detect end-of-stream
///   by checking `*len == SIZE_MAX`.
/// - [`Frame::Empty`] → `(null, 0)`.
/// - [`Frame::Data`] → transfers ownership of the buffer to the caller.
///   The returned length is the payload length excluding the NUL terminator.
///   The caller is responsible for releasing it with [`super::FGC_freeBuffer`].
pub(crate) fn frame_into_c_buffer(frame: Frame<NulTerminatedBytes>) -> (*mut c_void, usize) {
    match frame {
        Frame::Terminator => (ptr::null_mut(), usize::MAX),
        Frame::Empty => (ptr::null_mut(), 0),
        Frame::Data(data) => {
            let (ptr, len) = data.into_inner().into_parts();
            (ptr.cast(), len)
        }
    }
}
