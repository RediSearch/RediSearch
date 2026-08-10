/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

use std::{error::Error, ffi::c_void, ptr};

use fork_gc::{Frame, HandleError, HandleOutcome};
use nul_terminated_bytes::NulTerminatedBytes;
use tracing::Level;
use tracing_log_error::log_error;

use crate::FGCError;

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

/// Convert a scanner's parent-side handler result into the [`FGCError`] the C
/// caller expects, logging the failures that warrant it.
///
/// `scanner` names the scanner in the log line, for example `"existing docs"`.
pub(crate) fn into_fgc_error<C: Error>(
    result: Result<HandleOutcome, HandleError<C>>,
    scanner: &str,
) -> FGCError {
    match result {
        Ok(HandleOutcome::Collected) => FGCError::Collected,
        Ok(HandleOutcome::Done) => FGCError::Done,
        Err(e @ HandleError::Codec { .. }) => {
            log_error!(e, level: Level::WARN, "ForkGC: {scanner}: codec error");
            FGCError::ChildError
        }
        Err(HandleError::SpecDeleted) => FGCError::SpecDeleted,
        Err(e @ HandleError::Custom(_)) => {
            log_error!(e, level: Level::WARN, "ForkGC: {scanner}: apply error");
            FGCError::ParentError
        }
    }
}
