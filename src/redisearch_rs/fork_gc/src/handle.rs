/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared outcome and error types for the fork-GC scanners.
//!
//! Every scanner's parent-side handler returns the same [`HandleOutcome`] and a
//! [`HandleError`] that differs only in its scanner-specific failures. The
//! frame-protocol failures common to all scanners are captured as dedicated
//! variants; the rest go in [`HandleError::Custom`].

use std::io;

/// Successful outcome of a scanner's parent-side handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HandleOutcome {
    /// A unit of GC data was received and applied; iteration may continue.
    Collected,
    /// The child sent the terminator; iteration is complete.
    Done,
}

/// Error returned by a scanner's parent-side handler and its sub-functions.
///
/// The failure modes shared by every scanner are captured as dedicated variants.
/// A scanner's own failure modes live in [`HandleError::Custom`], parameterised
/// by a scanner-specific type `C`.
#[derive(Debug, thiserror::Error)]
pub enum HandleError<C> {
    /// Reading a frame from the child's pipe failed; the child likely crashed.
    #[error("pipe read error")]
    PipeReadError(#[from] io::Error),

    /// A frame's MessagePack payload could not be decoded; the child likely crashed.
    #[error("deserialisation failed")]
    DeserializationFailed(#[from] rmp_serde::decode::Error),

    /// A valid frame of an unexpected kind arrived where a specific frame was required.
    #[error("child sent a valid frame of an unexpected kind")]
    UnexpectedFrame,

    /// The weak spec reference could not be promoted; the index was dropped before apply.
    #[error("the index spec was deleted before the delta could be applied")]
    SpecDeleted,

    /// A scanner-specific failure.
    #[error(transparent)]
    Custom(C),
}
