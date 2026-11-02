/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Shared outcome and error types for the fork-GC scanners.

use std::error::Error;

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
/// Every way the child's frame stream can fail us is a [`HandleError::Codec`].
/// [`HandleError::SpecDeleted`] covers the index being dropped before the parent
/// could apply the delta. Scanner-specific apply failures are represented by
/// [`HandleError::ApplyError`].
#[derive(Debug, thiserror::Error)]
pub enum HandleError {
    /// The child's frame stream could not be read or decoded; the child likely
    /// crashed or is speaking a different protocol.
    #[error("{msg}")]
    Codec {
        /// The step that failed, phrased for a log line.
        msg: &'static str,
        /// The underlying failure, type-erased.
        #[source]
        source: Box<dyn Error + Send + Sync>,
    },

    /// The weak spec reference could not be promoted; the index was dropped before apply.
    #[error("the index spec was deleted before the delta could be applied")]
    SpecDeleted,

    /// A scanner-specific failure while applying a delta.
    #[error("{0}")]
    ApplyError(&'static str),
}

impl HandleError {
    /// Build a [`HandleError::Codec`] from the step that failed and its cause.
    ///
    /// `source` takes anything convertible into a boxed error, which includes
    /// `&'static str` — use that for protocol violations we detect ourselves
    /// and that have no underlying error to wrap.
    pub fn codec(msg: &'static str, source: impl Into<Box<dyn Error + Send + Sync>>) -> Self {
        Self::Codec {
            msg,
            source: source.into(),
        }
    }
}
