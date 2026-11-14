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
/// Every way the child's frame stream can fail us is a [`GcError::Codec`].
/// [`GcError::SpecDeleted`] covers the index being dropped before the parent
/// could apply the delta, [`GcError::TargetGone`] the index a delta was for
/// being removed or replaced since its scan. Other scanner-specific apply
/// failures are [`GcError::ApplyError`].
#[derive(Debug, thiserror::Error)]
pub enum GcError {
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

    /// The index a delta was for was removed, or replaced by a new one, between the
    /// scan and the apply.
    ///
    /// The only error a caller may skip a delta on and still count its deleted IDs
    /// collected: a removed index's entries went with it, and a replacement holds
    /// none of the IDs deleted before the scan, since an ID is never reused.
    #[error("{0}")]
    TargetGone(&'static str),

    /// A scanner-specific failure while applying a delta.
    #[error("{0}")]
    ApplyError(&'static str),

    /// The doc table does not track deleted documents, so an in-process cycle has
    /// nothing to find garbage with.
    #[error("the document table does not track deleted documents")]
    DeletionsUntracked,
}

impl GcError {
    /// Build a [`GcError::Codec`] from the step that failed and its cause.
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
