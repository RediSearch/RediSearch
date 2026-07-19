/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Plain enums mirroring the VecSim C constants.

use ffi::{VecSimQueryReply_Order, VecSimQueryReply_Order_BY_ID, VecSimQueryReply_Order_BY_SCORE};

/// Sort order for results returned by a VecSim query.
///
/// Only `ByScore` and `ById` are accepted by the public VecSim C API
/// (`VecSimIndex_TopKQuery`, `VecSimBatchIterator_Next`). The `BY_SCORE_THEN_ID`
/// constant exists in `VecSimQueryReply_Order` but is reserved for internal
/// use inside VecSim's tiered algorithms and is rejected by both entry points
/// with an assertion failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplyOrder {
    ByScore,
    ById,
}

impl ReplyOrder {
    pub(crate) const fn as_raw(self) -> VecSimQueryReply_Order {
        match self {
            ReplyOrder::ByScore => VecSimQueryReply_Order_BY_SCORE,
            ReplyOrder::ById => VecSimQueryReply_Order_BY_ID,
        }
    }
}

/// Errors produced by VecSim query calls.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum QueryError {
    /// VecSim reported `VecSim_QueryReply_TimedOut` via the query reply code.
    TimedOut,
}
