/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Plain enums mirroring the VecSim C constants.

use ffi::{
    VecSimBool, VecSimBool_VecSimBool_FALSE, VecSimBool_VecSimBool_TRUE, VecSimQueryReply_Order,
    VecSimQueryReply_Order_BY_ID, VecSimQueryReply_Order_BY_SCORE,
};

/// The disk-HNSW reranking preference carried in a query's
/// `hnswDiskRuntimeParams.shouldRerank`.
///
/// Mirrors the tri-state `VecSimBool`: a disk query can leave the flag unset
/// (no preference) or pin it on or off explicitly.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DiskRerank {
    /// Rerank the disk adhoc-BF top-k with exact distances (`VecSimBool_TRUE`).
    Enabled,
    /// Skip reranking (`VecSimBool_FALSE`).
    Disabled,
    /// No preference set by the query (`VecSimBool_UNSET`).
    Unset,
}

impl DiskRerank {
    /// Map a raw `shouldRerank` value. An unrecognised value is treated as
    /// [`Unset`](Self::Unset), the neutral default.
    #[expect(nonstandard_style, reason = "VecSimBool_VecSimBool comes from FFI")]
    pub(crate) const fn from_raw(value: VecSimBool) -> Self {
        match value {
            VecSimBool_VecSimBool_TRUE => DiskRerank::Enabled,
            VecSimBool_VecSimBool_FALSE => DiskRerank::Disabled,
            _ => DiskRerank::Unset,
        }
    }
}

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
