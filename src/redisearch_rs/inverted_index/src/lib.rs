/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

pub mod codec;
pub mod controlled_cursor;
pub mod debug;
pub(crate) mod gc;
mod index;
mod index_result;
pub mod reader;
#[doc(hidden)]
pub mod test_utils;

// Re-export codec traits at crate root.
pub use codec::*;

// Re-export index types.
pub use index::*;

// Re-export GC types.
pub use gc::{GcApplyInfo, GcScanDelta};

// Re-export result types.
pub use index_result::{
    RSAggregateResult, RSAggregateResultIter, RSIndexResult, RSOffsetSlice, RSOffsetVector,
    RSQueryTerm, RSResultData, RSResultKind, RSResultKindMask, RSTermRecord,
    ResultMetrics_Reset_func,
};

// Re-export reader types.
pub use reader::{IndexReader, IndexReaderCore, NumericFilter, NumericReader, TermReader};

// Re-export filter types.
pub use reader::{FilterGeoReader, FilterMaskReader, FilterNumericReader, ReadFilter};

// Re-export FFI types.
pub use ffi::{t_docId, t_fieldMask};

#[cfg(test)]
mod tests;
