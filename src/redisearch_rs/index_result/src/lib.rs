/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

mod aggregate;
mod core;
mod kind;
mod metrics;
mod offsets;
mod result_data;
mod term_record;

pub use query_term::RSQueryTerm;

pub use self::core::{
    RSIndexResult, RawIndexResult, RawIndexResultBuilder, RawTermResultBuilder,
    SuspendedIndexResult,
};
pub use aggregate::{RSAggregateResult, RSAggregateResultIter, RawAggregateResult};
pub use kind::{RSResultKind, RSResultKindMask};
pub use metrics::{MetricEntry, MetricsSlice, MetricsVec};
pub use offsets::{RSOffsetSlice, RSOffsetVector, RawOffsetSlice};
pub use result_data::{RSResultData, RawResultData};
pub use term_record::{RSTermRecord, RawTermRecord};
