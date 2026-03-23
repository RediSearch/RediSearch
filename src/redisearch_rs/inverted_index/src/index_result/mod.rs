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
mod offsets;
mod result_data;
mod term_record;

pub use query_term::RSQueryTerm;

pub use self::core::{RSIndexResult, ResultMetrics_Reset_func};
pub use aggregate::{RSAggregateResult, RSAggregateResultIter};
pub use kind::{RSResultKind, RSResultKindMask};
pub use offsets::{RSOffsetSlice, RSOffsetVector};
pub use result_data::RSResultData;
pub use term_record::RSTermRecord;
