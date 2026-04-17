/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A fixed-capacity heap that retains the top-k scored documents.
//! Generic top-k iterator shared by the vector (hybrid) and numeric (optimizer)
//! query iterators.
//!
//! # Architecture
//!
//! The core abstraction is [`TopKIterator<S>`], a state machine that drives
//! top-k collection in three modes:
//!
//! - **Unfiltered** — no child filter; stream results directly from the source's batch.
//! - **Batches** — intersect score-ordered batches with a child filter (merge-join).
//! - **Adhoc-BF** — walk the child filter and call [`ScoreSource::lookup_score`]
//!   for each document.
//!
//! The score-producing logic is abstracted behind the [`ScoreSource`] / [`ScoreBatch`]
//! traits.  Concrete implementations (`VectorScoreSource`, `NumericScoreSource`) are
//! introduced in M2 and M3 respectively.
//!
//! # Milestones
//!
//! - **M1 (this crate)**: shared skeleton — `TopKHeap`, `TopKIterator`, traits, mock sources.
//! - **M2**: `VectorScoreSource` + planner wiring (`TopKIterator<VectorScoreSource>`).
//! - **M3**: `NumericScoreSource` + planner wiring (`TopKIterator<NumericScoreSource>`).

pub mod heap;
pub mod iterator;
pub mod traits;

#[cfg(feature = "test-utils")]
pub mod mock;

pub use heap::{ScoredResult, TopKHeap};
pub use iterator::{TopKIterator, TopKMetrics, TopKMode};
pub use traits::{CollectionStrategy, ScoreBatch, ScoreSource};
