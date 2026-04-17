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
//!   for each document.
//!
//! The score-producing logic is abstracted behind the [`ScoreSource`] / [`ScoreBatch`]
//! traits.
//!

pub mod heap;
pub mod iterator;
pub mod traits;

#[cfg(feature = "test-utils")]
pub mod mock;

pub use heap::{ScoredResult, TopKHeap};
pub use iterator::{TopKIterator, TopKMetrics, TopKMode};
pub use traits::{CollectionStrategy, ScoreBatch, ScoreSource};
