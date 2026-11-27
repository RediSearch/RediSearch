/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Types and the drop rule shared by the score-bound hooks of [`RQEIterator`].
//!
//! Defined in [`inverted_index::score_bound`], so that readers apply the same drop rule as the
//! iterators built on them.

#[cfg(doc)]
use crate::RQEIterator;

pub use inverted_index::score_bound::{BlockBound, MinCompetitiveScore, can_drop};
