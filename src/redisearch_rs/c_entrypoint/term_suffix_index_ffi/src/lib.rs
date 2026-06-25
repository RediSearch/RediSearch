/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! C entry points for [`term_suffix_index::TermSuffixIndex`].
//!
//! All string parameters are byte pointers with an explicit length and
//! must be valid UTF-8. Invalid UTF-8 is rejected — mutations become
//! no-ops, lookups yield no matches — and trips a debug assertion.

mod index;
mod iter;

pub use index::*;
pub use iter::*;

/// A set of indexed terms supporting substring, ends-with, exact and
/// wildcard lookups.
///
/// Opaque to C; obtained from [`TermSuffixIndex_New`] and freed with
/// [`TermSuffixIndex_Free`].
pub use term_suffix_index::TermSuffixIndex;
