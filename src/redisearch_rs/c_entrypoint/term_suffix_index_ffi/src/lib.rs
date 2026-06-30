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
//! must be valid UTF-8. Passing invalid UTF-8 panics.
//!
//! The index is **not** thread-safe and requires exclusive access: no two
//! calls may run concurrently on the same index — including two read-only
//! calls — and while an iterator is alive no other call on the index it
//! came from may run. (Read-only calls race too: wildcard iteration clones
//! the index's non-atomic reference-counted term handles.)

mod index;
mod iter_callback;
mod iter_cursor;

pub use index::*;
pub use iter_callback::*;
pub use iter_cursor::*;

/// A set of indexed terms supporting substring, ends-with, exact and
/// wildcard lookups.
///
/// Opaque to C; obtained from [`TermSuffixIndex_New`] and freed with
/// [`TermSuffixIndex_Free`].
pub use term_suffix_index::TermSuffixIndex;
