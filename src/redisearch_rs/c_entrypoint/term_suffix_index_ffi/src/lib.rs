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
//! The index follows a readers-writer contract: read-only calls (the
//! iterate functions, cursors and [`TermSuffixIndex_MemUsage`]) may run
//! concurrently with each other, while [`TermSuffixIndex_Add`],
//! [`TermSuffixIndex_Remove`] and [`TermSuffixIndex_Free`] require
//! exclusive access — no other call on the same index, and no live
//! iterator obtained from it. A cursor iterator itself is
//! single-threaded: it may not be advanced or freed from two threads at
//! once, though separate iterators over the same index may.

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

// The safety contracts in this crate permit concurrent read-only
// calls, each reborrowing the same `*const TermSuffixIndex` — sound
// only if the type is `Sync` — and place no thread affinity on
// creation, mutation and destruction — sound only if it is `Send`.
const _: () = {
    const fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TermSuffixIndex>();
};
