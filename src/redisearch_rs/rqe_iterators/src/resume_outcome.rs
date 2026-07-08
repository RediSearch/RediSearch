/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The outcome shape returned when resuming a suspended iterator.

/// Outcome of resuming a suspended iterator.
///
/// Mirrors the status returned by the legacy `RQEIterator::revalidate`, but
/// *owns* the resumed iterator in its recoverable variants: resuming produces a
/// brand-new active value (suspended and active are distinct types), so — unlike
/// `revalidate`, which keeps the iterator in place — the outcome carries the
/// resumed iterator rather than borrowing `current` from `self`. Callers query
/// `current` on the returned iterator for the [`Moved`](ResumeOutcome::Moved)
/// case.
///
/// Generic over the carried iterator `I` so the same shape serves both the
/// concrete resume path (which carries `Box<Resumed>`) and the dyn-safe resume
/// path (which carries a type-erased iterator).
pub enum ResumeOutcome<I> {
    /// Resumed at the same position.
    Ok(I),
    /// Resumed, but the position moved forward (the previous `last_doc_id` was
    /// deleted or otherwise no longer present); query `current` on the iterator.
    Moved(I),
    /// Unrecoverable: no active iterator is produced and the suspended iterator
    /// was dropped.
    Aborted,
}
