/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Windowing over the value-ordered range stream of a numeric range tree.

use inverted_index::NumericFilter;

/// A slice of a tree's value-ordered document stream, for callers that consume
/// the stream one window at a time.
///
/// Both bounds are counted in documents, in the traversal order fixed by the
/// filter's `ascending` flag, and both are approximate: the tree can only skip
/// and stop at range boundaries, and it counts a range's whole document set —
/// including documents outside the filter bounds and documents deleted but not
/// yet collected.
///
/// A window is therefore only usable by a caller that walks the stream
/// end-to-end, advancing `offset` by the document count of the ranges it was
/// handed. Anything else — a window taken from a user-supplied `LIMIT`, say —
/// re-serves documents from the range straddling `offset` and yields fewer than
/// `limit` fresh ones.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeWindow {
    /// Documents to skip before the first returned range.
    pub offset: usize,
    /// Documents to cover before the walk stops. `0` means "no bound".
    pub limit: usize,
}

impl RangeWindow {
    /// The whole stream: skip nothing, stop at nothing.
    pub const UNBOUNDED: Self = Self {
        offset: 0,
        limit: 0,
    };

    /// Read the window a filter carries.
    ///
    /// The single bridge from the window fields on [`NumericFilter`]; it goes
    /// away with them, leaving callers to pass their own [`RangeWindow`].
    #[expect(deprecated, reason = "this is the bridge that retires them")]
    pub const fn from_filter(filter: &NumericFilter) -> Self {
        Self {
            offset: filter.offset,
            limit: filter.limit,
        }
    }

    /// Whether this window restricts the stream at all.
    pub const fn is_bounded(&self) -> bool {
        self.offset > 0 || self.limit > 0
    }
}
