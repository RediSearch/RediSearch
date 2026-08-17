/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Windowing over the ranges a numeric range tree returns.

use inverted_index::NumericFilter;

/// A slice of the ranges a tree returns, measured in documents.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RangeWindow {
    /// Documents to skip before the first returned range.
    pub offset: usize,
    /// Documents to cover before stopping. `0` means no bound.
    pub limit: usize,
}

impl RangeWindow {
    /// Every range: skip nothing, stop at nothing.
    pub const UNBOUNDED: Self = Self {
        offset: 0,
        limit: 0,
    };

    /// Take the window from a filter's deprecated `offset`/`limit` fields.
    ///
    /// Goes away with those fields; new callers build their own window.
    #[expect(deprecated, reason = "reads the fields it replaces")]
    pub const fn from_filter(filter: &NumericFilter) -> Self {
        Self {
            offset: filter.offset,
            limit: filter.limit,
        }
    }

    /// Whether this window leaves out any range.
    pub const fn is_bounded(&self) -> bool {
        self.offset > 0 || self.limit > 0
    }
}
