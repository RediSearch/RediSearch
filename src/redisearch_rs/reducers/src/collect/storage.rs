/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Bounded storage shared by the COLLECT reducer variants. The effective
//! `(offset, count)` is resolved once by [`Storage::new`], with defaults
//! filling in for a missing `LIMIT`. The maximum number of buffered rows
//! is `offset + count`, enforced on each insert.

/// Default count for `SORTBY` results when no explicit `LIMIT` is provided,
/// matching the C implementation's `DEFAULT_LIMIT`.
pub const DEFAULT_LIMIT: u64 = 10;

pub struct Storage<T> {
    buf: Vec<T>,
    offset: usize,
    count: usize,
}

impl<T> Storage<T> {
    /// Resolve `(offset, count)` and pre-size the buffer.
    pub fn new(sortby: bool, limit: Option<(u64, u64)>) -> Self {
        let (offset, count) = match (sortby, limit) {
            (_, Some((o, c))) => (o as usize, c as usize),
            // SAFETY: `ffi::RSGlobalConfig` is the module-global config
            // instance initialised once during module load; we only read
            // a single `usize` field here.
            (false, None) => (0, unsafe { ffi::RSGlobalConfig.maxAggregateResults }),
            (true, None) => (0, DEFAULT_LIMIT as usize),
        };
        let buf = if sortby || limit.is_some() {
            Vec::with_capacity(offset.saturating_add(count))
        } else {
            Vec::new()
        };
        Self { buf, offset, count }
    }

    /// Insert an entry if there is room under the cap, dropping excess
    /// inserts in arrival order. `project` is only called when the entry fits.
    /// Returns `true` if the entry was buffered, `false` if it was dropped.
    pub fn insert_entry<F>(&mut self, project: F) -> bool
    where
        F: FnOnce() -> T,
    {
        if self.buf.len() < self.offset.saturating_add(self.count) {
            self.buf.push(project());
            true
        } else {
            false
        }
    }

    /// Drain in insertion order, optionally applying the offset/count slice.
    ///
    /// When `apply_limit` is `true`, the buffered rows are yielded through
    /// `skip(offset).take(count)`. In the no-`LIMIT` cases the buffered
    /// length is already `≤ count`, so the slice degenerates to "everything
    /// buffered". When `apply_limit` is `false`, every buffered row is
    /// yielded — used by the remote reducer when `is_internal` is set,
    /// where the coordinator owns the global offset.
    pub fn drain(&mut self, apply_limit: bool) -> impl Iterator<Item = T> {
        let (offset, count) = if apply_limit {
            (self.offset, self.count)
        } else {
            (0, usize::MAX)
        };
        std::mem::take(&mut self.buf)
            .into_iter()
            .skip(offset)
            .take(count)
    }

    /// Iterate buffered rows in insertion order, read-only.
    pub fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.buf.iter()
    }
}
