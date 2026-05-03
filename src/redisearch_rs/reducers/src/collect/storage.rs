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
pub(crate) const DEFAULT_LIMIT: u64 = 10;

pub(crate) struct Storage<T> {
    buf: Vec<T>,
    offset: usize,
    count: usize,
}

impl<T> Storage<T> {
    /// Resolve `(offset, count)` and pre-size the buffer.
    pub(crate) fn new(sortby: bool, limit: Option<(u64, u64)>) -> Self {
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
    pub(crate) fn insert_entry<F>(&mut self, project: F) -> bool
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

    /// Drain in insertion order and apply `skip(offset).take(count)`. In
    /// the no-`LIMIT` cases the buffered length is already `≤ count`, so
    /// the slice degenerates to "everything buffered".
    pub(crate) fn drain(&mut self) -> Vec<T> {
        self.take_buffer()
            .into_iter()
            .skip(self.offset)
            .take(self.count)
            .collect()
    }

    /// Drain in insertion order without applying the offset/count slice.
    /// Used by the remote reducer when `include_sort_keys` is set, where the
    /// coordinator owns the global offset.
    pub(crate) fn drain_unlimited(&mut self) -> Vec<T> {
        self.take_buffer()
    }

    /// Iterate buffered rows in insertion order, read-only.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "added for upcoming DISTINCT support")
    )]
    pub(crate) fn iter(&self) -> impl Iterator<Item = &T> + '_ {
        self.buf.iter()
    }

    fn take_buffer(&mut self) -> Vec<T> {
        std::mem::take(&mut self.buf)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use value::SharedValue;

    /// `project` closure that increments `counter` on each call.
    fn counting_project(
        counter: &AtomicUsize,
        tag: f64,
    ) -> impl FnOnce() -> Box<[SharedValue]> + '_ {
        move || {
            counter.fetch_add(1, AtomicOrdering::Relaxed);
            vec![SharedValue::new_num(tag)].into_boxed_slice()
        }
    }

    fn drained_nums(drained: &[Box<[SharedValue]>]) -> Vec<f64> {
        drained
            .iter()
            .map(|p| p[0].as_num().expect("expected num"))
            .collect()
    }

    #[test]
    fn insert_entry_array_caps_at_len_in_insertion_order() {
        let mut s = Storage::new(/* sortby */ false, Some((0, 3)));
        for i in 0..5 {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let drained = s.drain();
        assert_eq!(drained.len(), 3, "array variant must cap at `cap`");
        assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn insert_entry_heap_caps_at_len_in_insertion_order() {
        let mut s = Storage::new(/* sortby */ true, Some((0, 3)));
        for i in 0..5 {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let drained = s.drain();
        assert_eq!(drained.len(), 3, "heap variant must cap at `cap`");
        assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn insert_entry_drops_after_cap_without_calling_project() {
        let counter = AtomicUsize::new(0);
        let mut s = Storage::new(/* sortby */ false, Some((0, 2)));
        for v in [1.0_f64, 2.0, 3.0, 4.0] {
            s.insert_entry(counting_project(&counter, v));
        }
        assert_eq!(
            counter.load(AtomicOrdering::Relaxed),
            2,
            "`project` must not run for entries beyond the cap"
        );
    }

    #[test]
    fn drain_applies_skip_take() {
        let mut s = Storage::new(false, Some((1, 2)));
        for i in 0..5 {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let drained = s.drain();
        assert_eq!(drained_nums(&drained), vec![1.0, 2.0]);
    }

    #[test]
    fn drain_unlimited_ignores_stored_limit() {
        let mut s = Storage::new(false, Some((1, 2)));
        for i in 0..3 {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let drained = s.drain_unlimited();
        assert_eq!(drained_nums(&drained), vec![0.0, 1.0, 2.0]);
    }

    #[test]
    fn insert_entry_heap_uses_default_limit_when_no_explicit_limit() {
        let mut s = Storage::new(/* sortby */ true, None);
        for i in 0..(DEFAULT_LIMIT as usize + 5) {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let drained = s.drain();
        assert_eq!(drained.len(), DEFAULT_LIMIT as usize);
    }

    #[test]
    fn iter_yields_buffered_rows_in_insertion_order_under_cap() {
        let mut s = Storage::new(/* sortby */ false, Some((0, 3)));
        for i in 0..5 {
            s.insert_entry(|| vec![SharedValue::new_num(i as f64)].into_boxed_slice());
        }
        let nums: Vec<f64> = s
            .iter()
            .map(|row| row[0].as_num().expect("expected num"))
            .collect();
        assert_eq!(nums, vec![0.0, 1.0, 2.0]);
    }
}
