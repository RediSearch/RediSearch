/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Insertion-order-preserving collection with active/inactive tracking.
//!
//! [`ActiveChildren`] wraps a `Vec<Entry<I>>` where each entry pairs a child
//! with an `active` flag. Children are never reordered, so their indices
//! remain stable across deactivation and reactivation.

/// A single element in [`ActiveChildren`], pairing a child with its active flag.
struct Entry<I> {
    /// The child value stored at this position.
    child: I,
    /// Whether this child is currently active. Inactive children are skipped
    /// by [`ActiveChildren::iter_active`] and [`ActiveChildren::iter_active_mut`]
    /// but remain accessible by index via [`ActiveChildren::get`].
    active: bool,
}

/// A collection that tracks which children are active without reordering them.
///
/// Used by the union iterator variants to manage child iterators.
/// Children are kept in their original insertion order and each entry
/// carries an `active` flag. Deactivating a child is O(1) — the element
/// stays in place and is simply skipped during iteration.
pub struct ActiveChildren<I> {
    /// Children in insertion order, each paired with an active flag.
    entries: Vec<Entry<I>>,
    /// Cached count of entries where `active == true`, kept in sync by
    /// [`deactivate`](Self::deactivate), [`activate_all`](Self::activate_all),
    /// [`activate_range`](Self::activate_range), and [`remove`](Self::remove).
    num_active: usize,
}

impl<I> ActiveChildren<I> {
    /// Creates a new `ActiveChildren` with all children marked as active.
    pub fn new(children: Vec<I>) -> Self {
        let num_active = children.len();
        let entries = children
            .into_iter()
            .map(|child| Entry {
                child,
                active: true,
            })
            .collect();
        Self {
            entries,
            num_active,
        }
    }

    /// Creates a new `ActiveChildren` where only `children[start..end]` are active.
    pub fn with_active_range(children: Vec<I>, start: usize, end: usize) -> Self {
        debug_assert!(start <= end && end <= children.len());
        let num_active = end - start;
        let entries = children
            .into_iter()
            .enumerate()
            .map(|(i, child)| Entry {
                child,
                active: i >= start && i < end,
            })
            .collect();
        Self {
            entries,
            num_active,
        }
    }

    /// Marks the child at `idx` as inactive.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds or the child is already inactive.
    pub fn deactivate(&mut self, idx: usize) {
        debug_assert!(self.entries[idx].active, "child {idx} is already inactive");
        self.entries[idx].active = false;
        self.num_active -= 1;
    }

    /// Marks all children as active.
    pub fn activate_all(&mut self) {
        for entry in &mut self.entries {
            entry.active = true;
        }
        self.num_active = self.entries.len();
    }

    /// Marks only `children[start..end]` as active, all others as inactive.
    pub fn activate_range(&mut self, start: usize, end: usize) {
        debug_assert!(start <= end && end <= self.entries.len());
        for (i, entry) in self.entries.iter_mut().enumerate() {
            entry.active = i >= start && i < end;
        }
        self.num_active = end - start;
    }

    /// Returns whether the child at `idx` is active.
    pub fn is_active(&self, idx: usize) -> bool {
        self.entries[idx].active
    }

    /// Returns the number of currently active children.
    pub const fn num_active(&self) -> usize {
        self.num_active
    }

    /// Returns the total number of children (active and inactive).
    pub const fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are no children at all.
    pub const fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Returns a shared reference to the child at `idx` regardless of active state.
    pub fn get(&self, idx: usize) -> &I {
        &self.entries[idx].child
    }

    /// Returns a mutable reference to the child at `idx` regardless of active state.
    pub fn get_mut(&mut self, idx: usize) -> &mut I {
        &mut self.entries[idx].child
    }

    /// Returns an iterator over `(index, &child)` for all active children.
    pub fn iter_active(&self) -> impl DoubleEndedIterator<Item = (usize, &I)> {
        self.entries
            .iter()
            .enumerate()
            .filter(|(_, e)| e.active)
            .map(|(idx, e)| (idx, &e.child))
    }

    /// Returns an iterator over `(index, &mut child)` for all active children.
    pub fn iter_active_mut(&mut self) -> impl DoubleEndedIterator<Item = (usize, &mut I)> {
        self.entries
            .iter_mut()
            .enumerate()
            .filter(|(_, e)| e.active)
            .map(|(idx, e)| (idx, &mut e.child))
    }

    /// Returns an iterator over all children regardless of active state.
    pub fn iter_all(&self) -> impl Iterator<Item = &I> {
        self.entries.iter().map(|e| &e.child)
    }

    /// Returns a mutable iterator over all children regardless of active state.
    pub fn iter_all_mut(&mut self) -> impl Iterator<Item = &mut I> {
        self.entries.iter_mut().map(|e| &mut e.child)
    }

    /// Permanently removes the child at `idx` from the collection.
    ///
    /// This is an O(n) operation that shifts subsequent elements. Use for
    /// permanent removal (e.g., aborted children during revalidation), not
    /// for temporary exhaustion.
    pub fn remove(&mut self, idx: usize) -> I {
        let entry = self.entries.remove(idx);
        if entry.active {
            self.num_active -= 1;
        }
        entry.child
    }

    /// Consumes the wrapper and returns the children as a `Vec<I>`.
    pub fn into_inner(self) -> Vec<I> {
        self.entries.into_iter().map(|e| e.child).collect()
    }
}
