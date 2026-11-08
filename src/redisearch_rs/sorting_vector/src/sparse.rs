/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! The sparse layout of an [`RSSortingVector`].
//!
//! A document of a schema with many sortable fields that fills few of them pays for a slot per
//! sortable field in the dense layout. The sparse layout stores a `(sort index, value)` pair per
//! value the document has, sorted by sort index, so that its size follows the document.
//!
//! It reuses the dense vector's allocation, so the type stays pointer-sized and the existing
//! drop, clone and free paths apply. The slots are
//!
//! ```text
//!   [marker, index_0, value_0, index_1, value_1, ...]
//! ```
//!
//! The marker is a value no dense vector can hold, which tells the two layouts apart by looking at
//! the first slot. Each index is a shared number taken from one process-wide table, so a stored
//! index costs a slot and no allocation.

use std::sync::OnceLock;
use std::sync::atomic::{AtomicPtr, Ordering};

use value::SharedValue;
use value::shared::SHARED_VALUE_CONTENT_SIZE;

use crate::{IndexOutOfBounds, RS_SORTABLES_MAX, RSSortingVector};

/// A [`SharedValue`] that is never mutated after it is published, so it may be shared between
/// threads: cloning and dropping it only touch the atomic reference count.
struct Immutable(SharedValue);

// SAFETY: see [`Immutable`]; the table and the marker are only ever cloned from.
unsafe impl Send for Immutable {}
// SAFETY: see [`Immutable`].
unsafe impl Sync for Immutable {}

/// Identifies the sparse layout by pointer identity. It lives for the whole process.
static MARKER: OnceLock<Immutable> = OnceLock::new();
/// The address of [`MARKER`]'s value, or null until the first sparse vector exists. It lets the
/// dense read path, which runs for every document of a query, test the layout with one load.
static MARKER_ADDR: AtomicPtr<()> = AtomicPtr::new(std::ptr::null_mut());
/// The shared number for every sort index, in order.
static INDEXES: OnceLock<Box<[Immutable]>> = OnceLock::new();

fn marker() -> &'static SharedValue {
    &MARKER
        .get_or_init(|| {
            let marker = SharedValue::new_num(f64::NAN);
            MARKER_ADDR.store(marker.as_ptr().cast_mut().cast(), Ordering::Release);
            Immutable(marker)
        })
        .0
}

fn index_value(idx: usize) -> SharedValue {
    INDEXES.get_or_init(|| {
        (0..RS_SORTABLES_MAX)
            .map(|idx| Immutable(SharedValue::new_num(idx as f64)))
            .collect()
    })[idx]
        .0
        .clone()
}

/// The sort index a stored index slot holds.
#[inline]
fn slot_index(slot: &SharedValue) -> usize {
    let num = slot.as_num().expect("a sparse index slot holds a number");
    // The table only holds integers below `RS_SORTABLES_MAX`.
    num as usize
}

impl RSSortingVector {
    /// Creates an empty sparse vector with room for `entries` values.
    pub fn new_sparse(entries: usize) -> Self {
        let mut vector = Self::empty();
        vector.inner.reserve_exact(1 + 2 * entries);
        vector.inner.push(marker().clone());
        vector
    }

    /// Whether this vector uses the sparse layout.
    #[inline]
    pub fn is_sparse(&self) -> bool {
        // Before the first sparse vector exists there is no marker to compare with.
        let marker = MARKER_ADDR.load(Ordering::Relaxed);
        !marker.is_null()
            && self
                .inner
                .first()
                .is_some_and(|first| first.as_ptr().cast::<()>().cast_mut() == marker)
    }

    /// The `(sort index, value)` pairs of a sparse vector, by increasing sort index; empty for a
    /// dense one.
    pub fn sparse_entries(&self) -> impl Iterator<Item = (usize, &SharedValue)> {
        let pairs = if self.is_sparse() {
            &self.as_slice()[1..]
        } else {
            &[]
        };
        pairs
            .chunks_exact(2)
            .map(|pair| (slot_index(&pair[0]), &pair[1]))
    }

    /// Returns the value stored for sort index `idx`, or `None` if there is none.
    ///
    /// A dense vector yields its slot, which holds the null sentinel where the document has no
    /// value; a sparse one only has the values the document set.
    #[inline]
    pub fn get_by_index(&self, idx: usize) -> Option<&SharedValue> {
        if !self.is_sparse() {
            return self.as_slice().get(idx);
        }
        let pairs = &self.as_slice()[1..];
        self.search(idx).ok().map(|pos| &pairs[2 * pos + 1])
    }

    /// Binary search of a sparse vector: the position of the entry for `idx` among the entries,
    /// or where it would be inserted.
    fn search(&self, idx: usize) -> Result<usize, usize> {
        let pairs = &self.as_slice()[1..];
        let (mut lo, mut hi) = (0, pairs.len() / 2);
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            match slot_index(&pairs[2 * mid]).cmp(&idx) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Ok(mid),
            }
        }
        Err(lo)
    }

    /// Stores `value` for sort index `idx`.
    ///
    /// A dense vector overwrites its slot and fails if there is none. A sparse one adds or
    /// replaces the entry, and fails only for an index no schema can have.
    pub(crate) fn set_at(
        &mut self,
        idx: usize,
        value: SharedValue,
    ) -> Result<(), IndexOutOfBounds> {
        if !self.is_sparse() {
            let spot = self
                .as_mut_slice()
                .get_mut(idx)
                .ok_or(IndexOutOfBounds(()))?;
            *spot = value;
            return Ok(());
        }
        if idx >= RS_SORTABLES_MAX {
            return Err(IndexOutOfBounds(()));
        }
        match self.search(idx) {
            Ok(pos) => self.inner[2 + 2 * pos] = value,
            Err(pos) => {
                let at = 1 + 2 * pos;
                self.inner.insert(at, value);
                self.inner.insert(at, index_value(idx));
            }
        }
        Ok(())
    }

    /// The memory a sparse vector accounts for, by the rules of the dense layout: its slots and
    /// the values it owns. The marker and the indexes are shared and not counted.
    pub(crate) fn sparse_memory_size(&self) -> usize {
        let mut sz = std::mem::size_of_val(self.as_slice());
        for (_, value) in self.sparse_entries() {
            if value.is_null_static() {
                continue;
            }
            sz += SHARED_VALUE_CONTENT_SIZE;
            if let Some(bytes) = value.fully_dereferenced_ref().as_str_bytes() {
                sz += bytes.len();
            }
        }
        sz
    }
}
