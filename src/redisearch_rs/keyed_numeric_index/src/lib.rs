/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! A numeric index keyed by an arbitrary byte string.
//!
//! A [`KeyedNumericIndex`] holds one [`NumericRangeTree`] per key, so a query
//! for "key `k` has a value in `[lo, hi]`" seeks the key's tree and scans only
//! the entries that belong to it.

use numeric_range_tree::{AddResult, NumericRangeTree};
use rqe_core::DocId;
use rustc_hash::FxHashMap;
use std::sync::atomic::{AtomicU32, Ordering};

/// Source of [`KeyedNumericIndexId`]s.
static UNIQUE_ID_COUNTER: AtomicU32 = AtomicU32::new(0);

/// Tells two [`KeyedNumericIndex`] instances apart.
///
/// Allocated from a global counter, so a replaced index never shares an ID
/// with the one a concurrent reader (an iterator, a GC scan) saw before the
/// replacement, until the counter wraps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(transparent)]
pub struct KeyedNumericIndexId(u32);

impl KeyedNumericIndexId {
    fn next() -> Self {
        Self(UNIQUE_ID_COUNTER.fetch_add(1, Ordering::Relaxed))
    }
}

impl From<KeyedNumericIndexId> for u32 {
    fn from(id: KeyedNumericIndexId) -> Self {
        id.0
    }
}

/// One numeric range tree per key.
///
/// A tree is created on the first [`add`](KeyedNumericIndex::add) for its key. A key whose
/// tree holds no entries is dropped by [`remove_empty_keys`](KeyedNumericIndex::remove_empty_keys).
#[derive(Debug)]
pub struct KeyedNumericIndex {
    /// Boxed so a tree keeps its address when the map grows: a query iterator
    /// reads a tree while the lock guarding the index is released.
    keys: FxHashMap<Box<[u8]>, Box<NumericRangeTree>>,
    /// Bytes owned by the trees and their keys, kept equal to
    /// [`computed_memory_usage`](Self::computed_memory_usage) by every mutator.
    memory_usage: usize,
    unique_id: KeyedNumericIndexId,
    compress_floats: bool,
}

impl KeyedNumericIndex {
    /// Create an empty index. `compress_floats` applies to every tree it creates.
    pub fn new(compress_floats: bool) -> Self {
        Self {
            keys: FxHashMap::default(),
            memory_usage: std::mem::size_of::<Self>(),
            unique_id: KeyedNumericIndexId::next(),
            compress_floats,
        }
    }

    /// Memory charged to one key, beyond its tree's own [`NumericRangeTree::mem_usage`].
    const fn key_overhead(key: &[u8]) -> usize {
        key.len() + std::mem::size_of::<Box<[u8]>>() + std::mem::size_of::<Box<NumericRangeTree>>()
    }

    /// Add the pair (`doc_id`, `value`) under `key`, creating the key's tree if needed.
    ///
    /// `doc_id` must not be lower than the last one added under the same key.
    /// See [`NumericRangeTree::add`] for the meaning of the other parameters.
    ///
    /// The returned [`AddResult`] describes the change to the key's tree only.
    /// The change to [`memory_usage`](Self::memory_usage) also covers a new key and the tree's
    /// nodes.
    pub fn add(
        &mut self,
        doc_id: DocId,
        key: &[u8],
        value: f64,
        has_field_expiration: bool,
        max_depth_range: usize,
    ) -> AddResult {
        if !self.keys.contains_key(key) {
            let tree = Box::new(NumericRangeTree::new(self.compress_floats));
            self.memory_usage += Self::key_overhead(key) + tree.mem_usage();
            self.keys.insert(key.into(), tree);
        }
        let tree = self.keys.get_mut(key).expect("the key was just ensured");
        let before = tree.mem_usage();
        let result = tree.add(doc_id, value, has_field_expiration, false, max_depth_range);
        self.memory_usage = self.memory_usage - before + tree.mem_usage();
        result
    }

    /// The tree holding the values added under `key`, if any.
    pub fn get(&self, key: &[u8]) -> Option<&NumericRangeTree> {
        self.keys.get(key).map(|tree| &**tree)
    }

    /// Run `f` on the tree of `key`, keeping [`memory_usage`](Self::memory_usage)
    /// correct if `f` changes the tree's size. This is how GC mutates a tree.
    ///
    /// Returns `None` without calling `f` if the key has no tree.
    pub fn update_tree<R>(
        &mut self,
        key: &[u8],
        f: impl FnOnce(&mut NumericRangeTree) -> R,
    ) -> Option<R> {
        let tree = self.keys.get_mut(key)?;
        let before = tree.mem_usage();
        let result = f(tree);
        self.memory_usage = self.memory_usage - before + tree.mem_usage();
        Some(result)
    }

    /// Drop every key whose tree holds no entries, and return how many were dropped.
    pub fn remove_empty_keys(&mut self) -> usize {
        let mut freed = 0;
        let mut removed = 0;
        self.keys.retain(|key, tree| {
            let keep = tree.num_entries() > 0;
            if !keep {
                freed += Self::key_overhead(key) + tree.mem_usage();
                removed += 1;
            }
            keep
        });
        self.memory_usage -= freed;
        removed
    }

    /// Iterate over every key and its tree, in no particular order.
    pub fn iter(&self) -> impl Iterator<Item = (&[u8], &NumericRangeTree)> {
        self.keys.iter().map(|(key, tree)| (&**key, &**tree))
    }

    /// The number of keys that currently have a tree.
    pub fn num_keys(&self) -> usize {
        self.keys.len()
    }

    /// Bytes used by the index: its own header, its keys and their trees.
    pub const fn memory_usage(&self) -> usize {
        self.memory_usage
    }

    /// Identifies this instance. See [`KeyedNumericIndexId`].
    pub const fn unique_id(&self) -> KeyedNumericIndexId {
        self.unique_id
    }

    /// Recompute [`memory_usage`](Self::memory_usage) from scratch.
    pub fn computed_memory_usage(&self) -> usize {
        std::mem::size_of::<Self>()
            + self
                .keys
                .iter()
                .map(|(key, tree)| Self::key_overhead(key) + tree.mem_usage())
                .sum::<usize>()
    }
}
