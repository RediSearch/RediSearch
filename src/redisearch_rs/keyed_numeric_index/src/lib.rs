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

use numeric_range_tree::{AddResult, NumericRangeTree, TreeUniqueId};
use redis_json_api::{JsonType, JsonValueRef};
use rqe_core::DocId;
use rustc_hash::FxHashMap;
use std::ptr::NonNull;
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

/// The `(key, value)` members of one document's object, collected before the index is locked.
///
/// Collecting first lets the caller validate the whole object, and fail the document
/// without touching the index, before anything is added.
#[derive(Debug, Default)]
pub struct KeyedNumericBatch {
    members: Vec<(Box<[u8]>, f64)>,
}

/// Why a JSON value cannot be collected into a [`KeyedNumericBatch`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BatchFromJsonError {
    /// The value is not a JSON object.
    NotAnObject,
    /// A member is not a number. `null`, booleans, strings, arrays and nested objects all count.
    MemberNotNumber,
}

impl KeyedNumericBatch {
    /// An empty batch, which a document with an empty object also yields.
    pub fn new() -> Self {
        Self::default()
    }

    /// Append a member.
    ///
    /// Keys should be distinct, as the members of a JSON object are. If a key repeats,
    /// [`KeyedNumericIndex::add_batch`] drops the later member, since a document is
    /// added at most once per key.
    pub fn push(&mut self, key: &[u8], value: f64) {
        self.members.push((key.into(), value));
    }

    /// The number of members.
    pub const fn len(&self) -> usize {
        self.members.len()
    }

    /// Whether the batch has no member.
    pub const fn is_empty(&self) -> bool {
        self.members.is_empty()
    }

    /// Collect the members of the JSON object `obj`.
    ///
    /// The whole object is validated first, so a failure leaves nothing partially collected.
    /// Integers are converted to `f64`, which rounds those beyond 2^53.
    ///
    /// # Safety
    ///
    /// 1. `ctx` must be a valid Redis module context.
    pub unsafe fn from_json(
        obj: JsonValueRef<'_>,
        ctx: NonNull<redis_module::RedisModuleCtx>,
    ) -> Result<Self, BatchFromJsonError> {
        if obj.get_type() != JsonType::Object {
            return Err(BatchFromJsonError::NotAnObject);
        }
        // SAFETY: ensured by caller (1.).
        let iter = unsafe { obj.key_values(ctx) }.ok_or(BatchFromJsonError::NotAnObject)?;
        let mut batch = Self {
            members: Vec::with_capacity(iter.len()),
        };
        for (key, value) in iter {
            let value = value.as_ref();
            let number = match value.get_type() {
                // An integer beyond `i64` may be reported as an int that `get_int` rejects.
                JsonType::Int => value
                    .get_int()
                    .map(|i| i as f64)
                    .or_else(|| value.get_double()),
                JsonType::Double => value.get_double(),
                _ => None,
            };
            let number = number.ok_or(BatchFromJsonError::MemberNotNumber)?;
            batch.push(key.as_slice(), number);
        }
        Ok(batch)
    }
}

/// One numeric range tree per key.
///
/// A tree is created on the first successful [`add`](KeyedNumericIndex::add) for its key, so
/// a key never maps to a tree that was never filled. GC empties trees through
/// [`update_tree`](KeyedNumericIndex::update_tree) and then drops them with
/// [`remove_key_if_empty`](KeyedNumericIndex::remove_key_if_empty).
///
/// # Tree identity
///
/// A tree's address does not identify it: once its key is removed and added again, a new
/// tree can be allocated at the same address. Anything that holds on to a tree across a
/// release of the lock guarding the index must identify it by
/// [`NumericRangeTree::unique_id`] and look it up again with
/// [`get_checked`](KeyedNumericIndex::get_checked).
#[derive(Debug)]
pub struct KeyedNumericIndex {
    /// Boxed so growing the map moves a pointer instead of a whole tree, and so a tree's
    /// address stays stable while the lock is held. It is not an identity guarantee; see
    /// the type's docs.
    keys: FxHashMap<Box<[u8]>, Box<NumericRangeTree>>,
    /// Bytes owned by the trees, the keys and the map's table, kept equal to
    /// [`computed_memory_usage`](Self::computed_memory_usage) by every mutator.
    memory_usage: usize,
    unique_id: KeyedNumericIndexId,
    compress_floats: bool,
}

impl KeyedNumericIndex {
    /// Create an empty index. `compress_floats` applies to every tree it creates.
    pub fn new(compress_floats: bool) -> Self {
        let mut index = Self {
            keys: FxHashMap::default(),
            memory_usage: 0,
            unique_id: KeyedNumericIndexId::next(),
            compress_floats,
        };
        index.memory_usage = index.computed_memory_usage();
        index
    }

    /// Estimate of the heap bytes the map's table holds: one entry and one control byte
    /// per slot, following [`FxHashMap::capacity`]. The exact allocation size is not
    /// available from the standard map, so this is what [`memory_usage`](Self::memory_usage)
    /// tracks. The capacity, and so the estimate, can change when entries are removed.
    fn table_bytes(&self) -> usize {
        self.keys.capacity() * (std::mem::size_of::<(Box<[u8]>, Box<NumericRangeTree>)>() + 1)
    }

    /// Add the pair (`doc_id`, `value`) under `key`, creating the key's tree if needed.
    ///
    /// A document must be added at most once per key, and `doc_id` must not be lower than
    /// the last one added under the same key. A pair that violates either rule is dropped
    /// and does not change the index. A JSON object cannot hold a key twice, so this holds
    /// for a document indexed from one object; a path that resolves to several objects must
    /// be rejected by the caller.
    ///
    /// `NaN` is rejected like the numeric field type rejects it: nothing is added and no
    /// key is created. The infinities are accepted.
    ///
    /// The returned [`AddResult`] describes the change to the key's tree only. A dropped
    /// pair yields the default result. The change to [`memory_usage`](Self::memory_usage)
    /// also covers a new key, the tree's base size and the map's growth.
    ///
    /// See [`NumericRangeTree::add`] for the meaning of the other parameters.
    pub fn add(
        &mut self,
        doc_id: DocId,
        key: &[u8],
        value: f64,
        has_field_expiration: bool,
        max_depth_range: usize,
    ) -> AddResult {
        if value.is_nan() {
            return AddResult::default();
        }
        if let Some(tree) = self.keys.get_mut(key) {
            let before = tree.mem_usage();
            let result = tree.add(doc_id, value, has_field_expiration, false, max_depth_range);
            self.memory_usage = self.memory_usage - before + tree.mem_usage();
            return result;
        }

        // Insert only a tree that took the pair, so a rejected add leaves no empty key behind.
        let mut tree = Box::new(NumericRangeTree::new(self.compress_floats));
        let result = tree.add(doc_id, value, has_field_expiration, false, max_depth_range);
        if tree.num_entries() == 0 {
            return result;
        }
        let table_before = self.table_bytes();
        self.memory_usage += key.len() + tree.mem_usage();
        self.keys.insert(key.into(), tree);
        self.memory_usage = self.memory_usage - table_before + self.table_bytes();
        result
    }

    /// Add every member of `batch` for `doc_id`, as [`add`](Self::add) does one by one.
    ///
    /// The returned [`AddResult`] sums the changes to the members' trees; its `changed`
    /// flag is set if any tree changed structurally.
    pub fn add_batch(
        &mut self,
        doc_id: DocId,
        batch: &KeyedNumericBatch,
        has_field_expiration: bool,
        max_depth_range: usize,
    ) -> AddResult {
        let mut total = AddResult::default();
        for (key, value) in &batch.members {
            let r = self.add(doc_id, key, *value, has_field_expiration, max_depth_range);
            total.size_delta += r.size_delta;
            total.num_records_delta += r.num_records_delta;
            total.changed |= r.changed;
            total.num_ranges_delta += r.num_ranges_delta;
            total.num_leaves_delta += r.num_leaves_delta;
            total.block_count_delta += r.block_count_delta;
        }
        total
    }

    /// The tree holding the values added under `key`, if any.
    ///
    /// The returned reference is only valid while the index is borrowed. A pointer derived
    /// from it must not be used after the lock guarding the index is released and
    /// re-acquired for writing, since the key may have been removed and re-added; use
    /// [`get_checked`](Self::get_checked) with a remembered [`TreeUniqueId`] instead.
    pub fn get(&self, key: &[u8]) -> Option<&NumericRangeTree> {
        self.keys.get(key).map(|tree| &**tree)
    }

    /// The tree of `key`, if there is one and its [`unique_id`](NumericRangeTree::unique_id)
    /// is `expected`.
    ///
    /// `None` means the key is gone or its tree was replaced since `expected` was read.
    pub fn get_checked(&self, key: &[u8], expected: TreeUniqueId) -> Option<&NumericRangeTree> {
        self.get(key).filter(|tree| tree.unique_id() == expected)
    }

    /// Run `f` on the tree of `key`, keeping [`memory_usage`](Self::memory_usage)
    /// correct if `f` changes the tree's size. This is how GC mutates a tree.
    ///
    /// `f` must mutate the tree in place and never replace it: a replacement carries
    /// another [`unique_id`](NumericRangeTree::unique_id), which readers use to tell
    /// trees apart.
    ///
    /// Returns `None` without calling `f` if the key has no tree, or if its tree is not
    /// the one identified by `expected`.
    pub fn update_tree<R>(
        &mut self,
        key: &[u8],
        expected: TreeUniqueId,
        f: impl FnOnce(&mut NumericRangeTree) -> R,
    ) -> Option<R> {
        let tree = self.keys.get_mut(key)?;
        if tree.unique_id() != expected {
            return None;
        }
        let before = tree.mem_usage();
        let result = f(tree);
        self.memory_usage = self.memory_usage - before + tree.mem_usage();
        Some(result)
    }

    /// Drop the tree of `key` if it is the one identified by `expected` and holds no entries.
    ///
    /// Returns whether it was dropped. The table is not shrunk, but its reported capacity
    /// can still drop, and [`memory_usage`](Self::memory_usage) follows it.
    pub fn remove_key_if_empty(&mut self, key: &[u8], expected: TreeUniqueId) -> bool {
        let Some(tree) = self.keys.get(key) else {
            return false;
        };
        if tree.unique_id() != expected || tree.num_entries() > 0 {
            return false;
        }
        let table_before = self.table_bytes();
        let (key, tree) = self.keys.remove_entry(key).expect("the key was just found");
        self.memory_usage -= key.len() + tree.mem_usage();
        self.memory_usage = self.memory_usage - table_before + self.table_bytes();
        true
    }

    /// Drop every key whose tree holds no entries, and return how many were dropped.
    ///
    /// Visits every key, so GC of a known tree should use
    /// [`remove_key_if_empty`](Self::remove_key_if_empty).
    pub fn remove_empty_keys(&mut self) -> usize {
        let table_before = self.table_bytes();
        let mut freed = 0;
        let mut removed = 0;
        self.keys.retain(|key, tree| {
            let keep = tree.num_entries() > 0;
            if !keep {
                freed += key.len() + tree.mem_usage();
                removed += 1;
            }
            keep
        });
        self.memory_usage -= freed;
        self.memory_usage = self.memory_usage - table_before + self.table_bytes();
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

    /// Bytes used by the index: its own header, its keys, their trees and the map's table.
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
            + self.table_bytes()
            + self
                .keys
                .iter()
                .map(|(key, tree)| key.len() + tree.mem_usage())
                .sum::<usize>()
    }
}
