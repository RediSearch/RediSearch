/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection for the per-key numeric trees of MAP NUMERIC fields.
//!
//! Each key's tree is scanned and patched with the code the plain numeric scanner
//! uses; this module only names a tree across spec lock releases and drops a key
//! whose tree GC emptied. A query iterator re-resolves its key when it revalidates,
//! which is what makes freeing a tree safe.

use std::io::{self, Read, Write};
use std::ops::RangeInclusive;

use field_spec::{FieldSpec, FieldSpecType};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocId, DocLiveness};
use keyed_numeric_index::KeyedNumericIndex;
use numeric_range_tree::{NodeIndex, NumericRangeTree, TreeUniqueId};
use rqe_core::FieldIndex;
use serde::{Deserialize, Serialize};

use super::numeric::{
    NumericNodeDelta, NumericNodeSnapshot, apply_numeric_node, apply_numeric_node_with,
    compact_stats, listed_nodes, ranges_of_nodes_added_to, snapshot_tree_node, tree_node_deltas,
};
use crate::fork::wire::{deserialize, serialize};
use crate::spec_access::SpecWriteAccess;
use crate::{ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// The keyed index behind `fs`, or `None` when the field is not a MAP NUMERIC field
/// or no document has been written to it yet.
fn fetch_index(fs: &FieldSpec) -> Option<&KeyedNumericIndex> {
    if !fs.types().contains(FieldSpecType::MapNumeric) {
        return None;
    }
    let raw_fs = std::ptr::from_ref(fs).cast::<ffi::FieldSpec>();
    // SAFETY: `FieldSpec` is a `repr(transparent)` wrapper of `ffi::FieldSpec`.
    let raw_fs = unsafe { &*raw_fs };
    // SAFETY: the field is a MAP NUMERIC field, so `mapNumericOpts` is the active member.
    let index = unsafe { raw_fs.__bindgen_anon_1.mapNumericOpts.index };
    // SAFETY: a non-null index is the `KeyedNumericIndex` the FFI allocated for this
    // field, owned by it and kept alive by the caller's spec lock.
    unsafe { index.cast::<KeyedNumericIndex>().as_ref() }
}

/// [`fetch_index`], exclusively.
fn fetch_index_mut(fs: &mut FieldSpec) -> Option<&mut KeyedNumericIndex> {
    if !fs.types().contains(FieldSpecType::MapNumeric) {
        return None;
    }
    let raw_fs = std::ptr::from_mut(fs).cast::<ffi::FieldSpec>();
    // SAFETY: as in `fetch_index`.
    let raw_fs = unsafe { &mut *raw_fs };
    // SAFETY: as in `fetch_index`.
    let index = unsafe { raw_fs.__bindgen_anon_1.mapNumericOpts.index };
    // SAFETY: as in `fetch_index`; the caller's write lock makes the borrow unique.
    unsafe { index.cast::<KeyedNumericIndex>().as_mut() }
}

fn field_index_of(position: usize) -> FieldIndex {
    FieldIndex::try_from(position).expect("a spec has fewer fields than a field index can count")
}

/// One key's tree, named so it can be found again across spec lock releases, and
/// the header of its stream on the fork pipe.
///
/// The field position survives lock releases because a spec's field array is only
/// ever appended to. Every lookup re-checks both unique ids regardless, so a replaced
/// index, or a key removed and added again, is never mistaken for the tree named.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct KeyedTreeId {
    /// Position of the field in the spec's field array.
    pub field_index: FieldIndex,
    /// [`KeyedNumericIndex::unique_id`] of the field's index at scan time.
    pub index_unique_id: u32,
    /// The key the tree is stored under.
    #[serde(with = "serde_bytes")]
    pub key: Vec<u8>,
    /// [`NumericRangeTree::unique_id`] of the key's tree at scan time.
    pub tree_unique_id: u32,
}

impl KeyedTreeId {
    fn of(position: usize, index: &KeyedNumericIndex, key: &[u8], tree: &NumericRangeTree) -> Self {
        Self {
            field_index: field_index_of(position),
            index_unique_id: u32::from(index.unique_id()),
            key: key.to_vec(),
            tree_unique_id: u32::from(tree.unique_id()),
        }
    }

    /// The index under the read lock; `None` when the field no longer holds the one named.
    fn index<'s>(&self, spec: &'s IndexSpecReadGuard) -> Option<&'s KeyedNumericIndex> {
        let fs = spec.field_specs().get(usize::from(self.field_index))?;
        fetch_index(fs).filter(|index| u32::from(index.unique_id()) == self.index_unique_id)
    }

    /// The tree under the read lock; `None` when the field, its index or the key no
    /// longer holds the one named.
    pub fn tree<'s>(&self, spec: &'s IndexSpecReadGuard) -> Option<&'s NumericRangeTree> {
        let tree = self.index(spec)?.get(&self.key)?;
        (u32::from(tree.unique_id()) == self.tree_unique_id).then_some(tree)
    }

    /// The index under the write lock, or an apply error when the field no longer
    /// holds the one named.
    fn index_mut<'g>(
        &self,
        guard: &'g mut IndexSpecWriteGuard<'_>,
    ) -> Result<&'g mut KeyedNumericIndex, GcError> {
        let fs = guard
            .field_specs_mut()
            .get_mut(usize::from(self.field_index))
            .ok_or(GcError::TargetGone(
                "no field in the spec is at the scanned field index",
            ))?;
        fetch_index_mut(fs)
            .filter(|index| u32::from(index.unique_id()) == self.index_unique_id)
            .ok_or(GcError::TargetGone(
                "the field's keyed numeric index is not the one the scan saw",
            ))
    }

    /// The identity [`KeyedNumericIndex::update_tree`] wants, if `index` holds the tree named.
    fn expected(&self, index: &KeyedNumericIndex) -> Option<TreeUniqueId> {
        let id = index.get(&self.key)?.unique_id();
        (u32::from(id) == self.tree_unique_id).then_some(id)
    }

    /// Run `f` on the named tree under the write lock, keeping the index's memory
    /// accounting exact; an apply error when the tree is no longer the one named.
    fn update<R>(
        &self,
        guard: &mut IndexSpecWriteGuard<'_>,
        f: impl FnOnce(&mut NumericRangeTree) -> R,
    ) -> Result<R, GcError> {
        let index = self.index_mut(guard)?;
        let expected = self.expected(index);
        expected
            .and_then(|expected| index.update_tree(&self.key, expected, f))
            .ok_or(GcError::TargetGone(
                "the key's numeric tree was removed or replaced since the scan",
            ))
    }

    /// Finish a tree whose deltas were applied: compact it if `compact` and it is
    /// sparse, then drop its key if it holds no entry.
    ///
    /// The tree's memory beyond what its entries and blocks account for was never
    /// added to the spec's statistics, so only the blocks an emptied tree still holds
    /// are returned.
    fn finish(
        &self,
        guard: &mut IndexSpecWriteGuard<'_>,
        compact: bool,
    ) -> Result<GcApplyStats, GcError> {
        let mut stats = GcApplyStats::default();
        if compact {
            stats.record(self.update(guard, compact_stats)?);
        }
        let index = self.index_mut(guard)?;
        let expected = self.expected(index).ok_or(GcError::TargetGone(
            "the key's numeric tree was removed or replaced since the scan",
        ))?;
        let blocks_held = index
            .get_checked(&self.key, expected)
            .filter(|tree| tree.num_entries() == 0)
            .map(|tree| {
                tree.iter()
                    .filter_map(|node| node.range())
                    .map(|range| range.entries().num_blocks())
                    .sum::<usize>()
            });
        if let Some(blocks_held) = blocks_held
            && index.remove_key_if_empty(&self.key, expected)
        {
            stats.block_count_delta -= blocks_held as i64;
        }
        Ok(stats)
    }
}

/// Every initialised MAP NUMERIC index of the spec with its fields' positions.
fn indexes<'s>(
    spec: &'s IndexSpecReadGuard,
) -> impl Iterator<Item = (usize, &'s KeyedNumericIndex)> {
    spec.field_specs()
        .iter()
        .enumerate()
        .filter_map(|(position, fs)| Some((position, fetch_index(fs)?)))
}

/// Collect GC deltas for every key of every MAP NUMERIC field in the spec and write
/// them to the parent process.
///
/// For each key whose tree has GC work, sends a `Some` [`KeyedTreeId`], then one `Some`
/// [`NumericNodeDelta`] per tree node with work, followed by a `None` node terminator.
/// Keys without work send nothing. A final `None` header terminates the scanner.
///
/// Write errors are surfaced so the caller can terminate the child process.
///
/// `live` says which documents still exist; see [`DocLiveness`].
pub fn collect_keyed_numeric(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    live: impl DocLiveness + Copy,
) -> io::Result<()> {
    for (position, index) in indexes(spec) {
        for (key, tree) in index.iter() {
            let mut deltas = tree_node_deltas(tree, live).peekable();
            if deltas.peek().is_none() {
                continue;
            }
            serialize(writer, Some(KeyedTreeId::of(position, index, key, tree)))?;
            for delta in deltas {
                serialize(writer, Some(delta))?;
            }
            serialize(writer, None::<NumericNodeDelta>)?;
        }
    }
    serialize(writer, None::<KeyedTreeId>)
}

/// Receive and apply one key's node deltas, then compact its tree if
/// `clean_numeric_empty_nodes` and drop the key if it is empty.
///
/// A tree that is gone or replaced by the time its deltas arrive is skipped, with its
/// remaining deltas read and dropped so the stream stays aligned.
///
/// Return [`HandleOutcome::Done`] when the child sent the global terminator instead of a header.
pub fn handle_keyed_numeric_with(
    reader: &mut impl Read,
    spec: &mut impl SpecWriteAccess,
    stats: &mut GcApplyStats,
    clean_numeric_empty_nodes: bool,
) -> Result<HandleOutcome, GcError> {
    let Some(id) = deserialize::<Option<KeyedTreeId>>(reader, "decoding keyed numeric tree")?
    else {
        return Ok(HandleOutcome::Done);
    };

    let mut skipped = false;
    let mut applied = false;
    while let Some(node) =
        deserialize::<Option<NumericNodeDelta>>(&mut *reader, "decoding keyed numeric node")?
    {
        if skipped {
            continue;
        }
        let outcome =
            spec.with_write(|guard| Ok(id.update(guard, |tree| apply_numeric_node(tree, node))))?;
        match outcome {
            Ok(Some(node_stats)) => {
                stats.record(node_stats);
                applied = true;
            }
            Ok(None) => stats.numeric_nodes_missed += 1,
            Err(GcError::TargetGone(reason)) => {
                tracing::debug!(reason, "fork GC skipped a stale keyed numeric tree");
                skipped = true;
            }
            Err(error) => return Err(error),
        }
    }

    if applied {
        match spec.with_write(|guard| Ok(id.finish(guard, clean_numeric_empty_nodes)))? {
            Ok(finished) => stats.record(finished),
            Err(GcError::TargetGone(_)) => {}
            Err(error) => return Err(error),
        }
    }
    Ok(HandleOutcome::Collected)
}

/// Handle one key off `fgc`'s pipe, flushing the tallied [`GcApplyStats`] to the spec and the GC.
///
/// Return [`HandleOutcome::Done`] once the child has sent every key.
pub fn handle_keyed_numeric(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    let mut spec = fgc.index_spec();
    let mut stats = GcApplyStats::default();
    let clean_numeric_empty_nodes = fgc.clean_numeric_empty_nodes();

    let result = handle_keyed_numeric_with(
        &mut fgc.reader(),
        &mut spec,
        &mut stats,
        clean_numeric_empty_nodes,
    );

    // No need to lock the spec and update the stats if there are none.
    if stats != GcApplyStats::default() {
        spec.with_write(|guard| {
            stats.apply(fgc, guard);
            Ok(())
        })?;
    }

    result
}

/// The nodes of one key's tree, listed so they can be scanned one at a time across
/// spec lock releases.
#[derive(Debug, Clone)]
pub struct KeyedTreeKey {
    /// The tree the nodes belong to.
    pub id: KeyedTreeId,
    /// [`NumericRangeTree::revision_id`] when the nodes were listed.
    pub revision_id: u32,
    /// Every node of the tree, children before parents.
    pub nodes: Vec<NodeIndex>,
}

/// List the nodes of every key's tree of every MAP NUMERIC field in the spec.
pub fn keyed_tree_keys(spec: &IndexSpecReadGuard) -> Vec<KeyedTreeKey> {
    indexes(spec)
        .flat_map(|(position, index)| {
            index.iter().map(move |(key, tree)| KeyedTreeKey {
                id: KeyedTreeId::of(position, index, key, tree),
                revision_id: tree.revision_id(),
                nodes: listed_nodes(tree),
            })
        })
        .collect()
}

/// The doc IDs spanned by the ranges of the nodes each of `keys` gained since it
/// listed its nodes; see [`ranges_of_nodes_added_since`](super::numeric::ranges_of_nodes_added_since).
/// A key or index gone since contributes nothing.
pub fn keyed_ranges_of_nodes_added_since(
    spec: &IndexSpecReadGuard,
    keys: &[KeyedTreeKey],
) -> Vec<RangeInclusive<DocId>> {
    keys.iter()
        .filter_map(|key| {
            Some(ranges_of_nodes_added_to(
                key.id.tree(spec)?,
                key.revision_id,
                &key.nodes,
            ))
        })
        .flatten()
        .collect()
}

/// Copy the range index of node `node_idx` of the tree `id` names; see
/// [`snapshot_numeric_node`](super::numeric::snapshot_numeric_node).
pub fn snapshot_keyed_node(
    spec: &IndexSpecReadGuard,
    id: &KeyedTreeId,
    node_idx: NodeIndex,
    live: &impl DocLiveness,
) -> Option<NumericNodeSnapshot> {
    snapshot_tree_node(id.tree(spec)?, node_idx, live)
}

/// Apply one node's delta to the tree `id` names, rescanning a grown last block
/// against `live`; see [`apply_numeric_node_with`].
///
/// A node gone by now counts as missed; an apply error when the tree is gone or replaced.
pub fn apply_keyed_node_with(
    guard: &mut IndexSpecWriteGuard<'_>,
    id: &KeyedTreeId,
    node: NumericNodeDelta,
    live: &dyn DocLiveness,
) -> Result<GcApplyStats, GcError> {
    let applied = id.update(guard, |tree| {
        apply_numeric_node_with(tree, node, Some(live))
    })?;
    Ok(applied.unwrap_or(GcApplyStats {
        numeric_nodes_missed: 1,
        ..GcApplyStats::default()
    }))
}

/// [`KeyedTreeId::finish`] for the thread policy, which tracks the trees it applied to.
pub fn finish_keyed_tree(
    guard: &mut IndexSpecWriteGuard<'_>,
    id: &KeyedTreeId,
    compact: bool,
) -> Result<GcApplyStats, GcError> {
    id.finish(guard, compact)
}
