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

use super::field_index_of;
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
    /// Dropping a key subtracts its whole [`TreeCharge`](keyed_numeric_index::TreeCharge), which is exactly what the
    /// spec's statistics hold for it (see [`KeyedNumericIndex::add`]), so they return
    /// to what they were before the key existed.
    pub(crate) fn finish(
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
        if let Some(charge) = index.remove_empty_key(&self.key, expected) {
            stats.records_removed += charge.records;
            stats.bytes_collected += charge.inverted_size;
            stats.block_count_delta -= charge.blocks as i64;
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

/// Whether the spec has a MAP NUMERIC field with an initialised index.
pub fn has_map_numeric(spec: &IndexSpecReadGuard) -> bool {
    indexes(spec).next().is_some()
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

/// One key's stream off the fork pipe: its tree and the deltas of the nodes with work.
struct KeyStream {
    id: KeyedTreeId,
    deltas: Vec<NumericNodeDelta>,
}

/// Read one key's whole stream, so applying it needs a single write hold. `None` is
/// the global terminator.
///
/// The deltas of a key are held in memory together, which is bounded by the size of its tree.
fn read_key_stream(reader: &mut impl Read) -> Result<Option<KeyStream>, GcError> {
    let Some(id) = deserialize::<Option<KeyedTreeId>>(reader, "decoding keyed numeric tree")?
    else {
        return Ok(None);
    };
    let mut deltas = Vec::new();
    while let Some(node) =
        deserialize::<Option<NumericNodeDelta>>(&mut *reader, "decoding keyed numeric node")?
    {
        deltas.push(node);
    }
    Ok(Some(KeyStream { id, deltas }))
}

/// Apply one key's deltas, compact its tree if `clean_numeric_empty_nodes` and drop the
/// key if it is empty, all under one write hold.
///
/// `flush` receives this key's [`GcApplyStats`] under the same hold, so the spec's
/// statistics change atomically with the tree. A tree that is gone or replaced is skipped.
fn apply_key_stream(
    spec: &mut impl SpecWriteAccess,
    stream: KeyStream,
    stats: &mut GcApplyStats,
    clean_numeric_empty_nodes: bool,
    flush: impl FnOnce(&mut IndexSpecWriteGuard<'_>, &GcApplyStats),
) -> Result<(), GcError> {
    let KeyStream { id, deltas } = stream;
    spec.with_write(|guard| {
        let mut key_stats = GcApplyStats::default();
        let mut reached_tree = false;
        let mut failure = None;
        for node in deltas {
            match id.update(guard, |tree| apply_numeric_node(tree, node)) {
                Ok(Some(node_stats)) => {
                    key_stats.record(node_stats);
                    reached_tree = true;
                }
                Ok(None) => {
                    key_stats.numeric_nodes_missed += 1;
                    reached_tree = true;
                }
                Err(GcError::TargetGone(reason)) => {
                    tracing::debug!(reason, "fork GC skipped a stale keyed numeric tree");
                    reached_tree = false;
                    break;
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        if reached_tree && failure.is_none() {
            match id.finish(guard, clean_numeric_empty_nodes) {
                Ok(finished) => key_stats.record(finished),
                Err(GcError::TargetGone(_)) => {}
                Err(error) => failure = Some(error),
            }
        }
        flush(guard, &key_stats);
        stats.record(key_stats);
        failure.map_or(Ok(()), Err)
    })
}

/// Receive and apply one key's stream; see [`apply_key_stream`] for `flush`.
///
/// Return [`HandleOutcome::Done`] when the child sent the global terminator instead of a header.
pub fn handle_keyed_numeric_with(
    reader: &mut impl Read,
    spec: &mut impl SpecWriteAccess,
    stats: &mut GcApplyStats,
    clean_numeric_empty_nodes: bool,
    flush: impl FnOnce(&mut IndexSpecWriteGuard<'_>, &GcApplyStats),
) -> Result<HandleOutcome, GcError> {
    let Some(stream) = read_key_stream(reader)? else {
        return Ok(HandleOutcome::Done);
    };
    apply_key_stream(spec, stream, stats, clean_numeric_empty_nodes, flush)?;
    Ok(HandleOutcome::Collected)
}

/// Handle one key off `fgc`'s pipe, flushing its [`GcApplyStats`] to the spec and the GC
/// under the write hold that applied it.
///
/// Return [`HandleOutcome::Done`] once the child has sent every key.
pub fn handle_keyed_numeric(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    let Some(stream) = read_key_stream(&mut fgc.reader())? else {
        return Ok(HandleOutcome::Done);
    };
    let mut spec = fgc.index_spec();
    let clean_numeric_empty_nodes = fgc.clean_numeric_empty_nodes();
    apply_key_stream(
        &mut spec,
        stream,
        &mut GcApplyStats::default(),
        clean_numeric_empty_nodes,
        |guard, key_stats| key_stats.apply(fgc, guard),
    )?;
    Ok(HandleOutcome::Collected)
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
