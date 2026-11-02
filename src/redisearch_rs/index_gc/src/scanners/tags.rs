/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the per-tag posting lists of every TAG
//! field in a spec.

use std::{
    ffi::c_char,
    io::{self, Write},
    ptr::NonNull,
};

use field_spec::{FieldSpec, FieldSpecType};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{GcScanDelta, opaque::InvertedIndex};
use lending_iterator::LendingIterator;
use serde::{Deserialize, Serialize};
use trie_rs::{
    TrieMapOpaque,
    iter::{RangeBoundary, RangeFilter},
};

use crate::fork::wire::{deserialize, serialize};
use crate::{DocExistence, ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// Return the in-memory C tag index behind `fs`.
///
/// Disk-backed fields are reported as absent: their postings live behind the
/// RSE API, so a fork GC pass has nothing in memory to collect for them.
fn fetch_tag_index(fs: &FieldSpec) -> Option<&ffi::TagIndex> {
    if !fs.types().contains(FieldSpecType::Tag) {
        return None;
    }
    let raw_fs = std::ptr::from_ref(fs).cast::<ffi::FieldSpec>();
    // SAFETY: the wrapper is repr(transparent) over the C field spec, so the
    // cast pointer is valid for the lifetime of the borrow.
    let raw_fs = unsafe { &*raw_fs };
    // SAFETY: the field type check above establishes that `tagOpts` is the active member
    // of that field's C union.
    let index = unsafe { raw_fs.__bindgen_anon_1.tagOpts.tagIndex };
    // SAFETY: a non-null tag-index pointer is owned by the field, and the
    // caller holds the spec's read lock for the returned borrow.
    let index = unsafe { index.as_ref()? };

    if index.diskSpec.is_null() {
        Some(index)
    } else {
        None
    }
}

/// [`fetch_tag_index()`], exclusively.
fn fetch_tag_index_mut(fs: &mut FieldSpec) -> Option<&mut ffi::TagIndex> {
    if !fs.types().contains(FieldSpecType::Tag) {
        return None;
    }
    let raw_fs = std::ptr::from_mut(fs).cast::<ffi::FieldSpec>();
    // SAFETY: the wrapper is repr(transparent) over the C field spec, so the
    // cast pointer is valid for the lifetime of the borrow.
    let raw_fs = unsafe { &mut *raw_fs };
    // SAFETY: the field type check above establishes that `tagOpts` is the active member
    // of that field's C union.
    let index = unsafe { raw_fs.__bindgen_anon_1.tagOpts.tagIndex };
    // SAFETY: a non-null tag-index pointer is owned by the field. The
    // exclusive field borrow and the spec write lock make the returned mutable
    // borrow unique.
    let index = unsafe { index.as_mut()? };

    if index.diskSpec.is_null() {
        Some(index)
    } else {
        None
    }
}

/// One tag value's worth of GC work, as it travels the pipe.
///
/// Each message locates its field and tag index, so the parent can route and
/// check it on its own. `T` lets the child serialize borrowed tag bytes without
/// copying; the parent deserializes the default owned form.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: serde_bytes::Serialize",
    deserialize = "T: serde_bytes::Deserialize<'de>"
))]
pub struct TagEntry<T = Box<[u8]>> {
    /// Position of the TAG field in the spec's fields. Fields are only ever
    /// appended, so a position names one field for the spec's lifetime.
    pub field_index: ffi::t_fieldIndex,
    /// Unique id of the field's tag index. Ids come from a wrapping counter, so
    /// this only tells a replaced index apart from the scanned one at
    /// [`Self::field_index`], not one field from another.
    pub tag_index_unique_id: u32,
    /// The tag value, possibly empty (`INDEXEMPTY`).
    #[serde(with = "serde_bytes")]
    pub tag: T,
    /// Unique id of the tag's inverted index.
    pub inverted_index_unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// Collect GC deltas for every tag of every TAG field in the spec and write them
/// to the parent process.
///
/// Scan failures skip the tag, leaving it for the next GC cycle. Write errors are
/// surfaced so the caller can terminate the child process.
///
/// `docs` says which documents still exist.
pub fn collect_tags(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    docs: &DocExistence,
) -> io::Result<()> {
    for (field_index, tag_index) in spec
        .field_specs()
        .iter()
        .enumerate()
        .filter_map(|(i, fs)| fetch_tag_index(fs).map(|tag_index| (i, tag_index)))
    {
        let field_index = ffi::t_fieldIndex::try_from(field_index)
            .expect("a spec has fewer fields than a field index can count");

        for_each_tag(tag_index, |tag, ii| {
            let Ok(Some(delta)) = ii.scan_gc(|id| docs.contains(id)) else {
                return Ok(());
            };

            serialize(
                writer,
                Some(TagEntry {
                    field_index,
                    tag_index_unique_id: tag_index.uniqueId,
                    tag,
                    inverted_index_unique_id: u32::from(ii.unique_id()),
                    delta,
                }),
            )
        })?;
    }

    serialize(writer, None::<TagEntry<&[u8]>>)
}

/// Visit every tag value of `tag_index` with its posting list, in trie order.
///
/// Stops at, and returns, the first error `visit` reports.
fn for_each_tag(
    tag_index: &ffi::TagIndex,
    mut visit: impl FnMut(&[u8], &InvertedIndex) -> io::Result<()>,
) -> io::Result<()> {
    debug_assert!(!tag_index.values.is_null());
    // SAFETY: the C tag index owns a TrieMapOpaque, kept live and immutable
    // by the spec guard for the entire traversal.
    let values = unsafe { &*tag_index.values.cast::<TrieMapOpaque>() };
    let mut iterator = values.0.lending_iter();
    while let Some((tag, value)) = iterator.next() {
        let value = NonNull::new(*value)
            .ok_or_else(|| io::Error::other("an in-memory tag index has a null posting list"))?;

        // SAFETY: C TagIndex values are Rust opaque inverted indexes.
        // The spec's read lock prevents their removal during iteration.
        let ii = unsafe { value.cast::<InvertedIndex>().as_ref() };

        visit(tag, ii)?;
    }
    Ok(())
}

/// Where a [`scan_tags_after`] walk resumes: a TAG field, and the last tag
/// value of that field it visited.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TagsCursor {
    /// Position of the field in the spec's field array, which only grows.
    field_index: usize,
    /// The last tag value visited in that field; `None` before the first.
    after: Option<Vec<u8>>,
}

/// Scan the tag values of every in-memory TAG field, in field then
/// lexicographic order, starting strictly after `cursor`, until `should_stop`
/// returns `true` after a value or no value is left.
///
/// Appends each entry to `found` and moves `cursor` past every value visited,
/// so a later call resumes after them even if the tag indexes changed in
/// between. Each entry carries the unique ids [`apply_tag_entry`] checks.
/// Returns whether `should_stop` ended the walk, in which case values may
/// remain; a call after the last value returns `false`.
pub fn scan_tags_after(
    spec: &IndexSpecReadGuard,
    cursor: &mut TagsCursor,
    mut should_stop: impl FnMut() -> bool,
    found: &mut Vec<TagEntry>,
) -> bool {
    let fields = spec.field_specs();
    while let Some(fs) = fields.get(cursor.field_index) {
        if fs.types().contains(FieldSpecType::Tag)
            && let Some(tag_index) = fetch_tag_index(fs)
        {
            let field_index = ffi::t_fieldIndex::try_from(cursor.field_index)
                .expect("a spec has fewer fields than a field index can count");
            // SAFETY: an in-memory C tag index owns its values map, created by
            // `NewTrieMap` as a boxed `TrieMapOpaque`, and the read lock keeps it
            // alive and unmodified for this borrow.
            let values = unsafe { &*tag_index.values.cast::<TrieMapOpaque>() };
            let after = cursor.after.take();
            let filter = RangeFilter {
                min: after.as_deref().map(|value| RangeBoundary {
                    value,
                    is_included: false,
                }),
                max: None,
            };

            for (tag, &value) in values.0.range_iter(filter) {
                if !value.is_null() {
                    // SAFETY: C TagIndex values are Rust opaque inverted indexes,
                    // kept alive by the read lock.
                    let ii = unsafe { &*value.cast::<InvertedIndex>() };
                    if let Ok(Some(delta)) = ii.scan_gc(|id| spec.doc_exists(id)) {
                        found.push(TagEntry {
                            field_index,
                            tag_index_unique_id: tag_index.uniqueId,
                            tag: tag.as_slice().into(),
                            inverted_index_unique_id: u32::from(ii.unique_id()),
                            delta,
                        });
                    }
                }
                cursor.after = Some(tag);
                if should_stop() {
                    return true;
                }
            }
        }
        cursor.field_index += 1;
        cursor.after = None;
    }
    false
}

/// Apply one decoded tag message to the field's tag index.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
pub fn apply_tag_entry(
    entry: TagEntry,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    let fs = guard
        .field_specs_mut()
        .get_mut(usize::from(entry.field_index))
        .ok_or(GcError::ApplyError(
            "the spec has no field at the scanned position",
        ))?;

    let tag_index = fetch_tag_index_mut(fs).ok_or(GcError::ApplyError(
        "the field no longer has an in-memory tag index",
    ))?;

    if tag_index.uniqueId != entry.tag_index_unique_id {
        return Err(GcError::ApplyError(
            "the field's tag index is not the one the child scanned",
        ));
    }

    debug_assert!(!tag_index.values.is_null());
    // SAFETY: the tag index owns this trie, and the write guard keeps it live
    // and excludes concurrent changes throughout lookup and application.
    let values = unsafe { &*tag_index.values.cast::<TrieMapOpaque>() };
    let ii = values.find(&entry.tag).ok_or(GcError::ApplyError(
        "the tag was removed before the delta could be applied",
    ))?;

    // SAFETY: the successful trie lookup returns a live opaque inverted
    // index owned by `tag_index`. The write lock makes this mutable borrow unique.
    let ii = unsafe { &mut *ii.cast::<InvertedIndex>().as_ptr() };

    if u32::from(ii.unique_id()) != entry.inverted_index_unique_id {
        return Err(GcError::ApplyError(
            "the tag's posting list is not the one the child scanned",
        ));
    }

    let info = ii.apply_gc(entry.delta);

    let (extra, remaining_blocks) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();

        // SAFETY: the successful lookup above proved the tag still belongs to
        // this index. TagIndex_DeleteTagValue frees its opaque inverted index.
        if unsafe {
            ffi::TagIndex_DeleteTagValue(
                std::ptr::from_mut(tag_index),
                entry.tag.as_ptr().cast::<c_char>(),
                entry.tag.len(),
            )
        } == 0
        {
            return Err(GcError::ApplyError(
                "the tag could not be removed after its posting list was emptied",
            ));
        }

        // Empty values are never inserted in the suffix trie.
        if !entry.tag.is_empty() && !tag_index.suffix.is_null() {
            // SAFETY: a non-empty tag in an index with a suffix trie has its
            // suffix entry maintained by the C tag index.
            unsafe {
                ffi::TagIndex_DeleteTagSuffix(
                    std::ptr::from_mut(tag_index),
                    entry.tag.as_ptr().cast::<c_char>(),
                    entry.tag.len(),
                )
            };
        }

        (extra, remaining_blocks)
    } else {
        (0, 0)
    };

    Ok(GcApplyStats {
        records_removed: info.entries_removed,
        bytes_collected: info.bytes_freed + extra,
        bytes_allocated: info.bytes_allocated,
        block_count_delta: info.block_count_delta - remaining_blocks as i64,
        blocks_denied: info.ignored_last_block as u64,
        ..GcApplyStats::default()
    })
}

/// Parent-side handler for one iteration of the tags GC protocol.
///
/// Reads one message from the pipe: a tag entry is applied and reported as
/// [`HandleOutcome::Collected`]; a terminator ends the iteration with
/// [`HandleOutcome::Done`].
pub fn handle_tags(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    crate::fork::wire::handle_one(
        fgc,
        |reader| deserialize(reader, "decoding tag entry"),
        apply_tag_entry,
    )
}
