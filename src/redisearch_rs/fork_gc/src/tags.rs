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
    io::{self, Read, Write},
    ptr::NonNull,
};

use field_spec::{FieldSpec, FieldSpecType};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{GcScanDelta, opaque::InvertedIndex};
use lending_iterator::LendingIterator;
use serde::{Deserialize, Serialize};
use trie_rs::TrieMapOpaque;

use crate::{ForkGC, Frame, GcApplyStats, HandleError, HandleOutcome};

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

/// Routing information shared by one field's tag deltas.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: serde_bytes::Serialize",
    deserialize = "T: serde_bytes::Deserialize<'de>"
))]
pub struct TagFieldHeader<T = Box<[u8]>> {
    /// Name of the TAG field.
    #[serde(with = "serde_bytes")]
    pub field_name: T,
    /// Unique id of the field's tag index.
    pub tag_index_unique_id: u32,
}

/// One tag's GC delta, routed by the preceding [`TagFieldHeader`].
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: serde_bytes::Serialize",
    deserialize = "T: serde_bytes::Deserialize<'de>"
))]
pub struct TagEntry<T = Box<[u8]>> {
    /// The tag value, possibly empty (`INDEXEMPTY`).
    #[serde(with = "serde_bytes")]
    pub tag: T,
    /// Unique id of the tag's inverted index.
    pub inverted_index_unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

impl<T: serde_bytes::Serialize> TagEntry<T> {
    /// Encode one length-prefixed delta, buffering MessagePack's small writes.
    pub fn encode(&self, writer: &mut impl Write) -> io::Result<()> {
        let bytes = rmp_serde::to_vec(self).map_err(io::Error::other)?;
        Frame::data(&bytes).encode(writer)
    }
}

/// Collect GC deltas for every tag of every TAG field in the spec and write them
/// to the parent process.
///
/// Scan failures skip the tag, leaving it for the next GC cycle. Write errors are
/// surfaced so the caller can terminate the child process.
pub fn collect_tags(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    for (fs, tag_index) in spec
        .field_specs()
        .iter()
        .filter_map(|fs| fetch_tag_index(fs).map(|tag_index| (fs, tag_index)))
    {
        let mut pending_header = Some(TagFieldHeader {
            field_name: fs.field_name().secret_value().to_bytes(),
            tag_index_unique_id: tag_index.uniqueId,
        });

        debug_assert!(!tag_index.values.is_null());
        // SAFETY: the C tag index owns a TrieMapOpaque, kept live and immutable
        // by the spec guard for the entire traversal.
        let values = unsafe { &*tag_index.values.cast::<TrieMapOpaque>() };
        let mut iterator = values.0.lending_iter();
        while let Some((tag, value)) = iterator.next() {
            let value = NonNull::new(*value).ok_or_else(|| {
                io::Error::other("an in-memory tag index has a null posting list")
            })?;

            // SAFETY: C TagIndex values are Rust opaque inverted indexes.
            // The spec's read lock prevents their removal during iteration.
            let ii = unsafe { value.cast::<InvertedIndex>().as_ref() };

            let Ok(Some(delta)) = ii.scan_gc(|id| spec.doc_exists(id)) else {
                continue;
            };

            if let Some(header) = pending_header.take() {
                Some(header)
                    .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
                    .map_err(io::Error::other)?;
            }
            TagEntry {
                tag,
                inverted_index_unique_id: u32::from(ii.unique_id()),
                delta,
            }
            .encode(writer)?;
        }
        if pending_header.is_none() {
            Frame::Terminator.encode(writer)?;
        }
    }

    Option::<TagFieldHeader<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(writer))
        .map_err(io::Error::other)
}

/// Decode a field header, or `None` at the end of the TAG scanner's stream.
pub fn receive_field_header(reader: &mut impl Read) -> Result<Option<TagFieldHeader>, HandleError> {
    rmp_serde::from_read(reader).map_err(|e| HandleError::codec("decoding tag field header", e))
}

/// Decode one tag delta, or `None` at the end of the current field.
pub fn receive_tag_entry(reader: &mut impl Read) -> Result<Option<TagEntry>, HandleError> {
    match Frame::decode(reader).map_err(|e| HandleError::codec("reading tag entry", e))? {
        Frame::Data(bytes) => rmp_serde::from_slice(&bytes)
            .map(Some)
            .map_err(|e| HandleError::codec("decoding tag entry", e)),
        Frame::Terminator => Ok(None),
        Frame::Empty => Err(HandleError::codec("reading tag entry", "empty tag delta")),
    }
}

/// Apply one decoded tag message to the field's tag index.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
pub fn apply_tag_entry(
    field: &TagFieldHeader,
    entry: TagEntry,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError> {
    let fs = guard
        .field_specs_mut()
        .iter_mut()
        .find(|fs| fs.field_name().secret_value().to_bytes() == &*field.field_name)
        .ok_or(HandleError::ApplyError(
            "no field in the spec matches the scanned field name",
        ))?;

    let tag_index = fetch_tag_index_mut(fs).ok_or(HandleError::ApplyError(
        "the field no longer has an in-memory tag index",
    ))?;

    if tag_index.uniqueId != field.tag_index_unique_id {
        return Err(HandleError::ApplyError(
            "the field's tag index is not the one the child scanned",
        ));
    }

    debug_assert!(!tag_index.values.is_null());
    // SAFETY: the tag index owns this trie, and the write guard keeps it live
    // and excludes concurrent changes throughout lookup and application.
    let values = unsafe { &*tag_index.values.cast::<TrieMapOpaque>() };
    let ii = values.find(&entry.tag).ok_or(HandleError::ApplyError(
        "the tag was removed before the delta could be applied",
    ))?;

    // SAFETY: the successful trie lookup returns a live opaque inverted
    // index owned by `tag_index`. The write lock makes this mutable borrow unique.
    let ii = unsafe { &mut *ii.cast::<InvertedIndex>().as_ptr() };

    if u32::from(ii.unique_id()) != entry.inverted_index_unique_id {
        return Err(HandleError::ApplyError(
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
            return Err(HandleError::ApplyError(
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

/// Handle one field's tag deltas, acquiring the spec lock separately for each tag.
///
/// Returns [`HandleOutcome::Done`] at the end of the TAG scanner's stream.
pub fn handle_tags(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError> {
    let Some(field) = receive_field_header(&mut fgc.reader())? else {
        return Ok(HandleOutcome::Done);
    };

    while let Some(entry) = receive_tag_entry(&mut fgc.reader())? {
        let mut spec_ref = fgc.index_spec().promote().ok_or(HandleError::SpecDeleted)?;
        let mut guard = spec_ref.write();
        let stats = apply_tag_entry(&field, entry, &mut guard)?;
        stats.apply(fgc, &mut guard);
    }
    Ok(HandleOutcome::Collected)
}
