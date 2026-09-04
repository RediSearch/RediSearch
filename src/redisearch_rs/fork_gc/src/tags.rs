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
};

use field_spec::{FieldSpec, FieldSpecType};
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{GcScanDelta, opaque::InvertedIndex};
use serde::{Deserialize, Serialize};

use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// Return the in-memory C tag index behind `fs`.
///
/// Disk-backed fields are reported as absent: their postings live behind the
/// RSE API, so a fork GC pass has nothing in memory to collect for them.
fn fetch_tag_index(fs: &FieldSpec) -> Option<&ffi::TagIndex> {
    let raw_fs = std::ptr::from_ref(fs).cast::<ffi::FieldSpec>();
    // SAFETY: the wrapper is repr(transparent) over the C field spec, so the
    // cast pointer is valid for the lifetime of the borrow.
    let raw_fs = unsafe { &*raw_fs };
    // SAFETY: callers only pass TAG fields, so `tagOpts` is the active member
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
    let raw_fs = std::ptr::from_mut(fs).cast::<ffi::FieldSpec>();
    // SAFETY: the wrapper is repr(transparent) over the C field spec, so the
    // cast pointer is valid for the lifetime of the borrow.
    let raw_fs = unsafe { &mut *raw_fs };
    // SAFETY: callers only pass TAG fields, so `tagOpts` is the active member
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

/// A tag GC message could not be applied to the index the child scanned.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("{msg}")]
pub struct TagError {
    msg: &'static str,
}

impl TagError {
    /// Build a [`HandleError::Custom`] carrying `msg`.
    const fn new(msg: &'static str) -> HandleError<Self> {
        HandleError::Custom(Self { msg })
    }
}

/// One tag value's worth of GC work, as it travels the pipe.
///
/// `T` lets the child serialize borrowed field and tag bytes without copying;
/// the parent deserializes the default owned form.
#[derive(Debug, Serialize, Deserialize)]
pub struct TagEntry<T = Box<[u8]>> {
    /// Name of the TAG field the tag belongs to.
    pub field_name: T,
    /// Unique id of the field's tag index.
    pub tag_index_unique_id: u32,
    /// The tag value, possibly empty (`INDEXEMPTY`).
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
pub fn collect_tags(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    for (fs, tag_index) in spec
        .field_specs()
        .iter()
        .filter(|fs| fs.types().contains(FieldSpecType::Tag))
        .filter_map(|fs| fetch_tag_index(fs).map(|tag_index| (fs, tag_index)))
    {
        let field_name = fs.field_name().secret_value().to_bytes();
        let tag_index_unique_id = tag_index.uniqueId;

        // SAFETY: `tag_index` and its values trie remain live for this traversal.
        let iterator = unsafe { ffi::TrieMap_Iterate(tag_index.values) };

        let result = loop {
            let mut key = std::ptr::null_mut();
            let mut key_len = 0;
            let mut value = std::ptr::null_mut();
            // SAFETY: the output locations are valid for the call and the
            // iterator remains live until freed below.
            if unsafe { ffi::TrieMapIterator_Next(iterator, &mut key, &mut key_len, &mut value) }
                == 0
            {
                break Ok(());
            }

            if value.is_null() {
                break Err(io::Error::other(
                    "an in-memory tag index has a null posting list",
                ));
            }

            let tag = if key_len == 0 {
                &[]
            } else {
                // SAFETY: TrieMapIterator_Next returned this `key` with
                // `key_len` readable bytes, valid until the next advance.
                unsafe { std::slice::from_raw_parts(key.cast::<u8>(), key_len as usize) }
            };

            // SAFETY: C TagIndex values are Rust opaque inverted indexes.
            // The spec's read lock prevents their removal during iteration.
            let ii = unsafe { &*value.cast::<InvertedIndex>() };

            let Ok(Some(delta)) = ii.scan_gc(|id| spec.doc_exists(id)) else {
                continue;
            };

            if let Err(error) = Some(TagEntry {
                field_name,
                tag_index_unique_id,
                tag,
                inverted_index_unique_id: u32::from(ii.unique_id()),
                delta,
            })
            .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
            .map_err(io::Error::other)
            {
                break Err(error);
            }
        };

        // SAFETY: TrieMap_Iterate always returns a boxed iterator for a valid
        // trie, and this is the one returned above.
        unsafe { ffi::TrieMapIterator_Free(iterator) };
        result?;
    }

    Option::<TagEntry<&[u8]>>::None
        .serialize(&mut rmp_serde::Serializer::new(writer))
        .map_err(io::Error::other)
}

/// Decode one tag message from `reader`, or `None` at the stream terminator.
pub fn receive_tag_entry(
    reader: &mut impl Read,
) -> Result<Option<TagEntry>, HandleError<TagError>> {
    rmp_serde::from_read(reader).map_err(|e| HandleError::codec("decoding tag entry", e))
}

/// Apply one decoded tag message to the field's tag index.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
pub fn apply_tag_entry(
    entry: TagEntry,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError<TagError>> {
    let fs = guard
        .field_specs_mut()
        .iter_mut()
        .find(|fs| fs.field_name().secret_value().to_bytes() == &*entry.field_name)
        .ok_or(TagError::new(
            "no field in the spec matches the scanned field name",
        ))?;

    let tag_index = fetch_tag_index_mut(fs).ok_or(TagError::new(
        "the field no longer has an in-memory tag index",
    ))?;

    if tag_index.uniqueId != entry.tag_index_unique_id {
        return Err(TagError::new(
            "the field's tag index is not the one the child scanned",
        ));
    }

    let mut size = 0;
    // SAFETY: `entry.tag` is readable for its length. A zero-length tag is
    // valid and TagIndex_OpenIndex does not dereference its pointer in that case.
    let ii = unsafe {
        ffi::TagIndex_OpenIndex(
            std::ptr::from_mut(tag_index).cast_const(),
            entry.tag.as_ptr().cast::<c_char>(),
            entry.tag.len(),
            0,
            &mut size,
        )
    };

    // SAFETY: TRIEMAP_NOTFOUND is the C trie's process-global not-found
    // sentinel. It is initialized before an index can be queried.
    if ii.is_null() || ii.cast() == unsafe { ffi::TRIEMAP_NOTFOUND } {
        return Err(TagError::new(
            "the tag was removed before the delta could be applied",
        ));
    }

    // SAFETY: the preceding null/sentinel check leaves a live opaque inverted
    // index owned by `tag_index`. The write lock makes this mutable borrow unique.
    let ii = unsafe { &mut *ii.cast::<InvertedIndex>() };

    if u32::from(ii.unique_id()) != entry.inverted_index_unique_id {
        return Err(TagError::new(
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
            return Err(TagError::new(
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
pub fn handle_tags(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError<TagError>> {
    crate::util::handle_one(fgc, |reader| receive_tag_entry(reader), apply_tag_entry)
}
