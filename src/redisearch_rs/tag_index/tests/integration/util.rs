/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Helpers shared by the integration test modules.

use ffi::timespec;
use index_result::RSIndexResult;
use inverted_index::{DocId, GcApplyInfo, GcScanDelta, IndexUniqueId, RepairContext};
use tag_index::{InMemoryMode, MemTagIndexIterator, OnDiskMode, Tag, TagIndex, WritePostingsDelta};

/// Wrap a NUL-free test literal into a [`Tag`].
pub fn as_tag(bytes: &[u8]) -> Tag<'_> {
    Tag::new(bytes).expect("test literal is NUL-free")
}

/// Wrap every NUL-free literal `tags` passes as a test fixture into a [`Tag`].
fn tag_values<'a>(tags: &[&'a [u8]]) -> Vec<Tag<'a>> {
    tags.iter().copied().map(as_tag).collect()
}

/// A deadline that has already elapsed. Any `CLOCK_MONOTONIC_RAW` value one
/// second after boot is in the past on a running system, so
/// `duration_from_redis_timespec` maps it to a zero remaining budget.
pub const fn elapsed_deadline() -> timespec {
    timespec {
        tv_sec: 1,
        tv_nsec: 0,
    }
}

/// Index `doc_id` under `tags` in a memory-mode index, with no field expiration.
///
/// Tests that care about the expiration flag call
/// [`TagIndex::index`] directly.
pub fn index_mem(
    idx: &mut TagIndex<InMemoryMode>,
    tags: &[&[u8]],
    doc_id: DocId,
) -> WritePostingsDelta {
    idx.index(&tag_values(tags), doc_id, false)
}

/// Run the post-indexing commit phase for `tags` on a memory-mode index.
pub fn commit_mem(idx: &mut TagIndex<InMemoryMode>, tags: &[&[u8]]) -> u32 {
    idx.commit(&tag_values(tags))
}

/// Run the post-indexing commit phase for `tags` on a disk-mode index.
pub fn commit_disk(idx: &mut TagIndex<OnDiskMode>, tags: &[&[u8]]) -> u32 {
    idx.commit(&tag_values(tags))
}

/// Drain a [`MemTagIndexIterator`] into its yielded keys, in iteration order.
pub fn value_iter_keys(mut it: MemTagIndexIterator<'_>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.as_bytes().to_vec());
    }
    keys
}

/// The [`IndexUniqueId`] of `tag`'s posting list — what the GC child ships back and
/// [`TagIndex::gc`] checks the delta against.
pub fn unique_id(idx: &TagIndex<InMemoryMode>, tag: &[u8]) -> IndexUniqueId {
    idx.find_value(tag).expect("tag is indexed").unique_id()
}

/// Scan `tag`'s postings the way the GC child does, keeping only the documents
/// `doc_exists` accepts. `None` when nothing needs repairing.
pub fn scan(
    idx: &TagIndex<InMemoryMode>,
    tag: &[u8],
    doc_exists: impl Fn(DocId) -> bool,
) -> Option<GcScanDelta> {
    idx.find_value(tag)
        .expect("tag is indexed")
        .scan_gc(
            doc_exists,
            None::<for<'i> fn(&RSIndexResult<'i>, &RepairContext<'i>)>,
        )
        .expect("scanning a tag's postings should not fail")
}

/// Run a whole fork-GC cycle over `tag`: [`scan`] its postings keeping only the
/// documents `doc_exists` accepts, then apply the resulting delta through
/// [`TagIndex::gc`].
///
/// Tests that need the two halves to disagree — a delta scanned against another
/// index, or against a tag that has since been removed — drive [`scan`],
/// [`unique_id`] and [`TagIndex::gc`] separately instead.
pub fn gc_mem(
    idx: &mut TagIndex<InMemoryMode>,
    tag: &[u8],
    doc_exists: impl Fn(DocId) -> bool,
) -> GcApplyInfo {
    let delta = scan(idx, tag, doc_exists).expect("at least one document must need repairing");
    let id = unique_id(idx, tag);
    idx.gc(as_tag(tag), id, delta)
        .expect("the delta was just scanned, so it cannot be stale")
}
