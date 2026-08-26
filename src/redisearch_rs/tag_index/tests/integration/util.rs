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
use inverted_index::DocId;
use tag_index::{InMemoryMode, MemTagIndexIterator, Tag, TagIndex, WritePostingsDelta};

/// Wrap a NUL-free test literal into a [`Tag`].
pub fn as_tag(bytes: &[u8]) -> Tag<'_> {
    Tag::new(bytes).expect("test literal is NUL-free")
}

/// Wrap every NUL-free literal `tags` passes as a test fixture into a [`Tag`].
fn tag_values<'a>(tags: &[&'a [u8]]) -> Vec<Tag<'a>> {
    tags.iter().map(|t| as_tag(t)).collect()
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

/// Drain a [`MemTagIndexIterator`] into its yielded keys, in iteration order.
pub fn value_iter_keys(mut it: MemTagIndexIterator<'_>) -> Vec<Vec<u8>> {
    let mut keys: Vec<Vec<u8>> = Vec::new();
    while let Some((key, _)) = it.advance() {
        keys.push(key.as_bytes().to_vec());
    }
    keys
}
