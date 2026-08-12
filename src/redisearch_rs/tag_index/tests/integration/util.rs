/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Helpers shared by the integration test modules.

use inverted_index::DocId;
use tag_index::{TagIndex, WritePostingsDelta};

/// Index `doc_id` under `tags` in a memory-mode index, with no field expiration.
///
/// [`TagIndex::index`] is `unsafe` only because of its disk-mode contract, which
/// memory mode cannot violate: `ctx`/`batch` are ignored and the tag bytes are
/// never read past their length. Discharging that once here keeps the obligation
/// out of every memory-mode test. Tests that care about the expiration flag call
/// [`TagIndex::index`] directly.
pub fn index_mem(idx: &mut TagIndex, tags: &[&[u8]], doc_id: DocId) -> Option<WritePostingsDelta> {
    // SAFETY: memory mode, so neither disk-mode condition applies.
    unsafe { idx.index(std::ptr::null(), std::ptr::null(), tags, doc_id, false) }
}

/// Run the post-indexing commit phase for `tags`.
///
/// [`TagIndex::commit`] is `unsafe` because the tags must be NUL-free; every tag
/// the tests pass is a NUL-free literal, so discharging that once here keeps the
/// obligation out of every test.
pub fn commit(idx: &mut TagIndex, tags: &[&[u8]]) -> u32 {
    // SAFETY: as above — the tags are NUL-free.
    unsafe { idx.commit(tags) }
}
