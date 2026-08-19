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
use tag_index::{InMemoryMode, Tag, TagIndex, WritePostingsDelta};

/// Wrap every NUL-free literal `tags` passes as a test fixture into a [`Tag`].
fn tag_values<'a>(tags: &[&'a [u8]]) -> Vec<Tag<'a>> {
    tags.iter()
        .map(|t| Tag::new(t).expect("test literal is NUL-free"))
        .collect()
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
    // SAFETY: caller passes `doc_id`s in non-decreasing order, as `TagIndex::index` requires.
    unsafe { idx.index(&tag_values(tags), doc_id, false) }
}

/// Run the post-indexing commit phase for `tags` on a memory-mode index.
pub fn commit_mem(idx: &mut TagIndex<InMemoryMode>, tags: &[&[u8]]) -> u32 {
    idx.commit(&tag_values(tags))
}
