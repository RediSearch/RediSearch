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
use tag_index::{TagIndex, TagValue, WritePostingsDelta};

/// Wrap every NUL-free literal `tags` passes as a test fixture into a [`TagValue`].
fn tag_values<'a>(tags: &[&'a [u8]]) -> Vec<TagValue<'a>> {
    tags.iter()
        .map(|t| TagValue::new(t).expect("test literal is NUL-free"))
        .collect()
}

/// Index `doc_id` under `tags` in a memory-mode index, with no field expiration.
///
/// Tests that care about the expiration flag call
/// [`TagIndex::index_in_memory`] directly.
pub fn index_mem(idx: &mut TagIndex, tags: &[&[u8]], doc_id: DocId) -> WritePostingsDelta {
    idx.index_in_memory(&tag_values(tags), doc_id, false)
}

/// Run the post-indexing commit phase for `tags`.
pub fn commit(idx: &mut TagIndex, tags: &[&[u8]]) -> u32 {
    idx.commit(&tag_values(tags))
}
