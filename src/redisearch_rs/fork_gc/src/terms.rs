/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! GC collection and application for the spec's terms trie (`IndexSpec::terms`)
//! and the per-term inverted indexes it indexes.

use std::{
    borrow::Cow,
    io::{self, Read, Write},
    ops::ControlFlow,
};

use c_trie::TrieTerm;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::{DocLiveness, GcScanDelta, GcSnapshotScope, opaque::InvertedIndex};
use serde::{Deserialize, Serialize};
use string_utils::{obfuscation::obfuscate_text, runes::runes_to_bytes};

use crate::util::{deserialize, serialize};
use crate::{ForkGC, GcApplyStats, HandleError, HandleOutcome};

/// One term's worth of GC work, as it travels the pipe.
///
/// `T` lets the child serialize borrowed term bytes without copying; the
/// parent deserializes the default owned form.
#[derive(Debug, Serialize, Deserialize)]
pub struct TermEntry<T = Box<[u8]>> {
    /// The term whose inverted index the child scanned.
    pub term: T,
    /// Unique id of the inverted index scanned, so the delta is not applied to a
    /// replacement that reused the term.
    pub unique_id: u32,
    /// What the scan found to collect.
    pub delta: GcScanDelta,
}

/// Collect GC deltas for every term in the spec's terms trie and write them to
/// the parent process.
///
/// Walks the trie in its natural order and, for each term with a non-null
/// inverted index, attempts a GC scan. When a scan produces a delta, a
/// serialised [`TermEntry`] is written. Terms that produce no delta or fail the
/// scan are skipped. A serialised `None` terminates the stream.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_terms(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    for term in spec.terms() {
        let Some((unique_id, delta)) = scan_term(spec, &term, |id| spec.doc_exists(id)) else {
            continue;
        };

        serialize(
            writer,
            Some(TermEntry {
                term: &*term,
                unique_id,
                delta,
            }),
        )?;
    }

    serialize(writer, None::<TermEntry<&[u8]>>)
}

/// Run the GC scan of `term`'s inverted index, returning the index's unique id
/// with the delta.
///
/// Returns `None` when the term has no inverted index, the scan found nothing
/// to collect, or the scan failed; a failed scan is retried on the next cycle.
pub fn scan_term(
    spec: &IndexSpecReadGuard,
    term: &TrieTerm,
    live: impl DocLiveness + Copy,
) -> Option<(u32, GcScanDelta)> {
    let ii = spec.keys_dict().fetch(term)?;
    Some((u32::from(ii.unique_id()), ii.scan_gc(live).ok().flatten()?))
}

/// A private copy of what a GC scan of `term`'s index has to decode, so the scan can
/// run without the lock; see [`InvertedIndex::gc_snapshot`].
pub fn snapshot_term(
    spec: &IndexSpecReadGuard,
    term: &TrieTerm,
    live: &impl DocLiveness,
) -> Option<InvertedIndex> {
    spec.keys_dict()
        .fetch(term)?
        .gc_snapshot(live, GcSnapshotScope::DirtyBlocks)
}

/// Where a [`scan_terms_after`] walk resumes: the last term it visited.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TermsCursor(Vec<ffi::rune>);

/// Scan terms in lexicographic order, starting strictly after `cursor`, until
/// `should_stop` returns `true` after a term or no term is left.
///
/// Appends a snapshot of each term whose index `live` finds a gone document in,
/// with the term, to `found` (see [`snapshot_term`]), and moves `cursor` past
/// every term visited, so a later call resumes after them even if the trie
/// changed in between. Returns whether the walk stopped early, leaving terms
/// to scan.
///
/// What `found` holds between stops is bounded only by `live` ruling ranges
/// clean (see [`DocLiveness::any_gone`]) and by `should_stop`: a source that
/// rules nothing out copies every non-empty index visited.
pub fn scan_terms_after(
    spec: &IndexSpecReadGuard,
    cursor: &mut TermsCursor,
    live: impl DocLiveness + Copy,
    mut should_stop: impl FnMut() -> bool,
    found: &mut Vec<(TrieTerm, InvertedIndex)>,
) -> bool {
    let mut stopped = false;
    spec.terms().visit_after(&cursor.0.clone(), |runes| {
        cursor.0.clear();
        cursor.0.extend_from_slice(runes);

        // Stored keys are capped at `TRIE_INITIAL_STRING_LEN` runes, inside the
        // `MAX_RUNE_STR_LEN` `runes_to_bytes` requires.
        let bytes = runes_to_bytes(runes).expect("a stored trie key fits within MAX_RUNE_STR_LEN");
        // SAFETY: the walk only visits non-empty keys the terms trie accepted, and
        // `runes_to_bytes` emits a complete encoding of each rune, none of them zero.
        let term = unsafe { TrieTerm::from_bytes_unchecked(bytes.into_boxed_slice()) };
        if let Some(snapshot) = snapshot_term(spec, &term, &live) {
            found.push((term, snapshot));
        }

        if should_stop() {
            stopped = true;
            ControlFlow::Break(())
        } else {
            ControlFlow::Continue(())
        }
    });
    stopped
}

/// Decode one terms message from `reader`.
pub fn receive_terms(reader: &mut impl Read) -> Result<Option<TermEntry<TrieTerm>>, HandleError> {
    deserialize::<Option<TermEntry>>(reader, "decoding terms entry").map(|entry| {
        entry.map(|entry| TermEntry {
            // SAFETY: terms messages are produced by `collect_terms`, which only serializes
            // non-empty terms returned by the terms trie iterator.
            term: unsafe { TrieTerm::from_bytes_unchecked(entry.term) },
            unique_id: entry.unique_id,
            delta: entry.delta,
        })
    })
}

fn warn_term_deletion_failed(term: &TrieTerm, guard: &IndexSpecWriteGuard<'_>) {
    let obfuscate = global_config::hide_user_data_from_log();
    let term_for_log = if obfuscate {
        Cow::Borrowed(obfuscate_text(term))
    } else {
        String::from_utf8_lossy(term)
    };
    let index_name = guard.display_name(obfuscate).to_string_lossy();
    tracing::warn!(
        "RedisSearch fork GC: deleting a term '{term_for_log}' from trie in index \
         '{index_name}' failed"
    );
}

/// [`apply_terms_with`] without a liveness source: a grown last block is left for the next cycle.
pub fn apply_terms(
    term: &TrieTerm,
    unique_id: u32,
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError> {
    apply_terms_with(term, unique_id, delta, None, guard)
}

/// Apply a pre-decoded GC delta to `term`'s inverted index.
///
/// The term's inverted index is removed from the trie/keys dict once it has no
/// unique docs left.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
///
/// With `live`, a last block that grew since the scan is rescanned against it under
/// the write lock instead of being denied; it must be the source the scan used, see
/// [`InvertedIndex::gc_snapshot`](inverted_index::InvertedIndex::gc_snapshot).
pub fn apply_terms_with(
    term: &TrieTerm,
    unique_id: u32,
    delta: GcScanDelta,
    live: Option<&dyn DocLiveness>,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, HandleError> {
    let Some(ii) = guard.keys_dict_mut().fetch_mut(term) else {
        return Err(HandleError::ApplyError(
            "the term's inverted index was removed before the delta could be applied",
        ));
    };
    if u32::from(ii.unique_id()) != unique_id {
        return Err(HandleError::ApplyError(
            "the term's inverted index is not the one scanned",
        ));
    }

    let info = ii.apply_gc_with(delta, live);

    let (extra, remaining_blocks, term_removed) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();

        let removed = guard.keys_dict_mut().remove(term);
        debug_assert!(
            removed,
            "`fetch_mut` found this term in the same dict, under the write lock we still hold"
        );
        if !guard.terms_mut().delete(term) {
            warn_term_deletion_failed(term, guard);
        }
        if let Some(suffix) = guard.suffix_mut() {
            suffix.delete(term);
        }

        (extra, remaining_blocks, true)
    } else {
        (0, 0, false)
    };

    Ok(GcApplyStats {
        records_removed: info.entries_removed,
        bytes_collected: info.bytes_freed + extra,
        bytes_allocated: info.bytes_allocated,
        block_count_delta: info.block_count_delta - remaining_blocks as i64,
        blocks_denied: info.ignored_last_block as u64,
        terms_removed: usize::from(term_removed),
        terms_size_removed: if term_removed { term.len() } else { 0 },
        ..GcApplyStats::default()
    })
}

/// Parent-side handler for one iteration of the terms GC protocol.
///
/// Reads one [`TermEntry`] from the pipe, applies its delta, updates stats, and
/// returns `Ok(HandleOutcome::Collected)`. A serialised `None` returns
/// `Ok(HandleOutcome::Done)`.
pub fn handle_terms(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError> {
    crate::util::handle_one(
        fgc,
        |reader| receive_terms(reader),
        |entry, guard| apply_terms(&entry.term, entry.unique_id, entry.delta, guard),
    )
}
