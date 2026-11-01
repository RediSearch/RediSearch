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
use inverted_index::GcScanDelta;
use serde::{Deserialize, Serialize};
use string_utils::{obfuscation::obfuscate_text, runes::runes_to_bytes};

use crate::fork::wire::{deserialize, serialize};
use crate::{DocExistence, ForkPipe, GcApplyStats, GcError, HandleOutcome};

/// One term's worth of GC work, as it travels the pipe.
///
/// `T` lets the child serialize borrowed term bytes without copying; the
/// parent deserializes the default owned form.
#[derive(Debug, Serialize, Deserialize)]
#[serde(bound(
    serialize = "T: serde_bytes::Serialize",
    deserialize = "T: serde_bytes::Deserialize<'de>"
))]
pub struct TermEntry<T = Box<[u8]>> {
    /// The term whose inverted index the child scanned.
    #[serde(with = "serde_bytes")]
    pub term: T,
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
/// `docs` says which documents still exist.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_terms(
    writer: &mut impl Write,
    spec: &IndexSpecReadGuard,
    docs: &DocExistence,
) -> io::Result<()> {
    for term in spec.terms() {
        let Some(ii) = spec.keys_dict().fetch(&term) else {
            continue;
        };

        let Ok(Some(delta)) = ii.scan_gc(|id| docs.contains(id)) else {
            continue;
        };

        serialize(
            writer,
            Some(TermEntry {
                term: &*term,
                delta,
            }),
        )?;
    }

    serialize(writer, None::<TermEntry<&[u8]>>)
}

/// Run the GC scan of `term`'s inverted index.
///
/// Returns `None` when the term has no inverted index, the scan found nothing
/// to collect, or the scan failed; a failed scan is retried on the next cycle.
pub fn scan_term(spec: &IndexSpecReadGuard, term: &TrieTerm) -> Option<GcScanDelta> {
    let ii = spec.keys_dict().fetch(term)?;
    ii.scan_gc(|id| spec.doc_exists(id)).ok().flatten()
}

/// Where a [`scan_terms_after`] walk resumes: the last term it visited.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct TermsCursor(Vec<ffi::rune>);

/// Scan terms in lexicographic order, starting strictly after `cursor`, until
/// `should_stop` returns `true` after a term or no term is left.
///
/// Appends each delta, with its term, to `found`, and moves `cursor` past every
/// term visited, so a later call resumes after them even if the trie changed
/// in between. Returns whether `should_stop` ended the walk, in which case
/// terms may remain; a call after the last term returns `false`.
pub fn scan_terms_after(
    spec: &IndexSpecReadGuard,
    cursor: &mut TermsCursor,
    mut should_stop: impl FnMut() -> bool,
    found: &mut Vec<(TrieTerm, GcScanDelta)>,
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
        if let Some(delta) = scan_term(spec, &term) {
            found.push((term, delta));
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
pub fn receive_terms(reader: &mut impl Read) -> Result<Option<TermEntry<TrieTerm>>, GcError> {
    let Some(entry) = deserialize::<Option<TermEntry>>(reader, "decoding terms entry")? else {
        return Ok(None);
    };
    if entry.term.is_empty() {
        return Err(GcError::codec("decoding terms entry", "empty term"));
    }
    // SAFETY: the term is non-empty, and collect_terms obtains its bytes from
    // the trie iterator, preserving the trie's remaining validity invariants.
    let term = unsafe { TrieTerm::from_bytes_unchecked(entry.term) };
    Ok(Some(TermEntry {
        term,
        delta: entry.delta,
    }))
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

/// Apply a pre-decoded GC delta to `term`'s inverted index.
///
/// The term's inverted index is removed from the trie/keys dict once it has no
/// unique docs left.
///
/// Returns [`GcApplyStats`] the caller flushes to the spec and the GC via
/// [`GcApplyStats::apply`].
pub fn apply_terms(
    term: &TrieTerm,
    delta: GcScanDelta,
    guard: &mut IndexSpecWriteGuard<'_>,
) -> Result<GcApplyStats, GcError> {
    let Some(ii) = guard.keys_dict_mut().fetch_mut(term) else {
        return Err(GcError::ApplyError(
            "the term's inverted index was removed before the delta could be applied",
        ));
    };

    let info = ii.apply_gc(delta);

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
pub fn handle_terms(fgc: &mut ForkPipe) -> Result<HandleOutcome, GcError> {
    crate::fork::wire::handle_one(
        fgc,
        |reader| receive_terms(reader),
        |entry, guard| apply_terms(&entry.term, entry.delta, guard),
    )
}
