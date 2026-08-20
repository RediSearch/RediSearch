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
};

use c_trie::TrieTerm;
use index_spec::{IndexSpecReadGuard, IndexSpecWriteGuard};
use inverted_index::GcScanDelta;
use serde::Serialize as _;
use string_utils::obfuscation::obfuscate_text;

use crate::{ForkGC, Frame, GcApplyStats, HandleError, HandleOutcome};

/// The term's inverted index was removed between the child's scan and the
/// parent's apply (a race between GC and a concurrent term/document drop).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("the term's inverted index was removed before the delta could be applied")]
pub struct TermNotFound;

/// Collect GC deltas for every term in the spec's terms trie and write them to
/// the parent process.
///
/// Walks the trie in its natural order and, for each term with a non-null
/// inverted index, attempts a GC scan. When a scan produces a delta, the term
/// header (its raw bytes — the trie never stores a zero-length key, so this is
/// never empty) followed by the serialised [`GcScanDelta`] is written. Terms
/// that produce no delta or fail the scan are skipped. A terminator is written
/// once every term has been processed.
///
/// Scan errors are silently ignored (same block is retried on the next GC
/// cycle). Write errors are surfaced to the caller so they can terminate the
/// child process.
///
/// [`GcScanDelta`]: inverted_index::GcScanDelta
pub fn collect_terms(writer: &mut impl Write, spec: &IndexSpecReadGuard) -> io::Result<()> {
    for term in spec.terms() {
        let Some(ii) = spec.keys_dict().fetch(&term) else {
            continue;
        };

        let Ok(Some(deltas)) = ii.scan_gc(|id| spec.doc_exists(id)) else {
            continue;
        };

        Frame::data(&term).encode(writer)?;
        deltas
            .serialize(&mut rmp_serde::Serializer::new(&mut *writer))
            .map_err(io::Error::other)?;
    }

    Frame::Terminator.encode(writer)
}

/// Decode one terms message from `reader`.
pub fn receive_terms(
    reader: &mut impl Read,
) -> Result<Option<(TrieTerm, GcScanDelta)>, HandleError<TermNotFound>> {
    let frame = Frame::decode(reader)
        .map_err(|e| HandleError::codec("reading the terms term-name frame", e))?;

    match frame {
        Frame::Terminator => Ok(None),
        Frame::Data(term) => {
            let delta = rmp_serde::from_read::<_, GcScanDelta>(reader)
                .map_err(|e| HandleError::codec("decoding the terms delta", e))?;
            // SAFETY: terms messages are produced by `collect_terms`, which only
            // serializes terms returned by the terms trie iterator.
            let term = unsafe { TrieTerm::from_bytes_unchecked(term.into_inner()) };
            Ok(Some((term, delta)))
        }
        Frame::Empty => Err(HandleError::codec(
            "expected a term-name or terminator frame for terms",
            "got an empty frame",
        )),
    }
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
) -> Result<GcApplyStats, HandleError<TermNotFound>> {
    let Some(ii) = guard.keys_dict_mut().fetch_mut(term) else {
        return Err(HandleError::Custom(TermNotFound));
    };

    let info = ii.apply_gc(delta);

    let (extra, remaining_blocks, term_removed) = if ii.unique_docs() == 0 {
        let extra = ii.memory_usage();
        let remaining_blocks = ii.number_of_blocks();

        guard.keys_dict_mut().remove(term);
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
/// Reads one frame from the pipe:
/// - A term header, followed by a [`GcScanDelta`] → applies the delta, updates
///   stats, returns `Ok(HandleOutcome::Collected)`.
/// - A [`Frame::Terminator`] → all terms processed, returns `Ok(HandleOutcome::Done)`.
pub fn handle_terms(fgc: &mut ForkGC) -> Result<HandleOutcome, HandleError<TermNotFound>> {
    crate::util::handle_one(
        fgc,
        |reader| receive_terms(reader),
        |(term, delta), guard| apply_terms(&term, delta, guard),
    )
}
