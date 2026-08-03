/*
 * Copyright (c) 2006-Present, Redis Ltd.
 * All rights reserved.
 *
 * Licensed under your choice of the Redis Source Available License 2.0
 * (RSALv2); or (b) the Server Side Public License v1 (SSPLv1); or (c) the
 * GNU Affero General Public License v3 (AGPLv3).
*/

//! Evaluation of `QN_TOKEN` query nodes.

use std::ptr::NonNull;

use c_trie::CTrieRef;
use field::FieldMaskOrIndex;
use query_error::QueryErrorCode;
use query_term::RSQueryTerm;
use query_types::QueryNodeOptions;
use rqe_core::FieldMask;
use rqe_iterators::build_term_iterator;
use rs_token::RSTokenRef;
use search_disk::SearchDiskHandle;

use crate::{Config, Evaluated, QueryEvalContext, QueryNodeRef, expansion_needs_offsets};

/// `QN_TOKEN` — a single-term lookup.
///
/// In the in-memory path the term's inverted index is opened via
/// [`Redis_OpenReaderIndex`](ffi::Redis_OpenReaderIndex) and wrapped in a term
/// iterator with [`build_term_iterator`]. In search-on-disk mode the work is
/// delegated to [`eval_disk`].
/// Returns `None` when the term has no matching inverted index (e.g. it is absent).
pub(crate) fn eval<'index>(
    ctx: &'index mut QueryEvalContext,
    node: &QueryNodeRef,
    tok: RSTokenRef,
    config: Config,
) -> Option<Evaluated<'index>> {
    let opts = node.opts();
    let weight = opts.weight;
    // the node's field mask narrowed to the query's.
    let effective_field_mask = opts.field_mask & ctx.opts().fieldmask;
    let token_id = ctx.next_token_id() as i32;
    let term_bytes = tok.as_bytes();
    // A `QN_TOKEN` node always carries a non-null term string.
    debug_assert!(term_bytes.is_some(), "token string should not be null");
    let term = RSQueryTerm::new_bytes(term_bytes.unwrap_or_default(), token_id, tok.flags());

    // SAFETY: `ctx.spec().diskSpec` is either null or a valid
    // `RedisSearchDiskIndexSpec` that stays valid for `'index` (`QueryEvalContext`
    // invariants 1/2). `SearchDiskHandle::new` yields `None` for the null
    // (in-memory) case, which falls through to the in-memory reader below.
    if let Some(disk) = unsafe { SearchDiskHandle::new(ctx.spec().diskSpec) } {
        eval_disk(
            ctx,
            disk,
            tok,
            term,
            opts,
            weight,
            effective_field_mask,
            config,
        )
    } else {
        open_term_reader(ctx, tok, term, weight, effective_field_mask)
    }
}

/// Open an in-memory term reader for a `QN_TOKEN` node.
///
/// Opens and validates the term's inverted index and, on success, wraps it in a
/// term iterator that takes ownership of `term`. Returns `None` when the term
/// has no matching inverted index (absent, empty, or no results in the queried
/// field(s)), dropping `term`.
fn open_term_reader<'index>(
    ctx: &'index mut QueryEvalContext,
    tok: RSTokenRef,
    term: Box<RSQueryTerm>,
    weight: f64,
    effective_field_mask: FieldMask,
) -> Option<Evaluated<'index>> {
    debug_assert!(!ctx.sctx_ptr().is_null(), "sctx must not be null");

    // Open and validate the term's inverted index. A null result means the term
    // has no matching index (absent, empty, or no results in the queried
    // field(s)), so there is nothing to read.
    // SAFETY: `ctx.sctx_ptr()` is valid (`QueryEvalContext` invariant 2) and
    // `tok` is the query node's `RSToken`; `Redis_OpenReaderIndex` only reads the
    // token and does not retain it.
    let idx =
        unsafe { ffi::Redis_OpenReaderIndex(ctx.sctx_ptr(), tok.as_ptr(), effective_field_mask) };
    let idx = NonNull::new(idx)?;

    // SAFETY: `ctx.sctx_ptr()` is non-null (`QueryEvalContext` invariant 2).
    let sctx = unsafe { NonNull::new_unchecked(ctx.sctx_ptr().cast_mut()) };
    // SAFETY: `idx` is the term's inverted index just opened for this spec and
    // stays valid for `'index` (`QueryEvalContext` invariants 1/2); `sctx` and its
    // spec are valid for `'index` (invariant 2); `term` is a freshly
    // heap-allocated query term whose ownership transfers to the iterator.
    let iter = unsafe {
        build_term_iterator(
            idx.as_ptr(),
            sctx,
            FieldMaskOrIndex::Mask(effective_field_mask),
            term,
            weight,
        )
    };

    Some(Evaluated::RustLeaf(Box::new(iter)))
}

/// Search-on-disk evaluation of a `QN_TOKEN` node.
///
/// Looks up the term's document count in the terms trie to compute the IDF
/// then builds a disk term iterator.
///
/// `disk` must wrap the spec's own disk index.
///
/// Returns `None` — after setting the query status —
/// when the disk iterator cannot be built.
#[expect(clippy::too_many_arguments)]
fn eval_disk<'index>(
    ctx: &'index mut QueryEvalContext,
    disk: SearchDiskHandle,
    tok: RSTokenRef,
    mut term: Box<RSQueryTerm>,
    opts: &QueryNodeOptions,
    weight: f64,
    effective_field_mask: FieldMask,
    config: Config,
) -> Option<Evaluated<'index>> {
    let spec = ctx.spec();
    // Look up the term's document count in the terms trie to compute IDF, then
    // build a disk term iterator through the enterprise API.
    // SAFETY: in search-on-disk mode the terms trie is always initialised.
    debug_assert!(!spec.terms.is_null(), "terms trie should be initialized");
    let term_bytes = tok.as_bytes();
    // A `QN_TOKEN` node always carries a non-null term string; the term lookup
    // below relies on it.
    debug_assert!(term_bytes.is_some(), "token string should not be null");

    // SAFETY: `spec.terms` is a valid `Trie` (checked non-null above) that
    // outlives this lookup.
    let terms = unsafe { CTrieRef::from_raw(spec.terms) };
    let num_docs_in_term = terms.num_docs(term_bytes.unwrap_or_default());
    let num_documents = spec.stats.scoring.numDocuments;
    let idf = idf::calculate_idf(num_documents, num_docs_in_term);
    let bm25_idf = idf::calculate_idf_bm25(num_documents, num_docs_in_term);
    term.set_idf(idf);
    term.set_bm25_idf(bm25_idf);

    let needs_offsets = expansion_needs_offsets(ctx, opts, config);

    let snapshot = NonNull::new(ctx.sctx().diskSnapshot)
        .expect("query.sctx.diskSnapshot is null for a disk-backed token query");
    // SAFETY: `disk` wraps the spec's disk index, valid for `'index`
    // (`QueryEvalContext` invariants 1/2), and single-threaded query evaluation
    // gives us the only live reference to it; the enterprise iterators are
    // registered whenever a disk index is in use; `snapshot` is the disk snapshot
    // taken at query start.
    let iter = unsafe {
        disk.new_term_iterator(term, effective_field_mask, weight, needs_offsets, snapshot)
    };

    match iter {
        Ok(it) => Some(Evaluated::RustLeaf(it)),
        Err(err) => {
            // Surface the failure via `status` so the query aborts with an
            // error rather than silently returning empty results.
            ctx.status()
                .set_error(QueryErrorCode::DiskIteratorCreation, &err.to_string());
            None
        }
    }
}
